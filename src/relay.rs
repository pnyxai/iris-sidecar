// ─── relay.rs ────────────────────────────────────────────────
// This is the heart of the application.
//
// When a user sends an HTTP request to Iris, the `relay` function is called.
// Its job is:
//   1. Authenticate the request (if the user configured a local token).
//   2. Check which local models are currently healthy.
//   3. Forward the request to the PNYX gateway, adding:
//      • `Pnyx-Sidecar-Request: true` (tells the gateway this is a side-car)
//      • `Pnyx-Local-Suppliers: <model1>,<model2>` (tells the gateway which
//        local models are available and healthy right now)
//   4. Read the gateway response.
//      a. If the gateway says `Pnyx-Local-Request: false` (or omits the header),
//         stream the response straight back to the user.
//      b. If the gateway says `Pnyx-Local-Request: true`, parse the JSON,
//         extract `relay_data.model_tag`, look up the model in Iris's local
//         config, and call the local model. Then stream that response back.
// ──────────────────────────────────────────────────────────────

use std::collections::HashMap;    
use std::sync::Arc;               
use std::time::Duration;          

// ── Axum (web framework) imports ──
use axum::{
    body::Body,                        // Axum's request/response body type
    extract::{Request, State},         // `Request` = incoming HTTP data; `State` = shared application state
    http::{HeaderMap, HeaderValue, StatusCode},  // HTTP primitives
    response::{IntoResponse, Response},           // Traits/types for building responses
};

// `http_body_util` provides helpers for reading request bodies as bytes
use http_body_util::BodyExt;
use reqwest::Client;                  // The HTTP client we use to call the gateway and local models
use serde_json::json;                 

use crate::config::Config;          
use crate::models::{LocalModel, RelayData};  

// ──────────────────────────────────────────────────────────────
// AppState – shared state that every request handler can access
// ──────────────────────────────────────────────────────────────
// In Axum, when you create a `Router`, you attach a `State` object to it.
// Every time a request hits the server, Axum passes a clone of this state
// into the handler function (`relay`). Because we use `Arc` (atomic reference
// counting), cloning is cheap – it just increments a pointer, it does NOT
// duplicate the data in memory.
// ──────────────────────────────────────────────────────────────
#[derive(Clone)]
pub struct AppState {
    pub client: Client,                          // Reusable HTTP client (connection pooling)
    pub config: Config,                          // The loaded YAML config + env overrides
    pub models: Arc<HashMap<String, LocalModel>>, // All local models, keyed by name
}

// ──────────────────────────────────────────────────────────────
// get_healthy_models
// ──────────────────────────────────────────────────────────────
// Before we forward a request to the gateway, we ask every local model:
// "Are you alive?" by calling `GET {endpoint}/health`.
//
// This function runs *on-demand* (per request).
// It returns a list of model names that DID respond with HTTP 200.
// The caller will attach these names to the `Pnyx-Local-Suppliers` header
// so the gateway knows which local models are available to route traffic to.
//
// Arguments:
//   • `client` – the shared reqwest HTTP client
//   • `models` – the map of local models we are watching
//
// Returns: Vec<String> containing the names of healthy models.
// ──────────────────────────────────────────────────────────────
async fn get_healthy_models(
    client: &Client,
    models: &HashMap<String, LocalModel>,
) -> Vec<String> {
    let mut healthy = Vec::new();

    // Loop through every model we know about
    for (name, model) in models.iter() {
        // Build the health URL: e.g. http://localhost:1234/health
        let url = format!("{}/health", model.endpoint.trim_end_matches('/'));

        // Send a GET request with a 5-second timeout
        match client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    // HTTP 200–299 = healthy
                    tracing::debug!("❤️ Health check OK for {}", name);
                    healthy.push(name.clone());
                } else {
                    tracing::debug!("💀 Health check FAILED for {}: HTTP {}", name, status);
                }
            }
            Err(e) => {
                // Network error, timeout, DNS failure, etc. → unhealthy
                tracing::debug!("💀 Health check FAILED for {}: {}", name, e);
            }
        };
    }

    healthy
}

// ──────────────────────────────────────────────────────────────
// relay – the main HTTP request handler
// ──────────────────────────────────────────────────────────────
// This function is called for every incoming request. It is `async` because
// it performs network I/O (calling the gateway and/or local models), which
// must not block the CPU.
//
// `State(state)` is Axum syntax: it extracts the `AppState` attached to the router.
// `req` is the user's HTTP request (method, headers, body, path, etc.).
//
// The return type is `Result<Response, StatusCode>`:
//   • `Ok(Response)` = a successful HTTP response we send back to the user.
//   • `Err(StatusCode)` = a short error response (e.g. 500, 502) with no body.
// ──────────────────────────────────────────────────────────────
pub async fn relay(
    State(state): State<AppState>,
    req: Request,
) -> Result<Response, StatusCode> {

    // ── Step 1: Optional local authentication ──
    // If the user put a token in the YAML (`iris.token`), we require every
    // incoming request to carry `Authorization: Bearer <that_token>`.
    // This mimics how OpenAI API clients already send tokens.
    if let Some(ref token) = state.config.iris.token {
        let auth = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok());     // Convert header bytes to a UTF-8 string

        let expected = format!("Bearer {}", token);

        if auth != Some(&expected) {
            tracing::warn!("Unauthorized request");
            // Return HTTP 401 (Unauthorized) with a plain-text body
            return Ok((StatusCode::UNAUTHORIZED, "Unauthorized").into_response());
        }
    }

    // ── Step 2: Read the request body into memory ──
    // Axum bodies are *streams* – we must collect them into a single byte buffer
    // before we can forward them. `into_parts()` separates the HTTP metadata
    // (method, headers, URI) from the body stream.
    let (parts, body) = req.into_parts();

    let body_bytes = body
        .collect()                // Collect all chunks of the stream
        .await
        .map_err(|e| {
            tracing::error!("Failed to read request body: {}", e);
            StatusCode::BAD_REQUEST
        })?
        .to_bytes();              // Convert the collected chunks into a `bytes::Bytes` object

    // Remember the path and query string so we can reconstruct the upstream URL
    let path = parts.uri.path();
    let query = parts
        .uri
        .query()
        .map(|q| format!("?{}", q))   // If there is a query, prepend `?`
        .unwrap_or_default();         // Otherwise use an empty string

    // ── Step 3: Prepare the request to the PNYX gateway ──
    let gateway_base = state.config.pnyx.gateway_url.trim_end_matches('/');
    let upstream_url = format!("{}{}{}", gateway_base, path, query);

    // Start with a fresh header map for the upstream request
    let mut upstream_headers = HeaderMap::new();

    // Copy headers from the user's request, but strip a few that we must not forward:
    //   • Host        – must be the gateway's host, not Iris's host
    //   • Content-Length – reqwest will compute this automatically
    //   • Authorization  – we will replace it with the PNYX token below
    for (key, value) in &parts.headers {
        let key_str = key.as_str().to_lowercase();
        if key_str == "host" || key_str == "content-length" || key_str == "authorization" {
            continue;
        }
        upstream_headers.insert(key.clone(), value.clone());
    }

    // Add the header that tells the gateway: "this request came from a side-car"
    upstream_headers.insert(
        "Pnyx-Sidecar-Request",
        HeaderValue::from_static("true"),
    );

    // Check local model health and, if any are up, tell the gateway which are available
    let healthy = get_healthy_models(&state.client, &state.models).await;
    if !healthy.is_empty() {
        let value = healthy.join(",");   // e.g. "model-a,model-b"
        upstream_headers.insert(
            "Pnyx-Local-Suppliers",
            HeaderValue::from_str(&value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
    }

    // Attach the PNYX access token (from YAML or PNYX_TOKEN env var)
    upstream_headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", state.config.pnyx.access_token))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );

    // Build the reqwest request object
    let upstream_req = state
        .client
        .request(parts.method.clone(), &upstream_url)
        .headers(upstream_headers)
        .body(body_bytes.clone());

    // Actually send the request to the gateway and wait for the response
    let upstream_resp = upstream_req.send().await.map_err(|e| {
        tracing::error!("Gateway request failed: {}", e);
        StatusCode::BAD_GATEWAY   // HTTP 502 = upstream server error
    })?;

    // ── Step 4: Inspect the gateway response for the local-request flag ──
    let local_request = upstream_resp
        .headers()
        .get("Pnyx-Local-Request")
        .and_then(|v| v.to_str().ok())          // Convert header bytes to string
        .map(|v| v.eq_ignore_ascii_case("true")) // Case-insensitive comparison
        .unwrap_or(false);                       // Missing header → default to false

    // If the gateway does NOT want a local model, just stream the response back
    if !local_request {

        tracing::debug!("📦 Routing gateway response!");

        let status = StatusCode::from_u16(upstream_resp.status().as_u16())
            .unwrap_or(StatusCode::OK);

        let mut builder = Response::builder().status(status);

        // Copy headers from the gateway response, skipping ones that Axum manages
        for (key, value) in upstream_resp.headers() {
            let key_str = key.as_str().to_lowercase();
            if key_str == "content-length" || key_str == "transfer-encoding" {
                continue;
            }
            builder = builder.header(key.as_str(), value.as_bytes());
        }

        // Convert the reqwest body stream into an Axum body stream
        let stream = upstream_resp.bytes_stream();
        let body = Body::from_stream(stream);
        return Ok(builder.body(body).unwrap());
    }

    // ── Step 5: Local model execution ──
    // The gateway told us to call a local model instead. We must:
    //   a) Read the gateway response body as JSON
    //   b) Extract the `relay_data` field and parse `model_tag`
    //   c) Remove `relay_data` from the payload so it stays OpenAI-compatible
    //   d) Look up the model in Iris's local config using `model_tag` as the key
    //   e) Override the `model` field in the JSON with the configured local model name
    //   f) Call the local model using the original method and path (or a custom override)
    //   g) Stream the local model's response back to the user

    tracing::debug!("🏠 Routing local response!");

    // a) Read the entire gateway body into a byte buffer
    let gateway_body = upstream_resp.bytes().await.map_err(|e| {
        tracing::error!("Failed to read gateway response body: {}", e);
        StatusCode::BAD_GATEWAY
    })?;

    // b) Parse the bytes as generic JSON (`serde_json::Value`)
    let mut payload: serde_json::Value = serde_json::from_slice(&gateway_body).map_err(|e| {
        tracing::error!("Failed to parse gateway JSON: {}", e);
        StatusCode::BAD_GATEWAY
    })?;

    // Extract `relay_data` (clone it so we can remove it from the payload later)
    let relay_data = payload
        .get("relay_data")
        .cloned()
        .ok_or_else(|| {
            tracing::error!("Missing relay_data in gateway response");
            StatusCode::BAD_GATEWAY
        })?;

    // Convert the generic JSON into our strongly typed `RelayData` struct
    let relay_data = RelayData::from_json_value(&relay_data).map_err(|e| {
        tracing::error!("Failed to parse relay_data: {}", e);
        StatusCode::BAD_GATEWAY
    })?;

    // c) Remove `relay_data` from the JSON so the payload is a clean OpenAI request
    if let Some(obj) = payload.as_object_mut() {
        obj.remove("relay_data");
    }

    // d) Look up the model in our local map using the tag from the gateway
    let model = state
        .models
        .get(&relay_data.model_tag)
        .ok_or_else(|| {
            tracing::error!(
                "Missing local model '{}'. This is unexpected.",
                relay_data.model_tag
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // e) Override the model name in the JSON payload with the configured local model name
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("model".to_string(), json!(model.model_name));
    }

    // f) Build the URL to the local model.
    //
    // We use the original request method and path (the ones Iris received from the user).
    // If the model config has a `custom_api_path`, we use that instead of the original path.
    let local_path = model.custom_api_path.as_deref().unwrap_or(path);
    let local_url = format!("{}{}", model.endpoint.trim_end_matches('/'), local_path);

    // Use the original HTTP method that the user sent to Iris.
    // The local model receives the same method (GET, POST, etc.) as the original request.
    let local_method = parts.method.clone();

    // g) Send the request to the local model
    let local_resp = state
        .client
        .request(local_method, &local_url)
        .json(&payload)               // Send the modified JSON as the request body
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Local model request failed: {}", e);
            StatusCode::BAD_GATEWAY
        })?;

    // Build the final HTTP response back to the user
    let status =
        StatusCode::from_u16(local_resp.status().as_u16()).unwrap_or(StatusCode::OK);

    let mut builder = Response::builder().status(status);

    for (key, value) in local_resp.headers() {
        let key_str = key.as_str().to_lowercase();
        if key_str == "content-length" || key_str == "transfer-encoding" {
            continue;
        }
        builder = builder.header(key.as_str(), value.as_bytes());
    }

    let stream = local_resp.bytes_stream();
    let body = Body::from_stream(stream);
    Ok(builder.body(body).unwrap())
}

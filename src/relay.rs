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
use rand::seq::SliceRandom;           // For picking a random healthy model                 

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
// stream_reqwest_response
// ──────────────────────────────────────────────────────────────
// Converts a `reqwest::Response` into an Axum `Response`, copying
// status and headers and streaming the body back to the caller.
// ──────────────────────────────────────────────────────────────
fn stream_reqwest_response(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);

    for (key, value) in resp.headers() {
        let key_str = key.as_str().to_lowercase();
        if key_str == "content-length" || key_str == "transfer-encoding" {
            continue;
        }
        builder = builder.header(key.as_str(), value.as_bytes());
    }

    let stream = resp.bytes_stream();
    let body = Body::from_stream(stream);
    builder.body(body).unwrap()
}

// ──────────────────────────────────────────────────────────────
// call_local_model
// ──────────────────────────────────────────────────────────────
// Sends a JSON payload to a local model, overriding the `model` field
// with the configured local model name.  Returns the raw reqwest
// response so the caller can stream it back to the user.
// ──────────────────────────────────────────────────────────────
async fn call_local_model(
    client: &Client,
    parts: &axum::http::request::Parts,
    mut payload: serde_json::Value,
    model: &LocalModel,
) -> Result<reqwest::Response, StatusCode> {
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("model".to_string(), json!(model.model_name));
    }

    let path = parts.uri.path();
    let local_path = model.custom_api_path.as_deref().unwrap_or(path);
    let local_url = format!("{}{}", model.endpoint.trim_end_matches('/'), local_path);

    client
        .request(parts.method.clone(), &local_url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Local model request failed: {}", e);
            StatusCode::BAD_GATEWAY
        })
}

// ──────────────────────────────────────────────────────────────
// should_fallback
// ──────────────────────────────────────────────────────────────
// Returns true when the user has enabled gateway-error fallback
// and at least one local model is currently healthy.
// ──────────────────────────────────────────────────────────────
fn should_fallback(state: &AppState, healthy: &[String]) -> bool {
    if let Some(ref cfg) = state.config.iris.fallback {
        cfg.on_gateway_error && !healthy.is_empty()
    } else {
        false
    }
}

// ──────────────────────────────────────────────────────────────
// try_fallback
// ──────────────────────────────────────────────────────────────
// Attempts to forward the original request to a local model when
// the gateway fails.  Picks the target model according to the
// fallback configuration ("random" or a specific tag) and returns
// the local model's response streamed back to the user.
// ──────────────────────────────────────────────────────────────
async fn try_fallback(
    state: &AppState,
    parts: &axum::http::request::Parts,
    body_bytes: &bytes::Bytes,
    healthy: &[String],
) -> Result<Response, StatusCode> {
    let fallback_cfg = state.config.iris.fallback.as_ref().unwrap();

    let model_tag = if fallback_cfg.fallback_model == "random" {
        healthy
            .choose(&mut rand::thread_rng())
            .cloned()
            .ok_or_else(|| {
                tracing::warn!("No healthy local models available for random fallback");
                StatusCode::BAD_GATEWAY
            })?
    } else {
        fallback_cfg.fallback_model.clone()
    };

    let model = state.models.get(&model_tag).ok_or_else(|| {
        tracing::warn!("Configured fallback model '{}' not found in local models", model_tag);
        StatusCode::BAD_GATEWAY
    })?;

    if !healthy.contains(&model_tag) {
        tracing::warn!("Configured fallback model '{}' is not healthy", model_tag);
        return Err(StatusCode::BAD_GATEWAY);
    }

    let payload: serde_json::Value = serde_json::from_slice(body_bytes).map_err(|e| {
        tracing::error!("Failed to parse request body for fallback: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    let local_resp = call_local_model(&state.client, parts, payload, model).await?;
    Ok(stream_reqwest_response(local_resp))
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
    let upstream_resp = match upstream_req.send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!("Gateway request failed: {}", e);
            // Check if we should use the fallback
            if should_fallback(&state, &healthy) {
                tracing::debug!("Falling back to local");
                return try_fallback(&state, &parts, &body_bytes, &healthy).await;
            } else {
                // If not, just return an error to the user
                return Err(StatusCode::BAD_GATEWAY); // HTTP 502 = upstream server error
            }
            
        }
    };

    // ── Step 4: Inspect the gateway response status and local-request flag ──
    let status = upstream_resp.status();
    if !status.is_success() {
        match status.as_u16() {
            401 => tracing::warn!("Gateway returned 401 Unauthorized"),
            402 => tracing::warn!("Gateway returned 402 Payment Required"),
            403 => tracing::warn!("Gateway returned 403 Forbidden"),
            _ => tracing::warn!("Gateway returned {} error", status),
        }

        // Check if we should use the fallback
        if should_fallback(&state, &healthy) {
            return try_fallback(&state, &parts, &body_bytes, &healthy).await;
        } else {
            // If not, just relay the gateway error to the user
            return Ok(stream_reqwest_response(upstream_resp));
        }

        
    }

    let local_request = upstream_resp
        .headers()
        .get("Pnyx-Local-Request")
        .and_then(|v| v.to_str().ok())          // Convert header bytes to string
        .map(|v| v.eq_ignore_ascii_case("true")) // Case-insensitive comparison
        .unwrap_or(false);                       // Missing header → default to false

    // If the gateway is providing us an external generation, just stream the response back
    if !local_request {
        tracing::debug!("📦 Routing gateway response!");
        return Ok(stream_reqwest_response(upstream_resp));
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

    let local_resp = call_local_model(&state.client, &parts, payload, model).await?;
    Ok(stream_reqwest_response(local_resp))
}

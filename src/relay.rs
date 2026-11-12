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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

// ── Axum (web framework) imports ──
use axum::{
    body::Body,                                 // Axum's request/response body type
    extract::{Request, State}, // `Request` = incoming HTTP data; `State` = shared application state
    http::{HeaderMap, HeaderValue, StatusCode}, // HTTP primitives
    response::{IntoResponse, Response}, // Traits/types for building responses
    Json,                      // JSON response helper
};

// `http_body_util` provides helpers for reading request bodies as bytes
use http_body_util::BodyExt;
use rand::seq::SliceRandom;
use reqwest::Client; // The HTTP client we use to call the gateway and local models
use serde_json::json; // For picking a random healthy model
use futures_core::Stream; // For the pass-through usage-tracking body stream

use crate::config::Config;
use crate::models::{LocalModel, Metrics, RelayData};
use crate::web::SharedState;

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
async fn get_healthy_models(client: &Client, models: &HashMap<String, LocalModel>) -> Vec<String> {
    let mut healthy = Vec::new();

    // Loop through every model we know about
    for (name, model) in models.iter() {
        // Build the health URL: e.g. http://localhost:1234/health
        let url = format!("{}/health", model.endpoint.trim_end_matches('/'));

        // Send a GET request with a 5-second timeout
        match model
            .apply_custom_headers(client.get(&url))
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
// Local-generation usage/timing tracking
// ──────────────────────────────────────────────────────────────
// Everything below inspects a LOCAL model response (streaming or not)
// to extract OpenAI `usage` + generation timings and (eventually) report
// them to the gateway. Gateway-passthrough responses never use this.

/// Max bytes we buffer when inspecting a non-streaming JSON body to find
/// `usage` (forwarded to the client regardless). OpenAI puts `usage` at the
/// end, so 10 MiB is generous; beyond it we give up parsing and mark tokens
/// absent (ignore-me) rather than hold the whole body.
const JSON_USAGE_CAP: usize = 1024 * 1024 * 10;

/// Everything the response tee needs to record metrics and (later) report
/// to the gateway. Cheap to clone/move into the body stream.
struct LocalGenContext {
    metrics: Arc<Metrics>,
    client: Client,
    gateway_traces_url: String,
    access_token: String,
    model_tag: String,
    local_call_start: Instant,
    // Gateway routing time for a gateway-directed generation. `None` (recorded
    // as ignore-me) when the answer didn't come from the gateway (fallbacks).
    routing_ms: Option<u64>,
    trace_router: Option<String>,
}

/// A pass-through `Stream` that forwards every byte chunk to the client
/// unchanged (no added latency / no full buffering for SSE) while
/// incrementally sniffing out the OpenAI `usage` object and the time of the
/// first generated token. Records metrics + builds the report when the
/// response ends, errors, or is dropped (client disconnect) — exactly once.
struct UsageTeeStream<S> {
    inner: Pin<Box<S>>,
    ctx: LocalGenContext,
    is_sse: bool,
    status_code: u16,
    line_buf: Vec<u8>,       // SSE: partial line across chunk boundaries
    json_buf: Vec<u8>,       // non-streaming: buffered body (capped)
    json_overflow: bool,     // body exceeded JSON_USAGE_CAP
    usage: Option<serde_json::Value>, // last `usage` object seen (SSE)
    ttft_ms: Option<u64>,    // time of first content token vs local_call_start
    finalized: bool,
}

impl<S> UsageTeeStream<S> {
    fn new(body: S, ctx: LocalGenContext, is_sse: bool, status_code: u16) -> Self {
        UsageTeeStream {
            inner: Box::pin(body),
            ctx,
            is_sse,
            status_code,
            line_buf: Vec::new(),
            json_buf: Vec::new(),
            json_overflow: false,
            usage: None,
            ttft_ms: None,
            finalized: false,
        }
    }

    /// Feed a raw chunk. Bytes were already (or will be) forwarded to the
    /// client by `poll`; here we only observe them.
    fn consume(&mut self, bytes: &[u8], now: Instant) {
        if self.is_sse {
            self.line_buf.extend_from_slice(bytes);
            while let Some(pos) = self.line_buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.line_buf.drain(..=pos).collect();
                self.handle_sse_line(&line, now);
            }
            // Defensive: a pathological "line" with no newline must not grow forever.
            if self.line_buf.len() > JSON_USAGE_CAP {
                self.line_buf.clear();
            }
        } else if !self.json_overflow {
            if self.json_buf.len() + bytes.len() <= JSON_USAGE_CAP {
                self.json_buf.extend_from_slice(bytes);
            } else {
                self.json_overflow = true;
            }
        }
    }

    fn handle_sse_line(&mut self, line: &[u8], now: Instant) {
        let s = String::from_utf8_lossy(line);
        let s = s.trim_end_matches(['\r', '\n']);
        let data = match s.strip_prefix("data:") {
            Some(d) => d.trim(),
            None => return, // ignore `event:`, comments (`:`), blanks
        };
        if data == "[DONE]" {
            return;
        }
        let v: serde_json::Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => return,
        };
        if self.ttft_ms.is_none() && first_token_present(&v) {
            self.ttft_ms = Some(now.duration_since(self.ctx.local_call_start).as_millis() as u64);
        }
        if let Some(u) = v.get("usage") {
            if !u.is_null() {
                self.usage = Some(u.clone());
            }
        }
    }

    /// Finalize exactly once: record usage + timings and fire the report.
    fn finalize(&mut self) {
        if self.finalized {
            return;
        }
        self.finalized = true;

        let generation_ms = Some(Instant::now().duration_since(self.ctx.local_call_start).as_millis() as u64);

        // Resolve the usage object: SSE captured it incrementally; for a
        // non-streaming body we parse the (capped) buffer now.
        let usage_val = if self.is_sse {
            self.usage.take()
        } else if !self.json_overflow {
            serde_json::from_slice::<serde_json::Value>(&self.json_buf)
                .ok()
                .and_then(|v| v.get("usage").cloned())
        } else {
            None
        };
        let (prompt, completion, cached) = extract_usage(usage_val.as_ref());

        // Tokens are recorded on every local generation (decision #1).
        self.ctx.metrics.record_local_usage(prompt, completion, cached);

        // If the gateway never sent a trace id we cannot correlate the report,
        // so record ALL times as ignore-me (-1); otherwise record measured ones.
        let (routing, ttft, gen) = if self.ctx.trace_router.is_some() {
            (self.ctx.routing_ms, self.ttft_ms, generation_ms)
        } else {
            (None, None, None)
        };
        self.ctx.metrics.record_local_times(routing, ttft, gen);

        let payload = build_usage_payload(prompt, completion, cached, ttft, gen, self.status_code);
        report_local_usage(&self.ctx, payload);

        // Release buffers promptly (they may be sizable).
        self.line_buf = Vec::new();
        self.json_buf = Vec::new();
    }
}

impl<S: Stream<Item = Result<bytes::Bytes, reqwest::Error>> + 'static> Stream for UsageTeeStream<S> {
    type Item = Result<bytes::Bytes, reqwest::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        match me.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let now = Instant::now();
                me.consume(&bytes, now);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => {
                me.finalize();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                me.finalize();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for UsageTeeStream<S> {
    fn drop(&mut self) {
        // Covers client disconnects / early drops where `poll` never reaches
        // the terminal `None`/`Err`. `finalize` is idempotent.
        self.finalize();
    }
}

/// Like `stream_reqwest_response`, but wraps the body in a usage-tracking tee.
/// Only used for LOCAL model responses (fallback + gateway-directed-local).
fn stream_local_response(resp: reqwest::Response, ctx: LocalGenContext) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let status_code = resp.status().as_u16();
    let is_sse = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("text/event-stream"))
        .unwrap_or(false);

    let mut builder = Response::builder().status(status);
    for (key, value) in resp.headers() {
        let key_str = key.as_str().to_lowercase();
        if key_str == "content-length" || key_str == "transfer-encoding" {
            continue;
        }
        builder = builder.header(key.as_str(), value.as_bytes());
    }

    let body = Body::from_stream(UsageTeeStream::new(resp.bytes_stream(), ctx, is_sse, status_code));
    builder.body(body).unwrap()
}

/// True if an SSE chunk carries the first generated token (non-empty delta /
/// message content, tool calls, or completions-style text). The leading
/// role-only delta (empty content) is correctly treated as *not* a token.
fn first_token_present(v: &serde_json::Value) -> bool {
    let arr = match v.get("choices").and_then(|c| c.as_array()) {
        Some(a) => a,
        None => return false,
    };
    for ch in arr {
        let delta = ch.get("delta").or_else(|| ch.get("message"));
        let has_content = delta
            .and_then(|d| d.get("content"))
            .and_then(|c| c.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let has_tool = delta.and_then(|d| d.get("tool_calls")).is_some();
        let has_text = ch
            .get("text")
            .and_then(|t| t.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if has_content || has_tool || has_text {
            return true;
        }
    }
    false
}

/// Pull the three canonical OpenAI token counts; each is `None` when absent.
fn extract_usage(usage: Option<&serde_json::Value>) -> (Option<u64>, Option<u64>, Option<u64>) {
    match usage {
        Some(u) => {
            let prompt = u.get("prompt_tokens").and_then(|n| n.as_u64());
            let completion = u.get("completion_tokens").and_then(|n| n.as_u64());
            let cached = u
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|n| n.as_u64());
            (prompt, completion, cached)
        }
        None => (None, None, None),
    }
}

/// Build the gateway report payload. Ignore-me (`None`) fields become `-1`;
/// `cache_creation_tokens` has no OpenAI source and is always `-1`.
fn build_usage_payload(
    prompt: Option<u64>,
    completion: Option<u64>,
    cached: Option<u64>,
    ttft: Option<u64>,
    generation: Option<u64>,
    status_code: u16,
) -> serde_json::Value {
    let i = |v: Option<u64>| v.map(|x| x as i64).unwrap_or(-1);
    json!({
        "prompt_tokens": i(prompt),
        "completion_tokens": i(completion),
        "cached_tokens": i(cached),
        "cache_creation_tokens": -1,
        "time_to_first_token_ms": i(ttft),
        "total_generation_time_ms": i(generation),
        "status_code": status_code,
    })
}

/// Report locally-generated usage/timings to the gateway via a fire-and-forget
/// `POST /<id>/update-usage`. Correlation is the trace id only; without
/// it the report is unmatchable → warn + skip.
fn report_local_usage(ctx: &LocalGenContext, payload: serde_json::Value) {
    let trace = match ctx.trace_router.as_deref() {
        Some(t) => t,
        None => {
            tracing::warn!(
                model_tag = %ctx.model_tag,
                "Pnyx-Trace-Router absent; skipped usage report (all times recorded as -1)"
            );
            return;
        }
    };
    let url = format!(
        "{}/{}/update-usage",
        ctx.gateway_traces_url.trim_end_matches('/'),
        trace
    );

    // Fire-and-forget report.
    let client = ctx.client.clone();
    let token = ctx.access_token.clone();
    tokio::spawn(async move {
        let result = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("Pnyx-Sidecar-Request", "true")
            .json(&payload)
            .send()
            .await;
        match result {
            Ok(resp) => {
                let status = resp.status();
                if status == reqwest::StatusCode::OK {
                    tracing::debug!("usage report POST -> HTTP {}", status);
                } else {
                    let body = resp.text().await.unwrap_or_default();
                    tracing::warn!(
                        "usage report POST on {} returned non-200 ({}): {}",
                        url,
                        status,
                        body
                    );
                }
            }
            Err(e) => {
                tracing::warn!("usage report POST on {} failed: {}", url, e);
            }
        }
    });
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

    let request = client
        .request(parts.method.clone(), &local_url)
        .json(&payload);

    model
        .apply_custom_headers(request)
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
fn should_fallback(config: &Config, healthy: &[String]) -> bool {
    if let Some(ref cfg) = config.iris.fallback {
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
// Routing time is recorded as ignore-me: a fallback is never a
// gateway-directed generation, so there is no completed routing.
// ──────────────────────────────────────────────────────────────
#[allow(clippy::too_many_arguments)]
async fn try_fallback(
    config: &Config,
    models: &HashMap<String, LocalModel>,
    client: &Client,
    parts: &axum::http::request::Parts,
    body_bytes: &bytes::Bytes,
    healthy: &[String],
    metrics: &Arc<Metrics>,
    gateway_traces_url: &str,
    access_token: &str,
    trace_router: Option<String>,
) -> Result<Response, StatusCode> {
    let fallback_cfg = config.iris.fallback.as_ref().unwrap();

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

    let model = models.get(&model_tag).ok_or_else(|| {
        tracing::warn!(
            "Configured fallback model '{}' not found in local models",
            model_tag
        );
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

    let local_call_start = Instant::now();
    let local_resp = call_local_model(client, parts, payload, model).await?;
    let ctx = LocalGenContext {
        metrics: metrics.clone(),
        client: client.clone(),
        gateway_traces_url: gateway_traces_url.to_string(),
        access_token: access_token.to_string(),
        model_tag: model.model_tag.clone(),
        local_call_start,
        // Fallback is never a gateway-directed generation: no routing time.
        routing_ms: None,
        trace_router,
    };
    Ok(stream_local_response(local_resp, ctx))
}

// ──────────────────────────────────────────────────────────────
// Gateway error enrichment
// ──────────────────────────────────────────────────────────────
// The gateway returns informative JSON bodies on error (OpenAI-style
// `{"error": {"message": ...}}`). We read those bodies so the warning logs
// and the client-facing error both carry the gateway's own explanation
// instead of a bare status code.

/// Max bytes buffered when reading a gateway error body for logging and
/// enrichment. Error bodies are small JSON; the cap only guards against
/// pathological responses.
const ERROR_BODY_CAP: usize = 64 * 1024;

/// Read a gateway error body into a JSON value (a string value when the body
/// isn't valid JSON), capped at `ERROR_BODY_CAP`. Safe to buffer: error
/// responses are never streaming SSE bodies.
async fn read_gateway_error_body(
    mut resp: reqwest::Response,
) -> Result<serde_json::Value, StatusCode> {
    let mut buf: Vec<u8> = Vec::new();
    let mut capped = false;
    while let Some(chunk) = resp.chunk().await.map_err(|e| {
        tracing::error!("Failed to read gateway response body: {}", e);
        StatusCode::BAD_GATEWAY
    })? {
        if buf.len() + chunk.len() > ERROR_BODY_CAP {
            let take = ERROR_BODY_CAP - buf.len();
            buf.extend_from_slice(&chunk[..take]);
            capped = true;
            break;
        }
        buf.extend_from_slice(&chunk);
    }
    if capped {
        tracing::warn!(
            "Gateway error body exceeded {} bytes; truncated before logging/return",
            ERROR_BODY_CAP
        );
    }
    Ok(serde_json::from_slice::<serde_json::Value>(&buf).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(&buf).to_string())
    }))
}

/// Extract the informative message from a gateway error body: `error.message`
/// (OpenAI-style) first, then bare strings under `error`, `detail`, `message`.
/// `None` when nothing string-shaped matches.
fn gateway_error_message(value: &serde_json::Value) -> Option<&str> {
    value
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .or_else(|| {
            ["error", "detail", "message"]
                .iter()
                .find_map(|k| value.get(*k).and_then(|v| v.as_str()))
        })
}

/// Compact JSON (or raw string) preview for logs when no structured message
/// was found, char-safely truncated.
fn short_body_preview(value: &serde_json::Value, max_chars: usize) -> String {
    let compact = value.to_string();
    let mut chars = compact.chars();
    let head: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{}…[truncated]", head)
    } else {
        head
    }
}

/// Sidecar-facing advice embedded in the client error response and log.
fn gateway_error_hint(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "Sidecar misconfigured.",
        StatusCode::UNAUTHORIZED => "Sidecar token unauthorized.",
        StatusCode::PAYMENT_REQUIRED => "Insufficient credits / payment required at the gateway.",
        StatusCode::FORBIDDEN => "Access forbidden by the gateway.",
        StatusCode::NOT_FOUND => "Endpoint not found on the gateway.",
        StatusCode::REQUEST_TIMEOUT => "Gateway timed out.",
        StatusCode::TOO_MANY_REQUESTS => "Rate limited by the gateway.",
        s if s.is_server_error() => "Gateway unavailable.",
        _ => "Gateway rejected the request.",
    }
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
    State(state): State<Arc<tokio::sync::RwLock<SharedState>>>,
    req: Request,
) -> Result<Response, StatusCode> {
    let start = Instant::now();

    // Lock briefly to read config and models, then release
    let (config, models, client, metrics) = {
        let guard = state.read().await;
        (
            guard.config.clone(),
            guard.models.clone(),
            guard.client.clone(),
            guard.metrics.clone(),
        )
    };

    // ── Step 1: Optional local authentication ──
    // If the user put a token in the YAML (`iris.token`), we require every
    // incoming request to carry `Authorization: Bearer <that_token>`.
    // This mimics how OpenAI API clients already send tokens.
    if let Some(ref token) = config.iris.token {
        let auth = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok()); // Convert header bytes to a UTF-8 string

        let expected = format!("Bearer {}", token);

        if auth != Some(&expected) {
            tracing::warn!("Unauthorized request");
            let body = json!({ "error": "Unauthorized" });
            return Ok((StatusCode::UNAUTHORIZED, Json(body)).into_response());
        }
    }

    // ── Step 2: Read the request body into memory ──
    // Axum bodies are *streams* – we must collect them into a single byte buffer
    // before we can forward them. `into_parts()` separates the HTTP metadata
    // (method, headers, URI) from the body stream.
    let (parts, body) = req.into_parts();

    let body_bytes = body
        .collect() // Collect all chunks of the stream
        .await
        .map_err(|e| {
            tracing::error!("Failed to read request body: {}", e);
            StatusCode::BAD_REQUEST
        })?
        .to_bytes(); // Convert the collected chunks into a `bytes::Bytes` object

    // Remember the path and query string so we can reconstruct the upstream URL
    let path = parts.uri.path();
    let query = parts
        .uri
        .query()
        .map(|q| format!("?{}", q)) // If there is a query, prepend `?`
        .unwrap_or_default(); // Otherwise use an empty string

    // ── Step 3: Prepare the request to the PNYX gateway ──
    let gateway_url = config.pnyx.gateway_relay_url();
    let gateway_base = gateway_url.trim_end_matches('/').to_string();
    let gateway_traces_url = config.pnyx.gateway_traces_url();
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
    upstream_headers.insert("Pnyx-Sidecar-Request", HeaderValue::from_static("true"));

    // Check local model health and, if any are up, tell the gateway which are available
    let healthy = get_healthy_models(&client, &models).await;

    // Update shared model health cache so the UI is fresh
    {
        let mut guard = state.write().await;
        for tag in &healthy {
            guard.model_health.insert(tag.clone(), true);
        }
        for (tag, _) in &models {
            if !healthy.contains(tag) {
                guard.model_health.insert(tag.clone(), false);
            }
        }
    }

    if !healthy.is_empty() {
        let value = healthy.join(","); // e.g. "model-a,model-b"
        upstream_headers.insert(
            "Pnyx-Local-Suppliers",
            HeaderValue::from_str(&value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
    }

    // Attach the PNYX access token (from YAML or PNYX_TOKEN env var)
    upstream_headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", config.pnyx.access_token))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );

    // Build the reqwest request object
    let upstream_req = client
        .request(parts.method.clone(), &upstream_url)
        .headers(upstream_headers)
        .body(body_bytes.clone());

    // Actually send the request to the gateway and wait for the response
    // headers. `trace_router` is the correlation key for the usage report;
    // `gateway_resp_at` marks when the gateway answered. Routing time is
    // derived from it only when that answer is a local-generation request —
    // gateway-passthrough and fallbacks have no routing to measure.
    let (upstream_resp, trace_router, gateway_resp_at) = match upstream_req.send().await {
        Ok(resp) => {
            let gateway_resp_at = Instant::now();
            let trace_router = resp
                .headers()
                .get("Pnyx-Trace-Router")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            (resp, trace_router, Some(gateway_resp_at))
        }
        Err(e) => {
            tracing::error!("Gateway request failed: {}", e);
            // Check if we should use the fallback
            if should_fallback(&config, &healthy) {
                tracing::debug!("Falling back to local");
                let resp = try_fallback(
                    &config,
                    &models,
                    &client,
                    &parts,
                    &body_bytes,
                    &healthy,
                    &metrics,
                    &gateway_traces_url,
                    &config.pnyx.access_token,
                    None,
                )
                .await;
                if let Ok(ref _r) = resp {
                    let ms = start.elapsed().as_millis() as u64;
                    metrics.record_local(ms);
                }
                return resp;
            } else {
                // If not, just return an error to the user
                return Err(StatusCode::BAD_GATEWAY); // HTTP 502 = upstream server error
            }
        }
    };

    // ── Step 4: Inspect the gateway response status ──
    let status = upstream_resp.status();
    if !status.is_success() {
        // Read the gateway's informative error body FIRST so both the warning
        // log and the client-facing response carry the gateway's own message
        // instead of a bare status code.
        let gateway_response = read_gateway_error_body(upstream_resp).await?;
        let message = gateway_error_message(&gateway_response)
            .map(str::to_string)
            .unwrap_or_else(|| short_body_preview(&gateway_response, 512));

        tracing::warn!(
            status = %status,
            hint = gateway_error_hint(status),
            trace = %trace_router.as_deref().unwrap_or("-"),
            gateway_message = %message,
            "Gateway returned non-success status"
        );

        // 4xx (bad config/token/credits/…) cannot be fixed by calling a local
        // model, so only 5xx may fall back. Otherwise return an enriched JSON
        // error carrying the gateway's own response.
        if status.is_server_error() && should_fallback(&config, &healthy) {
            let resp = try_fallback(
                &config,
                &models,
                &client,
                &parts,
                &body_bytes,
                &healthy,
                &metrics,
                &gateway_traces_url,
                &config.pnyx.access_token,
                trace_router.clone(),
            )
            .await;
            if let Ok(ref _r) = resp {
                let ms = start.elapsed().as_millis() as u64;
                metrics.record_local(ms);
            }
            return resp;
        }

        let body = json!({
            "error": gateway_error_hint(status),
            "gateway_response": gateway_response
        });
        let ms = start.elapsed().as_millis() as u64;
        metrics.record_pnyx(ms);
        return Ok((status, Json(body)).into_response());
    }

    let local_request = upstream_resp
        .headers()
        .get("Pnyx-Local-Request")
        .and_then(|v| v.to_str().ok()) // Convert header bytes to string
        .map(|v| v.eq_ignore_ascii_case("true")) // Case-insensitive comparison
        .unwrap_or(false); // Missing header → default to false

    // If the gateway is providing us an external generation, just stream the response back
    if !local_request {
        tracing::debug!("📦 Routing gateway response!");
        let ms = start.elapsed().as_millis() as u64;
        metrics.record_pnyx(ms);
        return Ok(stream_reqwest_response(upstream_resp));
    }

    // ── Step 5: Local model execution ──
    // The gateway told us to call a local model instead. We must:
    //   a) Read the gateway response body as JSON
    //   b) Extract the `relay_data` field and parse `model_tag`
    //   c) Remove `relay_data` from the payload so it stays OpenAI-compatible
    //   d) Look up the model in our local map using `model_tag` as the key
    //   e) Override the `model` field in the JSON with the configured local model name
    //   f) Call the local model using the original method and path (or a custom override)
    //   g) Stream the local model's response back to the user

    tracing::debug!("🏠 Routing local response!");

    // Collect any pnyx-* headers from the gateway response so they can
    // be forwarded back to the client even though the body is generated locally.
    let mut pnyx_headers = HeaderMap::new();
    for (key, value) in upstream_resp.headers() {
        if key.as_str().to_lowercase().starts_with("pnyx-") {
            pnyx_headers.insert(key.clone(), value.clone());
        }
    }

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
    let relay_data = payload.get("relay_data").cloned().ok_or_else(|| {
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
    let model = models.get(&relay_data.model_tag).ok_or_else(|| {
        tracing::error!(
            "Missing local model '{}'. This is unexpected.",
            relay_data.model_tag
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    tracing::debug!("🏠 Model: {}", relay_data.model_tag);

    // The gateway answered asking us to run the generation: routing ended
    // when that answer's headers arrived.
    let routing_ms = gateway_resp_at.map(|at| at.duration_since(start).as_millis() as u64);

    let local_call_start = Instant::now();
    let local_resp = call_local_model(&client, &parts, payload, model).await?;
    let ctx = LocalGenContext {
        metrics: metrics.clone(),
        client: client.clone(),
        gateway_traces_url: gateway_traces_url.clone(),
        access_token: config.pnyx.access_token.clone(),
        model_tag: relay_data.model_tag.clone(),
        local_call_start,
        routing_ms,
        trace_router,
    };
    let mut response = stream_local_response(local_resp, ctx);

    // Inject the pnyx-* headers from the gateway into the final response
    for (key, value) in &pnyx_headers {
        response.headers_mut().insert(key.clone(), value.clone());
    }

    // Record local metrics
    let ms = start.elapsed().as_millis() as u64;
    metrics.record_local(ms);

    // Response is actually written to the client here, so order of response generation and headers does not matter.
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Instant;

    // A Stream that yields nothing; the tee is driven directly via
    // consume()/finalize() in these tests, so `inner` is never polled.
    struct NullStream;
    impl Stream for NullStream {
        type Item = Result<bytes::Bytes, reqwest::Error>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    fn test_ctx(metrics: Arc<Metrics>, trace: Option<&str>) -> LocalGenContext {
        LocalGenContext {
            metrics,
            client: reqwest::Client::new(),
            gateway_traces_url: "https://gw.example".to_string(),
            access_token: "tok".to_string(),
            model_tag: "m".to_string(),
            local_call_start: Instant::now(),
            routing_ms: Some(10),
            trace_router: trace.map(|s| s.to_string()),
        }
    }

    #[test]
    fn extract_usage_reads_canonical_fields() {
        let v = serde_json::json!({
            "prompt_tokens": 120, "completion_tokens": 128, "total_tokens": 248,
            "prompt_tokens_details": { "cached_tokens": 30 }
        });
        assert_eq!(extract_usage(Some(&v)), (Some(120), Some(128), Some(30)));

        let v2 = serde_json::json!({ "prompt_tokens": 5, "completion_tokens": 6 });
        assert_eq!(extract_usage(Some(&v2)), (Some(5), Some(6), None));

        assert_eq!(extract_usage(None), (None, None, None));
    }

    #[test]
    fn first_token_present_detects_content_variants() {
        let role = serde_json::json!({ "choices": [{ "delta": { "role": "assistant", "content": "" } }] });
        assert!(!first_token_present(&role));

        let content = serde_json::json!({ "choices": [{ "delta": { "content": "hi" } }] });
        assert!(first_token_present(&content));

        let tools = serde_json::json!({ "choices": [{ "delta": { "tool_calls": [{ "index": 0 }] } }] });
        assert!(first_token_present(&tools));

        let text = serde_json::json!({ "choices": [{ "text": "abc" }] });
        assert!(first_token_present(&text));

        let usage_only = serde_json::json!({ "choices": [], "usage": { "prompt_tokens": 1 } });
        assert!(!first_token_present(&usage_only));
    }

    #[test]
    fn payload_uses_minus_one_for_ignored_and_constant_cache_creation() {
        let p = build_usage_payload(Some(120), Some(128), None, None, Some(2000), 200);
        assert_eq!(p["prompt_tokens"], serde_json::json!(120));
        assert_eq!(p["completion_tokens"], serde_json::json!(128));
        assert_eq!(p["cached_tokens"], serde_json::json!(-1));
        assert_eq!(p["cache_creation_tokens"], serde_json::json!(-1));
        assert_eq!(p["time_to_first_token_ms"], serde_json::json!(-1));
        assert_eq!(p["total_generation_time_ms"], serde_json::json!(2000));
        assert_eq!(p["status_code"], serde_json::json!(200));
    }

    #[tokio::test]
    async fn nonstreaming_tee_records_tokens_and_ignores_ttft() {
        let m = Arc::new(Metrics::default());
        let mut tee = UsageTeeStream::new(NullStream, test_ctx(m.clone(), Some("trace-1")), false, 200);
        let body =
            br#"{"usage":{"prompt_tokens":120,"completion_tokens":128,"prompt_tokens_details":{"cached_tokens":30}}}"#;
        tee.consume(body, Instant::now());
        tee.finalize();

        assert_eq!(m.prompt_tokens_stats().total, 120);
        assert_eq!(m.completion_tokens_stats().total, 128);
        assert_eq!(m.cached_tokens_stats().total, 30);
        assert_eq!(m.ttft_stats().ignored, 1); // non-streaming -> ignore-me
        assert_eq!(m.ttft_stats().valid, 0);
        assert_eq!(m.routing_stats().valid, 1);
        assert_eq!(m.generation_stats().valid, 1);

        tee.finalize(); // idempotent
        assert_eq!(m.local_observations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn missing_trace_ignores_all_times_but_keeps_tokens() {
        let m = Arc::new(Metrics::default());
        let mut tee = UsageTeeStream::new(NullStream, test_ctx(m.clone(), None), false, 200);
        let body = br#"{"usage":{"prompt_tokens":7,"completion_tokens":9}}"#;
        tee.consume(body, Instant::now());
        tee.finalize();

        assert_eq!(m.prompt_tokens_stats().total, 7);
        assert_eq!(m.completion_tokens_stats().total, 9);
        assert_eq!(m.routing_stats().ignored, 1);
        assert_eq!(m.ttft_stats().ignored, 1);
        assert_eq!(m.generation_stats().ignored, 1);
        assert_eq!(m.generation_stats().valid, 0);
    }

    #[tokio::test]
    async fn streaming_sse_split_across_chunks_captures_usage_and_ttft() {
        let m = Arc::new(Metrics::default());
        let mut tee = UsageTeeStream::new(NullStream, test_ctx(m.clone(), Some("trace-2")), true, 200);

        // A data line split across two chunks (no newline in the first chunk).
        let part1 = br#"data: {"choices":[{"delta":{"content":"hi"}}]}"#;
        let part2 = b"\n\n";
        tee.consume(part1, Instant::now());
        assert_eq!(m.local_observations.load(Ordering::Relaxed), 0);
        tee.consume(part2, Instant::now());

        // Terminal usage event with empty choices.
        let usage =
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4,\"prompt_tokens_details\":{\"cached_tokens\":2}}}\n\n";
        tee.consume(usage, Instant::now());
        tee.finalize();

        assert_eq!(m.prompt_tokens_stats().total, 3);
        assert_eq!(m.completion_tokens_stats().total, 4);
        assert_eq!(m.cached_tokens_stats().total, 2);
        assert_eq!(m.ttft_stats().valid, 1);
        assert_eq!(m.generation_stats().valid, 1);
    }

    // Drives the tee through the real `poll_next` path and asserts bytes are
    // forwarded to the client unchanged and finalize fires exactly once.
    struct ChunkStream {
        chunks: std::collections::VecDeque<bytes::Bytes>,
    }
    impl Stream for ChunkStream {
        type Item = Result<bytes::Bytes, reqwest::Error>;
        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let me = self.get_mut();
            match me.chunks.pop_front() {
                Some(b) => Poll::Ready(Some(Ok(b))),
                None => Poll::Ready(None),
            }
        }
    }

    struct NoopWake;
    impl std::task::Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    #[tokio::test]
    async fn tee_forwards_bytes_unchanged_and_finalizes_once() {
        let m = Arc::new(Metrics::default());
        let body: &[u8] = br#"{"choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":2,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":1}}}"#;
        let mid = body.len() / 2;
        let chunks = vec![
            bytes::Bytes::copy_from_slice(&body[..mid]),
            bytes::Bytes::copy_from_slice(&body[mid..]),
        ];
        let inner = ChunkStream {
            chunks: chunks.into(),
        };
        let tee = UsageTeeStream::new(inner, test_ctx(m.clone(), Some("t")), false, 200);
        let mut pin = Box::pin(tee);

        let waker = std::task::Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);

        let mut out: Vec<u8> = Vec::new();
        loop {
            match pin.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(Ok(b))) => out.extend_from_slice(&b),
                Poll::Ready(Some(Err(_))) => panic!("unexpected stream error"),
                Poll::Ready(None) => break,
                Poll::Pending => panic!("unexpected pending in finite stream"),
            }
        }

        // Byte-for-byte passthrough to the client.
        assert_eq!(out.as_slice(), body);
        // Exactly one finalize, with tokens parsed from the reassembled JSON.
        assert_eq!(m.local_observations.load(Ordering::Relaxed), 1);
        assert_eq!(m.prompt_tokens_stats().total, 2);
        assert_eq!(m.completion_tokens_stats().total, 3);
        assert_eq!(m.cached_tokens_stats().total, 1);
    }

    #[test]
    fn gateway_error_message_extracts_openai_and_flat_shapes() {
        let openai = serde_json::json!({ "error": { "message": "bad model", "type": "invalid_request" } });
        assert_eq!(gateway_error_message(&openai), Some("bad model"));

        let bare_error = serde_json::json!({ "error": "nope" });
        assert_eq!(gateway_error_message(&bare_error), Some("nope"));

        let detail = serde_json::json!({ "detail": "quota exhausted" });
        assert_eq!(gateway_error_message(&detail), Some("quota exhausted"));

        let message = serde_json::json!({ "message": "slow down" });
        assert_eq!(gateway_error_message(&message), Some("slow down"));

        let opaque = serde_json::json!({ "code": 42 });
        assert_eq!(gateway_error_message(&opaque), None);

        let raw = serde_json::Value::String("plain text".to_string());
        assert_eq!(gateway_error_message(&raw), None);
    }

    #[test]
    fn gateway_error_hint_maps_statuses() {
        assert_eq!(gateway_error_hint(StatusCode::BAD_REQUEST), "Sidecar misconfigured.");
        assert_eq!(gateway_error_hint(StatusCode::UNAUTHORIZED), "Sidecar token unauthorized.");
        assert_eq!(
            gateway_error_hint(StatusCode::PAYMENT_REQUIRED),
            "Insufficient credits / payment required at the gateway."
        );
        assert_eq!(gateway_error_hint(StatusCode::TOO_MANY_REQUESTS), "Rate limited by the gateway.");
        assert_eq!(gateway_error_hint(StatusCode::INTERNAL_SERVER_ERROR), "Gateway unavailable.");
        assert_eq!(gateway_error_hint(StatusCode::IM_A_TEAPOT), "Gateway rejected the request.");
    }

    #[test]
    fn short_body_preview_truncates_on_char_boundary() {
        let v = serde_json::json!({ "note": "ünïcödé".repeat(100) });
        let p = short_body_preview(&v, 20);
        assert!(p.ends_with("…[truncated]"));
        assert!(p.chars().count() <= 20 + "…[truncated]".chars().count());

        let small = serde_json::json!({ "a": 1 });
        assert_eq!(short_body_preview(&small, 512), small.to_string());
    }
}

// ─── web.rs ────────────────────────────────────────────────
// Built-in admin dashboard and live config editor.
// Serves a single-page vanilla-JS UI on a dedicated port.
// ──────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::config::{Config, FallbackConfig, PnyxConfig};
use crate::models::{FieldStats, LocalModel, Metrics};

// ──────────────────────────────────────────────────────────────
// Shared state (mirrors relay.rs, but mutable)
// ──────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct SharedState {
    pub client: reqwest::Client,
    pub config: Config,
    pub models: HashMap<String, LocalModel>,
    pub model_health: HashMap<String, bool>,
    pub last_health_check: Option<Instant>,
    pub metrics: Arc<Metrics>,
    pub available_models: Vec<String>,
}

pub type WebState = Arc<RwLock<SharedState>>;

// ──────────────────────────────────────────────────────────────
// Build the web UI router
// ──────────────────────────────────────────────────────────────
pub fn build_router(state: WebState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/api/health", get(api_health))
        .route("/api/stats", get(api_stats))
        .route("/api/available_models", get(api_available_models))
        .route("/api/v1_models", get(api_v1_models))
        .route("/api/client_config", get(api_client_config))
        .route("/api/refresh_models", post(api_refresh_models))
        .route("/api/config", get(api_config).post(api_config_update))
        .route("/api/models", post(api_models_update))
        .with_state(state)
}

// ──────────────────────────────────────────────────────────────
// Background health checker task
// ──────────────────────────────────────────────────────────────
pub async fn health_checker_task(client: reqwest::Client, state: WebState) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.tick().await; // first tick fires immediately

    loop {
        interval.tick().await;

        let mut locked = state.write().await;
        let mut new_health = HashMap::new();

        // Check every local model
        for (name, model) in &locked.models {
            let url = format!("{}/health", model.endpoint.trim_end_matches('/'));
            let healthy = match model
                .apply_custom_headers(client.get(&url))
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(resp) => resp.status().is_success(),
                Err(_) => false,
            };
            new_health.insert(name.clone(), healthy);
        }
        locked.model_health = new_health;
        locked.last_health_check = Some(Instant::now());

        // Check PNYX gateway health endpoint
        let pnyx_health = format!("{}/healthz", locked.config.pnyx.base_url.trim_end_matches('/'));
        let pnyx_status = match client.get(&pnyx_health).timeout(Duration::from_secs(5)).send().await {
            Ok(resp) => {
                if resp.status() == StatusCode::OK {
                    "200 OK".to_string()
                } else {
                    format!("Error: HTTP {}", resp.status())
                }
            }
            Err(e) => format!("Error: {}", e),
        };
        locked.metrics.set_pnyx_status(pnyx_status);

        drop(locked);
    }
}

// ──────────────────────────────────────────────────────────────
// Fetch available models from PNYX API
// ──────────────────────────────────────────────────────────────
pub async fn fetch_available_models(
    client: &reqwest::Client,
    pnyx_config: &PnyxConfig,
) -> Result<Vec<String>, reqwest::Error> {
    let url = format!(
        "{}/models?service={}",
        pnyx_config.gateway_insights_url(),
        pnyx_config.service_name
    );
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", pnyx_config.access_token))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        tracing::warn!(
            "Failed to fetch available models signatures: HTTP {} from {}",
            status,
            url
        );
        return Ok(Vec::new());
    }

    let body = resp.json::<serde_json::Value>().await?;

    let mut tags = Vec::new();
    if let Some(array) = body.as_array() {
        for item in array {
            if let Some(tag) = item.get("supplier_string").and_then(|v| v.as_str()) {
                let tag = tag.strip_prefix("external_").unwrap_or(tag);
                if !tag.starts_with("pokt1") {
                    tags.push(tag.to_string());
                }
            }
        }
    }

    tags.sort();

    tracing::info!("Fetched {} available models signatures from PNYX", tags.len());
    Ok(tags)
}

// ──────────────────────────────────────────────────────────────
// Fetch OpenAI-style /v1/models list from PNYX gateway
// ──────────────────────────────────────────────────────────────
async fn api_v1_models(State(state): State<WebState>) -> Result<Json<serde_json::Value>, StatusCode> {
    let body = fetch_v1_models_body(&state).await?;
    Ok(Json(body))
}

// Shared helper: fetch the OpenAI-style /v1/models body from the PNYX gateway,
// advertising healthy local suppliers the same way the relay does.
async fn fetch_v1_models_body(state: &WebState) -> Result<serde_json::Value, StatusCode> {
    let (client, gateway_url, access_token, local_suppliers) = {
        let guard = state.read().await;
        let healthy: Vec<String> = guard
            .model_health
            .iter()
            .filter(|(_, healthy)| **healthy)
            .map(|(name, _)| name.clone())
            .collect();
        (
            guard.client.clone(),
            guard.config.pnyx.gateway_relay_url(),
            guard.config.pnyx.access_token.clone(),
            healthy.join(","),
        )
    };

    let url = format!("{}/v1/models", gateway_url.trim_end_matches('/'));
    let mut req = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Pnyx-Sidecar-Request", "true");
    if !local_suppliers.is_empty() {
        req = req.header("Pnyx-Local-Suppliers", local_suppliers);
    }
    match req.timeout(Duration::from_secs(10)).send().await
    {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                tracing::warn!("Failed to fetch /v1/models: HTTP {} from {}", status, url);
                return Err(StatusCode::BAD_GATEWAY);
            }
            match resp.json::<serde_json::Value>().await {
                Ok(body) => Ok(body),
                Err(e) => {
                    tracing::warn!("Failed to parse /v1/models response: {}", e);
                    Err(StatusCode::BAD_GATEWAY)
                }
            }
        }
        Err(e) => {
            tracing::warn!("Failed to fetch /v1/models: {}", e);
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

// ──────────────────────────────────────────────────────────────
// API: /api/client_config?platform=opencode|hermes
// Renders a ready-to-paste config block for third-party clients.
// Secrets are never included: when iris.token is set, a placeholder is used.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Deserialize)]
struct ClientConfigQuery {
    platform: String,
}

async fn api_client_config(
    State(state): State<WebState>,
    Query(q): Query<ClientConfigQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let bad = |code: StatusCode, msg: String| {
        tracing::warn!("client_config request failed: {}", msg);
        (code, Json(json!({ "error": msg })))
    };

    let (iris_port, token_configured) = {
        let guard = state.read().await;
        (guard.config.iris.port, guard.config.iris.token.is_some())
    };

    let body = fetch_v1_models_body(&state)
        .await
        .map_err(|code| bad(code, "Failed to fetch model list from PNYX gateway".to_string()))?;

    // Live gateway list is the single source of truth: healthy local models are
    // included because the fetch advertises them via Pnyx-Local-Suppliers.
    let mut seen = std::collections::HashSet::new();
    let mut models: Vec<String> = Vec::new();
    if let Some(arr) = body.get("data").and_then(|d| d.as_array()) {
        for m in arr {
            if let Some(id) = m.get("id").and_then(|v| v.as_str()) {
                if seen.insert(id.to_string()) {
                    models.push(id.to_string());
                }
            }
        }
    }

    let base_url = format!("http://localhost:{}/v1", iris_port);
    let default_model = if models.iter().any(|m| m == "PNYX/pocket_network") {
        "PNYX/pocket_network".to_string()
    } else if let Some(first) = models.first() {
        first.clone()
    } else {
        "<model-id>".to_string()
    };

    let content = match q.platform.as_str() {
        "opencode" => render_opencode_config(&base_url, &models, &default_model, token_configured),
        "hermes" => render_hermes_config(&base_url, &models, &default_model, token_configured),
        other => return Err(bad(StatusCode::BAD_REQUEST, format!("Unknown platform '{}'", other))),
    };

    Ok(Json(json!({
        "platform": q.platform,
        "content": content,
        "model_count": models.len(),
    })))
}

fn render_opencode_config(base_url: &str, models: &[String], default_model: &str, token_configured: bool) -> String {
    let provider_id = "pnyx-iris";
    let mut models_json = serde_json::Map::new();
    for id in models {
        models_json.insert(id.clone(), json!({ "name": id }));
    }

    let mut options = serde_json::Map::new();
    options.insert("baseURL".to_string(), json!(base_url));
    if token_configured {
        options.insert("apiKey".to_string(), json!("<YOUR-IRIS-TOKEN>"));
    }

    let mut provider = serde_json::Map::new();
    provider.insert("npm".to_string(), json!("@ai-sdk/openai-compatible"));
    provider.insert("name".to_string(), json!("PNYX Iris Sidecar"));
    provider.insert("options".to_string(), serde_json::Value::Object(options));
    provider.insert("models".to_string(), serde_json::Value::Object(models_json));

    let value = json!({
        "$schema": "https://opencode.ai/config.json",
        "provider": {
            provider_id: serde_json::Value::Object(provider),
        },
        "model": format!("{}/{}", provider_id, default_model),
    });

    let mut out = String::new();
    out.push_str(&serde_json::to_string_pretty(&value).unwrap_or_default());
    out.push('\n');
    out
}

fn render_hermes_config(base_url: &str, models: &[String], default_model: &str, token_configured: bool) -> String {
    let mut out = String::new();
    out.push_str("# Paste into Hermes config.yaml\n");
    out.push_str("# Points to the local Iris sidecar (serves gateway + healthy local models)\n");
    out.push_str("custom_providers:\n");
    out.push_str("  - name: Iris (PNYX Sidecar)\n");
    out.push_str(&format!("    base_url: {}\n", base_url));
    if token_configured {
        out.push_str("    api_key: \"<YOUR-IRIS-TOKEN>\"  # Replace with iris.token from config.yaml\n");
    }
    out.push_str(&format!("    model: {}\n", default_model));
    out.push_str("    api_mode: chat_completions\n");
    out.push_str("    models:\n");
    if models.is_empty() {
        out.push_str("      {}  # No models returned by the gateway\n");
    } else {
        for id in models {
            out.push_str(&format!("      \"{}\": {{}}\n", id));
        }
    }
    out
}

// ──────────────────────────────────────────────────────────────
// Dashboard page (embedded HTML + vanilla JS)
// ──────────────────────────────────────────────────────────────
async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Iris Sidecar Dashboard</title>
<style>
  :root { --bg:#0f1117; --card:#161b22; --text:#c9d1d9; --accent:#58a6ff; --good:#3fb950; --bad:#f85149; --warn:#d29922; }
  body { font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 2rem; }
  h1, h2 { margin: 0 0 1rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
  .card { background: var(--card); border-radius: 8px; padding: 1rem; border: 1px solid #30363d; }
  .indicator { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 6px; }
  .good { background: var(--good); }
  .bad { background: var(--bad); }
  .warn { background: var(--warn); }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.5rem; border-bottom: 1px solid #30363d; }
  input, select, button { background: #21262d; color: var(--text); border: 1px solid #30363d; border-radius: 4px; padding: 0.4rem 0.6rem; font-size: 1rem; }
  button { cursor: pointer; background: var(--accent); border: none; color: #fff; }
  button:hover { opacity: 0.9; }
  button:disabled { background: #30363d; cursor: not-allowed; }
  .row { display: flex; gap: 1rem; align-items: center; margin-bottom: 0.6rem; flex-wrap: wrap; }
  .mono { font-family: ui-monospace, monospace; font-size: 0.85rem; }
  #msg { position: fixed; top: 1rem; right: 1rem; padding: 0.8rem 1.2rem; border-radius: 6px; background: var(--good); color: #fff; display: none; }
  .tooltip { position: relative; cursor: help; color: var(--accent); font-size: 0.85rem; }
  .tooltip:hover::after { content: attr(data-tip); position: absolute; bottom: 120%; left: 50%; transform: translateX(-50%); background: #21262d; color: var(--text); padding: 0.4rem 0.6rem; border-radius: 4px; white-space: nowrap; font-size: 0.8rem; border: 1px solid #30363d; z-index: 100; }
  .badge { display: inline-block; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.75rem; margin-left: 0.5rem; }
  .badge-bad { background: var(--bad); color: #fff; }
  .badge-readonly { background: var(--warn); color: #000; }
  .hide-readonly { display: none !important; }
</style>
</head>
<body>
<h1>Iris Sidecar Dashboard</h1>

<div id="msg"></div>

<div class="grid">
  <div class="card">
    <h2>Local Models Health</h2>
    <div id="models-health">Loading…</div>
  </div>
  <div class="card">
    <h2>PNYX Backend</h2>
    <div id="pnyx-status">Loading…</div>
  </div>
  <div class="card">
    <h2>Request Stats</h2>
    <div id="stats">Loading…</div>
  </div>
</div>

<div class="card">
  <h2>Configuration <span id="readonly-badge" class="badge badge-readonly" style="display:none">Read-only</span></h2>
  <div class="row" id="token-row">
    <label>PNYX Access Token:</label>
    <input type="password" id="token-input" size="40" placeholder="(hidden)" />
    <button onclick="saveToken()">Update Token</button>
  </div>
  <div class="row" id="token-help-row" style="margin-top: -0.3rem; font-size: 0.85rem;">
    <a href="https://app.pnyxai.com" target="_blank" style="color: var(--accent); text-decoration: none;">Don't have a token? Create an account</a>
  </div>
  <div class="row">
    <label>Fallback on gateway error:</label>
    <input type="checkbox" id="fallback-enabled" />
    <label>Fallback model:</label>
    <select id="fallback-model">
      <option value="random">random</option>
    </select>
    <button id="save-fallback-btn" onclick="saveFallback()">Save Fallback</button>
  </div>
</div>

<div class="card">
  <h2>Local Models</h2>
  <table id="models-table">
    <thead><tr><th>PNYX Reference Tag</th><th>PNYX Remote Configured</th><th>Endpoint</th><th>Model Name</th><th>Custom API Path</th><th>Custom Headers</th><th></th></tr></thead>
    <tbody></tbody>
  </table>
  <div id="add-model-section">
  <h3>Add Model</h3>
  <div class="row">
    <label>PNYX Tag:</label>
    <select id="new-tag"></select>
    <button onclick="refreshPnyxModels()">Refresh PNYX Models</button>
  </div>
  <div class="row">
    <label>Endpoint <span class="tooltip" data-tip="The local URL of your model server. It must accept OpenAI-compatible /v1 completions.">(?)</span></label>
    <input id="new-endpoint" placeholder="http://localhost:1234" />
  </div>
  <div class="row">
    <label>Model Name <span class="tooltip" data-tip="The model identifier your local server expects in the JSON payload (e.g., gpt-oss-20b-INT4).">(?)</span></label>
    <input id="new-model-name" placeholder="local-llama" />
  </div>
  <div class="row">
    <label>Custom API Path <span class="tooltip" data-tip="Only needed if your endpoint does not serve /v1. Provide the actual path it supports for OpenAI-style calls.">(?)</span></label>
    <input id="new-custom-path" placeholder="/v1/chat/completions (optional)" />
  </div>
  <div class="row">
    <label>Custom Headers <span class="tooltip" data-tip="Optional JSON object of headers sent on generation and health-check requests, e.g. an Authorization token.">(?)</span></label>
    <input id="new-custom-headers" size="50" placeholder='{"Authorization":"Bearer local-token"} (optional)' />
    <button id="add-btn" onclick="addModel()" disabled>Add</button>
  </div>
  </div>
</div>

<div class="card">
  <h2>External Models <a href="https://app.pnyxai.com/routing/groups" target="_blank" style="font-size:0.85rem; color:var(--accent); text-decoration:none; margin-left:0.5rem;">Configure models in PNYX →</a></h2>
  <table id="external-models-table">
    <thead><tr><th>PNYX Reference Tag</th><th>Provider</th></tr></thead>
    <tbody></tbody>
  </table>
</div>


<div class="card">
  <h2>Client Config</h2>
  <div class="row">
    <label for="client-platform">Platform:</label>
    <select id="client-platform" onchange="resetClientConfig()">
      <option value="opencode">OpenCode (opencode.json)</option>
      <option value="hermes">Hermes (config.yaml)</option>
    </select>
    <button id="generate-btn" onclick="loadClientConfig()">Generate</button>
    <button id="copy-btn" onclick="copyClientConfig()" disabled>Copy</button>
    <span id="copy-feedback" style="color: var(--good); display:none">Copied!</span>
  </div>
  <div class="row" style="font-size: 0.85rem; color: #8b949e; margin-top: -0.3rem;">
    Generated live from the gateway: only models healthy right now (see Local Models Health) are included. Paste into the tool's config file. If it contains <span class="mono">&lt;YOUR-IRIS-TOKEN&gt;</span>, replace it with the iris.token from config.yaml.
  </div>
  <pre id="client-config-out" class="mono" style="background:#0d1117; border:1px solid #30363d; border-radius:6px; padding:0.8rem; overflow:auto; max-height:400px; white-space:pre; margin:0;">Check your local models' health above, then click Generate.</pre>
</div>


<script>
  let availableModels = [];
  let v1Models = [];
  let currentConfig = null;
  let lastHealthSnapshot = null;

  function showMsg(txt, good=true) {
    const m = document.getElementById('msg');
    m.textContent = txt;
    m.style.background = good ? 'var(--good)' : 'var(--bad)';
    m.style.display = 'block';
    setTimeout(() => m.style.display = 'none', 3000);
  }

  function formatAgo(secs) {
    if (secs < 60) return secs + 's ago';
    if (secs < 3600) return Math.floor(secs/60) + 'm ago';
    return Math.floor(secs/3600) + 'h ago';
  }

  function normalizeModelId(id) {
    return typeof id === 'string' && id.startsWith('PNYX/') ? id.slice(5) : id;
  }

  function stripProviderPrefix(ownedBy) {
    if (typeof ownedBy !== 'string') return ownedBy || '';
    return ownedBy.startsWith('User Backend by ') ? ownedBy.slice(16) : ownedBy;
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  async function loadV1Models() {
    try {
      const r = await fetch('/api/v1_models');
      if (!r.ok) throw new Error('Failed to fetch /v1/models');
      const d = await r.json();
      v1Models = Array.isArray(d.data) ? d.data : [];
      renderModels();
    } catch (e) { console.error(e); }
  }

  function renderModels() {
    if (!currentConfig) return;
    const configured = currentConfig.models || {};
    const configuredIds = new Set(Object.keys(configured));

    const normalizedV1 = v1Models.map(m => ({
      id: m.id,
      normalizedId: normalizeModelId(m.id),
      owned_by: m.owned_by,
    }));

    const localBackendIds = new Set(
      normalizedV1
        .filter(m => m.owned_by === 'Local User Backend' && configuredIds.has(m.normalizedId))
        .map(m => m.normalizedId)
    );

    const externalModels = normalizedV1.filter(m => {
      if (m.id === 'PNYX/router' || m.id === 'PNYX/pocket_network') return false;
      return !configuredIds.has(m.normalizedId);
    });

    const tbody = document.querySelector('#models-table tbody');
    tbody.innerHTML = '';
    const availableSet = new Set(availableModels);

    for (const [tag, m] of Object.entries(configured)) {
      const isLocalBackend = localBackendIds.has(tag);
      const configuredMark = isLocalBackend ? '<span title="Listed as Local User Backend">✓</span>' : '';
      const valid = availableSet.has(tag);
      const badge = valid ? '' : '<span class="badge badge-bad">Not available in PNYX</span>';
      const removeCell = currentConfig && currentConfig.read_only
        ? ''
        : `<td><button onclick="removeModel('${escapeHtml(tag)}')">Remove</button></td>`;
      tbody.insertAdjacentHTML('beforeend', `<tr><td>${escapeHtml(tag)}${badge}</td><td style="text-align:center">${configuredMark}</td><td>${escapeHtml(m.endpoint)}</td><td>${escapeHtml(m.model_name)}</td><td>${escapeHtml(m.custom_api_path || '')}</td><td>${escapeHtml(Object.keys(m.custom_headers || {}).join(', '))}</td>${removeCell}</tr>`);
    }

    const externalTbody = document.querySelector('#external-models-table tbody');
    externalTbody.innerHTML = '';
    if (externalModels.length === 0) {
      externalTbody.insertAdjacentHTML('beforeend', '<tr><td colspan="2">No external models available.</td></tr>');
    } else {
      for (const m of externalModels) {
        externalTbody.insertAdjacentHTML('beforeend', `<tr><td>${escapeHtml(m.normalizedId)}</td><td>${escapeHtml(stripProviderPrefix(m.owned_by))}</td></tr>`);
      }
    }
  }

  async function loadHealth() {
    try {
      const r = await fetch('/api/health');
      const d = await r.json();
      let html = '';
      for (const [tag, ok] of Object.entries(d.models)) {
        html += `<span class="indicator ${ok ? 'good' : 'bad'}"></span>${tag}<br>`;
      }
      if (d.last_checked) {
        html += `<br><span class="mono">Last checked: ${d.last_checked}</span>`;
      }
      document.getElementById('models-health').innerHTML = html || 'No models configured.';
      const ps = document.getElementById('pnyx-status');
      ps.innerHTML = `<span class="indicator ${d.pnyx.status === '200 OK' ? 'good' : 'bad'}"></span>Status: ${d.pnyx.status}<br><span class="mono">Last checked: ${d.pnyx.last_checked}</span>`;

      const snapshot = JSON.stringify({ models: d.models, pnyx: d.pnyx.status });
      if (lastHealthSnapshot !== null && lastHealthSnapshot !== snapshot) {
        lastHealthSnapshot = snapshot;
        await loadV1Models();
      } else {
        lastHealthSnapshot = snapshot;
      }
    } catch (e) { console.error(e); }
  }

  async function loadStats() {
    try {
      const r = await fetch('/api/stats');
      const d = await r.json();
      const ign = (n) => n && n.ignored > 0 ? ' <span style="color:#e3b341">(' + n.ignored + ' ignored)</span>' : '';
      const u = d.local.usage || {};
      const t = d.local.timings || {};
      const tok = 'in ' + (u.prompt_tokens ? u.prompt_tokens.total.toLocaleString() + ign(u.prompt_tokens) : '0')
        + ' · out ' + (u.completion_tokens ? u.completion_tokens.total.toLocaleString() + ign(u.completion_tokens) : '0')
        + ' · cached ' + (u.cached_tokens ? u.cached_tokens.total.toLocaleString() + ign(u.cached_tokens) : '0');
      const tim = 'routing ' + (t.routing_ms ? t.routing_ms.avg + ' ms' + ign(t.routing_ms) : '—')
        + ' · TTFT ' + (t.ttft_ms ? t.ttft_ms.avg + ' ms' + ign(t.ttft_ms) : '—')
        + ' · generation ' + (t.generation_ms ? t.generation_ms.avg + ' ms' + ign(t.generation_ms) : '—');
      document.getElementById('stats').innerHTML = `
        <b>Local:</b> ${d.local.count} reqs, avg ${d.local.avg_ms} ms<br>
        <b>PNYX:</b> ${d.pnyx.count} reqs, avg ${d.pnyx.avg_ms} ms<br>
        <b>Tokens (since start):</b> ${tok}<br>
        <b>Timings (since start):</b> ${tim}
      `;
    } catch (e) { console.error(e); }
  }

  async function loadAvailableModels() {
    try {
      const r = await fetch('/api/available_models');
      const d = await r.json();
      availableModels = d.models || [];
      updateTagDropdown();
    } catch (e) { console.error(e); }
  }

  function updateTagDropdown() {
    const sel = document.getElementById('new-tag');
    sel.innerHTML = '';
    if (availableModels.length === 0) {
      sel.innerHTML = '<option value="">-- Click Refresh PNYX Models --</option>';
      document.getElementById('add-btn').disabled = true;
    } else {
      for (const tag of availableModels) {
        sel.insertAdjacentHTML('beforeend', `<option value="${tag}">${tag}</option>`);
      }
      document.getElementById('add-btn').disabled = false;
    }
  }

  async function refreshPnyxModels() {
    const btn = document.querySelector('button[onclick="refreshPnyxModels()"]');
    btn.disabled = true;
    btn.textContent = 'Loading...';
    try {
      const r = await fetch('/api/refresh_models', { method: 'POST' });
      if (r.ok) {
        showMsg('PNYX models refreshed');
        await loadAvailableModels();
        renderModels();
      } else {
        showMsg('Failed to refresh PNYX models', false);
      }
    } catch (e) {
      showMsg('Failed to refresh PNYX models', false);
    } finally {
      btn.disabled = false;
      btn.textContent = 'Refresh PNYX Models';
    }
  }

  function applyReadOnly(readOnly) {
    const badge = document.getElementById('readonly-badge');
    const tokenRow = document.getElementById('token-row');
    const tokenHelpRow = document.getElementById('token-help-row');
    const saveFallbackBtn = document.getElementById('save-fallback-btn');
    const fallbackEnabled = document.getElementById('fallback-enabled');
    const fallbackModel = document.getElementById('fallback-model');
    const addSection = document.getElementById('add-model-section');

    badge.style.display = readOnly ? 'inline-block' : 'none';
    tokenRow.classList.toggle('hide-readonly', readOnly);
    tokenHelpRow.classList.toggle('hide-readonly', readOnly);
    saveFallbackBtn.classList.toggle('hide-readonly', readOnly);
    addSection.classList.toggle('hide-readonly', readOnly);
    fallbackEnabled.disabled = readOnly;
    fallbackModel.disabled = readOnly;
  }

  async function loadConfig() {
    try {
      const [cfgR, availR] = await Promise.all([fetch('/api/config'), fetch('/api/available_models')]);
      const d = await cfgR.json();
      currentConfig = d;
      applyReadOnly(!!d.read_only);
      const availD = await availR.json();
      availableModels = availD.models || [];
      updateTagDropdown();

      const fb = d.iris && d.iris.fallback;
      document.getElementById('fallback-enabled').checked = !!(fb && fb.on_gateway_error);
      const sel = document.getElementById('fallback-model');
      sel.innerHTML = '<option value="random">Random</option>';
      for (const tag of Object.keys(d.models || {})) {
        sel.insertAdjacentHTML('beforeend', `<option value="${tag}">${tag}</option>`);
      }
      if (fb) sel.value = fb.fallback_model || 'random';

      renderModels();
      await loadV1Models();
    } catch (e) { console.error(e); }
  }

  async function saveToken() {
    const token = document.getElementById('token-input').value;
    if (!token) return showMsg('Enter a token', false);
    const r = await fetch('/api/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({pnyx_access_token: token}) });
    if (r.ok) {
      showMsg('Token updated');
      document.getElementById('token-input').value = '';
      await loadConfig();
    }
    else showMsg('Failed to update token', false);
  }

  async function saveFallback() {
    const enabled = document.getElementById('fallback-enabled').checked;
    const model = document.getElementById('fallback-model').value;
    const r = await fetch('/api/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({fallback: {on_gateway_error: enabled, fallback_model: model}}) });
    if (r.ok) {
      showMsg('Fallback saved');
      await loadConfig();
    }
    else showMsg('Failed to save fallback', false);
  }

  async function addModel() {
    const tag = document.getElementById('new-tag').value;
    const endpoint = document.getElementById('new-endpoint').value.trim();
    const model_name = document.getElementById('new-model-name').value.trim();
    const custom_api_path = document.getElementById('new-custom-path').value.trim() || null;
    const headersRaw = document.getElementById('new-custom-headers').value.trim();
    let custom_headers = {};
    if (headersRaw) {
      try {
        custom_headers = JSON.parse(headersRaw);
      } catch (e) {
        return showMsg('Custom headers must be valid JSON', false);
      }
      if (typeof custom_headers !== 'object' || Array.isArray(custom_headers) || custom_headers === null) {
        return showMsg('Custom headers must be a JSON object', false);
      }
    }
    if (!tag || !endpoint || !model_name) return showMsg('Fill all required fields', false);
    const r = await fetch('/api/models', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({action:'add', tag, endpoint, model_name, custom_api_path, custom_headers}) });
    if (r.ok) { showMsg('Model added'); await loadConfig(); }
    else showMsg('Failed to add model', false);
  }

  async function removeModel(tag) {
    if (!confirm('Remove model "' + tag + '"?')) return;
    const r = await fetch('/api/models', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({action:'remove', tag}) });
    if (r.ok) { showMsg('Model removed'); await loadConfig(); }
    else showMsg('Failed to remove model', false);
  }

  let clientConfigText = '';

  function resetClientConfig() {
    clientConfigText = '';
    document.getElementById('copy-btn').disabled = true;
    document.getElementById('client-config-out').textContent = 'Check your local models\' health above, then click Generate.';
  }

  async function loadClientConfig() {
    const out = document.getElementById('client-config-out');
    const btn = document.getElementById('generate-btn');
    const copyBtn = document.getElementById('copy-btn');
    const platform = document.getElementById('client-platform').value;
    btn.disabled = true;
    btn.textContent = 'Generating...';
    copyBtn.disabled = true;
    clientConfigText = '';
    out.textContent = 'Fetching live model list from the gateway...';
    try {
      const r = await fetch('/api/client_config?platform=' + encodeURIComponent(platform));
      const d = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
      clientConfigText = d.content || '';
      out.textContent = clientConfigText;
      copyBtn.disabled = !clientConfigText;
    } catch (e) {
      clientConfigText = '';
      out.textContent = 'Failed to generate config: ' + e.message;
    } finally {
      btn.disabled = false;
      btn.textContent = 'Generate';
    }
  }

  function copyClientConfig() {
    if (!clientConfigText) return showMsg('Nothing to copy yet', false);
    const done = () => {
      const fb = document.getElementById('copy-feedback');
      fb.style.display = 'inline';
      setTimeout(() => fb.style.display = 'none', 2000);
    };
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(clientConfigText).then(done).catch(() => legacyCopy(clientConfigText, done));
    } else {
      legacyCopy(clientConfigText, done);
    }
  }

  function legacyCopy(text, done) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy');
      done();
    } catch (e) {
      showMsg('Copy failed', false);
    }
    document.body.removeChild(ta);
  }

  loadHealth(); loadStats(); loadConfig();
  setInterval(loadHealth, 10000);
  setInterval(loadStats, 10000);
</script>
</body>
</html>"#;

// ──────────────────────────────────────────────────────────────
// API: /api/health
// ──────────────────────────────────────────────────────────────
async fn api_health(State(state): State<WebState>) -> Json<serde_json::Value> {
    let locked = state.read().await;
    let health = &locked.model_health;

    let pnyx_status = locked.metrics.last_pnyx_status.lock().unwrap().clone();
    let pnyx_time = locked.metrics.last_pnyx_check_time.lock().unwrap();
        let pnyx_last = pnyx_time.map(|t| {
        let secs = t.elapsed().as_secs();
        format_ago(secs)
    }).unwrap_or_else(|| "never".to_string());

    let models_last = locked.last_health_check.map(|t| {
        let secs = t.elapsed().as_secs();
        format_ago(secs)
    }).unwrap_or_else(|| "never".to_string());

    Json(json!({
        "models": health,
        "last_checked": models_last,
        "pnyx": {
            "status": pnyx_status,
            "last_checked": pnyx_last,
        }
    }))
}

fn format_ago(secs: u64) -> String {
    if secs < 60 {
        format!("{}s ago", secs)
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else {
        format!("{}h ago", secs / 3600)
    }
}

// ──────────────────────────────────────────────────────────────
// API: /api/stats
// ──────────────────────────────────────────────────────────────
fn stat_json(f: FieldStats) -> serde_json::Value {
    json!({
        "total": f.total,
        "avg": f.avg(),
        "ignored": f.ignored,
    })
}

async fn api_stats(State(state): State<WebState>) -> Json<serde_json::Value> {
    let locked = state.read().await;
    let (local_count, local_total) = locked.metrics.local_stats();
    let (pnyx_count, pnyx_total) = locked.metrics.pnyx_stats();

    let local_avg = if local_count > 0 { local_total / local_count } else { 0 };
    let pnyx_avg = if pnyx_count > 0 { pnyx_total / pnyx_count } else { 0 };

    let usage = json!({
        "prompt_tokens": stat_json(locked.metrics.prompt_tokens_stats()),
        "completion_tokens": stat_json(locked.metrics.completion_tokens_stats()),
        "cached_tokens": stat_json(locked.metrics.cached_tokens_stats()),
    });
    let timings = json!({
        "routing_ms": stat_json(locked.metrics.routing_stats()),
        "ttft_ms": stat_json(locked.metrics.ttft_stats()),
        "generation_ms": stat_json(locked.metrics.generation_stats()),
    });

    Json(json!({
        "local": {
            "count": local_count,
            "avg_ms": local_avg,
            "usage": usage,
            "timings": timings,
        },
        "pnyx": { "count": pnyx_count, "avg_ms": pnyx_avg },
    }))
}

// ──────────────────────────────────────────────────────────────
// API: /api/available_models
// ──────────────────────────────────────────────────────────────
async fn api_available_models(State(state): State<WebState>) -> Json<serde_json::Value> {
    let locked = state.read().await;
    Json(json!({
        "models": locked.available_models,
    }))
}

// ──────────────────────────────────────────────────────────────
// API: /api/refresh_models
// ──────────────────────────────────────────────────────────────
async fn api_refresh_models(State(state): State<WebState>) -> Result<StatusCode, StatusCode> {
    let (client, pnyx_config) = {
        let guard = state.read().await;
        (guard.client.clone(), guard.config.pnyx.clone())
    };

    match fetch_available_models(&client, &pnyx_config).await {
        Ok(models) => {
            let mut locked = state.write().await;
            locked.available_models = models;
            Ok(StatusCode::OK)
        }
        Err(e) => {
            tracing::warn!("Failed to refresh available models signatures: {}", e);
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

// ──────────────────────────────────────────────────────────────
// API: /api/config
// ──────────────────────────────────────────────────────────────
async fn api_config(State(state): State<WebState>) -> Json<serde_json::Value> {
    let locked = state.read().await;
    let mut cfg = serde_json::to_value(&locked.config).unwrap_or_default();

    // Expose read-only mode (field is `serde(skip)`ped, so add it manually)
    if let Some(obj) = cfg.as_object_mut() {
        obj.insert("read_only".to_string(), json!(locked.config.read_only));
    }

    // Mask tokens for the UI
    if let Some(pnyx) = cfg.get_mut("pnyx") {
        if let Some(token) = pnyx.get_mut("access_token") {
            if let Some(s) = token.as_str() {
                if s.len() > 8 {
                    *token = json!(format!("{}...{}", &s[..4], &s[s.len()-4..]));
                }
            }
        }
    }
    if let Some(iris) = cfg.get_mut("iris") {
        if let Some(token) = iris.get_mut("token") {
            if let Some(s) = token.as_str() {
                if s.len() > 8 {
                    *token = json!(format!("{}...{}", &s[..4], &s[s.len()-4..]));
                }
            }
        }
    }
    if let Some(models) = cfg.get_mut("models").and_then(|m| m.as_object_mut()) {
        for model in models.values_mut() {
            if let Some(headers) = model.get_mut("custom_headers").and_then(|h| h.as_object_mut()) {
                for value in headers.values_mut() {
                    if value.as_str().is_some() {
                        *value = json!("***");
                    }
                }
            }
        }
    }

    Json(cfg)
}

#[derive(Debug, Deserialize)]
struct ConfigUpdate {
    pnyx_access_token: Option<String>,
    fallback: Option<FallbackUpdate>,
    base_url: Option<String>,
    service_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FallbackUpdate {
    on_gateway_error: bool,
    fallback_model: String,
}

async fn api_config_update(
    State(state): State<WebState>,
    Json(payload): Json<ConfigUpdate>,
) -> Result<StatusCode, StatusCode> {
    let mut locked = state.write().await;
    if locked.config.read_only {
        tracing::warn!("Rejected config update: running in read-only mode");
        return Err(StatusCode::FORBIDDEN);
    }
    let mut service_changed = false;

    if let Some(token) = payload.pnyx_access_token {
        locked.config.pnyx.access_token = token;
    }

    if let Some(base_url) = payload.base_url {
        locked.config.pnyx.base_url = base_url;
        service_changed = true;
    }

    if let Some(service_name) = payload.service_name {
        locked.config.pnyx.service_name = service_name;
        service_changed = true;
    }

    if let Some(fb) = payload.fallback {
        locked.config.iris.fallback = Some(FallbackConfig {
            on_gateway_error: fb.on_gateway_error,
            fallback_model: fb.fallback_model,
        });
    }

    if let Err(e) = locked.config.save() {
        tracing::error!("Failed to save config: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // If base_url or service_name changed, re-fetch available models signatures
    if service_changed {
        let pnyx_config = locked.config.pnyx.clone();
        let client = locked.client.clone();
        drop(locked);

        match fetch_available_models(&client, &pnyx_config).await {
            Ok(models) => {
                let mut locked = state.write().await;
                locked.available_models = models;
            }
            Err(e) => {
                tracing::warn!("Failed to re-fetch available models signatures after config change: {}", e);
            }
        }
    }

    Ok(StatusCode::OK)
}

// ──────────────────────────────────────────────────────────────
// API: /api/models
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Deserialize)]
struct ModelUpdate {
    action: String,
    tag: String,
    endpoint: Option<String>,
    model_name: Option<String>,
    custom_api_path: Option<String>,
    custom_headers: Option<HashMap<String, String>>,
}

async fn api_models_update(
    State(state): State<WebState>,
    Json(payload): Json<ModelUpdate>,
) -> Result<StatusCode, StatusCode> {
    let mut locked = state.write().await;
    if locked.config.read_only {
        tracing::warn!("Rejected models update: running in read-only mode");
        return Err(StatusCode::FORBIDDEN);
    }

    match payload.action.as_str() {
        "add" => {
            let endpoint = payload.endpoint.ok_or(StatusCode::BAD_REQUEST)?;
            let model_name = payload.model_name.ok_or(StatusCode::BAD_REQUEST)?;
            let custom_api_path = payload.custom_api_path;
            let custom_headers = payload.custom_headers.unwrap_or_default();

            let tag = payload.tag;
            locked.config.models.insert(
                tag.clone(),
                crate::config::ModelConfig {
                    endpoint: endpoint.clone(),
                    model_name: model_name.clone(),
                    custom_api_path: custom_api_path.clone(),
                    custom_headers: custom_headers.clone(),
                },
            );
            locked.models.insert(
                tag.clone(),
                LocalModel {
                    model_tag: tag.clone(),
                    endpoint,
                    model_name,
                    custom_api_path,
                    custom_headers,
                },
            );
            locked.model_health.insert(tag, false); // unknown until next check
        }
        "remove" => {
            locked.config.models.remove(&payload.tag);
            locked.models.remove(&payload.tag);
            locked.model_health.remove(&payload.tag);
        }
        _ => return Err(StatusCode::BAD_REQUEST),
    }

    if let Err(e) = locked.config.save() {
        tracing::error!("Failed to save config: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    Ok(StatusCode::OK)
}

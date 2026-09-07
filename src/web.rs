// ─── web.rs ────────────────────────────────────────────────
// Built-in admin dashboard and live config editor.
// Serves a single-page vanilla-JS UI on a dedicated port.
// ──────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::config::{Config, FallbackConfig, PnyxConfig};
use crate::models::{LocalModel, Metrics};

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
    pub metrics: Metrics,
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
            let healthy = match client.get(&url).timeout(Duration::from_secs(5)).send().await {
                Ok(resp) => resp.status().is_success(),
                Err(_) => false,
            };
            new_health.insert(name.clone(), healthy);
        }
        locked.model_health = new_health;
        locked.last_health_check = Some(Instant::now());

        // Check PNYX gateway (lightweight HEAD against the base origin)
        let pnyx_base = locked.config.pnyx.base_url.clone();
        let pnyx_status = match client.head(&pnyx_base).timeout(Duration::from_secs(5)).send().await {
            Ok(resp) => format!("{}", resp.status()),
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
        "{}/insights/models?service={}",
        pnyx_config.base_url.trim_end_matches('/'),
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
            "Failed to fetch available models: HTTP {} from {}",
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

    tracing::info!("Fetched {} available models from PNYX", tags.len());
    Ok(tags)
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
  <h2>Configuration</h2>
  <div class="row">
    <label>PNYX Access Token:</label>
    <input type="password" id="token-input" size="40" placeholder="(hidden)" />
    <button onclick="saveToken()">Update Token</button>
  </div>
  <div class="row" style="margin-top: -0.3rem; font-size: 0.85rem;">
    <a href="https://app.pnyxai.com" target="_blank" style="color: var(--accent); text-decoration: none;">Don't have a token? Create an account</a>
  </div>
  <div class="row">
    <label>Fallback on gateway error:</label>
    <input type="checkbox" id="fallback-enabled" />
    <label>Fallback model:</label>
    <select id="fallback-model">
      <option value="random">random</option>
    </select>
    <button onclick="saveFallback()">Save Fallback</button>
  </div>
</div>

<div class="card">
  <h2>Models</h2>
  <table id="models-table">
    <thead><tr><th>PNYX Reference Tag</th><th>Endpoint</th><th>Model Name</th><th>Custom API Path</th><th></th></tr></thead>
    <tbody></tbody>
  </table>
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
    <button id="add-btn" onclick="addModel()" disabled>Add</button>
  </div>
</div>

<script>
  let availableModels = [];

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
      ps.innerHTML = `<span class="indicator ${d.pnyx.status.startsWith('Error') ? 'bad' : 'good'}"></span>Status: ${d.pnyx.status}<br><span class="mono">Last checked: ${d.pnyx.last_checked}</span>`;
    } catch (e) { console.error(e); }
  }

  async function loadStats() {
    try {
      const r = await fetch('/api/stats');
      const d = await r.json();
      document.getElementById('stats').innerHTML = `
        <b>Local:</b> ${d.local.count} reqs, avg ${d.local.avg_ms}ms<br>
        <b>PNYX:</b> ${d.pnyx.count} reqs, avg ${d.pnyx.avg_ms}ms
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

  async function loadConfig() {
    try {
      const [cfgR, availR] = await Promise.all([fetch('/api/config'), fetch('/api/available_models')]);
      const d = await cfgR.json();
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

      const tbody = document.querySelector('#models-table tbody');
      tbody.innerHTML = '';
      const availableSet = new Set(availableModels);
      for (const [tag, m] of Object.entries(d.models || {})) {
        const valid = availableSet.has(tag);
        const badge = valid ? '' : '<span class="badge badge-bad">Not available in PNYX</span>';
        tbody.insertAdjacentHTML('beforeend', `<tr><td>${tag}${badge}</td><td>${m.endpoint}</td><td>${m.model_name}</td><td>${m.custom_api_path || ''}</td><td><button onclick="removeModel('${tag}')">Remove</button></td></tr>`);
      }
    } catch (e) { console.error(e); }
  }

  async function saveToken() {
    const token = document.getElementById('token-input').value;
    if (!token) return showMsg('Enter a token', false);
    const r = await fetch('/api/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({pnyx_access_token: token}) });
    if (r.ok) { showMsg('Token updated'); document.getElementById('token-input').value = ''; }
    else showMsg('Failed to update token', false);
  }

  async function saveFallback() {
    const enabled = document.getElementById('fallback-enabled').checked;
    const model = document.getElementById('fallback-model').value;
    const r = await fetch('/api/config', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({fallback: {on_gateway_error: enabled, fallback_model: model}}) });
    if (r.ok) showMsg('Fallback saved');
    else showMsg('Failed to save fallback', false);
  }

  async function addModel() {
    const tag = document.getElementById('new-tag').value;
    const endpoint = document.getElementById('new-endpoint').value.trim();
    const model_name = document.getElementById('new-model-name').value.trim();
    const custom_api_path = document.getElementById('new-custom-path').value.trim() || null;
    if (!tag || !endpoint || !model_name) return showMsg('Fill all required fields', false);
    const r = await fetch('/api/models', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({action:'add', tag, endpoint, model_name, custom_api_path}) });
    if (r.ok) { showMsg('Model added'); loadConfig(); }
    else showMsg('Failed to add model', false);
  }

  async function removeModel(tag) {
    if (!confirm('Remove model "' + tag + '"?')) return;
    const r = await fetch('/api/models', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({action:'remove', tag}) });
    if (r.ok) { showMsg('Model removed'); loadConfig(); }
    else showMsg('Failed to remove model', false);
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
async fn api_stats(State(state): State<WebState>) -> Json<serde_json::Value> {
    let locked = state.read().await;
    let (local_count, local_total) = locked.metrics.local_stats();
    let (pnyx_count, pnyx_total) = locked.metrics.pnyx_stats();

    let local_avg = if local_count > 0 { local_total / local_count } else { 0 };
    let pnyx_avg = if pnyx_count > 0 { pnyx_total / pnyx_count } else { 0 };

    Json(json!({
        "local": { "count": local_count, "avg_ms": local_avg },
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
            tracing::warn!("Failed to refresh available models: {}", e);
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

    // If base_url or service_name changed, re-fetch available models
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
                tracing::warn!("Failed to re-fetch available models after config change: {}", e);
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
}

async fn api_models_update(
    State(state): State<WebState>,
    Json(payload): Json<ModelUpdate>,
) -> Result<StatusCode, StatusCode> {
    let mut locked = state.write().await;

    match payload.action.as_str() {
        "add" => {
            let endpoint = payload.endpoint.ok_or(StatusCode::BAD_REQUEST)?;
            let model_name = payload.model_name.ok_or(StatusCode::BAD_REQUEST)?;
            let custom_api_path = payload.custom_api_path;

            let tag = payload.tag;
            locked.config.models.insert(
                tag.clone(),
                crate::config::ModelConfig {
                    endpoint: endpoint.clone(),
                    model_name: model_name.clone(),
                    custom_api_path: custom_api_path.clone(),
                },
            );
            locked.models.insert(
                tag.clone(),
                LocalModel {
                    model_tag: tag.clone(),
                    endpoint,
                    model_name,
                    custom_api_path,
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

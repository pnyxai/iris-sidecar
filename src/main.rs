// ─── main.rs ────────────────────────────────────────────────
// This is the entry point of the application.
//
// What happens here, in order:
//   1. Set up logging (so we can see info / warnings / errors in the console).
//   2. Load the configuration (YAML file + optional environment overrides).
//   3. Build the list of local models from the YAML config.
//   4. Build the shared application state (HTTP client, config, model map, metrics).
//   5. Spawn a background health-checker task.
//   6. Create an Axum web server for relay (`/*path`).
//   7. Create an Axum web server for the admin dashboard (if enabled).
//   8. Bind to TCP ports and serve both servers concurrently.
// ──────────────────────────────────────────────────────────────

// Modules
mod config; // Configuration parsing (YAML, env vars)
mod models; // Plain data structs (LocalModel, RelayData, Metrics)
mod relay; // The core request handler and forwarding logic
mod web;   // Admin dashboard + live config editor

use crate::config::Config;
use crate::models::{LocalModel, Metrics};
use crate::web::SharedState;

// Standard library imports
use std::collections::HashMap;
use std::sync::Arc; // Arc = "Atomic Reference Counted" pointer

// External crate imports
use anyhow::Result;
use axum::routing::any;
use axum::Router;

// Loggin
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// ──────────────────────────────────────────────────────────────
// main
// ──────────────────────────────────────────────────────────────
// We set up the Tokio runtime manually so `main` can remain a plain
// synchronous function. The async body is executed via `block_on`.
// ──────────────────────────────────────────────────────────────
fn main() -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        // 1. Initialize logging/tracing
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

        // 2. Load configuration from YAML + environment overrides
        let config = Config::load()?;
        tracing::info!("Loaded config");

        // 3. Build the list of local models directly from the YAML config.
        let models = build_models_from_config(&config);
        tracing::info!("Loaded {} local model(s) from config", models.len());

        // Convert the Vec<LocalModel> into a HashMap so we can look up models by name instantly.
        let models_map: HashMap<String, LocalModel> = models
            .into_iter()
            .map(|m| (m.model_tag.clone(), m))
            .collect();

        // 3b. Validate fallback configuration if a specific model tag is requested
        if let Some(ref fallback) = config.iris.fallback {
            if fallback.fallback_model != "random" {
                if !models_map.contains_key(&fallback.fallback_model) {
                    anyhow::bail!(
                        "Configured fallback_model '{}' is not listed in the models section",
                        fallback.fallback_model
                    );
                }
            }
            tracing::info!("Fallback configured for model: {}", fallback.fallback_model);
        }

        // 4. Build the shared mutable state that both servers will use
        let client = reqwest::Client::new();
        let shared = Arc::new(tokio::sync::RwLock::new(SharedState {
            client: client.clone(),
            config: config.clone(),
            models: models_map.clone(),
            model_health: models_map.keys().map(|k| (k.clone(), false)).collect(),
            last_health_check: None,
            metrics: Metrics::default(),
            available_models: Vec::new(),
        }));

        // 5. Spawn background health checker (updates model_health every 10s)
        tokio::spawn(web::health_checker_task(client.clone(), shared.clone()));

        // 5b. One-shot fetch of available models from PNYX on startup
        let startup_fetch = {
            let state = shared.clone();
            let client = client.clone();
            let pnyx_config = config.pnyx.clone();
            async move {
                match web::fetch_available_models(&client, &pnyx_config).await {
                    Ok(models) => {
                        tracing::info!("Loaded {} available models from PNYX on startup", models.len());
                        let mut locked = state.write().await;
                        locked.available_models = models;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to fetch available models on startup: {}", e);
                    }
                }
            }
        };
        tokio::spawn(startup_fetch);

        // 6. Set up the relay router
        let port = config.iris.port;
        let relay_app = Router::new()
            .route("/*path", any(relay::relay))
            .with_state(shared.clone());

        // 7. Set up the web UI router (if enabled)
        let web_ui_cfg = config.iris.web_ui.as_ref().cloned();
        let web_ui_enabled = web_ui_cfg.as_ref().map(|c| c.enabled).unwrap_or(true);
        let web_ui_port = web_ui_cfg.as_ref().map(|c| c.port).unwrap_or(8081);
        let web_ui_bind = web_ui_cfg.as_ref().map(|c| c.bind_address.clone()).unwrap_or_else(|| "0.0.0.0".to_string());

        let maybe_web_app = if web_ui_enabled {
            Some(web::build_router(shared.clone()))
        } else {
            None
        };

        // 8. Bind and serve concurrently
        let relay_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        tracing::info!("Iris relay listening on {}", relay_listener.local_addr()?);

        let relay_server = axum::serve(relay_listener, relay_app);

        if let Some(web_app) = maybe_web_app {
            let web_listener = tokio::net::TcpListener::bind(format!("{}:{}", web_ui_bind, web_ui_port)).await?;
            tracing::info!("Iris web UI listening on {}", web_listener.local_addr()?);
            let web_server = axum::serve(web_listener, web_app);
            let (relay_res, web_res) = tokio::join!(relay_server, web_server);
            relay_res?;
            web_res?;
        } else {
            relay_server.await?;
        }

        Ok(())
    })
}

// ──────────────────────────────────────────────────────────────
// build_models_from_config
// ──────────────────────────────────────────────────────────────
// This function reads the `models:` section from the YAML config and converts
// each entry into a `LocalModel` struct.
// ──────────────────────────────────────────────────────────────
fn build_models_from_config(config: &Config) -> Vec<LocalModel> {
    let mut models = Vec::new();

    for (name, cfg) in &config.models {
        models.push(LocalModel {
            model_tag: name.clone(),
            endpoint: cfg.endpoint.clone(),
            model_name: cfg.model_name.clone(),
            custom_api_path: cfg.custom_api_path.clone(),
        });
    }

    models
}

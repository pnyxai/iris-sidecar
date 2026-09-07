// ─── main.rs ────────────────────────────────────────────────
// This is the entry point of the application.
//
// What happens here, in order:
//   1. Set up logging (so we can see info / warnings / errors in the console).
//   2. Load the configuration (YAML file + optional environment overrides).
//   3. Build the list of local models from the YAML config.
//   4. Build the shared application state (HTTP client, config, model map).
//   5. Create an Axum web server with a single catch-all route (`/*path`).
//   6. Bind to a TCP port and start serving requests forever.
// ──────────────────────────────────────────────────────────────

// Modules
mod config; // Configuration parsing (YAML, env vars)
mod models; // Plain data structs (LocalModel, RelayData)
mod relay; // The core request handler and forwarding logic

use crate::config::Config;
use crate::models::LocalModel;
use crate::relay::AppState;

// Standard library imports
use std::collections::HashMap;
use std::sync::Arc; // Arc = "Atomic Reference Counted" pointer; allows safe sharing
                    // of data across many concurrent requests without copying it.

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
        //
        // `EnvFilter` lets us control log levels via the `RUST_LOG` environment variable.
        // If the user did not set `RUST_LOG`, we default to "info" level.
        // `fmt::layer()` prints the logs in a human-readable format to stdout.
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

        // 2. Load configuration from YAML + environment overrides
        let config = Config::load()?; // Load or error
        tracing::info!("Loaded config");

        // 3. Build the list of local models directly from the YAML config.
        let models = build_models_from_config(&config);
        tracing::info!("Loaded {} local model(s) from config", models.len());

        // Convert the Vec<LocalModel> into a HashMap so we can look up models by name instantly.
        // We wrap it in `Arc` so every request handler can read the same map without copying it.
        let models_map: Arc<HashMap<String, LocalModel>> = Arc::new(
            models
                .into_iter()
                .map(|m| (m.model_tag.clone(), m))
                .collect(),
        );

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

        // 4. Build the shared application state that Axum will hand to every request.
        let state = AppState {
            client: reqwest::Client::new(), // Reusable HTTP client (handles connection pooling)
            config,                         // The loaded configuration
            models: models_map,             // The map of local models
        };

        // 5. Set up the Axum router
        //
        // `Router::new()` creates an empty router.
        // `.route("/*path", any(relay::relay))` says:
        //   "For every URL path and every HTTP method, call the `relay` function in `relay.rs`."
        // `.with_state(state)` attaches our shared state so `relay` can access config and models.
        let port = state.config.iris.port; // Read the port before we move `state` into the router
        let app = Router::new()
            .route("/*path", any(relay::relay))
            .with_state(state);

        // 6. Bind to a TCP socket and start serving
        //
        // The port comes from the YAML or the `IRIS_PORT` environment variable.
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        tracing::info!("Iris sidecar listening on {}", listener.local_addr()?);

        // `axum::serve` runs the server loop forever (or until the process is killed).
        axum::serve(listener, app).await?;

        Ok(())
    })
}

// ──────────────────────────────────────────────────────────────
// build_models_from_config
// ──────────────────────────────────────────────────────────────
// This function reads the `models:` section from the YAML config and converts
// each entry into a `LocalModel` struct.
//
// Arguments:
//   • `config` – reference to the loaded `Config` struct
//
// Returns:
//   • `Vec<LocalModel>` – a list of models ready to use
//   • `Err(...)` – startup failed because of a missing field (handled by `?` in caller)
// ──────────────────────────────────────────────────────────────
fn build_models_from_config(config: &Config) -> Vec<LocalModel> {
    let mut models = Vec::new();

    // Iterate over every entry in the `models:` section of the YAML.
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

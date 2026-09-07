// ─── config.rs ───────────────────────────────────────────────
// This module is responsible for reading the user's YAML configuration file
// and turning it into typed Rust structs so the rest of the program can use
// the settings safely.
//
// We also allow certain sensitive values (tokens, port) to be overridden by
// environment variables at runtime, which is useful for Docker deployments.
// ──────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

// ──────────────────────────────────────────────────────────────
// Top-level configuration struct
// ──────────────────────────────────────────────────────────────
// This matches the *root* of the YAML file. Every field here is a section
// that appears at the top level in `config.yaml`.
//
// Example:
//   pnyx:
//     ...
//   iris:
//     ...
//   models:
//     ...
//
// `#[derive(Debug, Clone)]` tells Rust to automatically generate:
//   • Debug  – pretty-printing for log messages
//   • Clone  – ability to duplicate the struct in memory
//
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub pnyx: PnyxConfig, // Gateway connection settings
    pub iris: IrisConfig,
    pub models: HashMap<String, ModelConfig>,
    #[serde(skip)]
    pub config_path: PathBuf, // Not serialized — used for write-back
}

// ──────────────────────────────────────────────────────────────
// PNYX gateway connection settings
// ──────────────────────────────────────────────────────────────
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnyxConfig {
    pub base_url: String,     // e.g. "https://gateway.pnyxai.com"
    pub service_name: String, // e.g. "text-generation"
    pub access_token: String, // Token used to authenticate with the gateway
}

impl PnyxConfig {
    /// Build the full gateway URL: {base_url}/relay/{service_name}
    pub fn gateway_url(&self) -> String {
        format!("{}/relay/{}", self.base_url.trim_end_matches('/'), self.service_name)
    }
}

// ──────────────────────────────────────────────────────────────
// Iris-sidecar-specific settings
// ──────────────────────────────────────────────────────────────
// If the whole `iris:` block is missing, we create an `IrisConfig::default()`.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrisConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>, // Optional bearer token clients must send to Iris
    pub port: u16,             // TCP port to listen on (default 8080)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback: Option<FallbackConfig>, // Optional fallback to local model on gateway errors
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_ui: Option<WebUiConfig>, // Optional web UI dashboard configuration
}

impl Default for IrisConfig {
    fn default() -> Self {
        IrisConfig {
            token: None,
            port: default_port(),
            fallback: None,
            web_ui: None,
        }
    }
}

// ──────────────────────────────────────────────────────────────
// Fallback configuration
// ──────────────────────────────────────────────────────────────
// Controls whether Iris should forward requests to a local model when the
// gateway fails or returns a non-success status code.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FallbackConfig {
    pub on_gateway_error: bool, // When true, fallback on any gateway error or non-success status
    pub fallback_model: String, // "random" or a specific model tag
}

// ──────────────────────────────────────────────────────────────
// Web UI configuration
// ──────────────────────────────────────────────────────────────
// Controls the built-in admin dashboard server.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebUiConfig {
    pub enabled: bool,
    pub port: u16,
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
}

// ──────────────────────────────────────────────────────────────
// Per-model configuration (mandatory fields)
// ──────────────────────────────────────────────────────────────
// Each entry under the `models:` map in YAML becomes one of these structs.
//
// Fields:
//   • endpoint          – Base URL of the local model (e.g. http://localhost:1234)
//   • model_name        – Model identifier to send in the JSON payload
//   • custom_api_path   – Optional: overrides the incoming request path when
//                         Iris calls this local model.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub endpoint: String,                // Base URL of the local model
    pub model_name: String,              // Model identifier to use in the JSON payload
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_api_path: Option<String>, // Optional path override for local model calls
}

// ──────────────────────────────────────────────────────────────
// Default-value helpers
// ──────────────────────────────────────────────────────────────
// These plain functions are referenced by `#[serde(default = "...")]`.
// They must return the exact type of the field they serve.
// ──────────────────────────────────────────────────────────────
fn default_base_url() -> String {
    "https://gateway.pnyxai.com".to_string()
}

fn default_service_name() -> String {
    "text-generation".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}

// ──────────────────────────────────────────────────────────────
// Manual parsing from serde_yaml::Value
// ──────────────────────────────────────────────────────────────
impl Config {
    fn from_value(value: Value, config_path: PathBuf) -> Result<Self> {
        let pnyx = PnyxConfig::from_value(value.get("pnyx"))
            .with_context(|| "Missing or invalid required section: pnyx")?;
        let iris =
            IrisConfig::from_value(value.get("iris")).with_context(|| "Invalid section: iris")?;
        let models = match value.get("models") {
            Some(v) => models_from_value(v)?,
            None => HashMap::new(),
        };
        Ok(Config { pnyx, iris, models, config_path })
    }
}

impl PnyxConfig {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        let value = value.ok_or_else(|| anyhow::anyhow!("Missing required section: pnyx"))?;
        let base_url = value
            .get("base_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_base_url());
        let service_name = value
            .get("service_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_service_name());
        let access_token = value
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing required field: pnyx.access_token"))?;
        Ok(PnyxConfig {
            base_url,
            service_name,
            access_token,
        })
    }
}

impl IrisConfig {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        match value {
            Some(v) => {
                let token = v
                    .get("token")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let port = v
                    .get("port")
                    .and_then(|v| v.as_u64())
                    .map(|p| p as u16)
                    .unwrap_or_else(|| default_port());
                let fallback = v
                    .get("fallback")
                    .map(|f| FallbackConfig::from_value(f))
                    .transpose()
                    .with_context(|| "Invalid section: iris.fallback")?;
                let web_ui = v
                    .get("web_ui")
                    .and_then(|v| v.as_mapping())
                    .map(|_| WebUiConfig::from_value(v.get("web_ui")))
                    .transpose()
                    .with_context(|| "Invalid section: iris.web_ui")?;
                Ok(IrisConfig {
                    token,
                    port,
                    fallback,
                    web_ui,
                })
            }
            None => Ok(IrisConfig::default()),
        }
    }
}

impl FallbackConfig {
    fn from_value(value: &Value) -> Result<Self> {
        let on_gateway_error = value
            .get("on_gateway_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let fallback_model = value
            .get("fallback_model")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing required field: fallback.fallback_model"))?;
        Ok(FallbackConfig {
            on_gateway_error,
            fallback_model,
        })
    }
}

impl WebUiConfig {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        let value = value.ok_or_else(|| anyhow::anyhow!("Missing web_ui config value"))?;
        let enabled = value
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let port = value
            .get("port")
            .and_then(|v| v.as_u64())
            .map(|p| p as u16)
            .unwrap_or(8081);
        let bind_address = value
            .get("bind_address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_bind_address());
        Ok(WebUiConfig {
            enabled,
            port,
            bind_address,
        })
    }
}

impl ModelConfig {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        let value = value.ok_or_else(|| anyhow::anyhow!("Missing model config value"))?;
        let endpoint = value
            .get("endpoint")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing required field: endpoint"))?;
        let model_name = value
            .get("model_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing required field: model_name"))?;
        let custom_api_path = value
            .get("custom_api_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(ModelConfig {
            endpoint,
            model_name,
            custom_api_path,
        })
    }
}

fn models_from_value(value: &Value) -> Result<HashMap<String, ModelConfig>> {
    let mapping = value
        .as_mapping()
        .ok_or_else(|| anyhow::anyhow!("models section must be a map"))?;
    let mut models = HashMap::new();
    for (k, v) in mapping {
        let key = k
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Model name must be a string"))?
            .to_string();
        let cfg = ModelConfig::from_value(Some(v))
            .with_context(|| format!("Invalid config for model '{}'", key))?;
        models.insert(key, cfg);
    }
    Ok(models)
}

// ──────────────────────────────────────────────────────────────
// Loading logic
// ──────────────────────────────────────────────────────────────
impl Config {
    /// The main entry point for loading configuration.
    /// 1. Resolve the YAML file path (env var or hard-coded fallback).
    /// 2. Read the file into a string.
    /// 3. Parse the YAML into `Config`.
    /// 4. Apply environment-variable overrides (PNYX_TOKEN, PNYX_IRIS_TOKEN, IRIS_PORT).
    /// 5. Return the finished config.
    pub fn load() -> Result<Self> {
        // Step 1 – figure out which file to read
        let path = Self::resolve_path();

        // Step 2 – read the raw text from disk
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {:?}", path))?;

        // Step 3 – parse YAML into a generic Value, then convert to Config
        let value: Value = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {:?}", path))?;
        let mut config = Config::from_value(value, path.clone())
            .with_context(|| format!("Failed to convert config file: {:?}", path))?;

        // Step 4 – environment overrides (highest priority)
        if let Ok(token) = env::var("PNYX_TOKEN") {
            config.pnyx.access_token = token;
        }
        if let Ok(token) = env::var("PNYX_IRIS_TOKEN") {
            config.iris.token = Some(token);
        }
        if let Ok(port_str) = env::var("IRIS_PORT") {
            if let Ok(port) = port_str.parse::<u16>() {
                config.iris.port = port;
            }
        }

        Ok(config)
    }

    /// Save the current configuration back to disk.
    /// Uses serde_yaml to produce a clean YAML file.
    pub fn save(&self) -> Result<()> {
        let yaml = serde_yaml::to_string(self)
            .with_context(|| "Failed to serialize config to YAML")?;
        std::fs::write(&self.config_path, yaml)
            .with_context(|| format!("Failed to write config file: {:?}", self.config_path))?;
        tracing::info!("Saved config to {:?}", self.config_path);
        Ok(())
    }

    /// Decide where the YAML file lives.
    /// Priority:
    ///   1. `IRIS_CONFIG_PATH` environment variable
    ///   2. `$HOME/.config/iris-sidecar/config.yaml`
    ///   3. `config.yaml` in the current working directory (fallback of last resort)
    fn resolve_path() -> PathBuf {
        env::var("IRIS_CONFIG_PATH")
            .map(PathBuf::from) // env var is set → use it directly
            .unwrap_or_else(|_| {
                // env var is missing → compute a fallback
                if let Ok(home) = env::var("HOME") {
                    PathBuf::from(home)
                        .join(".config")
                        .join("iris-sidecar")
                        .join("config.yaml")
                } else {
                    PathBuf::from("config.yaml")
                }
            })
    }
}

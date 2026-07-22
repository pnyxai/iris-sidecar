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
#[derive(Debug, Clone)]
pub struct Config {
    pub pnyx: PnyxConfig, // Gateway connection settings
    pub iris: IrisConfig,
    pub models: HashMap<String, ModelConfig>,
}

// ──────────────────────────────────────────────────────────────
// PNYX gateway connection settings
// ──────────────────────────────────────────────────────────────
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct PnyxConfig {
    pub gateway_url: String,
    pub access_token: String, // Token used to authenticate with the gateway
}

// ──────────────────────────────────────────────────────────────
// Iris-sidecar-specific settings
// ──────────────────────────────────────────────────────────────
// If the whole `iris:` block is missing, we create an `IrisConfig::default()`.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Default)]
pub struct IrisConfig {
    pub token: Option<String>, // Optional bearer token clients must send to Iris
    pub port: u16,             // TCP port to listen on (default 8080)
    pub fallback: Option<FallbackConfig>, // Optional fallback to local model on gateway errors
}

// ──────────────────────────────────────────────────────────────
// Fallback configuration
// ──────────────────────────────────────────────────────────────
// Controls whether Iris should forward requests to a local model when the
// gateway fails or returns a non-success status code.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct FallbackConfig {
    pub on_gateway_error: bool, // When true, fallback on any gateway error or non-success status
    pub fallback_model: String, // "random" or a specific model tag
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
#[derive(Debug, Clone)]
pub struct ModelConfig {
    pub endpoint: String,                // Base URL of the local model
    pub model_name: String,              // Model identifier to use in the JSON payload
    pub custom_api_path: Option<String>, // Optional path override for local model calls
}

// ──────────────────────────────────────────────────────────────
// Default-value helpers
// ──────────────────────────────────────────────────────────────
// These plain functions are referenced by `#[serde(default = "...")]`.
// They must return the exact type of the field they serve.
// ──────────────────────────────────────────────────────────────
fn default_gateway_url() -> String {
    "https://gateway.pnyxai.com/relay/text-generation".to_string()
}

fn default_port() -> u16 {
    8080
}

// ──────────────────────────────────────────────────────────────
// Manual parsing from serde_yaml::Value
// ──────────────────────────────────────────────────────────────
impl Config {
    fn from_value(value: Value) -> Result<Self> {
        let pnyx = PnyxConfig::from_value(value.get("pnyx"))
            .with_context(|| "Missing or invalid required section: pnyx")?;
        let iris =
            IrisConfig::from_value(value.get("iris")).with_context(|| "Invalid section: iris")?;
        let models = match value.get("models") {
            Some(v) => models_from_value(v)?,
            None => HashMap::new(),
        };
        Ok(Config { pnyx, iris, models })
    }
}

impl PnyxConfig {
    fn from_value(value: Option<&Value>) -> Result<Self> {
        let value = value.ok_or_else(|| anyhow::anyhow!("Missing required section: pnyx"))?;
        let gateway_url = value
            .get("gateway_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_gateway_url());
        let access_token = value
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing required field: pnyx.access_token"))?;
        Ok(PnyxConfig {
            gateway_url,
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
                Ok(IrisConfig {
                    token,
                    port,
                    fallback,
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
        let mut config = Config::from_value(value)
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

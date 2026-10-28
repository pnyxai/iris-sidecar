// ─── models.rs ───────────────────────────────────────────────
// This file defines the plain data structures (structs) that represent
// local models and the JSON payload sent by the gateway when it wants Iris
// to relay a request to a local model.
//
// These structs are *dumb* containers – they hold data but have no logic.
// All the business logic lives in `relay.rs` and `main.rs`.
// ──────────────────────────────────────────────────────────────

use std::sync::atomic::{AtomicU64, Ordering};

// ──────────────────────────────────────────────────────────────
// LocalModel – a model running on the user's machine
// ──────────────────────────────────────────────────────────────
// After the config is loaded from YAML, every known local model is stored
// as one of these structs. It contains everything Iris needs to contact the
// model and check its health.
//
// The `custom_api_path` field is optional. If it is set, Iris will use it
// instead of the original request path when calling this local model.
//
// The `custom_headers` field is optional. Its entries are added to every
// request Iris makes to this model, including health checks. This is useful
// for authorization tokens or any other endpoint-specific headers.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct LocalModel {
    pub model_tag: String, // Name of the model tag on PNYX (this must match with a tracked model for correct routing)
    pub endpoint: String,  // Base URL, e.g. "http://localhost:1234"
    pub model_name: String, // The model identifier to send in the JSON payload
    pub custom_api_path: Option<String>, // Optional path override for local model calls
    pub custom_headers: std::collections::HashMap<String, String>, // Optional headers sent to the local model
}

impl LocalModel {
    /// Add this model's custom headers to a request builder.
    /// Invalid header names or values are skipped with a warning rather than
    /// failing the whole request.
    pub fn apply_custom_headers(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in &self.custom_headers {
            let header_name = match reqwest::header::HeaderName::try_from(name.as_str()) {
                Ok(name) => name,
                Err(_) => {
                    tracing::warn!(
                        "Ignoring invalid header name '{}' for model '{}'",
                        name,
                        self.model_tag
                    );
                    continue;
                }
            };
            let header_value = match reqwest::header::HeaderValue::try_from(value.as_str()) {
                Ok(value) => value,
                Err(_) => {
                    tracing::warn!(
                        "Ignoring invalid value for header '{}' on model '{}'",
                        name,
                        self.model_tag
                    );
                    continue;
                }
            };
            headers.insert(header_name, header_value);
        }
        builder.headers(headers)
    }
}

// ──────────────────────────────────────────────────────────────
// RelayData – the minimal instructions sent by the PNYX gateway
// ──────────────────────────────────────────────────────────────
// When the gateway responds with `Pnyx-Local-Request: true`, the JSON body
// contains a top-level field called `relay_data`. This struct maps that field.
//
// The gateway now sends ONLY the `model_tag` field. Iris uses this tag as
// the key to look up the full model configuration (endpoint, model_name, etc.)
// in its own local `HashMap`.
//
// Example JSON snippet:
//   "relay_data": {
//     "model_tag": "some-pnyx-tracked-model"
//   }
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct RelayData {
    pub model_tag: String, // The key to look up the model in Iris's local config
}

impl RelayData {
    pub fn from_json_value(value: &serde_json::Value) -> anyhow::Result<Self> {
        let model_tag = value
            .get("model_tag")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing field: model_tag"))?
            .to_string();
        Ok(RelayData { model_tag })
    }
}

// ──────────────────────────────────────────────────────────────
// Metrics – counters and timing for local vs PNYX relay
// ──────────────────────────────────────────────────────────────
#[derive(Debug)]
pub struct Metrics {
    pub local_requests: AtomicU64,
    pub local_total_ms: AtomicU64,
    pub pnyx_requests: AtomicU64,
    pub pnyx_total_ms: AtomicU64,
    pub last_pnyx_status: std::sync::Mutex<String>,
    pub last_pnyx_check_time: std::sync::Mutex<Option<std::time::Instant>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            local_requests: AtomicU64::new(0),
            local_total_ms: AtomicU64::new(0),
            pnyx_requests: AtomicU64::new(0),
            pnyx_total_ms: AtomicU64::new(0),
            last_pnyx_status: std::sync::Mutex::new("unknown".to_string()),
            last_pnyx_check_time: std::sync::Mutex::new(None),
        }
    }
}

impl Metrics {
    pub fn record_local(&self, duration_ms: u64) {
        self.local_requests.fetch_add(1, Ordering::Relaxed);
        self.local_total_ms.fetch_add(duration_ms, Ordering::Relaxed);
    }

    pub fn record_pnyx(&self, duration_ms: u64) {
        self.pnyx_requests.fetch_add(1, Ordering::Relaxed);
        self.pnyx_total_ms.fetch_add(duration_ms, Ordering::Relaxed);
    }

    pub fn local_stats(&self) -> (u64, u64) {
        let count = self.local_requests.load(Ordering::Relaxed);
        let total = self.local_total_ms.load(Ordering::Relaxed);
        (count, total)
    }

    pub fn pnyx_stats(&self) -> (u64, u64) {
        let count = self.pnyx_requests.load(Ordering::Relaxed);
        let total = self.pnyx_total_ms.load(Ordering::Relaxed);
        (count, total)
    }

    pub fn set_pnyx_status(&self, status: String) {
        let mut guard = self.last_pnyx_status.lock().unwrap();
        *guard = status;
        let mut time_guard = self.last_pnyx_check_time.lock().unwrap();
        *time_guard = Some(std::time::Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_model(custom_headers: std::collections::HashMap<String, String>) -> LocalModel {
        LocalModel {
            model_tag: "test-model".to_string(),
            endpoint: "http://localhost:1234".to_string(),
            model_name: "local".to_string(),
            custom_api_path: None,
            custom_headers,
        }
    }

    #[test]
    fn applies_custom_headers_to_requests() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer local-token".to_string());
        let model = test_model(headers);

        let request = model
            .apply_custom_headers(reqwest::Client::new().get("http://localhost:1234/health"))
            .build()
            .unwrap();

        assert_eq!(
            request.headers().get("authorization").unwrap(),
            "Bearer local-token"
        );
    }

    #[test]
    fn skips_invalid_custom_headers() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("Invalid Header".to_string(), "value".to_string());
        let model = test_model(headers);

        let request = model
            .apply_custom_headers(reqwest::Client::new().get("http://localhost:1234/health"))
            .build()
            .unwrap();

        assert!(request.headers().get("Invalid Header").is_none());
    }
}

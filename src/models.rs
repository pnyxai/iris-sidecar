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
// Metrics – counters and timing for local vs PNYX relay, plus
// cumulative OpenAI token usage and generation timings for local
// generations (totals since Iris start).
// ──────────────────────────────────────────────────────────────

/// Aggregate stats for one tracked quantity: the running `total`
/// (sum of valid readings since start), the number of `valid`
/// readings, and the number of `ignored` readings. Absent values
/// (the canonical "-1" ignore-me sentinel) are counted in `ignored`
/// and never added to `total`, so a missing field cannot corrupt
/// the running sum.
#[derive(Debug, Clone, Copy)]
pub struct FieldStats {
    pub total: u64,
    pub valid: u64,
    pub ignored: u64,
}

impl FieldStats {
    pub fn avg(&self) -> u64 {
        self.total.checked_div(self.valid).unwrap_or(0)
    }
}

#[derive(Debug)]
pub struct Metrics {
    pub local_requests: AtomicU64,
    pub local_total_ms: AtomicU64,
    pub pnyx_requests: AtomicU64,
    pub pnyx_total_ms: AtomicU64,
    pub last_pnyx_status: std::sync::Mutex<String>,
    pub last_pnyx_check_time: std::sync::Mutex<Option<std::time::Instant>>,

    // One observation per completed local generation; token and timing
    // "valid" counts are derived as (observations - ignored).
    pub local_observations: AtomicU64,

    // Token usage (canonical OpenAI names). No `cache_creation` storage:
    // it has no source in OpenAI-compatible responses and is always "-1".
    pub local_prompt_tokens_sum: AtomicU64,
    pub local_prompt_tokens_ignored: AtomicU64,
    pub local_completion_tokens_sum: AtomicU64,
    pub local_completion_tokens_ignored: AtomicU64,
    pub local_cached_tokens_sum: AtomicU64,
    pub local_cached_tokens_ignored: AtomicU64,

    // Timings, in milliseconds.
    pub local_routing_ms_sum: AtomicU64,
    pub local_routing_ms_ignored: AtomicU64,
    pub local_ttft_ms_sum: AtomicU64,
    pub local_ttft_ms_ignored: AtomicU64,
    pub local_generation_ms_sum: AtomicU64,
    pub local_generation_ms_ignored: AtomicU64,
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
            local_observations: AtomicU64::new(0),
            local_prompt_tokens_sum: AtomicU64::new(0),
            local_prompt_tokens_ignored: AtomicU64::new(0),
            local_completion_tokens_sum: AtomicU64::new(0),
            local_completion_tokens_ignored: AtomicU64::new(0),
            local_cached_tokens_sum: AtomicU64::new(0),
            local_cached_tokens_ignored: AtomicU64::new(0),
            local_routing_ms_sum: AtomicU64::new(0),
            local_routing_ms_ignored: AtomicU64::new(0),
            local_ttft_ms_sum: AtomicU64::new(0),
            local_ttft_ms_ignored: AtomicU64::new(0),
            local_generation_ms_sum: AtomicU64::new(0),
            local_generation_ms_ignored: AtomicU64::new(0),
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

    /// Record one OpenAI token-usage reading. `None` marks the field as
    /// absent: it bumps the ignored counter and is NOT added to the sum.
    pub fn record_local_usage(&self, prompt: Option<u64>, completion: Option<u64>, cached: Option<u64>) {
        let add = |sum: &AtomicU64, ign: &AtomicU64, v: Option<u64>| match v {
            Some(x) => {
                sum.fetch_add(x, Ordering::Relaxed);
            }
            None => {
                ign.fetch_add(1, Ordering::Relaxed);
            }
        };
        add(
            &self.local_prompt_tokens_sum,
            &self.local_prompt_tokens_ignored,
            prompt,
        );
        add(
            &self.local_completion_tokens_sum,
            &self.local_completion_tokens_ignored,
            completion,
        );
        add(
            &self.local_cached_tokens_sum,
            &self.local_cached_tokens_ignored,
            cached,
        );
    }

    /// Record one generation's timing set. Increments the shared observation
    /// count exactly once. `None` = ignore-me (`-1`), counted but not summed.
    pub fn record_local_times(&self, routing: Option<u64>, ttft: Option<u64>, generation: Option<u64>) {
        self.local_observations.fetch_add(1, Ordering::Relaxed);
        let add = |sum: &AtomicU64, ign: &AtomicU64, v: Option<u64>| match v {
            Some(x) => {
                sum.fetch_add(x, Ordering::Relaxed);
            }
            None => {
                ign.fetch_add(1, Ordering::Relaxed);
            }
        };
        add(&self.local_routing_ms_sum, &self.local_routing_ms_ignored, routing);
        add(&self.local_ttft_ms_sum, &self.local_ttft_ms_ignored, ttft);
        add(
            &self.local_generation_ms_sum,
            &self.local_generation_ms_ignored,
            generation,
        );
    }

    fn field(&self, sum: &AtomicU64, ignored: &AtomicU64) -> FieldStats {
        let observations = self.local_observations.load(Ordering::Relaxed);
        let ign = ignored.load(Ordering::Relaxed);
        FieldStats {
            total: sum.load(Ordering::Relaxed),
            valid: observations.saturating_sub(ign),
            ignored: ign,
        }
    }

    pub fn prompt_tokens_stats(&self) -> FieldStats {
        self.field(&self.local_prompt_tokens_sum, &self.local_prompt_tokens_ignored)
    }

    pub fn completion_tokens_stats(&self) -> FieldStats {
        self.field(
            &self.local_completion_tokens_sum,
            &self.local_completion_tokens_ignored,
        )
    }

    pub fn cached_tokens_stats(&self) -> FieldStats {
        self.field(&self.local_cached_tokens_sum, &self.local_cached_tokens_ignored)
    }

    pub fn routing_stats(&self) -> FieldStats {
        self.field(&self.local_routing_ms_sum, &self.local_routing_ms_ignored)
    }

    pub fn ttft_stats(&self) -> FieldStats {
        self.field(&self.local_ttft_ms_sum, &self.local_ttft_ms_ignored)
    }

    pub fn generation_stats(&self) -> FieldStats {
        self.field(
            &self.local_generation_ms_sum,
            &self.local_generation_ms_ignored,
        )
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

    #[test]
    fn usage_and_times_sum_valid_and_count_ignored() {
        let m = Metrics::default();
        // obs 1: prompt=100, completion=50, cached absent, routing absent, ttft absent, gen 2000
        m.record_local_usage(Some(100), Some(50), None);
        m.record_local_times(None, None, Some(2000));
        // obs 2: everything ignored except cached=30 and ttft=45
        m.record_local_usage(None, None, Some(30));
        m.record_local_times(None, Some(45), Some(1000));

        let p = m.prompt_tokens_stats();
        assert_eq!((p.total, p.valid, p.ignored), (100, 1, 1));
        assert_eq!(p.avg(), 100);

        let c = m.completion_tokens_stats();
        assert_eq!((c.total, c.valid, c.ignored), (50, 1, 1));

        let ca = m.cached_tokens_stats();
        assert_eq!((ca.total, ca.valid, ca.ignored), (30, 1, 1));

        let r = m.routing_stats();
        assert_eq!((r.total, r.valid, r.ignored), (0, 0, 2));
        assert_eq!(r.avg(), 0);

        let t = m.ttft_stats();
        assert_eq!((t.total, t.valid, t.ignored), (45, 1, 1));

        let g = m.generation_stats();
        assert_eq!((g.total, g.valid, g.ignored), (3000, 2, 0));
        assert_eq!(g.avg(), 1500);
    }
}

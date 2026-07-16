// ─── models.rs ───────────────────────────────────────────────
// This file defines the plain data structures (structs) that represent
// local models and the JSON payload sent by the gateway when it wants Iris
// to relay a request to a local model.
//
// These structs are *dumb* containers – they hold data but have no logic.
// All the business logic lives in `relay.rs` and `main.rs`.
// ──────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────
// LocalModel – a model running on the user's machine
// ──────────────────────────────────────────────────────────────
// After the config is loaded from YAML, every known local model is stored
// as one of these structs. It contains everything Iris needs to contact the
// model and check its health.
//
// The `custom_api_path` field is optional. If it is set, Iris will use it
// instead of the original request path when calling this local model.
// ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone)]
pub struct LocalModel {
    pub model_tag: String, // Name of the model tag on PNYX (this must match with a tracked model for correct routing)
    pub endpoint: String,  // Base URL, e.g. "http://localhost:1234"
    pub model_name: String, // The model identifier to send in the JSON payload
    pub custom_api_path: Option<String>, // Optional path override for local model calls
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

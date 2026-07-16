# Iris - PNYX Gateway Sidecar

A sidecar app to use your local models in the [Pnyx Gateway](https://www.pnyxai.com) router!


![Iris, por Pierre -Narcisse Guérin (detalle, óleo Iris y Morfeo.)](/assets/iris.png)

Iris bridges any client to Pnyx, handling request forwarding, local model routing and fallback.
- Ultra-lightweight (written in Rust) 
- Open-source
- Super easy to setup

## How it works

![](/assets/diagram.png)


Iris transparently relays HTTP traffic. For every request, Iris checks the health of all configured local models and reports to the gateway that they are available locally. If the gateway finds that the request is suitable for local generation (given the prompt difficulty and/or generation budget) it instructs Iris to generate locally. Iris intercepts the response, modifies it, and forwards the request to a **local model** instead using the configuration Iris owns.
If your local model is offline or the requests is too complex, the gateway will route it to the most suitable model in your Pnyx working group.

### Key features
- **Health monitoring**: Before every request, Iris checks the health of all local models and reports only the healthy ones to the gateway (no downtime due to local hiccups).
- **Full local model ownership**: Iris controls 100% of the local model configuration (endpoint, model name, etc). The gateway only needs a `model_tag` (i.e. `gpt-oss-20b`) key to understand the capabilities of your model.
- **Environment overrides**: Sensitive tokens and the listening port can be set via environment variables.
- **Streaming support**: Responses from both the gateway and local models are streamed back to the user (SSE/JSON).
- **Docker ready**: Includes a `Dockerfile` and `docker-compose.yml` for quick deployment without installing Rust.

## Configuration

Iris reads a YAML configuration file. By default it looks at:

```sh
~/.config/iris-sidecar/config.yaml
# and
./config.yaml
```

You can override the path with the `IRIS_CONFIG_PATH` environment variable.

### Example `config.yaml`

```yaml
pnyx:
  access_token: "your-pnyx-token"
  # Optional: defaults to 
  # gateway_url: "https://gateway.pnyxai.com/relay/text-generation"

iris:
  # Optional: OpenAI-style bearer token that clients must send to Iris
  # token: "local-iris-token"
  # Optional: defaults to
  # port: 8080

models:
  my-local-llm-model:
    # Mandatory: the base URL of the local model endpoint
    endpoint: "http://localhost:1234"
    # Mandatory: the model name to use in the OpenAI-compatible JSON payload
    model_name: "local-llama"
    # Optional: overrides the incoming request path when calling this local model
    # custom_api_path: "/v1/chat/completions"
```

### Environment variables

| Variable | Description |
|----------|-------------|
| `IRIS_CONFIG_PATH` | Path to the YAML configuration file. |
| `PNYX_TOKEN` | Overrides `pnyx.access_token` from the config. |
| `PNYX_IRIS_TOKEN` | Overrides `iris.token` from the config. |
| `IRIS_PORT` | Overrides `iris.port` from the config. |

## Running locally

### Prerequisites
- Rust toolchain (1.75+)

### Build & run

```bash
cargo build --release
./target/release/iris-sidecar
```

### Docker

```bash
docker-compose up --build
```

Mount your `config.yaml` into the container via the compose volume definition.

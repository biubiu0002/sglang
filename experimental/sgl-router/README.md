# sgl-router

Slim, KV-aware, OpenAI-compatible router for SGLang workers.

Serves a single model and routes across its workers. Exposes
`/v1/tokenize`, `/v1/detokenize`, `/v1/models`, `/v1/chat/completions`
(buffered and SSE), plus `/healthz` / `/readyz` and `/metrics`. Worker
pools come from either a static URL list or Kubernetes EndpointSlice
discovery.

## Building

```bash
cd experimental/sgl-router
cargo build --release
```

## Running

The router is configured entirely through CLI flags (run
`sgl-router --help` for the full list). It serves exactly one model, so
`--model-id` is required, along with exactly one discovery backend.
`--tokenizer-path` is optional: give it a local `tokenizer.json` path or a
HuggingFace repo id, and when omitted the router downloads the tokenizer
for `--model-id` from HuggingFace (honoring `HF_TOKEN` / `HF_HOME`).

Static worker list:

```bash
sgl-router \
  --host 0.0.0.0 --port 30000 \
  --model-id qwen3 \
  --tokenizer-path /models/qwen3/tokenizer.json \
  --worker-urls http://10.0.0.1:30000 http://10.0.0.2:30000
```

ACA-style environment variables are also supported when the process starts
without CLI flags. `WORKER_URLS` has priority. If it is unset, the router can
load the static worker list from a macaron worker registry at startup:

```bash
MODEL_ID=zai-org/GLM-5.2-FP8 \
WORKER_REGISTRY_POOL=glm52-main \
WORKER_REGISTRY_APP_CONFIG_ENDPOINT=https://macaron-llm-deploy-prod.azconfig.io \
WORKER_REGISTRY_APP_CONFIG_KEY=macaron/prod/worker-registry/glm52/current \
WORKER_REGISTRY_APP_CONFIG_LABEL=prod \
sgl-router
```

Registry source options are mutually exclusive:

- `WORKER_REGISTRY_JSON`: inline registry JSON.
- `WORKER_REGISTRY_FILE`: local registry JSON file.
- `WORKER_REGISTRY_APP_CONFIG_ENDPOINT` + `WORKER_REGISTRY_APP_CONFIG_KEY`
  with optional `WORKER_REGISTRY_APP_CONFIG_LABEL`.

The App Configuration path authenticates with Azure managed identity. Set
`WORKER_REGISTRY_APP_CONFIG_MANAGED_IDENTITY_CLIENT_ID` when a user-assigned
identity should be used. `WORKER_REGISTRY_URL_SUFFIX` may append a common
static-discovery suffix such as `@tier=shared`; per-worker
`pool_url_suffixes` in the registry take precedence. This is startup discovery,
not hot reload: changing the registry requires a new revision/restart unless a
future runtime polling path is enabled.

Kubernetes EndpointSlice discovery:

```bash
sgl-router \
  --host 0.0.0.0 --port 30000 \
  --model-id qwen3 \
  --tokenizer-path /models/qwen3/tokenizer.json \
  --service-discovery \
  --service-discovery-namespace prod \
  --selector app=engines-qwen3
```

Omit `--service-discovery-namespace` to watch all namespaces (requires
cluster-wide RBAC). For prefill/decode disaggregation, replace `--selector`
with `--prefill-selector` and `--decode-selector`.

## External Queue Admission

External routers can opt in to fail-fast overload protection:

```bash
sgl-router \
  --external-queue-admission-enabled \
  --external-queue-admission-threshold 8 \
  ...
```

When enabled, generation requests are rejected with HTTP 429
`external_queue_overloaded` before policy selection only if every healthy,
priority-eligible worker has effective load greater than the threshold.
Equal-to-threshold is admitted. With load polling enabled, effective load uses
reported worker queue depth plus router-local pending reservations; otherwise
it uses router-local pending reservations only.

The feature is disabled by default. Enabling it on live ACA/APIM deployments
requires a separate rollout approval and configuration change.

## License

Apache-2.0.

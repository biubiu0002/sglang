## Why

The first GLM-5.2 cache-state ACA trial showed that a gateway configured with a remote cache-state URL can lose useful local route-history affinity when the remote service is empty or unreachable. This change makes remote cache-state safe to enable incrementally by preserving local fallback behavior and by letting gateways feed route-history prefixes into the remote service over HTTP.

## What Changes

- Add gateway-side local route-history fallback when remote cache-state queries fail, time out, return malformed data, or return no useful match.
- Add a remote cache-state insert/feed path so route-history prefixes can populate an external cache-state service without relying on worker ZMQ ports.
- Add bounded metrics for remote cache-state query and feed outcomes.
- Keep distributed cache-state opt-in; no production routing behavior changes unless `CACHE_STATE_URL` is configured.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `distributed-cache-state`: remote cache-state integration must preserve local route-history fallback and support gateway-fed prefix inserts.

## Impact

- Affected code: `experimental/sgl-router/src/cache_state/mod.rs`, `experimental/sgl-router/src/policies/cache_aware_zmq.rs`, `experimental/sgl-router/src/server/metrics.rs`, and router tests.
- API impact: extends the existing internal cache-state `/v1/cache_state/insert` usage from service tests to gateway feed clients. No public OpenAI/Anthropic API changes.
- Operational impact: enables a later shadow/canary rollout of `CACHE_STATE_URL` without disabling local `route_history` affinity when the service is empty or temporarily unavailable.

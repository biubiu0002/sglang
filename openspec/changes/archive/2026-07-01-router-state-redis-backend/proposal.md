## Why

The router now needs to run more than one gateway replica without losing the TTFT-first active-load correction that was previously held in one process. A Redis-backed router-state store gives all gateway replicas the same short-lived reservation view while avoiding a single custom HTTP router-state writer.

## What Changes

- Add a Redis router-state backend for request reservation, release, and snapshot aggregation.
- Allow gateway mode to select exactly one router-state backend: direct Redis via `ROUTER_STATE_REDIS_URL`, legacy HTTP via `ROUTER_STATE_URL`, or none.
- Keep reservations TTL-bound and per-request so crashes, missed releases, and retries expire without leaking worker load.
- Keep TTFT-first scoring semantics unchanged: Redis only feeds active-load overlay inputs.
- Preserve the existing in-memory HTTP router-state service as a local/transition backend.

## Capabilities

### New Capabilities

- `router-state-redis-backend`: Shared Redis-backed active-load reservation state for multi-replica gateway routing.

### Modified Capabilities

- None.

## Impact

- Affected code: `experimental/sgl-router/src/router_state.rs`, gateway startup wiring in `experimental/sgl-router/src/main.rs`, `AppContext`, route handlers that reserve router-state, and `Cargo.toml`.
- New optional dependency: Redis client crate with TLS support.
- New configuration: `ROUTER_STATE_REDIS_URL`, optional `ROUTER_STATE_REDIS_KEY_PREFIX`, and existing timeout/snapshot interval knobs reused for Redis operations.
- No production infrastructure is created by this change; provisioning Azure Redis / setting ACA env / rolling router revisions remains a separate approved deployment step.

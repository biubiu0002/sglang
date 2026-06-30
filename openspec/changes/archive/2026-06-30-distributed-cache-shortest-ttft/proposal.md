## Why

Current cache-aware routing keeps prefix-cache state inside each gateway process. That makes multi-replica gateways see different cache history and prevents a shared cache view for ACA-hosted gateway deployments.

## What Changes

- Add a new distributed cache-state capability that can run as a standalone service, including a prefix-match query API suitable for ACA internal ingress.
- Add a remote cache-index client abstraction so gateway policies can use either the existing in-process tree or the new service without changing request parsing.
- Extend TTFT-first routing so it can score workers from remote prefix-match results while preserving existing local behavior and fallback semantics.
- Keep the feature opt-in and safe for production: no existing policy defaults change, and remote query failures degrade to current load-based routing.

## Capabilities

### New Capabilities
- `distributed-cache-state`: Shared cache-state service and gateway client behavior for prefix-cache matching.

### Modified Capabilities
- `ttft-first-routing`: TTFT-first routing can use remote distributed cache-match results as its cache signal.

## Impact

- Affected code: `experimental/sgl-router/src/policies`, router config/CLI, tests, and OpenSpec specs.
- APIs: adds internal service endpoints for cache-state health and prefix matching.
- Systems: designed for ACA internal deployment, but this change only modifies local source and tests; it does not deploy, restart, or change production services.

## Why

macaron-0.5 must keep application-observed first-token latency inside a 5s cutoff, but the current cache-aware route-history policy can over-prefer prefix affinity when worker compute is otherwise available. A request that is queued behind long prefills misses the client TTFT budget even if the chosen worker has a better cache hit.

## What Changes

- Add TTFT-first routing behavior for `cache_aware_zmq`: worker selection ranks predicted first-token pressure before cache affinity.
- Add token-weighted local reservations so a long prompt immediately makes the selected worker look busier to later route decisions, even before the next `/get_load` poll.
- Allow cache affinity only inside a configurable TTFT pressure band; outside that band the router spills to the worker with lower predicted first-token pressure.
- Keep the existing default cache-aware behavior unless the operator enables TTFT-first routing.
- No production rollout, APIM change, worker restart, or timeout change is part of this change.

## Capabilities

### New Capabilities
- `ttft-first-routing`: Router behavior that protects first-token latency by preferring low predicted TTFT pressure and using cache affinity only as a bounded tie-breaker.

### Modified Capabilities
- None.

## Impact

- Affected code: `experimental/sgl-router` config parsing, cache-aware policy scoring, worker local reservation counters, and generation route handlers that hold pending-load guards.
- Affected APIs: new router CLI flags for TTFT-first cache-aware routing.
- Affected systems: B200 SGLang router images after a future build and production rollout. This proposal does not change running ACA revisions.

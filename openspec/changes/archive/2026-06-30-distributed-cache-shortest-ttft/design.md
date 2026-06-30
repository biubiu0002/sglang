## Context

The router already has cache-aware routing, route-history prefix indexing, ZMQ KV-event indexing, `/get_load` polling, and token-weighted local pending reservations. The remaining gap is shared cache state across multiple gateway replicas. ACA is a reasonable first deployment target if the cache-state component is treated as a rebuildable in-memory service and gateway routing has a safe fallback when it is unavailable.

## Goals / Non-Goals

**Goals:**
- Add a standalone cache-state service mode that can be run as an ACA internal app.
- Let that service optionally subscribe to worker discovery/KV events, while still supporting an empty-tree HTTP mode for local tests and manual state injection.
- Add a gateway-side cache-index abstraction and remote HTTP client.
- Let TTFT-first routing use remote prefix-match results without changing defaults.
- Keep all production-impacting behavior opt-in and locally testable.

**Non-Goals:**
- No Azure deployment, production restart, APIM change, ACA revision update, or live traffic migration.
- No persistent database or strong distributed consensus for cache state in this change.
- No final production tuning of the TTFT cost model; the existing block-count pressure model remains the first version.

## Decisions

1. Add HTTP remote cache-state before gRPC.
   - Rationale: the router already depends on `axum` and `reqwest`, and ACA internal ingress works naturally with HTTP.
   - Alternative considered: gRPC streaming. That is better for a high-throughput future version but adds more protocol surface before the contract is proven.

2. Use an in-memory `HashTree` inside the cache-state service.
   - Rationale: the existing tree already has prefix-match semantics and tests. The service can rebuild state from worker KV events when discovery is configured, or accept explicit insert calls for tests and controlled experiments.
   - Alternative considered: Redis or external database. This would complicate latency and operations before validating routing value.

3. Keep remote cache state optional in `cache_aware_zmq`.
   - Rationale: current B200 gateway behavior must stay unchanged unless explicitly configured.
   - Alternative considered: introduce a new policy name. That would duplicate most of the existing request-token and TTFT code.

4. Treat remote query failures as cache misses.
   - Rationale: cache-aware routing is an optimization. User requests must not fail because the cache-state service is down.

## Risks / Trade-offs

- Remote query latency can add route-selection overhead. Mitigation: short request timeout, fallback on failure, and metrics around query latency in follow-up work.
- Single-replica ACA cache state can restart and lose in-memory state. Mitigation: gateway fallback keeps serving; cache state rebuilds from worker KV events when discovery is configured.
- Multi-gateway pending reservations remain local in the first implementation. Mitigation: keep this as a known follow-up before large multi-replica rollout.

## Migration Plan

1. Build and test locally with remote cache-state flags disabled.
2. Run cache-state service mode locally and verify `/healthz` and `/v1/cache_state/match_prefix`; optionally pass worker discovery flags to subscribe to KV events.
3. Enable gateway remote cache-state URL in shadow or canary only.
4. For ACA, deploy the cache-state service as internal ingress with `min_replicas=1`, `max_replicas=1` for the first production trial.
5. Roll back by removing the remote cache-state URL; gateway returns to local cache-aware behavior.

## Open Questions

- Whether production should use single-replica rebuildable cache state or multi-replica full mirroring after the first shadow run.
- Whether distributed pending reservation should live in the same cache-state service or a separate low-latency reservation service.

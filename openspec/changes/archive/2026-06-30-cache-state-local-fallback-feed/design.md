## Context

The current distributed cache-state service supports `/v1/cache_state/match_prefix` and `/v1/cache_state/insert`. Gateway mode can opt into a remote cache-state client with `CACHE_STATE_URL`, but the first ACA trial exposed two gaps:

- Worker ZMQ KV event ports are not reachable from the ACA service in the current deployment, so the standalone service may be empty even while gateways have useful local route-history state.
- The cache-aware policy currently treats a remote query failure as a cache miss and skips the local tree, which removes existing route-history affinity when `CACHE_STATE_URL` is enabled.

This change keeps the service opt-in and implements safe remote use before any production re-enable.

## Goals / Non-Goals

**Goals:**

- Preserve local route-history matching whenever remote cache-state is unavailable or empty.
- Feed route-history prefixes from gateway selection into the remote cache-state service over HTTP.
- Record bounded query/feed outcome metrics for canary and shadow validation.
- Keep request routing non-blocking from a correctness perspective: remote cache-state must never fail a user request.

**Non-Goals:**

- Do not change production ACA configuration or enable `CACHE_STATE_URL`.
- Do not change worker ZMQ publisher behavior or require public access to worker ZMQ ports.
- Do not introduce durable storage for cache-state service in this change.
- Do not change OpenAI/Anthropic request/response schemas.

## Decisions

1. **Remote hit first, local fallback second**

   The policy will query remote cache-state when configured. A response is considered useful only when it has `matched_blocks > 0` and at least one worker URL. Failure, malformed response, zero match, or empty workers falls back to `self.tree.match_prefix`. This preserves current route-history behavior and lets remote state gradually become useful.

2. **Gateway feed reuses existing insert API**

   The gateway will call the existing `/v1/cache_state/insert` endpoint after selecting a worker in route-history mode. The feed mirrors local `feed_route_history`: it uses the chosen worker URL, `dp_rank=0` for route-history, `parent_hash=None`, and the computed block hashes. The request includes `model_id` for API consistency, although the current in-memory tree is not model-partitioned.

3. **Remote feed failures are best-effort**

   Insert failures are logged and counted but do not alter the selected worker. Local route-history insertion remains the source of immediate safety and affinity.

4. **Metrics use bounded outcome labels**

   Metrics will use fixed outcome enums rather than URL/model labels for remote cache-state query/feed counters. Existing overlap histogram remains model-labeled and records the selected match source result as before.

## Risks / Trade-offs

- **Remote service still in-memory** → A service restart loses remote state. Mitigation: local route-history fallback remains active, and remote state can be rebuilt from gateway feed.
- **Synchronous HTTP feed can add latency** → Mitigation: use short existing timeout and keep failure non-fatal. If canary shows measurable overhead, move feed to a background queue in a later change.
- **Remote and local state can disagree** → Mitigation: remote useful hits take precedence, but empty/unavailable remote results fall back to local. Canary metrics will expose remote hit/miss/failure rates before production use.
- **Model partitioning is not enforced by tree today** → Mitigation: preserve existing API field and behavior; add model partitioning only if multi-model remote cache-state sharing becomes a real deployment requirement.

## Migration Plan

1. Implement code and tests locally.
2. Build a new router image from `deploy-b200` only after tests pass.
3. Deploy the image without setting `CACHE_STATE_URL` to preserve current production behavior.
4. Enable `CACHE_STATE_URL` only on a shadow or canary gateway and verify remote query/feed metrics plus TTFT before production rollout.
5. Rollback by unsetting `CACHE_STATE_URL`; local route-history remains active.

## Open Questions

- Whether the remote insert path should become asynchronous before production enablement depends on canary latency measurements.
- Whether one remote service should store multiple model pools requires a later model-partitioned tree design.

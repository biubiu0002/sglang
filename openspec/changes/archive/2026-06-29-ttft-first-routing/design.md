## Context

The current `cache_aware_zmq` policy is cache-first with load guards. In the macaron-0.5 incident, switching production to `round_robin` immediately reduced 5s first-token timeouts, which shows that cache affinity was capable of creating worker hotspots even while cluster compute was available.

The router already has useful signals:
- Ingress route tokens for chat, completions, messages, and responses.
- Prefix overlap from the cache-aware hash tree.
- Worker `/get_load` polling for remote waiting requests.
- Router-local pending reservations held after a selection.

The gap is that local pending reservations are request-count based. A 200k-token prefill and a 2k-token prefill both add one unit, so later requests can still be routed behind a long prompt before remote load polling catches up.

## Goals / Non-Goals

**Goals:**
- Protect first-token latency before optimizing cache hit rate.
- Keep cache useful as an in-band tie-breaker.
- Make long prompts immediately visible to subsequent route decisions.
- Keep behavior opt-in so existing deployments do not change until explicitly configured.

**Non-Goals:**
- Do not change application timeouts.
- Do not restart or reconfigure production ACA/worker services in this change.
- Do not implement global queueing or admission control across router replicas.
- Do not require new worker APIs.

## Decisions

1. Extend `cache_aware_zmq` instead of adding a separate policy.

`cache_aware_zmq` already owns the tokenization, prefix-tree lookup, route-history feed, and load-poller integration. Adding a TTFT-first mode there keeps the change small and lets operators reuse the existing production route-history setup.

2. Use token-weighted local reservations for immediate pressure.

When route tokens are available, route handlers hold a pending reservation weighted by the prompt token count. The worker converts those tokens into integer pressure units using a configurable token scale. This makes long prefills influence subsequent selections immediately, without waiting for `/get_load`.

3. Score workers by predicted first-token pressure, then apply cache as a banded tie-breaker.

For each candidate, TTFT-first mode estimates:

`pressure = worker_ttft_pressure + uncached_blocks`

`worker_ttft_pressure` combines remote reported load with token-weighted local pending pressure. `uncached_blocks` is lower for workers with matching prefix blocks and higher for misses. The router chooses the lowest score band first; cache affinity can choose among workers only when their scores are within the configured additive band.

4. Preserve existing behavior by default.

TTFT-first mode is gated by an explicit CLI flag. Existing `cache_aware_zmq` deployments continue to use the current imbalance fast-path, prefix threshold, matched-worker selection, and hit-load guard.

## Risks / Trade-offs

- Token count is a proxy for prefill cost, not a direct TTFT prediction. Mitigation: keep the scale and cache band configurable, and use conservative defaults.
- Holding token-weighted pending until request completion overestimates pressure after prefill finishes. Mitigation: the overestimate protects TTFT and decays automatically when the request completes; remote `/get_load` still contributes when enabled.
- Route-history cache state is approximate. Mitigation: TTFT-first mode treats cache as a bounded tie-breaker, so stale or approximate cache signal cannot dominate pressure.
- The algorithm is per-router-process. Mitigation: production B200 router is single-replica for this path; multi-replica deployments still need single-replica routing or external shared load state for strict p95 control.

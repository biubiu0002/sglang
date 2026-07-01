## Context

TTFT-first routing already uses active-load pressure when scoring workers. That pressure is currently process-local, with an optional custom HTTP router-state service added as a single-writer bridge for multiple gateway replicas. The production direction is to let gateway replicas share active-load reservations through a managed Redis/Valkey service instead of depending on a bespoke single-replica router-state process.

The implementation must stay compatible with the existing route-history and cache-state work: Redis is only the shared active-load overlay. It must not change prefix matching, TTFT score calculation, cache-state event fanout, or worker `/get_load` semantics.

## Goals / Non-Goals

**Goals:**

- Let gateway mode reserve, release, and snapshot active-load state directly in Redis.
- Keep stale reservations bounded by TTL even when a gateway crashes before release.
- Keep the existing HTTP router-state service usable for local testing and transition deployments.
- Fail clearly when both Redis and HTTP router-state backends are configured.
- Reuse existing timeout and snapshot interval knobs where possible.

**Non-Goals:**

- Provision Azure Redis, mutate ACA env, or restart production routers.
- Replace cache-state KV-prefix storage with Redis.
- Change TTFT-first scoring weights or worker selection policy.
- Implement Redis cluster-specific routing beyond what the Redis client URL supports.

## Decisions

1. Gateway uses a `RouterStateClient` abstraction.

   Both the legacy HTTP backend and the new Redis backend implement the same `reserve`, `release`, and `snapshot` interface. Request handlers and snapshot pollers only depend on that interface, so route behavior remains independent of the backing store.

2. Redis stores per-request reservation records with TTL, not aggregate counters.

   Each reservation is stored as JSON at `<prefix>:reservation:<request_id>` with a millisecond TTL and indexed by `<prefix>:reservations`. Snapshot reads the index, fetches live reservation records, drops missing or malformed ids, and aggregates pending load by worker URL. This avoids counter leaks and stale-release races because an expired request simply disappears instead of requiring decrement repair.

3. Redis and HTTP router-state configuration are mutually exclusive.

   `ROUTER_STATE_REDIS_URL` enables direct Redis state. `ROUTER_STATE_URL` keeps the existing HTTP mode. Setting both is a startup error because two active-load authorities would make scoring nondeterministic.

4. Snapshot polling remains best-effort.

   The overlay is advisory pressure input. Redis errors cause the current poll to be skipped with debug logging, matching existing HTTP behavior. Local active-load guards still protect each gateway's own in-flight work.

## Risks / Trade-offs

- Redis snapshot is O(active reservations) because it aggregates per-request records. Mitigation: reservations are short-lived and only represent in-flight routed requests; this favors correctness over counter repair complexity.
- The index set can temporarily contain stale ids after reservation TTL expiry. Mitigation: every snapshot removes missing/malformed ids with `SREM`, and release also removes ids.
- Redis outage removes cross-replica load visibility. Mitigation: gateway keeps local active-load and cache-state behavior; startup only requires Redis URL parsing, while runtime operations are best-effort after short timeouts.
- Managed Redis latency is now on the routing hot path for reservation. Mitigation: operations use short timeouts and a pooled sync client; deploy should keep Redis in-region with the gateway.

## Migration Plan

1. Build and validate the router with Redis backend support.
2. Provision a managed Redis/Valkey service separately after operator approval.
3. Configure gateway replicas with `ROUTER_STATE_REDIS_URL`, optional `ROUTER_STATE_REDIS_KEY_PREFIX`, reduced `ROUTER_STATE_SNAPSHOT_INTERVAL_MS`, and no `ROUTER_STATE_URL`.
4. Roll one canary gateway revision and inspect Redis reservation churn, router logs, and request success rate.
5. Roll remaining gateway replicas if canary is stable.
6. Roll back by removing `ROUTER_STATE_REDIS_URL` or restoring the previous router image/revision. Reservation TTLs clear leftover Redis state automatically.

## Open Questions

- Exact Azure Redis SKU, private networking, and key rotation procedure are deployment decisions outside this code change.

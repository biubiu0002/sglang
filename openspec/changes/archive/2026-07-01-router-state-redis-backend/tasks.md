## 1. Router-State Backend Abstraction

- [x] 1.1 Add a shared `RouterStateClient` interface and migrate HTTP router-state client, reservation guards, and snapshot poller to use it.
- [x] 1.2 Add a Redis router-state client with TTL per-request reservations, release, snapshot aggregation, and stale index cleanup.

## 2. Gateway Wiring

- [x] 2.1 Add Redis dependency and gateway startup configuration for `ROUTER_STATE_REDIS_URL` / `ROUTER_STATE_REDIS_KEY_PREFIX`.
- [x] 2.2 Enforce mutual exclusion between Redis and HTTP router-state backends and attach the overlay for either backend.

## 3. Validation

- [x] 3.1 Add or update focused tests for Redis reservation serialization/config behavior and existing router-state service behavior.
- [x] 3.2 Run OpenSpec and Rust validation for the changed router code.

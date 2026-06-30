## 1. Cache State Service

- [x] 1.1 Add cache-state request/response types and an in-memory HTTP service mode.
- [x] 1.2 Add health and prefix-match endpoints suitable for ACA internal ingress.

## 2. Gateway Integration

- [x] 2.1 Add remote cache-state URL/timeout configuration and validation.
- [x] 2.2 Add a remote cache-index client and wire it into `cache_aware_zmq`.
- [x] 2.3 Update TTFT-first scoring to consume remote prefix matches when configured and fall back safely on errors.

## 3. Verification

- [x] 3.1 Add unit/component tests for cache-state match behavior and remote-failure fallback.
- [x] 3.2 Run OpenSpec validation and Rust formatting/tests for the router crate.

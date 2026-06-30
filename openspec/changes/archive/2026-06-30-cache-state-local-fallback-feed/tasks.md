## 1. Remote Cache-State Client

- [x] 1.1 Add remote insert/feed support to `RemoteCacheStateClient` using the existing `/v1/cache_state/insert` API.
- [x] 1.2 Add bounded remote cache-state query/feed outcome metrics to `MetricsRegistry`.

## 2. Cache-Aware Policy

- [x] 2.1 Change remote match handling so failure, malformed response, zero matched blocks, or empty worker sets fall back to the local cache tree.
- [x] 2.2 Feed selected route-history prefixes to the remote cache-state service after preserving local route-history insertion.

## 3. Verification

- [x] 3.1 Add unit tests for local fallback when the remote cache-state service is unavailable or empty.
- [x] 3.2 Add unit tests for remote route-history feed success/failure behavior and metrics.
- [x] 3.3 Run OpenSpec validation, formatting, and targeted Rust tests.

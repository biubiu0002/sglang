## 1. Configuration

- [x] 1.1 Add TTFT-first cache-aware config fields and CLI validation.
- [x] 1.2 Keep TTFT-first routing disabled by default and reject TTFT flags outside `cache_aware_zmq`.

## 2. Routing Pressure

- [x] 2.1 Add token-weighted local pending reservations to `Worker`.
- [x] 2.2 Update generation route handlers to reserve pending load with route-token weights when available.

## 3. Policy Selection

- [x] 3.1 Add TTFT-first scoring to `cache_aware_zmq` using worker pressure plus uncached prefix blocks.
- [x] 3.2 Preserve the existing cache-aware path when TTFT-first routing is disabled.

## 4. Verification

- [x] 4.1 Add unit tests for config validation and token-weighted pending pressure.
- [x] 4.2 Add policy tests proving cache loses outside the TTFT band and wins inside the band.
- [x] 4.3 Run OpenSpec validation and focused router tests.

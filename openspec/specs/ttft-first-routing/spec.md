# ttft-first-routing Specification

## Purpose
Define TTFT-first cache-aware routing semantics, including token-weighted local reservations and optional distributed cache-state matches.
## Requirements
### Requirement: TTFT pressure takes priority over cache affinity
When TTFT-first routing is enabled for cache-aware routing, the router SHALL select workers by predicted first-token pressure before considering cache affinity.

#### Scenario: Cache hit loses to lower predicted TTFT pressure
- **WHEN** a request has a prefix-cache hit on one worker and another healthy worker has lower predicted first-token pressure outside the configured cache band
- **THEN** the router SHALL select the lower-pressure worker even though it has a weaker or missing cache hit

#### Scenario: Cache hit wins inside the TTFT pressure band
- **WHEN** multiple healthy workers have predicted first-token pressure within the configured cache band
- **THEN** the router SHALL prefer the worker with the strongest prefix-cache match among those in-band workers

### Requirement: Local reservations are token weighted
The router SHALL make local pending-load reservations proportional to the routed prompt token count for requests with derived routing tokens.

#### Scenario: Long prompt immediately affects later routing decisions
- **WHEN** a long prompt is assigned to a worker and the request is still in flight
- **THEN** subsequent TTFT-first selections SHALL see a larger local reservation on that worker before the next worker `/get_load` poll completes

#### Scenario: Missing tokens still reserves one request
- **WHEN** the router cannot derive routing tokens for a request
- **THEN** it SHALL still reserve at least one unit of local pending load for the selected worker

### Requirement: TTFT-first routing is opt-in
The router SHALL preserve existing cache-aware routing semantics unless the operator explicitly enables TTFT-first routing.

#### Scenario: Default cache-aware behavior remains unchanged
- **WHEN** cache-aware routing is configured without TTFT-first routing
- **THEN** worker selection SHALL continue to use the existing prefix-match, imbalance, and hit-load guard behavior

#### Scenario: TTFT-first flags require cache-aware routing
- **WHEN** an operator supplies TTFT-first routing flags with a policy other than `cache_aware_zmq`
- **THEN** router configuration validation SHALL fail at startup

### Requirement: TTFT-first routing can consume distributed cache matches
When TTFT-first routing is enabled with a remote cache-state service, the router SHALL compute each worker's predicted first-token pressure from remote prefix-match results, worker load, and local pending reservations.

#### Scenario: Remote cache hit lowers TTFT score
- **WHEN** a remote cache-state query reports matched blocks for one healthy worker
- **THEN** TTFT-first scoring SHALL reduce that worker's uncached prefill cost by the matched block count before comparing it with other workers

#### Scenario: Remote cache miss falls back to load pressure
- **WHEN** the remote cache-state query returns no matched workers
- **THEN** TTFT-first routing SHALL rank workers by load and pending pressure without cache affinity

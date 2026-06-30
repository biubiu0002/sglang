## MODIFIED Requirements

### Requirement: Gateway can use remote cache state
The gateway SHALL support an opt-in remote cache-state client for cache-aware routing while preserving the existing in-process cache tree path when no remote service is configured, and while preserving local route-history fallback when a remote service is configured but cannot provide a useful match.

#### Scenario: Remote cache-state URL is configured
- **WHEN** cache-aware routing is enabled with a remote cache-state URL and the remote service returns a useful prefix match
- **THEN** the gateway SHALL use the remote match result for prefix-aware worker scoring

#### Scenario: Remote cache-state URL is configured but remote match is not useful
- **WHEN** cache-aware routing is enabled with a remote cache-state URL and the remote query fails, times out, returns malformed data, returns zero matched blocks, or returns no matching workers
- **THEN** the gateway SHALL fall back to the in-process cache tree match result before selecting by load-only fallback behavior

#### Scenario: Remote cache-state URL is omitted
- **WHEN** cache-aware routing is enabled without a remote cache-state URL
- **THEN** the gateway SHALL keep the existing in-process ZMQ or route-history cache behavior

### Requirement: Remote cache-state failures degrade safely
The gateway SHALL treat remote cache-state query failures, timeouts, malformed responses, and empty remote matches as non-fatal and continue routing with the best available local cache-tree and load-based fallback behavior.

#### Scenario: Remote service is unavailable
- **WHEN** the gateway cannot reach the configured remote cache-state service during a route selection
- **THEN** the gateway SHALL not fail the user request because of that cache-state error

#### Scenario: Remote service returns no useful match
- **WHEN** the remote cache-state response has zero matched blocks or no matching workers
- **THEN** the gateway SHALL query the local cache tree and use that local result if it contains a useful prefix match

#### Scenario: Neither remote nor local cache state has a useful match
- **WHEN** the remote cache-state response is unavailable or empty and the local cache tree also has no useful prefix match
- **THEN** the gateway SHALL select using the same fallback path used for local cache misses

## ADDED Requirements

### Requirement: Gateway can feed remote cache state
When route-history cache tree source is enabled with a remote cache-state URL, the gateway SHALL be able to submit the chosen worker and request block hashes to the remote cache-state service after worker selection.

#### Scenario: Route-history selection feeds remote cache state
- **WHEN** a cache-aware route-history gateway selects a worker for a request with non-empty block hashes and remote cache-state is configured
- **THEN** the gateway SHALL send an internal cache-state insert request containing the model id, chosen worker URL, dp rank, and block hash chain

#### Scenario: Remote feed failure is non-fatal
- **WHEN** the gateway cannot insert a selected route-history prefix into the remote cache-state service
- **THEN** the gateway SHALL still return the selected worker for the user request and keep the local route-history insertion behavior

### Requirement: Remote cache-state observability is bounded
The gateway SHALL expose bounded-cardinality metrics for remote cache-state query and feed outcomes.

#### Scenario: Remote query outcome is recorded
- **WHEN** the gateway attempts a remote cache-state match query
- **THEN** it SHALL increment a counter labeled by a bounded outcome such as hit, miss, failure, or fallback_local_hit

#### Scenario: Remote feed outcome is recorded
- **WHEN** the gateway attempts a remote cache-state insert/feed
- **THEN** it SHALL increment a counter labeled by a bounded outcome such as success or failure

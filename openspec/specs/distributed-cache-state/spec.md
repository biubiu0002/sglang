# distributed-cache-state Specification

## Purpose
Define the standalone cache-state service and gateway integration used to share prefix-cache state across gateway replicas while preserving safe fallback behavior.
## Requirements
### Requirement: Distributed cache state exposes prefix-match queries
The system SHALL provide a cache-state service mode that maintains a prefix-cache hash tree and exposes an internal HTTP API for matching request block hashes against cached worker prefixes.

#### Scenario: Prefix match returns matching workers
- **WHEN** the cache-state service has indexed a worker prefix and a gateway queries the same model and leading block hash chain
- **THEN** the service SHALL return the number of matched leading blocks and the worker URLs holding the deepest matched prefix

#### Scenario: Health endpoint reports readiness
- **WHEN** an operator or ACA probe calls the cache-state service health endpoint
- **THEN** the service SHALL return a successful response without requiring worker traffic

### Requirement: Gateway can use remote cache state
The gateway SHALL support an opt-in remote cache-state client for cache-aware routing while preserving the existing in-process cache tree path when no remote service is configured.

#### Scenario: Remote cache-state URL is configured
- **WHEN** cache-aware routing is enabled with a remote cache-state URL
- **THEN** the gateway SHALL query the remote service for prefix matches instead of using only the in-process tree match result

#### Scenario: Remote cache-state URL is omitted
- **WHEN** cache-aware routing is enabled without a remote cache-state URL
- **THEN** the gateway SHALL keep the existing in-process ZMQ or route-history cache behavior

### Requirement: Remote cache-state failures degrade safely
The gateway SHALL treat remote cache-state query failures, timeouts, and malformed responses as cache misses and continue routing with load-based fallback behavior.

#### Scenario: Remote service is unavailable
- **WHEN** the gateway cannot reach the configured remote cache-state service during a route selection
- **THEN** the gateway SHALL not fail the user request because of that cache-state error

#### Scenario: Remote service returns no useful match
- **WHEN** the remote cache-state response has zero matched blocks or no matching workers
- **THEN** the gateway SHALL select using the same fallback path used for local cache misses

### Requirement: Distributed cache-state operation is opt-in
The system SHALL require explicit configuration to run cache-state service mode or to make a gateway query a remote cache-state service.

#### Scenario: Existing gateway flags are unchanged
- **WHEN** an operator starts the gateway with the existing cache-aware flags and no distributed cache-state flags
- **THEN** startup and routing SHALL preserve the previous behavior

#### Scenario: ACA service can run separately
- **WHEN** an operator starts the binary in cache-state service mode with host and port configuration
- **THEN** it SHALL serve only cache-state endpoints and SHALL NOT require worker discovery or proxy configuration

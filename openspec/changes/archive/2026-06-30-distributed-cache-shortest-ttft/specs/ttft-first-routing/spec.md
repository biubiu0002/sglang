## ADDED Requirements

### Requirement: TTFT-first routing can consume distributed cache matches
When TTFT-first routing is enabled with a remote cache-state service, the router SHALL compute each worker's predicted first-token pressure from remote prefix-match results, worker load, and local pending reservations.

#### Scenario: Remote cache hit lowers TTFT score
- **WHEN** a remote cache-state query reports matched blocks for one healthy worker
- **THEN** TTFT-first scoring SHALL reduce that worker's uncached prefill cost by the matched block count before comparing it with other workers

#### Scenario: Remote cache miss falls back to load pressure
- **WHEN** the remote cache-state query returns no matched workers
- **THEN** TTFT-first routing SHALL rank workers by load and pending pressure without cache affinity

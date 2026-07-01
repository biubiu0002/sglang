# router-state-redis-backend Specification

## Purpose

Define the Redis-backed router-state behavior used to share active-load reservations across gateway replicas without changing TTFT-first scoring semantics.

## Requirements
### Requirement: Redis-backed router-state reservation
The gateway SHALL support a Redis router-state backend that records active-load reservations shared by all gateway replicas.

#### Scenario: Reserve active load in Redis
- **WHEN** gateway mode starts with `ROUTER_STATE_REDIS_URL` configured and routes a request to a worker
- **THEN** it SHALL write a reservation containing worker URL, request id, pending request count, pending token weight, and TTL to Redis

#### Scenario: Release active load in Redis
- **WHEN** the routed request completes or the local reservation guard is dropped
- **THEN** the gateway SHALL remove the reservation id from Redis best-effort

### Requirement: TTL-bounded reservations
Redis router-state reservations SHALL expire without requiring an explicit release.

#### Scenario: Gateway crashes before release
- **WHEN** a gateway reserves a request and exits before releasing it
- **THEN** Redis SHALL remove the reservation after the configured TTL and later snapshots SHALL stop counting it

### Requirement: Snapshot aggregation
The gateway SHALL aggregate Redis router-state reservations into per-worker pending request and pending token load.

#### Scenario: Poll shared active load
- **WHEN** the snapshot poller reads live Redis reservations for multiple workers
- **THEN** it SHALL update the router-state overlay with summed pending requests and pending tokens per worker URL

#### Scenario: Clean stale index ids
- **WHEN** the Redis reservation index contains ids whose reservation record is missing or malformed
- **THEN** the snapshot operation SHALL remove those ids from the index best-effort and exclude them from the overlay

### Requirement: Single active-load authority
Gateway startup SHALL accept at most one remote router-state backend.

#### Scenario: Redis and HTTP backends both configured
- **WHEN** `ROUTER_STATE_REDIS_URL` and `ROUTER_STATE_URL` are both non-empty
- **THEN** gateway startup SHALL fail before serving traffic

#### Scenario: No remote backend configured
- **WHEN** neither `ROUTER_STATE_REDIS_URL` nor `ROUTER_STATE_URL` is configured
- **THEN** gateway routing SHALL continue with process-local active-load only

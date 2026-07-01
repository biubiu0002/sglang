// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Single-writer router-state service for cross-replica dispatch reservations.
//!
//! `cache-state` externalizes the KV-prefix view. This module externalizes the
//! other multi-router prerequisite: router-local pending dispatch pressure. The
//! service keeps short-lived per-request reservations keyed by request id, and
//! exposes a compact per-worker snapshot for gateway replicas to fold into
//! TTFT-first scoring.

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use dashmap::DashMap;
use redis::{Commands, ConnectionLike};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouterStateReserveRequest {
    pub worker_url: String,
    pub request_id: String,
    pub pending_requests: usize,
    pub pending_tokens: usize,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouterStateReleaseRequest {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RouterStateWorkerLoad {
    pub pending_requests: usize,
    pub pending_tokens: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RouterStateSnapshotResponse {
    pub workers: HashMap<String, RouterStateWorkerLoad>,
}

pub trait RouterStateClient: fmt::Debug + Send + Sync {
    fn reserve(&self, req: &RouterStateReserveRequest) -> bool;
    fn release(&self, request_id: &str) -> bool;
    fn snapshot(&self) -> Option<RouterStateSnapshotResponse>;
}

#[derive(Debug)]
struct ReservationEntry {
    worker_url: String,
    pending_requests: usize,
    pending_tokens: usize,
    expires_at: Instant,
}

#[derive(Debug, Default)]
pub struct RouterStateService {
    reservations: DashMap<String, ReservationEntry>,
}

impl RouterStateService {
    pub fn new() -> Self {
        Self {
            reservations: DashMap::new(),
        }
    }

    pub fn router(self: Arc<Self>) -> Router {
        self.router_with_api_token(None)
    }

    pub fn router_with_api_token(self: Arc<Self>, api_token: Option<String>) -> Router {
        Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/router_state/reserve", post(reserve))
            .route("/v1/router_state/release", post(release))
            .route("/v1/router_state/snapshot", get(snapshot))
            .with_state(RouterStateAppState {
                service: self,
                api_token,
            })
    }

    pub fn reserve(&self, req: RouterStateReserveRequest) {
        let ttl = Duration::from_millis(req.ttl_ms.max(1));
        self.reservations.insert(
            req.request_id,
            ReservationEntry {
                worker_url: req.worker_url,
                pending_requests: req.pending_requests,
                pending_tokens: req.pending_tokens,
                expires_at: Instant::now() + ttl,
            },
        );
    }

    pub fn release(&self, request_id: &str) -> bool {
        self.reservations.remove(request_id).is_some()
    }

    pub fn snapshot(&self) -> RouterStateSnapshotResponse {
        self.sweep_expired();
        let mut workers: HashMap<String, RouterStateWorkerLoad> = HashMap::new();
        for entry in self.reservations.iter() {
            let e = entry.value();
            let load = workers.entry(e.worker_url.clone()).or_default();
            load.pending_requests = load.pending_requests.saturating_add(e.pending_requests);
            load.pending_tokens = load.pending_tokens.saturating_add(e.pending_tokens);
        }
        RouterStateSnapshotResponse { workers }
    }

    pub fn sweep_expired(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<String> = self
            .reservations
            .iter()
            .filter(|entry| entry.value().expires_at <= now)
            .map(|entry| entry.key().clone())
            .collect();
        let mut swept = 0;
        for id in expired {
            if self.reservations.remove(&id).is_some() {
                swept += 1;
            }
        }
        swept
    }
}

#[derive(Clone)]
struct RouterStateAppState {
    service: Arc<RouterStateService>,
    api_token: Option<String>,
}

fn check_auth(headers: &HeaderMap, token: Option<&str>) -> Result<(), StatusCode> {
    let Some(expected) = token else {
        return Ok(());
    };
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    if value == format!("Bearer {expected}") {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn healthz() -> &'static str {
    "ok"
}

async fn reserve(
    State(state): State<RouterStateAppState>,
    headers: HeaderMap,
    Json(req): Json<RouterStateReserveRequest>,
) -> Result<StatusCode, StatusCode> {
    check_auth(&headers, state.api_token.as_deref())?;
    if req.worker_url.trim().is_empty()
        || req.request_id.trim().is_empty()
        || (req.pending_requests == 0 && req.pending_tokens == 0)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    state.service.reserve(req);
    Ok(StatusCode::NO_CONTENT)
}

async fn release(
    State(state): State<RouterStateAppState>,
    headers: HeaderMap,
    Json(req): Json<RouterStateReleaseRequest>,
) -> Result<StatusCode, StatusCode> {
    check_auth(&headers, state.api_token.as_deref())?;
    if req.request_id.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    state.service.release(&req.request_id);
    Ok(StatusCode::NO_CONTENT)
}

async fn snapshot(
    State(state): State<RouterStateAppState>,
    headers: HeaderMap,
) -> Result<Json<RouterStateSnapshotResponse>, StatusCode> {
    check_auth(&headers, state.api_token.as_deref())?;
    Ok(Json(state.service.snapshot()))
}

#[derive(Debug, Clone)]
pub struct RemoteRouterStateClient {
    base_url: String,
    agent: ureq::Agent,
    api_token: Option<String>,
}

impl RemoteRouterStateClient {
    pub fn new(base_url: String, timeout: Duration) -> Self {
        let agent = ureq::AgentBuilder::new().timeout(timeout).build();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            agent,
            api_token: std::env::var("ROUTER_STATE_API_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }

    fn post(&self, path: &str) -> ureq::Request {
        let req = self.agent.post(&format!("{}{}", self.base_url, path));
        match self.api_token.as_ref() {
            Some(token) => req.set("Authorization", &format!("Bearer {token}")),
            None => req,
        }
    }

    fn get(&self, path: &str) -> ureq::Request {
        let req = self.agent.get(&format!("{}{}", self.base_url, path));
        match self.api_token.as_ref() {
            Some(token) => req.set("Authorization", &format!("Bearer {token}")),
            None => req,
        }
    }
}

impl RouterStateClient for RemoteRouterStateClient {
    fn reserve(&self, req: &RouterStateReserveRequest) -> bool {
        self.post("/v1/router_state/reserve")
            .send_json(req)
            .map(|resp| (200..300).contains(&resp.status()))
            .unwrap_or(false)
    }

    fn release(&self, request_id: &str) -> bool {
        self.post("/v1/router_state/release")
            .send_json(RouterStateReleaseRequest {
                request_id: request_id.to_string(),
            })
            .map(|resp| (200..300).contains(&resp.status()))
            .unwrap_or(false)
    }

    fn snapshot(&self) -> Option<RouterStateSnapshotResponse> {
        self.get("/v1/router_state/snapshot")
            .call()
            .ok()
            .and_then(|resp| resp.into_json::<RouterStateSnapshotResponse>().ok())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RedisReservation {
    worker_url: String,
    pending_requests: usize,
    pending_tokens: usize,
}

#[derive(Debug, Clone)]
pub struct RedisRouterStateClient {
    pool: r2d2::Pool<RedisConnectionManager>,
    key_prefix: String,
    index_key: String,
}

#[derive(Debug, Clone)]
struct RedisConnectionManager {
    client: redis::Client,
    timeout: Duration,
}

impl r2d2::ManageConnection for RedisConnectionManager {
    type Connection = redis::Connection;
    type Error = redis::RedisError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_connection_with_timeout(self.timeout)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        redis::cmd("PING").query::<String>(conn).map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.is_open()
    }
}

impl RedisRouterStateClient {
    pub fn new(redis_url: &str, key_prefix: &str, timeout: Duration) -> redis::RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        let key_prefix = normalize_redis_key_prefix(key_prefix);
        let manager = RedisConnectionManager { client, timeout };
        let pool = r2d2::Pool::builder()
            .max_size(16)
            .connection_timeout(timeout)
            .build(manager)
            .map_err(|e| {
                redis::RedisError::from((
                    redis::ErrorKind::IoError,
                    "build redis pool",
                    e.to_string(),
                ))
            })?;
        Ok(Self {
            pool,
            index_key: format!("{key_prefix}:reservations"),
            key_prefix,
        })
    }

    fn reservation_key(&self, request_id: &str) -> String {
        redis_reservation_key(&self.key_prefix, request_id)
    }

    fn connection(&self) -> Result<r2d2::PooledConnection<RedisConnectionManager>, r2d2::Error> {
        self.pool.get()
    }
}

impl RouterStateClient for RedisRouterStateClient {
    fn reserve(&self, req: &RouterStateReserveRequest) -> bool {
        if req.worker_url.trim().is_empty()
            || req.request_id.trim().is_empty()
            || (req.pending_requests == 0 && req.pending_tokens == 0)
        {
            return false;
        }
        let reservation = RedisReservation {
            worker_url: req.worker_url.clone(),
            pending_requests: req.pending_requests,
            pending_tokens: req.pending_tokens,
        };
        let Ok(value) = serde_json::to_string(&reservation) else {
            return false;
        };
        let Ok(mut conn) = self.connection() else {
            return false;
        };
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SET")
            .arg(self.reservation_key(&req.request_id))
            .arg(value)
            .arg("PX")
            .arg(req.ttl_ms.max(1))
            .ignore()
            .cmd("SADD")
            .arg(&self.index_key)
            .arg(&req.request_id)
            .ignore();
        pipe.query::<()>(&mut conn).is_ok()
    }

    fn release(&self, request_id: &str) -> bool {
        if request_id.trim().is_empty() {
            return false;
        }
        let Ok(mut conn) = self.connection() else {
            return false;
        };
        let mut pipe = redis::pipe();
        pipe.cmd("DEL")
            .arg(self.reservation_key(request_id))
            .ignore()
            .cmd("SREM")
            .arg(&self.index_key)
            .arg(request_id)
            .ignore();
        pipe.query::<()>(&mut conn).is_ok()
    }

    fn snapshot(&self) -> Option<RouterStateSnapshotResponse> {
        let mut conn = self.connection().ok()?;
        let ids: Vec<String> = conn.smembers(&self.index_key).ok()?;
        if ids.is_empty() {
            return Some(RouterStateSnapshotResponse::default());
        }

        let mut get_pipe = redis::pipe();
        for id in &ids {
            get_pipe.cmd("GET").arg(self.reservation_key(id));
        }
        let values: Vec<Option<String>> = get_pipe.query(&mut conn).ok()?;

        let mut workers: HashMap<String, RouterStateWorkerLoad> = HashMap::new();
        let mut stale_ids = Vec::new();
        for (id, value) in ids.iter().zip(values.into_iter()) {
            let Some(value) = value else {
                stale_ids.push(id.as_str());
                continue;
            };
            match serde_json::from_str::<RedisReservation>(&value) {
                Ok(reservation) => {
                    let load = workers.entry(reservation.worker_url).or_default();
                    load.pending_requests = load
                        .pending_requests
                        .saturating_add(reservation.pending_requests);
                    load.pending_tokens = load
                        .pending_tokens
                        .saturating_add(reservation.pending_tokens);
                }
                Err(_) => stale_ids.push(id.as_str()),
            }
        }
        if !stale_ids.is_empty() {
            let mut cleanup_pipe = redis::pipe();
            cleanup_pipe.cmd("SREM").arg(&self.index_key);
            for id in stale_ids {
                cleanup_pipe.arg(id);
            }
            cleanup_pipe.ignore();
            let _ = cleanup_pipe.query::<()>(&mut conn);
        }
        Some(RouterStateSnapshotResponse { workers })
    }
}

fn normalize_redis_key_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_matches(':');
    if trimmed.is_empty() {
        "sgl-router:router-state".to_string()
    } else {
        trimmed.to_string()
    }
}

fn redis_reservation_key(key_prefix: &str, request_id: &str) -> String {
    format!("{key_prefix}:reservation:{request_id}")
}

#[derive(Debug)]
pub struct RouterStateLoadOverlay {
    loads: DashMap<String, RouterStateWorkerLoad>,
}

impl RouterStateLoadOverlay {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            loads: DashMap::new(),
        })
    }

    pub fn update(&self, snapshot: RouterStateSnapshotResponse) {
        self.loads.clear();
        for (worker, load) in snapshot.workers {
            self.loads.insert(worker, load);
        }
    }

    pub fn pending_requests(&self, worker_url: &str) -> usize {
        self.loads
            .get(worker_url)
            .map(|v| v.pending_requests)
            .unwrap_or(0)
    }

    pub fn pending_tokens(&self, worker_url: &str) -> usize {
        self.loads
            .get(worker_url)
            .map(|v| v.pending_tokens)
            .unwrap_or(0)
    }
}

#[must_use = "RouterStateReservationGuard releases the remote reservation on drop"]
#[derive(Debug)]
pub struct RouterStateReservationGuard {
    client: Option<Arc<dyn RouterStateClient>>,
    request_id: String,
}

impl RouterStateReservationGuard {
    pub fn reserve(
        client: Arc<dyn RouterStateClient>,
        worker_url: String,
        pending_tokens: usize,
        ttl_ms: u64,
    ) -> Option<Self> {
        let request_id = Uuid::new_v4().to_string();
        let req = RouterStateReserveRequest {
            worker_url,
            request_id: request_id.clone(),
            pending_requests: 1,
            pending_tokens: pending_tokens.max(1),
            ttl_ms,
        };
        if client.reserve(&req) {
            Some(Self {
                client: Some(client),
                request_id,
            })
        } else {
            None
        }
    }
}

impl Drop for RouterStateReservationGuard {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.release(&self.request_id);
        }
    }
}

pub fn spawn_router_state_snapshot_poller(
    client: Arc<dyn RouterStateClient>,
    overlay: Arc<RouterStateLoadOverlay>,
    interval: Duration,
) -> crate::policies::active_load::JanitorHandle {
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancel_for_task.cancelled() => {
                    tracing::debug!("router-state snapshot poller: shutdown requested");
                    return;
                }
                _ = ticker.tick() => {
                    match client.snapshot() {
                        Some(snapshot) => overlay.update(snapshot),
                        None => tracing::debug!("router-state snapshot poll failed"),
                    }
                }
            }
        }
    });
    crate::policies::active_load::JanitorHandle::from_parts(cancel, join)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_reserve_snapshot_release_round_trip() {
        let service = RouterStateService::new();
        service.reserve(RouterStateReserveRequest {
            worker_url: "http://w0".into(),
            request_id: "r0".into(),
            pending_requests: 1,
            pending_tokens: 130,
            ttl_ms: 60_000,
        });
        let snap = service.snapshot();
        assert_eq!(
            snap.workers.get("http://w0"),
            Some(&RouterStateWorkerLoad {
                pending_requests: 1,
                pending_tokens: 130
            })
        );
        assert!(service.release("r0"));
        assert!(service.snapshot().workers.is_empty());
    }

    #[test]
    fn service_sweeps_expired_reservations() {
        let service = RouterStateService::new();
        service.reserve(RouterStateReserveRequest {
            worker_url: "http://w0".into(),
            request_id: "r0".into(),
            pending_requests: 1,
            pending_tokens: 1,
            ttl_ms: 1,
        });
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(service.sweep_expired(), 1);
        assert!(service.snapshot().workers.is_empty());
    }

    #[test]
    fn redis_key_prefix_normalizes_empty_and_colons() {
        assert_eq!(
            normalize_redis_key_prefix(""),
            "sgl-router:router-state".to_string()
        );
        assert_eq!(
            normalize_redis_key_prefix(":prod:glm52:"),
            "prod:glm52".to_string()
        );
        assert_eq!(
            redis_reservation_key("prod:glm52", "r0"),
            "prod:glm52:reservation:r0".to_string()
        );
    }

    #[test]
    fn redis_reservation_round_trips_json() {
        let reservation = RedisReservation {
            worker_url: "http://w0".into(),
            pending_requests: 1,
            pending_tokens: 64,
        };
        let json = serde_json::to_string(&reservation).unwrap();
        assert_eq!(
            serde_json::from_str::<RedisReservation>(&json).unwrap(),
            reservation
        );
    }
}

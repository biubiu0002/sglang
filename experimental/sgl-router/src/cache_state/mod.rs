// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Distributed cache-state service and client primitives.
//!
//! This module is intentionally small and HTTP-shaped for the first
//! production trial: the same router binary can run as a standalone
//! in-memory cache-state service, while gateway mode can query it as an
//! optional optimization. Query failures are surfaced as `None`, and feed
//! failures as `false`, so routing never fails requests because cache-state is
//! unavailable.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::policies::kv_events::tree::{HashTree, KvWorkerId};
use crate::policies::kv_events::wire::{decode_event_batch, DecodeError, KvCacheEvent};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheStateWorkerMatch {
    pub worker_url: String,
    pub dp_rank: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheStateMatchRequest {
    pub model_id: String,
    pub block_hashes: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheStateMatchResponse {
    pub matched_blocks: usize,
    pub workers: Vec<CacheStateWorkerMatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheStateInsertRequest {
    pub model_id: String,
    pub worker_url: String,
    #[serde(default)]
    pub dp_rank: u32,
    #[serde(default)]
    pub parent_hash: Option<i64>,
    pub block_hashes: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CacheStateKvEventsRequest {
    pub model_id: String,
    pub worker_url: String,
    #[serde(default)]
    pub dp_rank: u32,
    pub seq: i64,
    /// Raw SGLang msgpack `EventBatch` payload from the ZMQ frame, base64-encoded.
    pub payload_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheStateKvEventsResponse {
    pub applied_events: usize,
}

#[derive(Debug, Clone)]
pub struct CacheStateService {
    tree: Arc<HashTree>,
}

#[derive(Debug, Clone)]
struct CacheStateRouterState {
    service: Arc<CacheStateService>,
    api_token: Option<Arc<str>>,
}

impl CacheStateService {
    pub fn new(tree: Arc<HashTree>) -> Self {
        Self { tree }
    }

    pub fn with_empty_tree() -> Self {
        Self::new(Arc::new(HashTree::new()))
    }

    pub fn match_prefix(&self, req: &CacheStateMatchRequest) -> CacheStateMatchResponse {
        let matched = self.tree.match_prefix(None, &req.block_hashes);
        CacheStateMatchResponse {
            matched_blocks: matched.matched_blocks,
            workers: sorted_workers(matched.workers),
        }
    }

    pub fn insert(&self, req: &CacheStateInsertRequest) {
        let worker = KvWorkerId::new(req.worker_url.clone(), req.dp_rank);
        self.tree
            .insert(&worker, req.parent_hash, req.block_hashes.as_slice());
    }

    pub fn apply_kv_events(
        &self,
        req: &CacheStateKvEventsRequest,
    ) -> Result<CacheStateKvEventsResponse, CacheStateError> {
        let payload = decode_base64(&req.payload_b64).map_err(CacheStateError::BadBase64)?;
        let batch = decode_event_batch(&payload).map_err(CacheStateError::BadMsgpack)?;
        let worker = KvWorkerId::new(req.worker_url.clone(), req.dp_rank);
        let mut applied_events = 0usize;
        for event in &batch.events {
            match event {
                KvCacheEvent::BlockStored(block) => {
                    self.tree
                        .insert(&worker, block.parent_block_hash, &block.block_hashes);
                    applied_events += 1;
                }
                KvCacheEvent::BlockRemoved(block) => {
                    self.tree.remove(&worker, &block.block_hashes);
                    applied_events += 1;
                }
                KvCacheEvent::AllBlocksCleared => {
                    self.tree.clear_worker(&worker);
                    applied_events += 1;
                }
            }
        }
        Ok(CacheStateKvEventsResponse { applied_events })
    }

    pub fn router(self: Arc<Self>) -> Router {
        self.router_with_api_token(None)
    }

    pub fn router_with_api_token(self: Arc<Self>, api_token: Option<String>) -> Router {
        let state = CacheStateRouterState {
            service: self,
            api_token: api_token.map(Arc::from),
        };
        Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/cache_state/match_prefix", post(match_prefix))
            .route("/v1/cache_state/insert", post(insert_prefix))
            .route("/v1/cache_state/kv_events", post(kv_events))
            .with_state(state)
    }
}

fn sorted_workers(workers: HashSet<KvWorkerId>) -> Vec<CacheStateWorkerMatch> {
    let mut out: Vec<_> = workers
        .into_iter()
        .map(|w| CacheStateWorkerMatch {
            worker_url: w.url,
            dp_rank: w.dp_rank,
        })
        .collect();
    out.sort_by(|a, b| {
        a.worker_url
            .cmp(&b.worker_url)
            .then_with(|| a.dp_rank.cmp(&b.dp_rank))
    });
    out
}

async fn healthz() -> &'static str {
    "ok"
}

async fn match_prefix(
    State(state): State<CacheStateRouterState>,
    headers: HeaderMap,
    Json(req): Json<CacheStateMatchRequest>,
) -> Result<Json<CacheStateMatchResponse>, CacheStateError> {
    require_auth(&state, &headers)?;
    Ok(Json(state.service.match_prefix(&req)))
}

async fn insert_prefix(
    State(state): State<CacheStateRouterState>,
    headers: HeaderMap,
    Json(req): Json<CacheStateInsertRequest>,
) -> Result<StatusCode, CacheStateError> {
    require_auth(&state, &headers)?;
    state.service.insert(&req);
    Ok(StatusCode::NO_CONTENT)
}

async fn kv_events(
    State(state): State<CacheStateRouterState>,
    headers: HeaderMap,
    Json(req): Json<CacheStateKvEventsRequest>,
) -> Result<Json<CacheStateKvEventsResponse>, CacheStateError> {
    require_auth(&state, &headers)?;
    state.service.apply_kv_events(&req).map(Json)
}

#[derive(Debug)]
pub enum CacheStateError {
    Unauthorized,
    BadBase64(String),
    BadMsgpack(DecodeError),
}

impl IntoResponse for CacheStateError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            Self::BadBase64(err) => (
                StatusCode::BAD_REQUEST,
                format!("invalid payload_b64: {err}"),
            ),
            Self::BadMsgpack(err) => (
                StatusCode::BAD_REQUEST,
                format!("invalid KV event msgpack payload: {err}"),
            ),
        };
        (status, message).into_response()
    }
}

fn require_auth(state: &CacheStateRouterState, headers: &HeaderMap) -> Result<(), CacheStateError> {
    let Some(expected) = state.api_token.as_ref() else {
        return Ok(());
    };
    let Some(actual) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    else {
        return Err(CacheStateError::Unauthorized);
    };
    let Some(token) = actual.strip_prefix("Bearer ") else {
        return Err(CacheStateError::Unauthorized);
    };
    if token == expected.as_ref() {
        Ok(())
    } else {
        Err(CacheStateError::Unauthorized)
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>, String> {
    let bytes = input.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err("length is not a multiple of 4".into());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut chunk = [0u8; 4];
    for raw in bytes.chunks_exact(4) {
        for (i, b) in raw.iter().copied().enumerate() {
            chunk[i] = match b {
                b'A'..=b'Z' => b - b'A',
                b'a'..=b'z' => b - b'a' + 26,
                b'0'..=b'9' => b - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => 64,
                _ => return Err(format!("invalid byte 0x{b:02x}")),
            };
        }
        if chunk[0] == 64 || chunk[1] == 64 {
            return Err("padding in first two base64 positions".into());
        }
        out.push((chunk[0] << 2) | (chunk[1] >> 4));
        if chunk[2] != 64 {
            out.push((chunk[1] << 4) | (chunk[2] >> 2));
            if chunk[3] != 64 {
                out.push((chunk[2] << 6) | chunk[3]);
            }
        } else if chunk[3] != 64 {
            return Err("invalid single padding".into());
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct RemoteCacheStateClient {
    base_url: String,
    agent: ureq::Agent,
    api_token: Option<String>,
}

impl RemoteCacheStateClient {
    pub fn new(base_url: String, timeout: Duration) -> Self {
        let agent = ureq::AgentBuilder::new().timeout(timeout).build();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            agent,
            api_token: std::env::var("CACHE_STATE_API_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }

    pub fn match_prefix(&self, req: &CacheStateMatchRequest) -> Option<CacheStateMatchResponse> {
        let url = format!("{}/v1/cache_state/match_prefix", self.base_url);
        self.post(&url)
            .send_json(req)
            .ok()?
            .into_json::<CacheStateMatchResponse>()
            .ok()
    }

    pub fn insert(&self, req: &CacheStateInsertRequest) -> bool {
        let url = format!("{}/v1/cache_state/insert", self.base_url);
        self.post(&url)
            .send_json(req)
            .map(|resp| (200..300).contains(&resp.status()))
            .unwrap_or(false)
    }

    pub fn kv_events(&self, req: &CacheStateKvEventsRequest) -> bool {
        let url = format!("{}/v1/cache_state/kv_events", self.base_url);
        self.post(&url)
            .send_json(req)
            .map(|resp| (200..300).contains(&resp.status()))
            .unwrap_or(false)
    }

    fn post(&self, url: &str) -> ureq::Request {
        let req = self.agent.post(url);
        match self.api_token.as_ref() {
            Some(token) => req.set("Authorization", &format!("Bearer {token}")),
            None => req,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_match_returns_deepest_workers() {
        let service = CacheStateService::with_empty_tree();
        service.insert(&CacheStateInsertRequest {
            model_id: "m".into(),
            worker_url: "http://w0:30000".into(),
            dp_rank: 0,
            parent_hash: None,
            block_hashes: vec![1, 2, 3],
        });
        let matched = service.match_prefix(&CacheStateMatchRequest {
            model_id: "m".into(),
            block_hashes: vec![1, 2, 9],
        });
        assert_eq!(matched.matched_blocks, 2);
        assert_eq!(
            matched.workers,
            vec![CacheStateWorkerMatch {
                worker_url: "http://w0:30000".into(),
                dp_rank: 0,
            }]
        );
    }

    #[test]
    fn kv_events_ingest_updates_tree() {
        let service = CacheStateService::with_empty_tree();
        let payload = {
            let mut buf = Vec::new();
            rmp::encode::write_array_len(&mut buf, 3).unwrap();
            rmp::encode::write_f64(&mut buf, 1.0).unwrap();
            rmp::encode::write_array_len(&mut buf, 1).unwrap();
            rmp::encode::write_array_len(&mut buf, 7).unwrap();
            rmp::encode::write_str(&mut buf, "BlockStored").unwrap();
            rmp::encode::write_array_len(&mut buf, 2).unwrap();
            rmp::encode::write_sint(&mut buf, 10).unwrap();
            rmp::encode::write_sint(&mut buf, 20).unwrap();
            rmp::encode::write_nil(&mut buf).unwrap();
            rmp::encode::write_array_len(&mut buf, 0).unwrap();
            rmp::encode::write_uint(&mut buf, 64).unwrap();
            rmp::encode::write_nil(&mut buf).unwrap();
            rmp::encode::write_nil(&mut buf).unwrap();
            rmp::encode::write_uint(&mut buf, 1).unwrap();
            buf
        };
        let resp = service
            .apply_kv_events(&CacheStateKvEventsRequest {
                model_id: "m".into(),
                worker_url: "http://w0:30000".into(),
                dp_rank: 1,
                seq: 7,
                payload_b64: encode_base64_for_test(&payload),
            })
            .unwrap();
        assert_eq!(resp.applied_events, 1);

        let matched = service.match_prefix(&CacheStateMatchRequest {
            model_id: "m".into(),
            block_hashes: vec![10, 20, 30],
        });
        assert_eq!(matched.matched_blocks, 2);
        assert_eq!(
            matched.workers,
            vec![CacheStateWorkerMatch {
                worker_url: "http://w0:30000".into(),
                dp_rank: 1,
            }]
        );
    }

    fn encode_base64_for_test(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0];
            let b1 = *chunk.get(1).unwrap_or(&0);
            let b2 = *chunk.get(2).unwrap_or(&0);
            out.push(ALPHABET[(b0 >> 2) as usize] as char);
            out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
            if chunk.len() >= 2 {
                out.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() == 3 {
                out.push(ALPHABET[(b2 & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }
}

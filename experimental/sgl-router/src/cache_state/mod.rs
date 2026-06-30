// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Distributed cache-state service and client primitives.
//!
//! This module is intentionally small and HTTP-shaped for the first
//! production trial: the same router binary can run as a standalone
//! in-memory cache-state service, while gateway mode can query it as an
//! optional optimization. Query failures are surfaced as `None` to the
//! policy so routing degrades to a cache miss instead of failing requests.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::policies::kv_events::tree::{HashTree, KvWorkerId};

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

#[derive(Debug, Clone)]
pub struct CacheStateService {
    tree: Arc<HashTree>,
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

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(healthz))
            .route("/v1/cache_state/match_prefix", post(match_prefix))
            .route("/v1/cache_state/insert", post(insert_prefix))
            .with_state(self)
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
    State(service): State<Arc<CacheStateService>>,
    Json(req): Json<CacheStateMatchRequest>,
) -> Json<CacheStateMatchResponse> {
    Json(service.match_prefix(&req))
}

async fn insert_prefix(
    State(service): State<Arc<CacheStateService>>,
    Json(req): Json<CacheStateInsertRequest>,
) -> StatusCode {
    service.insert(&req);
    StatusCode::NO_CONTENT
}

#[derive(Debug, Clone)]
pub struct RemoteCacheStateClient {
    base_url: String,
    agent: ureq::Agent,
}

impl RemoteCacheStateClient {
    pub fn new(base_url: String, timeout: Duration) -> Self {
        let agent = ureq::AgentBuilder::new().timeout(timeout).build();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            agent,
        }
    }

    pub fn match_prefix(&self, req: &CacheStateMatchRequest) -> Option<CacheStateMatchResponse> {
        let url = format!("{}/v1/cache_state/match_prefix", self.base_url);
        self.agent
            .post(&url)
            .send_json(req)
            .ok()?
            .into_json::<CacheStateMatchResponse>()
            .ok()
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
}

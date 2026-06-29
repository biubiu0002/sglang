// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end HTTP coverage for reported-load pressure reservations.
//!
//! The router polls worker `/get_load` on a coarse interval in production.
//! This test pins both workers' remote load to the same stale value (0) and
//! sends two concurrent requests through the real Axum router. The first
//! selection creates a router-local pending reservation before proxy I/O; the
//! second selection must observe that reservation and pick the other worker.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use sgl_router::config::{
    ActiveLoadConfig, CacheAwareConfig, Config, DiscoveryBackend, ModelConfig, ObservabilityConfig,
    PolicyKind, ProxyConfig, ServerConfig, StaticUrlsDiscoveryConfig,
};
use sgl_router::discovery::{ModelId, WorkerId, WorkerMode, WorkerSpec};
use sgl_router::policies::factory::build_registry;
use sgl_router::policies::kv_events::{BlockSizeOracle, HashTree};
use sgl_router::proxy::Proxy;
use sgl_router::server::app::build_router;
use sgl_router::server::app_context::AppContext;
use sgl_router::tokenizer::TokenizerRegistry;
use sgl_router::workers::WorkerRegistry;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

use crate::common::mock_worker::MockWorker;

const MODEL: &str = "tiny";

fn config() -> Config {
    Config {
        server: ServerConfig {
            host: "0".into(),
            port: 0,
        },
        observability: ObservabilityConfig::default(),
        model: ModelConfig {
            id: MODEL.into(),
            tokenizer_path: "tests/fixtures/tiny_tokenizer.json".into(),
            policy: PolicyKind::CacheAwareZmq,
            circuit_breaker: None,
            cache_aware: Some(CacheAwareConfig {
                use_reported_load: true,
                ..CacheAwareConfig::default()
            }),
            sticky: None,
        },
        discovery: DiscoveryBackend::StaticUrls(StaticUrlsDiscoveryConfig {
            urls: vec!["http://placeholder:0".into()],
        }),
        proxy: ProxyConfig::default(),
        active_load: ActiveLoadConfig::default(),
        worker_introspect_key: None,
        load_poll_interval_secs: Some(2),
        cache_tree_page_size: None,
        cache_tree_bigram: false,
        cache_tree_max_nodes: 1_000_000,
    }
}

fn build_ctx(urls: [&str; 2]) -> Arc<AppContext> {
    let cfg = config();
    let tokenizers = Arc::new(TokenizerRegistry::default());
    let registry = Arc::new(WorkerRegistry::default());
    for (idx, url) in urls.iter().enumerate() {
        let id = WorkerId(format!("w{idx}"));
        registry
            .add(WorkerSpec {
                id: id.clone(),
                url: (*url).to_string(),
                mode: WorkerMode::Plain,
                model_ids: vec![ModelId(MODEL.into())],
                bootstrap_port: None,
                min_priority: None,
            })
            .unwrap();
        registry
            .get(&id)
            .expect("worker was registered")
            .set_reported_load(0);
    }

    let policies = Arc::new(
        build_registry(
            &cfg,
            Arc::new(HashTree::new()),
            Arc::clone(&tokenizers),
            BlockSizeOracle::new(),
        )
        .unwrap(),
    );
    let proxy = Arc::new(Proxy::new(Duration::from_secs(5)).unwrap());
    Arc::new(AppContext::new(cfg, tokenizers, proxy, registry, policies))
}

async fn send(app: axum::Router, body: Value) -> StatusCode {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    app.oneshot(req).await.unwrap().status()
}

fn captured(mock: &MockWorker) -> bool {
    mock.captured.lock().unwrap().last_body.is_some()
}

#[tokio::test]
async fn concurrent_reported_load_burst_uses_local_pending_pressure() {
    let a = MockWorker::start_hanging(Duration::from_millis(250)).await;
    let b = MockWorker::start_hanging(Duration::from_millis(250)).await;
    let ctx = build_ctx([&a.url, &b.url]);
    let app = build_router(ctx);

    let body = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "same stale load snapshot"}],
    });
    let (s1, s2) = tokio::join!(send(app.clone(), body.clone()), send(app, body));

    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert!(
        captured(&a) && captured(&b),
        "two concurrent requests with equal remote load should be split by local pending pressure",
    );
}

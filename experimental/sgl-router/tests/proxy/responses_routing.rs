// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the `/v1/responses` passthrough route.

use sgl_router::config::{
    ActiveLoadConfig, Config, DiscoveryBackend, ModelConfig, ObservabilityConfig, PolicyKind,
    ProxyConfig, ServerConfig, StaticUrlsDiscoveryConfig,
};
use sgl_router::discovery::{ModelId, WorkerId, WorkerMode, WorkerSpec};
use sgl_router::policies::factory::build_registry_with_defaults as build_policy_registry;
use sgl_router::proxy::Proxy;
use sgl_router::server::app::build_router;
use sgl_router::server::app_context::AppContext;
use sgl_router::tokenizer::TokenizerRegistry;
use sgl_router::workers::WorkerRegistry;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn build_ctx_with_worker(url: &str) -> Arc<AppContext> {
    let cfg = Config {
        runtime_mode: sgl_router::config::RuntimeMode::Gateway,
        server: ServerConfig {
            host: "0".into(),
            port: 0,
        },
        observability: ObservabilityConfig::default(),
        model: ModelConfig {
            id: "tiny".into(),
            tokenizer_path: "tests/fixtures/tiny_tokenizer.json".into(),
            policy: PolicyKind::RoundRobin,
            circuit_breaker: None,
            cache_aware: None,
            sticky: None,
        },
        discovery: DiscoveryBackend::StaticUrls(StaticUrlsDiscoveryConfig {
            urls: vec!["http://placeholder:0".into()],
            bearer_keys: Vec::new(),
        }),
        proxy: ProxyConfig::default(),
        active_load: ActiveLoadConfig::default(),
        trace: sgl_router::config::TraceConfig::default(),
        worker_introspect_key: None,
        load_poll_interval_secs: None,
        cache_tree_page_size: None,
        cache_tree_bigram: false,
        cache_tree_max_nodes: 1_000_000,
        cache_state_url: None,
        cache_state_timeout_ms: 20,
        alias_fallback: None,
    };
    let tokenizers = Arc::new(TokenizerRegistry::load_from_config(&cfg).unwrap());
    let registry = Arc::new(WorkerRegistry::default());
    let _ = registry.add(WorkerSpec {
        id: WorkerId("w1".into()),
        url: url.to_string(),
        mode: WorkerMode::Plain,
        model_ids: vec![ModelId("tiny".into())],
        bootstrap_port: None,
        min_priority: None,
        bearer_token: None,
    });
    let policies = Arc::new(build_policy_registry(&cfg).unwrap());
    let proxy = Arc::new(Proxy::new(TEST_TIMEOUT).unwrap());
    Arc::new(AppContext::new(cfg, tokenizers, proxy, registry, policies))
}

fn build_cache_aware_ctx_with_workers(urls: [&str; 2]) -> Arc<AppContext> {
    let cfg = Config {
        runtime_mode: sgl_router::config::RuntimeMode::Gateway,
        server: ServerConfig {
            host: "0".into(),
            port: 0,
        },
        observability: ObservabilityConfig::default(),
        model: ModelConfig {
            id: "tiny".into(),
            tokenizer_path: "tests/fixtures/tiny_tokenizer.json".into(),
            policy: PolicyKind::CacheAwareZmq,
            circuit_breaker: None,
            cache_aware: Some(sgl_router::config::CacheAwareConfig {
                cache_threshold: 0.0,
                balance_abs_threshold: usize::MAX,
                use_reported_load: true,
                tree_source: sgl_router::config::CacheTreeSource::RouteHistory,
                ..sgl_router::config::CacheAwareConfig::default()
            }),
            sticky: None,
        },
        discovery: DiscoveryBackend::StaticUrls(StaticUrlsDiscoveryConfig {
            urls: vec!["http://placeholder:0".into()],
            bearer_keys: Vec::new(),
        }),
        proxy: ProxyConfig::default(),
        active_load: ActiveLoadConfig::default(),
        trace: sgl_router::config::TraceConfig::default(),
        worker_introspect_key: None,
        load_poll_interval_secs: Some(2),
        cache_tree_page_size: Some(1),
        cache_tree_bigram: false,
        cache_tree_max_nodes: 1_000_000,
        cache_state_url: None,
        cache_state_timeout_ms: 20,
        alias_fallback: None,
    };
    let tokenizers = Arc::new(TokenizerRegistry::load_from_config(&cfg).unwrap());
    let registry = Arc::new(WorkerRegistry::default());
    for (idx, url) in urls.iter().enumerate() {
        let id = WorkerId(format!("w{idx}"));
        registry
            .add(WorkerSpec {
                id: id.clone(),
                url: (*url).to_string(),
                mode: WorkerMode::Plain,
                model_ids: vec![ModelId("tiny".into())],
                bootstrap_port: None,
                min_priority: None,
                bearer_token: None,
            })
            .unwrap();
        registry
            .get(&id)
            .expect("worker was registered")
            .set_reported_load(if idx == 0 { 0 } else { 10 });
    }
    let block_size_oracle = sgl_router::policies::kv_events::BlockSizeOracle::new();
    block_size_oracle
        .try_set(1)
        .expect("test block size should seed");
    let policies = Arc::new(
        sgl_router::policies::factory::build_registry(
            &cfg,
            Arc::new(sgl_router::policies::kv_events::HashTree::new()),
            Arc::clone(&tokenizers),
            block_size_oracle,
        )
        .unwrap(),
    );
    let proxy = Arc::new(Proxy::new(TEST_TIMEOUT).unwrap());
    Arc::new(AppContext::new(cfg, tokenizers, proxy, registry, policies))
}

async fn send_response(app: axum::Router, body: Value) -> StatusCode {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    app.oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn responses_body_forwarded_unchanged_no_input_ids() {
    let worker = crate::common::mock_worker::MockWorker::start(vec![]).await;
    let ctx = build_ctx_with_worker(&worker.url);
    let app = build_router(ctx);

    let sent = serde_json::to_vec(&json!({
        "model": "tiny",
        "instructions": "be brief",
        "input": "shared prefix",
        "max_output_tokens": 16,
        "stream": false,
    }))
    .unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(sent.clone()))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let forwarded = worker.captured.lock().unwrap().last_body.clone();
    let fwd: Value = serde_json::from_slice(&forwarded.unwrap()).unwrap();
    assert!(fwd.get("input_ids").is_none(), "must not inject input_ids");
    assert_eq!(fwd, serde_json::from_slice::<Value>(&sent).unwrap());
}

#[tokio::test]
async fn responses_prompt_tokens_feed_cache_aware_route_history() {
    let first = crate::common::mock_worker::MockWorker::start(vec![]).await;
    let second = crate::common::mock_worker::MockWorker::start(vec![]).await;
    let ctx = build_cache_aware_ctx_with_workers([&first.url, &second.url]);
    let app = build_router(ctx);

    let body = json!({
        "model": "tiny",
        "instructions": "shared responses system",
        "input": "repeatable responses prefix",
        "max_output_tokens": 16,
        "stream": false,
    });
    assert_eq!(
        send_response(app.clone(), body.clone()).await,
        StatusCode::OK
    );
    {
        let first_seen = first.captured.lock().unwrap().last_body.is_some();
        let second_seen = second.captured.lock().unwrap().last_body.is_some();
        assert!(
            first_seen && !second_seen,
            "first request should use min-load"
        );
    }

    first.captured.lock().unwrap().last_body = None;
    assert_eq!(send_response(app, body).await, StatusCode::OK);
    let first_seen = first.captured.lock().unwrap().last_body.is_some();
    let second_seen = second.captured.lock().unwrap().last_body.is_some();
    assert!(
        first_seen && !second_seen,
        "second request should reuse the route-history prefix hit"
    );
}

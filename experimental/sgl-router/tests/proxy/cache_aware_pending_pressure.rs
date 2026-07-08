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
use axum::response::Response;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sgl_router::config::{
    ActiveLoadConfig, CacheAwareConfig, Config, DiscoveryBackend, ExternalQueueAdmissionConfig,
    ModelConfig, ObservabilityConfig, PolicyKind, ProxyConfig, ServerConfig,
    StaticUrlsDiscoveryConfig,
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
        runtime_mode: sgl_router::config::RuntimeMode::Gateway,
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
            tiered_spillover: None,
            sticky: None,
        },
        discovery: DiscoveryBackend::StaticUrls(StaticUrlsDiscoveryConfig {
            urls: vec!["http://placeholder:0".into()],
            bearer_keys: Vec::new(),
        }),
        proxy: ProxyConfig::default(),
        active_load: ActiveLoadConfig::default(),
        trace: sgl_router::config::TraceConfig::default(),
        priority_override: sgl_router::config::PriorityOverrideConfig::default(),
        worker_introspect_key: None,
        load_poll_interval_secs: Some(2),
        cache_tree_page_size: None,
        cache_tree_bigram: false,
        cache_tree_max_nodes: 1_000_000,
        cache_state_url: None,
        cache_state_timeout_ms: 20,
        alias_fallback: None,
    }
}

fn ttft_first_config() -> Config {
    let mut cfg = config();
    cfg.model.cache_aware = Some(CacheAwareConfig {
        use_reported_load: true,
        ttft_first_routing: true,
        ttft_token_scale: 4,
        ttft_cache_score_margin: 0,
        ..CacheAwareConfig::default()
    });
    cfg
}

fn build_ctx(urls: [&str; 2]) -> Arc<AppContext> {
    build_ctx_with_config(urls, config())
}

fn build_ctx_with_config(urls: [&str; 2], cfg: Config) -> Arc<AppContext> {
    let specs = urls
        .iter()
        .enumerate()
        .map(|(idx, url)| (worker_spec(&format!("w{idx}"), url, None), 0))
        .collect();
    build_ctx_with_specs(cfg, specs)
}

fn build_ctx_with_specs(cfg: Config, specs: Vec<(WorkerSpec, i64)>) -> Arc<AppContext> {
    let tokenizers = Arc::new(TokenizerRegistry::default());
    let registry = Arc::new(WorkerRegistry::default());
    for (spec, reported_load) in specs {
        let id = spec.id.clone();
        registry.add(spec).unwrap();
        registry
            .get(&id)
            .expect("worker was registered")
            .set_reported_load(reported_load);
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

fn worker_spec(id: &str, url: &str, min_priority: Option<i64>) -> WorkerSpec {
    WorkerSpec {
        id: WorkerId(id.into()),
        url: url.to_string(),
        mode: WorkerMode::Plain,
        model_ids: vec![ModelId(MODEL.into())],
        bootstrap_port: None,
        min_priority,
        bearer_token: None,
        backend: Default::default(),
        tier: Default::default(),
    }
}

async fn send_response(app: axum::Router, body: Value) -> Response {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    app.oneshot(req).await.unwrap()
}

async fn send(app: axum::Router, body: Value) -> StatusCode {
    send_response(app, body).await.status()
}

async fn send_after(app: axum::Router, body: Value, delay: Duration) -> StatusCode {
    tokio::time::sleep(delay).await;
    send(app, body).await
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

#[tokio::test]
async fn ttft_first_burst_uses_token_weighted_local_pending_pressure() {
    let a = MockWorker::start_hanging(Duration::from_millis(250)).await;
    let b = MockWorker::start_hanging(Duration::from_millis(250)).await;
    let ctx = build_ctx_with_config([&a.url, &b.url], ttft_first_config());
    let app = build_router(ctx);

    let long_prompt = "long prompt ".repeat(128);
    let long_body = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": long_prompt}],
    });
    let short_body = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "short"}],
    });

    let (s1, s2) = tokio::join!(
        send(app.clone(), long_body),
        send_after(app, short_body, Duration::from_millis(25))
    );

    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert!(
        captured(&a) && captured(&b),
        "TTFT-first routing should see the long prompt's token-weighted local pending pressure and spill the next request",
    );
}

#[tokio::test]
async fn external_queue_admission_rejects_before_dispatch_using_priority_eligible_workers() {
    let eligible = MockWorker::start(Vec::new()).await;
    let reserved = MockWorker::start(Vec::new()).await;
    let mut cfg = config();
    cfg.proxy.external_queue_admission = ExternalQueueAdmissionConfig {
        enabled: true,
        queue_threshold: Some(0),
    };
    let ctx = build_ctx_with_specs(
        cfg,
        vec![
            (worker_spec("eligible", &eligible.url, None), 1),
            (worker_spec("reserved", &reserved.url, Some(100)), 0),
        ],
    );
    let app = build_router(ctx);

    let res = send_response(
        app,
        json!({
            "model": MODEL,
            "messages": [{"role": "user", "content": "low priority external request"}],
        }),
    )
    .await;

    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        res.headers()
            .get("x-router-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("external_queue_overloaded"),
    );
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert_eq!(body["error"]["code"], "external_queue_overloaded");
    assert!(
        !captured(&eligible) && !captured(&reserved),
        "admission rejection must happen before dispatch; ineligible low-load worker must not mask eligible overload",
    );
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use sgl_router::config::{
    ActiveLoadConfig, AliasFallbackConfig, Config, DiscoveryBackend, ModelConfig,
    ObservabilityConfig, PolicyKind, ProxyConfig, ServerConfig, StaticUrlsDiscoveryConfig,
};
use sgl_router::discovery::{ModelId, WorkerId, WorkerMode, WorkerSpec};
use sgl_router::health::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use sgl_router::policies::factory::build_registry_with_defaults as build_policy_registry;
use sgl_router::proxy::Proxy;
use sgl_router::server::app::build_router;
use sgl_router::server::app_context::AppContext;
use sgl_router::tokenizer::TokenizerRegistry;
use sgl_router::workers::WorkerRegistry;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

fn alias_config(primary_url: &str, fallback_url: &str) -> Config {
    Config {
        runtime_mode: sgl_router::config::RuntimeMode::Gateway,
        server: ServerConfig {
            host: "0".into(),
            port: 0,
        },
        observability: ObservabilityConfig::default(),
        model: ModelConfig {
            id: "primary".into(),
            tokenizer_path: "tests/fixtures/tiny_tokenizer.json".into(),
            policy: PolicyKind::RoundRobin,
            circuit_breaker: None,
            cache_aware: None,
            sticky: None,
        },
        discovery: DiscoveryBackend::StaticUrls(StaticUrlsDiscoveryConfig {
            urls: vec![primary_url.to_string()],
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
        alias_fallback: Some(AliasFallbackConfig {
            alias_model_id: "alias".into(),
            primary_model_id: "primary".into(),
            fallback_model_id: "fallback".into(),
            fallback_base_url: fallback_url.to_string(),
            fallback_bearer_token: None,
        }),
    }
}

fn build_alias_ctx(primary_url: &str, fallback_url: &str) -> Arc<AppContext> {
    let cfg = alias_config(primary_url, fallback_url);
    let tokenizers = Arc::new(TokenizerRegistry::load_from_config(&cfg).unwrap());
    let registry = Arc::new(WorkerRegistry::default());
    let _ = registry.add(WorkerSpec {
        id: WorkerId("primary".into()),
        url: primary_url.to_string(),
        mode: WorkerMode::Plain,
        model_ids: vec![ModelId("primary".into())],
        bootstrap_port: None,
        min_priority: None,
        bearer_token: None,
    });
    let policies = Arc::new(build_policy_registry(&cfg).unwrap());
    let proxy = Arc::new(Proxy::new(Duration::from_secs(5)).unwrap());
    Arc::new(AppContext::new(cfg, tokenizers, proxy, registry, policies))
}

#[tokio::test]
async fn alias_fallback_recovers_open_breaker_with_short_generation_probe() {
    let primary_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_url = format!("http://{}", primary_listener.local_addr().unwrap());
    drop(primary_listener);

    let fallback = crate::common::mock_worker::MockWorker::start(vec![]).await;
    let mut ctx = build_alias_ctx(&primary_url, &fallback.url);
    Arc::get_mut(&mut ctx)
        .expect("context is not shared before router construction")
        .alias_fallback_breaker = Some(Arc::new(CircuitBreaker::with_config(
        CircuitBreakerConfig {
            threshold: NonZeroU32::new(3).unwrap(),
            cool_down: Duration::from_millis(5),
        },
    )));
    let breaker = ctx
        .alias_fallback_breaker
        .as_ref()
        .expect("alias fallback configured")
        .clone();
    breaker.record_failure();
    breaker.record_failure();
    breaker.record_failure();
    assert!(
        !breaker.allow(),
        "test setup should start with an open alias fallback breaker"
    );

    tokio::time::sleep(Duration::from_millis(10)).await;

    let app = build_router(ctx);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "model": "alias",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": false
            }))
            .unwrap(),
        ))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let error_code = res
        .headers()
        .get("x-router-error-code")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "router error_code={error_code:?}, body={}",
        String::from_utf8_lossy(&body),
    );
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["model"], "fallback");
    assert!(
        breaker.would_allow(),
        "successful probe should close breaker"
    );

    let captured = fallback.captured.lock().unwrap();
    let last_body = captured
        .last_body
        .as_ref()
        .expect("fallback should receive the real user request");
    let last: serde_json::Value = serde_json::from_slice(last_body).unwrap();
    assert_eq!(last["model"], "fallback");
    assert_eq!(last["messages"][0]["content"], "hi");
}

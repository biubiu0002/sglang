// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use axum::body::Body;
use axum::http::Request;
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
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

fn build_test_app(cfg: Config, worker_url: String, bearer_token: Option<String>) -> axum::Router {
    let tokenizers = Arc::new(TokenizerRegistry::load_from_config(&cfg).unwrap());
    let registry = Arc::new(WorkerRegistry::default());
    let _ = registry.add(WorkerSpec {
        id: WorkerId("w1".into()),
        url: worker_url,
        mode: WorkerMode::Plain,
        model_ids: vec![ModelId("tiny".into())],
        bootstrap_port: None,
        min_priority: None,
        bearer_token,
        backend: Default::default(),
        tier: Default::default(),
    });
    let policies = Arc::new(build_policy_registry(&cfg).unwrap());
    let proxy = Arc::new(Proxy::new(Duration::from_secs(5)).unwrap());
    build_router(Arc::new(AppContext::new(
        cfg, tokenizers, proxy, registry, policies,
    )))
}

fn base_config() -> Config {
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
        load_poll_interval_secs: None,
        cache_tree_page_size: None,
        cache_tree_bigram: false,
        cache_tree_max_nodes: 1_000_000,
        cache_state_url: None,
        cache_state_timeout_ms: 20,
        alias_fallback: None,
    };
    cfg
}

#[tokio::test]
async fn forwards_whitelisted_headers_strips_others() {
    let worker = crate::common::mock_worker::MockWorker::start(vec![]).await;
    let app = build_test_app(base_config(), worker.url.clone(), None);

    let body = serde_json::to_vec(&serde_json::json!({
        "model":"tiny","messages":[{"role":"user","content":"hi"}]
    }))
    .unwrap();

    // Use a spoofed content-length that differs from the real body length so we
    // can distinguish "inbound value forwarded" from "reqwest auto-computed it".
    let spoofed_content_length = "99999";
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer test")
        .header("x-request-id", "abc-123")
        .header("x-trace-id", "trace-abc")
        .header("x-sgl-route-key", "k1")
        .header("cookie", "should-not-forward=true")
        .header("host", "example.com")
        .header("content-length", spoofed_content_length)
        .header("transfer-encoding", "chunked")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(
        res.headers()
            .get("x-trace-id")
            .and_then(|v| v.to_str().ok()),
        Some("trace-abc"),
        "router must echo the request trace id on the response",
    );

    let seen = worker.captured.lock().unwrap();
    // Whitelisted headers are forwarded with their inbound VALUES intact —
    // a regression that mangles, uppercases, or drops the value (e.g.,
    // forwarding the name but not the value) must fail this assertion.
    assert_eq!(
        seen.headers.get("authorization").map(String::as_str),
        Some("Bearer test"),
        "authorization must be forwarded with its inbound value verbatim",
    );
    assert_eq!(
        seen.headers.get("x-request-id").map(String::as_str),
        Some("abc-123"),
        "x-request-id must be forwarded with its inbound value verbatim",
    );
    assert_eq!(
        seen.headers.get("x-trace-id").map(String::as_str),
        Some("trace-abc"),
        "x-trace-id must be forwarded with its inbound value verbatim",
    );
    assert_eq!(
        seen.headers.get("x-sgl-route-key").map(String::as_str),
        Some("k1"),
        "x-sgl-route-key must be forwarded with its inbound value verbatim",
    );
    // Cookie must be stripped.
    assert!(!seen.seen.contains("cookie"));
    // transfer-encoding is hop-by-hop and must not be forwarded (reqwest does not
    // re-add it for a regular body, so absence check is reliable here).
    assert!(
        !seen.seen.contains("transfer-encoding"),
        "transfer-encoding is hop-by-hop and must be stripped"
    );
    // content-length: the inbound spoofed value must not reach the upstream.
    // reqwest may auto-compute its own content-length for the outbound body,
    // so we assert value-inequality rather than absence.
    assert_ne!(
        seen.headers.get("content-length").map(|s| s.as_str()),
        Some(spoofed_content_length),
        "router must not forward the inbound content-length value to upstream"
    );
    // Host: the inbound value must not reach the upstream.
    let captured_host: Option<&String> = seen.headers.get("host");
    assert_ne!(
        captured_host,
        Some(&"example.com".to_string()),
        "router must not forward the inbound Host header to upstream"
    );
}

#[tokio::test]
async fn worker_bearer_token_overrides_inbound_authorization() {
    let worker = crate::common::mock_worker::MockWorker::start(vec![]).await;
    let app = build_test_app(
        base_config(),
        worker.url.clone(),
        Some("worker-secret".into()),
    );

    let body = serde_json::to_vec(&serde_json::json!({
        "model":"tiny","messages":[{"role":"user","content":"hi"}]
    }))
    .unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer client-key")
        .body(Body::from(body))
        .unwrap();
    app.oneshot(req).await.unwrap();

    let seen = worker.captured.lock().unwrap();
    assert_eq!(
        seen.headers.get("authorization").map(String::as_str),
        Some("Bearer worker-secret"),
        "worker-local bearer token must override inbound client Authorization",
    );
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Priority-based worker eligibility — end-to-end at the HTTP layer.
//!
//! A worker tagged with `min_priority = N` is eligible only for requests
//! whose body `priority` is `>= N`. This exercises the full ingress path
//! (`parse_probe` → `effective_priority` → `filter_eligible` → policy
//! select → proxy) with `MockWorker` backends, asserting which worker
//! actually received the request via its captured body.
//!
//! Topology under test mirrors the production goal: one untagged worker
//! (a B200 that accepts anything) plus one `min_priority=100` worker (an
//! RTX-6000 reserved for high-priority production traffic).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use sgl_router::config::{
    ActiveLoadConfig, Config, DiscoveryBackend, ModelConfig, ObservabilityConfig, PolicyKind,
    ProxyConfig, ServerConfig, StaticUrlsDiscoveryConfig, TieredSpilloverConfig,
};
use sgl_router::discovery::{ModelId, WorkerBackend, WorkerId, WorkerMode, WorkerSpec, WorkerTier};
use sgl_router::policies::factory::build_registry_with_defaults;
use sgl_router::proxy::Proxy;
use sgl_router::server::app::build_router;
use sgl_router::server::app_context::AppContext;
use sgl_router::tokenizer::TokenizerRegistry;
use sgl_router::workers::WorkerRegistry;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

use crate::common::mock_worker::MockWorker;

fn config() -> Config {
    Config {
        runtime_mode: sgl_router::config::RuntimeMode::Gateway,
        server: ServerConfig {
            host: "0".into(),
            port: 0,
        },
        observability: ObservabilityConfig::default(),
        model: ModelConfig {
            id: "tiny".into(),
            tokenizer_path: "tests/fixtures/tiny_tokenizer.json".into(),
            // RoundRobin: load-agnostic, so the ONLY reason a request lands
            // on one worker vs another is eligibility filtering — exactly
            // what we want to pin.
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
    }
}

fn build_ctx(specs: Vec<WorkerSpec>) -> Arc<AppContext> {
    let cfg = config();
    build_ctx_with_config(cfg, specs)
}

fn build_ctx_with_config(cfg: Config, specs: Vec<WorkerSpec>) -> Arc<AppContext> {
    let tokenizers = Arc::new(TokenizerRegistry::load_from_config(&cfg).unwrap());
    let registry = Arc::new(WorkerRegistry::default());
    for s in specs {
        let _ = registry.add(s);
    }
    let policies = Arc::new(build_registry_with_defaults(&cfg).unwrap());
    let proxy = Arc::new(Proxy::new(Duration::from_secs(5)).unwrap());
    Arc::new(AppContext::new(cfg, tokenizers, proxy, registry, policies))
}

fn plain_spec(id: &str, url: &str, min_priority: Option<i64>) -> WorkerSpec {
    spec_with_backend(id, url, min_priority, WorkerBackend::Sglang)
}

fn vllm_spec(id: &str, url: &str, min_priority: Option<i64>) -> WorkerSpec {
    spec_with_backend(id, url, min_priority, WorkerBackend::Vllm)
}

fn spec_with_backend(
    id: &str,
    url: &str,
    min_priority: Option<i64>,
    backend: WorkerBackend,
) -> WorkerSpec {
    WorkerSpec {
        id: WorkerId(id.into()),
        url: url.into(),
        mode: WorkerMode::Plain,
        model_ids: vec![ModelId("tiny".into())],
        bootstrap_port: None,
        min_priority,
        bearer_token: None,
        backend,
        tier: WorkerTier::Default,
    }
}

fn forced_priority(value: i64) -> Config {
    let mut cfg = config();
    cfg.priority_override.force_request_priority = Some(value);
    cfg
}

fn captured_priority(w: &MockWorker) -> Option<i64> {
    let body = w.captured.lock().unwrap().last_body.clone()?;
    let value: serde_json::Value = serde_json::from_slice(&body).ok()?;
    value.get("priority").and_then(|v| v.as_i64())
}

fn chat_request(priority: Option<i64>) -> Request<Body> {
    let mut body = serde_json::json!({
        "model": "tiny",
        "messages": [{"role": "user", "content": "hi"}],
    });
    if let Some(p) = priority {
        body["priority"] = serde_json::json!(p);
    }
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn responses_request(priority: Option<i64>) -> Request<Body> {
    let mut body = serde_json::json!({
        "model": "tiny",
        "input": "hi",
        "max_output_tokens": 8,
        "stream": false,
    });
    if let Some(p) = priority {
        body["priority"] = serde_json::json!(p);
    }
    Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn messages_request(priority: Option<i64>) -> Request<Body> {
    let mut body = serde_json::json!({
        "model": "tiny",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 8,
        "stream": false,
    });
    if let Some(p) = priority {
        body["priority"] = serde_json::json!(p);
    }
    Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn was_hit(w: &MockWorker) -> bool {
    w.captured.lock().unwrap().last_body.is_some()
}

/// A low-priority (here: absent → 0) request must NEVER land on a
/// `min_priority=100` worker when an untagged worker is available — even
/// across many round-robin turns that would otherwise alternate.
#[tokio::test]
async fn low_priority_request_never_hits_gated_worker() {
    let untagged = MockWorker::start(vec![]).await;
    let gated = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![
        plain_spec("untagged", &untagged.url, None),
        plain_spec("gated", &gated.url, Some(100)),
    ]);

    // Several requests: round-robin would hit `gated` on alternate turns
    // if it were eligible. It must never be selected.
    for _ in 0..6 {
        let app = build_router(Arc::clone(&ctx));
        let res = app.oneshot(chat_request(None)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    assert!(
        was_hit(&untagged),
        "untagged worker should serve all traffic"
    );
    assert!(
        !was_hit(&gated),
        "gated (min_priority=100) worker must not receive priority-0 traffic",
    );
}

/// A high-priority (`priority=100`) request is eligible for the gated
/// worker. With round-robin over two eligible workers, the gated worker
/// must receive at least one of several requests.
#[tokio::test]
async fn high_priority_request_can_hit_gated_worker() {
    let untagged = MockWorker::start(vec![]).await;
    let gated = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![
        plain_spec("untagged", &untagged.url, None),
        plain_spec("gated", &gated.url, Some(100)),
    ]);

    for _ in 0..6 {
        let app = build_router(Arc::clone(&ctx));
        let res = app.oneshot(chat_request(Some(100))).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    assert!(
        was_hit(&gated),
        "gated worker must be eligible for priority-100 traffic and get a round-robin turn",
    );
}

/// Boundary: `priority == min_priority` is eligible (the rule is `>=`).
#[tokio::test]
async fn priority_equal_to_threshold_is_eligible() {
    let gated = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![plain_spec("gated", &gated.url, Some(100))]);

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(chat_request(Some(100))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(was_hit(&gated), "priority == min_priority must be eligible");
}

/// Hard isolation on empty set: when the ONLY healthy worker is gated above
/// the request's priority, the request is REJECTED (503) rather than spilled
/// onto the gated worker. Keeping a long internal (priority-0) request off a
/// small-context worker matters more than serving it — the gated worker is
/// exactly the capacity this request must never touch. The request must NOT
/// land on the gated worker.
#[tokio::test]
async fn empty_eligible_set_is_rejected_not_served() {
    let gated = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![plain_spec("gated", &gated.url, Some(100))]);

    // priority 0 (absent) qualifies for no worker → hard rejection.
    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(chat_request(None)).await.unwrap();
    assert_eq!(
        res.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a sub-threshold request with only gated capacity must be 503'd, not served",
    );
    assert!(
        !was_hit(&gated),
        "the gated worker must NOT receive a sub-threshold request, even as a last resort",
    );
}

#[tokio::test]
async fn low_priority_request_never_hits_gated_vllm_worker() {
    let untagged = MockWorker::start(vec![]).await;
    let vllm = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![
        plain_spec("untagged", &untagged.url, None),
        vllm_spec("h20-vllm", &vllm.url, Some(100)),
    ]);

    for _ in 0..6 {
        let app = build_router(Arc::clone(&ctx));
        let res = app.oneshot(chat_request(None)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    assert!(was_hit(&untagged));
    assert!(
        !was_hit(&vllm),
        "priority-0 traffic must not reach a gated vLLM worker"
    );
}

#[tokio::test]
async fn low_priority_only_gated_vllm_is_rejected() {
    let vllm = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![vllm_spec("h20-vllm", &vllm.url, Some(100))]);

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(chat_request(None)).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        !was_hit(&vllm),
        "sub-threshold traffic must not spill onto the only gated vLLM worker"
    );
}

#[tokio::test]
async fn high_priority_chat_can_hit_gated_vllm_worker() {
    let vllm = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![vllm_spec("h20-vllm", &vllm.url, Some(100))]);

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(chat_request(Some(100))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(was_hit(&vllm));
}

#[tokio::test]
async fn high_priority_responses_can_hit_gated_vllm_worker() {
    let vllm = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![vllm_spec("h20-vllm", &vllm.url, Some(100))]);

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(responses_request(Some(100))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(was_hit(&vllm));
}

#[tokio::test]
async fn high_priority_messages_can_hit_gated_vllm_worker() {
    let vllm = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![vllm_spec("h20-vllm", &vllm.url, Some(100))]);

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(messages_request(Some(100))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(was_hit(&vllm));
}

#[tokio::test]
async fn forced_priority_overrides_client_priority_before_routing_and_forwarding() {
    let bulk = MockWorker::start(vec![]).await;
    let gated = MockWorker::start(vec![]).await;
    let ctx = build_ctx_with_config(
        forced_priority(0),
        vec![
            plain_spec("bulk", &bulk.url, None),
            plain_spec("gated", &gated.url, Some(100)),
        ],
    );

    for _ in 0..4 {
        let app = build_router(Arc::clone(&ctx));
        let res = app.oneshot(chat_request(Some(100))).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    assert!(was_hit(&bulk));
    assert!(!was_hit(&gated));
    assert_eq!(captured_priority(&bulk), Some(0));
}

#[tokio::test]
async fn tiered_spillover_prefers_bulk_until_primary_pressure_crosses_threshold() {
    let bulk = MockWorker::start(vec![]).await;
    let shared = MockWorker::start(vec![]).await;
    let mut bulk_spec = vllm_spec("bulk-h20", &bulk.url, None);
    bulk_spec.tier = WorkerTier::Bulk;
    let mut shared_spec = plain_spec("shared-b200", &shared.url, None);
    shared_spec.tier = WorkerTier::Shared;

    let mut cfg = forced_priority(0);
    cfg.model.policy = PolicyKind::TieredSpillover;
    cfg.model.tiered_spillover = Some(TieredSpilloverConfig {
        primary_pressure_threshold: 0,
        ..TieredSpilloverConfig::default()
    });
    let ctx = build_ctx_with_config(cfg, vec![bulk_spec, shared_spec]);

    let bulk_worker = ctx
        .registry
        .all()
        .into_iter()
        .find(|w| w.tier() == WorkerTier::Bulk)
        .unwrap();

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(chat_request(Some(100))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(was_hit(&bulk));
    assert!(!was_hit(&shared));
    assert_eq!(captured_priority(&bulk), Some(0));

    let _pressure = bulk_worker.pending_guard_with_tokens(128);
    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(chat_request(Some(100))).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(was_hit(&shared));
    assert_eq!(captured_priority(&shared), Some(0));
}

/// Ordering regression: a request for a model that has registered workers but
/// NO policy entry must surface as 404 `ModelNotFound`, NOT 503 — even when
/// the only registered worker is gated above the request priority. The
/// eligibility filter's empty-set 503 must not mask the earlier "model not
/// served here" failure. (See codex super-review round 4.)
#[tokio::test]
async fn unknown_model_with_gated_worker_is_404_not_503() {
    let gated = MockWorker::start(vec![]).await;
    // Register a gated worker under a model id the policy registry doesn't
    // know about ("ghost"), while the config only builds a policy for "tiny".
    let spec = WorkerSpec {
        id: WorkerId("ghost-gated".into()),
        url: gated.url.clone(),
        mode: WorkerMode::Plain,
        model_ids: vec![ModelId("ghost".into())],
        bootstrap_port: None,
        min_priority: Some(100),
        bearer_token: None,
        backend: Default::default(),
        tier: Default::default(),
    };
    let ctx = build_ctx(vec![spec]);

    // Sub-threshold (absent → 0) request for the unknown model.
    let mut body = serde_json::json!({
        "model": "ghost",
        "messages": [{"role": "user", "content": "hi"}],
    });
    body["priority"] = serde_json::json!(0);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let app = build_router(Arc::clone(&ctx));
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(
        res.status(),
        StatusCode::NOT_FOUND,
        "unknown model must 404 before the priority filter can 503",
    );
    assert!(
        !was_hit(&gated),
        "an unknown-model request must not reach any worker"
    );
}

/// A malformed (non-integer) `priority` is treated as `0`, NOT rejected —
/// so it is excluded from a gated worker exactly like an absent priority.
#[tokio::test]
async fn malformed_priority_treated_as_low() {
    let untagged = MockWorker::start(vec![]).await;
    let gated = MockWorker::start(vec![]).await;
    let ctx = build_ctx(vec![
        plain_spec("untagged", &untagged.url, None),
        plain_spec("gated", &gated.url, Some(100)),
    ]);

    let body = serde_json::json!({
        "model": "tiny",
        "messages": [{"role": "user", "content": "hi"}],
        "priority": "definitely-not-an-int",
    });
    for _ in 0..6 {
        let app = build_router(Arc::clone(&ctx));
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    assert!(was_hit(&untagged));
    assert!(
        !was_hit(&gated),
        "string priority must coerce to 0 and stay off the gated worker",
    );
}

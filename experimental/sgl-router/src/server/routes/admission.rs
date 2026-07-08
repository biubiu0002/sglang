// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Router-side admission control hooks.

use crate::server::app_context::AppContext;
use crate::server::error::ApiError;
use crate::server::metrics::ExternalQueueAdmissionOutcome;
use crate::workers::Worker;
use std::sync::Arc;

/// Reject external traffic before policy selection when every eligible worker
/// is already above the configured effective-load threshold.
pub(crate) fn enforce_external_queue_admission(
    ctx: &AppContext,
    model: &str,
    workers: &[Arc<Worker>],
) -> Result<(), ApiError> {
    let cfg = &ctx.config.proxy.external_queue_admission;
    if !cfg.enabled {
        return Ok(());
    }
    let Some(threshold) = cfg.queue_threshold else {
        return Ok(());
    };
    if workers.is_empty() {
        return Ok(());
    }

    let use_reported = ctx.config.load_poll_interval_secs.is_some();
    let min_load = workers
        .iter()
        .map(|worker| worker.effective_load(use_reported))
        .min()
        .unwrap_or(usize::MAX);

    if min_load > threshold {
        ctx.metrics
            .record_external_queue_admission(ExternalQueueAdmissionOutcome::Rejected);
        tracing::warn!(
            model,
            eligible_workers = workers.len(),
            min_effective_load = min_load,
            threshold,
            "external queue admission rejected request",
        );
        return Err(ApiError::ExternalQueueOverloaded {
            model: model.to_string(),
        });
    }

    ctx.metrics
        .record_external_queue_admission(ExternalQueueAdmissionOutcome::Admitted);
    tracing::debug!(
        model,
        eligible_workers = workers.len(),
        min_effective_load = min_load,
        threshold,
        "external queue admission admitted request",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ActiveLoadConfig, Config, DiscoveryBackend, ExternalQueueAdmissionConfig, ModelConfig,
        ObservabilityConfig, PolicyKind, PriorityOverrideConfig, ProxyConfig, RuntimeMode,
        ServerConfig, StaticUrlsDiscoveryConfig, TraceConfig,
    };
    use crate::discovery::{ModelId, WorkerId, WorkerMode, WorkerSpec};
    use crate::policies::factory::build_registry;
    use crate::policies::kv_events::{BlockSizeOracle, HashTree};
    use crate::proxy::Proxy;
    use crate::server::app_context::AppContext;
    use crate::tokenizer::TokenizerRegistry;
    use crate::workers::{Worker, WorkerRegistry};
    use std::time::Duration;

    const MODEL: &str = "tiny";

    fn base_config(enabled: bool, threshold: Option<usize>) -> Config {
        Config {
            runtime_mode: RuntimeMode::Gateway,
            server: ServerConfig {
                host: "0".into(),
                port: 0,
            },
            observability: ObservabilityConfig::default(),
            model: ModelConfig {
                id: MODEL.into(),
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
            proxy: ProxyConfig {
                external_queue_admission: ExternalQueueAdmissionConfig {
                    enabled,
                    queue_threshold: threshold,
                },
                ..ProxyConfig::default()
            },
            active_load: ActiveLoadConfig::default(),
            trace: TraceConfig::default(),
            priority_override: PriorityOverrideConfig::default(),
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

    fn worker(id: &str, reported_load: i64, pending: usize) -> Arc<Worker> {
        let w = Arc::new(Worker::new(WorkerSpec {
            id: WorkerId(id.into()),
            url: format!("http://{id}:30000"),
            mode: WorkerMode::Plain,
            model_ids: vec![ModelId(MODEL.into())],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        }));
        w.set_reported_load(reported_load);
        for _ in 0..pending {
            std::mem::forget(w.pending_guard());
        }
        w
    }

    fn ctx(enabled: bool, threshold: Option<usize>) -> AppContext {
        let cfg = base_config(enabled, threshold);
        let tokenizers = Arc::new(TokenizerRegistry::default());
        let registry = Arc::new(WorkerRegistry::default());
        let policies = Arc::new(
            build_registry(
                &cfg,
                Arc::new(HashTree::new()),
                Arc::clone(&tokenizers),
                BlockSizeOracle::new(),
            )
            .unwrap(),
        );
        let proxy = Arc::new(Proxy::new(Duration::from_secs(1)).unwrap());
        AppContext::new(cfg, tokenizers, proxy, registry, policies)
    }

    #[test]
    fn disabled_config_never_rejects() {
        let ctx = ctx(false, Some(0));
        let workers = vec![worker("w0", 99, 0)];

        enforce_external_queue_admission(&ctx, MODEL, &workers).unwrap();
    }

    #[test]
    fn rejects_when_all_workers_exceed_threshold() {
        let ctx = ctx(true, Some(5));
        let workers = vec![worker("w0", 6, 0), worker("w1", 5, 1)];

        let err = enforce_external_queue_admission(&ctx, MODEL, &workers).unwrap_err();
        assert!(matches!(err, ApiError::ExternalQueueOverloaded { .. }));
    }

    #[test]
    fn admits_when_one_worker_is_within_threshold() {
        let ctx = ctx(true, Some(5));
        let workers = vec![worker("w0", 6, 0), worker("w1", 5, 0)];

        enforce_external_queue_admission(&ctx, MODEL, &workers).unwrap();
    }

    #[test]
    fn reported_load_failure_is_over_threshold() {
        let ctx = ctx(true, Some(10_000));
        let workers = vec![worker(
            "w0",
            crate::workers::worker::REPORTED_LOAD_FAILED,
            0,
        )];

        let err = enforce_external_queue_admission(&ctx, MODEL, &workers).unwrap_err();
        assert!(matches!(err, ApiError::ExternalQueueOverloaded { .. }));
    }
}

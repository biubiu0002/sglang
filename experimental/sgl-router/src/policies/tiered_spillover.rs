// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use crate::config::TieredSpilloverConfig;
use crate::policies::cache_aware_zmq::CacheAwareZmqPolicy;
use crate::policies::{Policy, SelectionContext};
use crate::server::metrics::MetricsRegistry;
use crate::workers::Worker;
use std::sync::Arc;

/// Prefer one worker tier and borrow another tier only when the preferred
/// tier is under TTFT pressure.
#[derive(Debug, Clone, Copy)]
pub struct TieredSpilloverPolicy {
    cfg: TieredSpilloverConfig,
}

impl TieredSpilloverPolicy {
    pub fn new(cfg: TieredSpilloverConfig) -> Self {
        Self { cfg }
    }

    fn pressure(&self, worker: &Worker) -> usize {
        worker.effective_ttft_load(self.cfg.use_reported_load, self.cfg.pressure_token_scale)
    }

    fn pick_min_pressure<'a>(
        &self,
        workers: impl Iterator<Item = &'a Arc<Worker>>,
    ) -> Option<Arc<Worker>> {
        workers.min_by_key(|w| self.pressure(w)).cloned()
    }
}

/// Cache-aware primary-tier borrowing with a hard pressure guard and a
/// spillover fallback tier.
#[derive(Debug)]
pub struct CacheAwareSpilloverPolicy {
    cfg: TieredSpilloverConfig,
    primary: CacheAwareZmqPolicy,
}

impl CacheAwareSpilloverPolicy {
    pub fn new(cfg: TieredSpilloverConfig, primary: CacheAwareZmqPolicy) -> Self {
        Self { cfg, primary }
    }

    fn pressure(&self, worker: &Worker) -> usize {
        worker.effective_ttft_load(self.cfg.use_reported_load, self.cfg.pressure_token_scale)
    }

    fn pick_min_pressure<'a>(
        &self,
        workers: impl Iterator<Item = &'a Arc<Worker>>,
    ) -> Option<Arc<Worker>> {
        workers.min_by_key(|w| self.pressure(w)).cloned()
    }

    fn tier_candidates(
        &self,
        workers: &[Arc<Worker>],
        tier: crate::discovery::WorkerTier,
    ) -> Vec<Arc<Worker>> {
        workers
            .iter()
            .filter(|w| w.tier() == tier)
            .cloned()
            .collect()
    }
}

impl Policy for CacheAwareSpilloverPolicy {
    fn select(&self, workers: &[Arc<Worker>], ctx: &SelectionContext<'_>) -> Option<Arc<Worker>> {
        let primary_candidates = self.tier_candidates(workers, self.cfg.primary_tier);
        let spillover_candidates = self.tier_candidates(workers, self.cfg.spillover_tier);

        let primary_allowed = primary_candidates
            .iter()
            .map(|w| self.pressure(w))
            .min()
            .is_some_and(|p| p <= self.cfg.primary_pressure_threshold);

        if primary_allowed {
            if let Some(chosen) = self
                .primary
                .select_from_candidates(&primary_candidates, ctx)
            {
                return Some(chosen);
            }
        }

        self.pick_min_pressure(spillover_candidates.iter())
            .or_else(|| {
                self.primary
                    .select_from_candidates(&primary_candidates, ctx)
            })
            .or_else(|| self.pick_min_pressure(workers.iter()))
    }

    fn needs_request_tokens(&self) -> bool {
        true
    }

    fn attach_metrics(&self, metrics: Arc<MetricsRegistry>) {
        self.primary.attach_metrics(metrics);
    }
}

impl Policy for TieredSpilloverPolicy {
    fn select(&self, workers: &[Arc<Worker>], _ctx: &SelectionContext<'_>) -> Option<Arc<Worker>> {
        let primary =
            self.pick_min_pressure(workers.iter().filter(|w| w.tier() == self.cfg.primary_tier));
        let spillover = self.pick_min_pressure(
            workers
                .iter()
                .filter(|w| w.tier() == self.cfg.spillover_tier),
        );

        match (primary, spillover) {
            (Some(primary), Some(spillover))
                if self.pressure(&primary) > self.cfg.primary_pressure_threshold =>
            {
                Some(spillover)
            }
            (Some(primary), _) => Some(primary),
            (None, Some(spillover)) => Some(spillover),
            (None, None) => self.pick_min_pressure(workers.iter()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CacheAwareConfig, CacheTreeSource, TieredSpilloverConfig};
    use crate::discovery::{ModelId, WorkerId, WorkerMode, WorkerSpec, WorkerTier};
    use crate::policies::kv_events::BlockSizeOracle;
    use crate::policies::kv_events::HashTree;
    use crate::tokenizer::TokenizerRegistry;

    fn worker(id: &str, tier: WorkerTier) -> Arc<Worker> {
        Arc::new(Worker::new(WorkerSpec {
            id: WorkerId(id.into()),
            url: format!("http://{id}:30000"),
            mode: WorkerMode::Plain,
            model_ids: vec![ModelId("tiny".into())],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier,
        }))
    }

    fn cache_aware_spillover(threshold: usize) -> CacheAwareSpilloverPolicy {
        let oracle = BlockSizeOracle::new();
        oracle.try_set(64).unwrap();
        CacheAwareSpilloverPolicy::new(
            TieredSpilloverConfig {
                primary_tier: WorkerTier::Shared,
                spillover_tier: WorkerTier::Bulk,
                primary_pressure_threshold: threshold,
                use_reported_load: true,
                pressure_token_scale: 64,
            },
            CacheAwareZmqPolicy::new(
                CacheAwareConfig {
                    tree_source: CacheTreeSource::RouteHistory,
                    ttft_first_routing: true,
                    use_reported_load: true,
                    ..CacheAwareConfig::default()
                },
                Arc::new(HashTree::new()),
                Arc::new(TokenizerRegistry::default()),
                oracle,
            ),
        )
    }

    fn ctx<'a>(model: &'a ModelId) -> SelectionContext<'a> {
        SelectionContext::new(model, None)
    }

    #[test]
    fn prefers_primary_tier_when_under_threshold() {
        let policy = TieredSpilloverPolicy::new(TieredSpilloverConfig {
            primary_pressure_threshold: 1,
            ..TieredSpilloverConfig::default()
        });
        let model = ModelId("tiny".into());
        let bulk = worker("bulk", WorkerTier::Bulk);
        let shared = worker("shared", WorkerTier::Shared);

        let selected = policy
            .select(&[Arc::clone(&shared), Arc::clone(&bulk)], &ctx(&model))
            .unwrap();
        assert_eq!(selected.id, bulk.id);
    }

    #[test]
    fn spills_to_shared_when_primary_exceeds_threshold() {
        let policy = TieredSpilloverPolicy::new(TieredSpilloverConfig {
            primary_pressure_threshold: 0,
            pressure_token_scale: 64,
            ..TieredSpilloverConfig::default()
        });
        let model = ModelId("tiny".into());
        let bulk = worker("bulk", WorkerTier::Bulk);
        let shared = worker("shared", WorkerTier::Shared);
        let _bulk_pending = bulk.pending_guard_with_tokens(65);

        let selected = policy
            .select(&[Arc::clone(&bulk), Arc::clone(&shared)], &ctx(&model))
            .unwrap();
        assert_eq!(selected.id, shared.id);
    }

    #[test]
    fn uses_primary_when_shared_is_absent_even_above_threshold() {
        let policy = TieredSpilloverPolicy::new(TieredSpilloverConfig {
            primary_pressure_threshold: 0,
            ..TieredSpilloverConfig::default()
        });
        let model = ModelId("tiny".into());
        let bulk = worker("bulk", WorkerTier::Bulk);
        let _bulk_pending = bulk.pending_guard();

        let selected = policy.select(&[Arc::clone(&bulk)], &ctx(&model)).unwrap();
        assert_eq!(selected.id, bulk.id);
    }

    #[test]
    fn cache_aware_spillover_uses_primary_tier_under_pressure_guard() {
        let policy = cache_aware_spillover(1);
        let model = ModelId("tiny".into());
        let bulk = worker("bulk", WorkerTier::Bulk);
        let shared = worker("shared", WorkerTier::Shared);

        let selected = policy
            .select(&[Arc::clone(&bulk), Arc::clone(&shared)], &ctx(&model))
            .unwrap();
        assert_eq!(selected.id, shared.id);
    }

    #[test]
    fn cache_aware_spillover_falls_back_when_primary_exceeds_guard() {
        let policy = cache_aware_spillover(0);
        let model = ModelId("tiny".into());
        let bulk = worker("bulk", WorkerTier::Bulk);
        let shared = worker("shared", WorkerTier::Shared);
        shared.set_reported_load(1);

        let selected = policy
            .select(&[Arc::clone(&shared), Arc::clone(&bulk)], &ctx(&model))
            .unwrap();
        assert_eq!(selected.id, bulk.id);
    }
}

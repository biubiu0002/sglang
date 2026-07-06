// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use crate::discovery::{ModelId, WorkerBackend, WorkerId, WorkerMode, WorkerTier};
use crate::health::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::router_state::RouterStateLoadOverlay;
use axum::http::{header, HeaderMap, HeaderValue};
use std::borrow::Cow;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

/// Parse a host from a worker URL. Matches SMG's `worker_builder.rs`
/// fallback chain: parse as-is, retry with `http://` prefix if missing,
/// fall back to `"localhost"` if both fail. The fallback is defensive —
/// discovery code should never emit an unparsable URL — but a panic
/// here would crash the whole router on a single bad config entry.
fn parse_bootstrap_host(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        if let Some(h) = parsed.host_str() {
            return h.to_string();
        }
    }
    if !url.contains("://") {
        if let Ok(parsed) = url::Url::parse(&format!("http://{url}")) {
            if let Some(h) = parsed.host_str() {
                return h.to_string();
            }
        }
    }
    tracing::warn!(
        worker_url = %url,
        "Failed to parse worker URL for bootstrap_host; defaulting to 'localhost'"
    );
    "localhost".to_string()
}

/// RAII guard that increments `active_requests` on construction and
/// decrements on drop.  Obtain via [`Worker::load_guard`].
///
/// `#[must_use]`: a statement-form call like `worker.load_guard();` would
/// drop the guard on the same line, so the counter would never see the
/// in-flight request.  The compile-time warning catches that misuse.
#[must_use = "LoadGuard must be held for the request's lifetime; dropping it immediately decrements active_requests"]
pub struct LoadGuard {
    counter: Arc<AtomicUsize>,
}

impl LoadGuard {
    pub(crate) fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII guard for router-local dispatch reservations. Unlike
/// `active_requests`, this counter is used only to bridge the gap between a
/// router-side selection decision and the next worker `/get_load` poll. It is
/// part of `effective_load(use_reported = true)` so bursty requests do not all
/// route on the same stale remote snapshot.
#[must_use = "PendingLoadGuard must be held for the request's lifetime; dropping it immediately clears the local reservation"]
pub struct PendingLoadGuard {
    counter: Arc<AtomicUsize>,
    token_counter: Arc<AtomicUsize>,
    tokens: usize,
}

impl PendingLoadGuard {
    pub(crate) fn with_tokens(
        counter: Arc<AtomicUsize>,
        token_counter: Arc<AtomicUsize>,
        tokens: usize,
    ) -> Self {
        let tokens = tokens.max(1);
        counter.fetch_add(1, Ordering::Relaxed);
        token_counter.fetch_add(tokens, Ordering::Relaxed);
        Self {
            counter,
            token_counter,
            tokens,
        }
    }
}

impl Drop for PendingLoadGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
        self.token_counter.fetch_sub(self.tokens, Ordering::Relaxed);
    }
}

impl WorkerMode {
    fn as_u8(self) -> u8 {
        match self {
            WorkerMode::Plain => 0,
            WorkerMode::Prefill => 1,
            WorkerMode::Decode => 2,
        }
    }

    /// Inverse of [`Self::as_u8`].  The only writers of the underlying
    /// `AtomicU8` are `as_u8`-derived values, so any out-of-range byte
    /// indicates memory corruption or a stale store from an
    /// incompatible build — fail loudly rather than silently mislabel
    /// the worker as `Decode`.
    fn from_u8(v: u8) -> Self {
        match v {
            0 => WorkerMode::Plain,
            1 => WorkerMode::Prefill,
            2 => WorkerMode::Decode,
            other => unreachable!("invalid WorkerMode discriminant {other}"),
        }
    }
}

pub struct Worker {
    pub id: WorkerId,
    pub url: String,
    /// Interior-mutable mode so `ModeChanged` can update in place without
    /// dropping the Worker (which would reset `active_requests` + breaker).
    mode: AtomicU8,
    pub model_ids: Vec<ModelId>,
    pub breaker: Arc<CircuitBreaker>,
    pub active_requests: Arc<AtomicUsize>,
    /// Router-local reservations made at worker selection time. This is
    /// intentionally separate from `active_requests`: reported-load routing
    /// uses worker-side queue depth as its remote signal, then adds this local
    /// pending count so concurrent selections inside the poll interval see
    /// each other immediately.
    pending_requests: Arc<AtomicUsize>,
    /// Token-weighted form of `pending_requests`. Route handlers reserve the
    /// prompt-token count at selection time so a long prefill immediately
    /// contributes more local TTFT pressure than a short request.
    pending_tokens: Arc<AtomicUsize>,
    /// Hostname parsed from `url` at construction time and cached.
    /// Used as the `bootstrap_host` field on PD-disagg requests so the
    /// prefill engine can match incoming KV-transfer requests from
    /// decode peers. Falls back to `"localhost"` if the URL fails to
    /// parse — a misconfigured worker will fail the prefill request
    /// downstream rather than panic here.
    bootstrap_host: String,
    /// SGLang bootstrap server port for prefill workers (`None` for
    /// decode and plain). Set via `--disaggregation-bootstrap-port` at
    /// worker startup; carried from `WorkerSpec`.
    bootstrap_port: Option<u16>,
    /// Minimum request priority this worker accepts (`None` = any). A
    /// request with effective priority below this value is filtered out
    /// of the candidate set before policy selection. Carried from
    /// `WorkerSpec`; see [`crate::discovery::WorkerSpec::min_priority`].
    min_priority: Option<i64>,
    /// Serving backend. Determines which worker-control endpoints the
    /// router may call; e.g. vLLM workers do not expose SGLang `/get_load`
    /// or KV event metadata.
    backend: WorkerBackend,
    /// Operator-defined routing tier used by tier-aware policies.
    tier: WorkerTier,
    /// Worker-reported real load (from the background load poller hitting
    /// the worker's `/get_load`). Decoupled from `active_requests`
    /// (router-side in-flight count), which is a poor signal for a mixed
    /// short/long workload. Sentinel values:
    ///   `REPORTED_LOAD_UNSET` (-1): no real data — poller disabled, or not
    ///     polled yet → consumers fall back to `active_load()`.
    ///   `REPORTED_LOAD_FAILED` (-2): last poll failed (timeout/401/parse)
    ///     → consumers treat this worker as HIGH load (don't spill onto it).
    ///   `>= 0`: real load signal (e.g. summed `num_waiting_reqs`).
    /// `Arc<AtomicI64>` so the poller updates it lock-free without a
    /// registry write-lock.
    reported_load: Arc<AtomicI64>,
    /// Optional global pending snapshot from the single-writer router-state
    /// service. Present only in multi-replica gateway deployments.
    global_pending: Option<Arc<RouterStateLoadOverlay>>,
    bearer_token: Option<String>,
}

/// `reported_load` sentinel: no real data (poller off / not yet polled).
/// Consumers fall back to the router-side in-flight `active_load()`.
pub const REPORTED_LOAD_UNSET: i64 = -1;
/// `reported_load` sentinel: last poll failed. Consumers treat the worker
/// as HIGH load so spill-to-idle never routes onto a possibly-dead worker.
pub const REPORTED_LOAD_FAILED: i64 = -2;

impl Worker {
    pub fn new(spec: crate::discovery::WorkerSpec) -> Self {
        Self::with_cb_config(spec, None)
    }

    /// Construct a worker with an explicit circuit-breaker configuration.
    /// Pass `None` to use the default config (threshold = 3, cool_down = 30 s).
    pub fn with_cb_config(
        spec: crate::discovery::WorkerSpec,
        cb: Option<CircuitBreakerConfig>,
    ) -> Self {
        let breaker = match cb {
            Some(cfg) => Arc::new(CircuitBreaker::with_config(cfg)),
            None => Arc::new(CircuitBreaker::new()),
        };
        let bootstrap_host = parse_bootstrap_host(&spec.url);
        Self {
            id: spec.id,
            url: spec.url,
            mode: AtomicU8::new(spec.mode.as_u8()),
            model_ids: spec.model_ids,
            breaker,
            active_requests: Arc::new(AtomicUsize::new(0)),
            pending_requests: Arc::new(AtomicUsize::new(0)),
            pending_tokens: Arc::new(AtomicUsize::new(0)),
            bootstrap_host,
            bootstrap_port: spec.bootstrap_port,
            min_priority: spec.min_priority,
            backend: spec.backend,
            tier: spec.tier,
            reported_load: Arc::new(AtomicI64::new(REPORTED_LOAD_UNSET)),
            global_pending: None,
            bearer_token: spec.bearer_token,
        }
    }

    pub fn attach_router_state_overlay(&mut self, overlay: Arc<RouterStateLoadOverlay>) {
        self.global_pending = Some(overlay);
    }

    /// Hostname carried on PD-disagg request bodies as `bootstrap_host`.
    pub fn bootstrap_host(&self) -> &str {
        &self.bootstrap_host
    }

    /// SGLang bootstrap server port. `None` for decode / plain workers.
    pub fn bootstrap_port(&self) -> Option<u16> {
        self.bootstrap_port
    }

    /// Minimum request priority this worker accepts. `None` means the
    /// worker accepts any request; `Some(N)` means it is eligible only for
    /// requests whose effective priority is `>= N`. Consumed by the
    /// pre-selection eligibility filter (see
    /// [`crate::policies::registry::filter_eligible`]).
    pub fn min_priority(&self) -> Option<i64> {
        self.min_priority
    }

    pub fn backend(&self) -> WorkerBackend {
        self.backend
    }

    pub fn tier(&self) -> WorkerTier {
        self.tier
    }

    /// Optional per-worker bearer token. Shared-key pools leave this unset
    /// and forward the inbound client Authorization header unchanged.
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    /// Headers to use for an upstream request to this worker. For legacy
    /// per-worker-key pools this clones the inbound header map and replaces
    /// Authorization with the worker's own key; otherwise it borrows the
    /// inbound headers unchanged.
    pub fn headers_for<'a>(
        &self,
        headers: &'a HeaderMap,
    ) -> Result<Cow<'a, HeaderMap>, header::InvalidHeaderValue> {
        let Some(token) = self.bearer_token() else {
            return Ok(Cow::Borrowed(headers));
        };
        let mut outbound = headers.clone();
        let mut value = HeaderValue::from_str(&format!("Bearer {token}"))?;
        value.set_sensitive(true);
        outbound.insert(header::AUTHORIZATION, value);
        Ok(Cow::Owned(outbound))
    }

    /// Returns the current [`WorkerMode`] of this worker.
    ///
    /// Uses `Relaxed` ordering: mode changes are rare discovery events and do
    /// not need to synchronise with any other memory access.
    pub fn mode(&self) -> WorkerMode {
        WorkerMode::from_u8(self.mode.load(Ordering::Relaxed))
    }

    /// Update the worker's mode in place.
    ///
    /// Preserves `active_requests` and `breaker` state — the same `Arc<Worker>`
    /// identity survives the mode transition.
    pub fn set_mode(&self, m: WorkerMode) {
        self.mode.store(m.as_u8(), Ordering::Relaxed);
    }

    pub fn active_load(&self) -> usize {
        self.active_requests.load(Ordering::Relaxed)
    }

    pub fn pending_load(&self) -> usize {
        self.pending_requests.load(Ordering::Relaxed)
    }

    pub fn pending_token_load(&self) -> usize {
        self.pending_tokens.load(Ordering::Relaxed)
    }

    pub fn global_pending_load(&self) -> usize {
        self.global_pending
            .as_ref()
            .map(|overlay| overlay.pending_requests(&self.url))
            .unwrap_or(0)
    }

    pub fn global_pending_token_load(&self) -> usize {
        self.global_pending
            .as_ref()
            .map(|overlay| overlay.pending_tokens(&self.url))
            .unwrap_or(0)
    }

    /// Worker-reported real load, or a sentinel (`REPORTED_LOAD_UNSET` /
    /// `REPORTED_LOAD_FAILED`). Updated by the background load poller.
    pub fn reported_load(&self) -> i64 {
        self.reported_load.load(Ordering::Relaxed)
    }

    /// Set the worker-reported real load (called by the load poller). Pass
    /// a sentinel (`REPORTED_LOAD_FAILED`) on poll failure.
    pub fn set_reported_load(&self, v: i64) {
        self.reported_load.store(v, Ordering::Relaxed);
    }

    /// Effective load for routing decisions, honoring the configured load
    /// source. When `use_reported` is false (poller disabled), use router-side
    /// in-flight plus pending reservations. When true: the real reported load if
    /// available (`>= 0`) plus router-local pending reservations; on poll
    /// failure (`REPORTED_LOAD_FAILED`) a very
    /// high value so spill-to-idle never targets a possibly-dead worker;
    /// before the first successful poll (`REPORTED_LOAD_UNSET`) fall back to
    /// in-flight plus pending so a just-started router still routes sanely.
    pub fn effective_load(&self, use_reported: bool) -> usize {
        let pending = self
            .pending_load()
            .saturating_add(self.global_pending_load());
        if !use_reported {
            return self.active_load().saturating_add(pending);
        }
        match self.reported_load() {
            REPORTED_LOAD_FAILED => usize::MAX / 2, // treat unreachable as very busy
            REPORTED_LOAD_UNSET => self.active_load().saturating_add(pending),
            v if v >= 0 => (v as usize).saturating_add(pending),
            _ => self.active_load().saturating_add(pending),
        }
    }

    /// TTFT-oriented load for routing decisions. This keeps the same remote
    /// load semantics as `effective_load(use_reported)`, but replaces the
    /// request-count local pending term with token-weighted pressure units.
    pub fn effective_ttft_load(&self, use_reported: bool, token_scale: usize) -> usize {
        let scale = token_scale.max(1);
        let pending_tokens = self
            .pending_token_load()
            .saturating_add(self.global_pending_token_load());
        let token_units = pending_tokens.saturating_add(scale - 1) / scale;
        if !use_reported {
            return self.active_load().saturating_add(token_units);
        }
        match self.reported_load() {
            REPORTED_LOAD_FAILED => usize::MAX / 2,
            REPORTED_LOAD_UNSET => self.active_load().saturating_add(token_units),
            v if v >= 0 => (v as usize).saturating_add(token_units),
            _ => self.active_load().saturating_add(token_units),
        }
    }

    /// Returns a RAII guard that increments `active_requests` now and
    /// decrements when the guard is dropped.
    pub fn load_guard(&self) -> LoadGuard {
        LoadGuard::new(self.active_requests.clone())
    }

    /// Returns a RAII guard that increments router-local pending load now and
    /// decrements when the guard is dropped.
    pub fn pending_guard(&self) -> PendingLoadGuard {
        self.pending_guard_with_tokens(1)
    }

    /// Returns a RAII guard that reserves one local pending request and the
    /// routed prompt-token count for TTFT-first pressure decisions.
    pub fn pending_guard_with_tokens(&self, tokens: usize) -> PendingLoadGuard {
        PendingLoadGuard::with_tokens(
            self.pending_requests.clone(),
            self.pending_tokens.clone(),
            tokens,
        )
    }
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("backend", &self.backend)
            .field("tier", &self.tier)
            .field("mode", &self.mode())
            .field("active_load", &self.active_load())
            .field("pending_load", &self.pending_load())
            .field("pending_token_load", &self.pending_token_load())
            .field("global_pending_load", &self.global_pending_load())
            .field(
                "global_pending_token_load",
                &self.global_pending_token_load(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{ModelId, WorkerId, WorkerMode, WorkerSpec};
    use crate::router_state::{
        RouterStateLoadOverlay, RouterStateSnapshotResponse, RouterStateWorkerLoad,
    };

    #[test]
    fn load_guard_increments_and_decrements() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("w".into()),
            url: "http://x".into(),
            mode: WorkerMode::Plain,
            model_ids: vec![ModelId("m".into())],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.active_load(), 0);
        let g = w.load_guard();
        assert_eq!(w.active_load(), 1);
        let g2 = w.load_guard();
        assert_eq!(w.active_load(), 2);
        drop(g);
        assert_eq!(w.active_load(), 1);
        drop(g2);
        assert_eq!(w.active_load(), 0);
    }

    #[test]
    fn effective_load_honors_source_and_sentinels() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("w".into()),
            url: "http://x".into(),
            mode: WorkerMode::Plain,
            model_ids: vec![],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        // Seed router-side in-flight = 2.
        let _g1 = w.load_guard();
        let _g2 = w.load_guard();
        assert_eq!(w.active_load(), 2);

        // Poller disabled: in-flight plus pending, ignoring reported_load.
        w.set_reported_load(99);
        assert_eq!(w.effective_load(false), 2);

        // Poller enabled, real value present: use it.
        w.set_reported_load(7);
        assert_eq!(w.effective_load(true), 7);

        // Router-local pending reservations bridge the poll interval when
        // real load is present.
        let pending = w.pending_guard();
        assert_eq!(w.pending_load(), 1);
        assert_eq!(w.effective_load(false), 3);
        assert_eq!(w.effective_load(true), 8);
        drop(pending);
        assert_eq!(w.pending_load(), 0);
        assert_eq!(w.effective_load(false), 2);
        assert_eq!(w.effective_load(true), 7);

        // Poller enabled, UNSET sentinel: fall back to in-flight plus pending.
        w.set_reported_load(REPORTED_LOAD_UNSET);
        let pending = w.pending_guard();
        assert_eq!(w.effective_load(true), 3);
        drop(pending);
        assert_eq!(w.effective_load(true), 2);

        // Poller enabled, FAILED sentinel: treat as very-high load so
        // spill-to-idle never targets a possibly-dead worker.
        w.set_reported_load(REPORTED_LOAD_FAILED);
        assert_eq!(w.effective_load(true), usize::MAX / 2);
    }

    #[test]
    fn effective_ttft_load_uses_token_weighted_pending_pressure() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("w".into()),
            url: "http://x".into(),
            mode: WorkerMode::Plain,
            model_ids: vec![],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });

        w.set_reported_load(2);
        let pending = w.pending_guard_with_tokens(130);
        assert_eq!(w.pending_load(), 1);
        assert_eq!(w.pending_token_load(), 130);
        assert_eq!(
            w.effective_load(true),
            3,
            "legacy load still counts one local pending request"
        );
        assert_eq!(
            w.effective_ttft_load(true, 64),
            5,
            "TTFT load counts ceil(130 / 64) = 3 local token units plus reported load 2"
        );
        drop(pending);
        assert_eq!(w.pending_load(), 0);
        assert_eq!(w.pending_token_load(), 0);
        assert_eq!(w.effective_ttft_load(true, 64), 2);
    }

    #[test]
    fn effective_load_includes_router_state_overlay_pending() {
        let mut w = Worker::new(WorkerSpec {
            id: WorkerId("w".into()),
            url: "http://x".into(),
            mode: WorkerMode::Plain,
            model_ids: vec![],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        let overlay = RouterStateLoadOverlay::new();
        overlay.update(RouterStateSnapshotResponse {
            workers: [(
                "http://x".to_string(),
                RouterStateWorkerLoad {
                    pending_requests: 3,
                    pending_tokens: 130,
                },
            )]
            .into_iter()
            .collect(),
        });
        w.attach_router_state_overlay(overlay);
        w.set_reported_load(2);

        assert_eq!(w.global_pending_load(), 3);
        assert_eq!(w.effective_load(false), 3);
        assert_eq!(w.effective_load(true), 5);
        assert_eq!(w.effective_ttft_load(true, 64), 5);
    }

    #[test]
    fn mode_accessor_round_trips_all_variants() {
        for m in [WorkerMode::Plain, WorkerMode::Prefill, WorkerMode::Decode] {
            let w = Worker::new(WorkerSpec {
                id: WorkerId("w".into()),
                url: "http://x".into(),
                mode: m,
                model_ids: vec![],
                bootstrap_port: None,
                min_priority: None,
                bearer_token: None,
                backend: Default::default(),
                tier: Default::default(),
            });
            assert_eq!(w.mode(), m);
        }
    }

    #[test]
    fn set_mode_updates_in_place() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("w".into()),
            url: "http://x".into(),
            mode: WorkerMode::Prefill,
            model_ids: vec![],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.mode(), WorkerMode::Prefill);
        w.set_mode(WorkerMode::Decode);
        assert_eq!(w.mode(), WorkerMode::Decode);
        w.set_mode(WorkerMode::Plain);
        assert_eq!(w.mode(), WorkerMode::Plain);
    }

    #[test]
    fn bootstrap_port_returns_spec_value_for_prefill() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("p1".into()),
            url: "http://10.0.0.1:30000".into(),
            mode: WorkerMode::Prefill,
            model_ids: vec![ModelId("m".into())],
            bootstrap_port: Some(8997),
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.bootstrap_port(), Some(8997));
    }

    #[test]
    fn bootstrap_port_defaults_to_none() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("w".into()),
            url: "http://10.0.0.1:30000".into(),
            mode: WorkerMode::Plain,
            model_ids: vec![],
            bootstrap_port: None,
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.bootstrap_port(), None);
    }

    #[test]
    fn bootstrap_host_parses_ipv4_from_url() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("p1".into()),
            url: "http://10.0.0.1:30000".into(),
            mode: WorkerMode::Prefill,
            model_ids: vec![],
            bootstrap_port: Some(8997),
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.bootstrap_host(), "10.0.0.1");
    }

    #[test]
    fn bootstrap_host_parses_dns_name_from_url() {
        let w = Worker::new(WorkerSpec {
            id: WorkerId("p1".into()),
            url: "http://prefill-0.svc.cluster.local:30000".into(),
            mode: WorkerMode::Prefill,
            model_ids: vec![],
            bootstrap_port: Some(8997),
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.bootstrap_host(), "prefill-0.svc.cluster.local");
    }

    #[test]
    fn bootstrap_host_falls_back_to_localhost_for_unparsable_url() {
        // An empty / invalid URL is not expected from discovery, but the
        // accessor must return a usable string rather than panic — the
        // prefill worker will reject the request body-side if the host
        // really is unreachable.
        let w = Worker::new(WorkerSpec {
            id: WorkerId("p1".into()),
            url: "not a url".into(),
            mode: WorkerMode::Prefill,
            model_ids: vec![],
            bootstrap_port: Some(8997),
            min_priority: None,
            bearer_token: None,
            backend: Default::default(),
            tier: Default::default(),
        });
        assert_eq!(w.bootstrap_host(), "localhost");
    }
}

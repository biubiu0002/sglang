// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Static-URL discovery backend.
//!
//! Takes a fixed list of worker URLs and fans one [`DiscoveryEvent::Added`]
//! per entry. After the initial fan-out the task exits — there is no
//! hot-reload; topology changes require a restart.
//!
//! Each emitted [`WorkerSpec`] uses the URL itself as the `WorkerId` and
//! seeds `mode = Plain` with empty `model_ids` and `bootstrap_port = None`.
//! The worker manager fills those in from each worker's `/server_info`
//! response (see [`crate::workers::introspect`]) and overrides the seeded
//! mode/bootstrap when the worker self-discloses a PD role — so prefill,
//! decode, and plain workers can all appear in the same `urls` list and
//! end up classified correctly.
//!
//! Requires modern SGLang that exposes `disaggregation_mode` in
//! `/server_info`. Workers on older SGLang versions that predate that
//! field stay seeded as `Plain` because the manager has no signal to
//! override with — operators running PD with such a worker should use
//! the K8s backend (which can still classify via pod labels).

use crate::config::StaticUrlsDiscoveryConfig;
use crate::discovery::{
    DiscoveryEvent, WorkerBackend, WorkerId, WorkerMode, WorkerSpec, WorkerTier,
};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Token separating a worker URL from an optional minimum-priority
/// capability suffix in `--worker-urls` entries:
/// `http://host:port@min_priority=100`. A distinctive literal (not a bare
/// `@`) so it cannot collide with URL userinfo (`user:pass@host`).
const MIN_PRIORITY_TOKEN: &str = "@min_priority=";
const BACKEND_TOKEN: &str = "@backend=";
const TIER_TOKEN: &str = "@tier=";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerCapabilities {
    pub min_priority: Option<i64>,
    pub backend: WorkerBackend,
    pub tier: WorkerTier,
}

impl Default for WorkerCapabilities {
    fn default() -> Self {
        Self {
            min_priority: None,
            backend: WorkerBackend::Sglang,
            tier: WorkerTier::Default,
        }
    }
}

/// Split a `--worker-urls` entry into its base URL and optional
/// `min_priority` capability. `http://h:p@min_priority=100` yields
/// `("http://h:p", Some(100))`; a plain URL yields `(url, None)`.
/// A deployment-only `@tier=...` suffix is stripped and ignored.
///
/// A present-but-unparseable suffix (e.g. `@min_priority=abc`) is a config
/// error the caller surfaces, rather than silently dropping the isolation
/// guarantee — a worker that should be priority-gated must never fall back
/// to "accept everything" because of a typo. `rsplit_once` so a `@` inside
/// the URL (userinfo) doesn't get mistaken for the capability token.
///
/// `pub(crate)` so config validation ([`crate::config::Config::validate`])
/// can strip the suffix BEFORE URL-parsing/deduping the base URL — otherwise
/// validation would run against the raw suffixed string and `url::Url` would
/// misparse `host:port@min_priority=N` as userinfo, letting malformed base
/// URLs and with/without-suffix duplicates slip past startup checks.
pub(crate) fn parse_worker_entry(entry: &str) -> Result<(String, WorkerCapabilities)> {
    let mut base = entry;
    let mut caps = WorkerCapabilities::default();
    loop {
        let Some((pos, token)) = [MIN_PRIORITY_TOKEN, BACKEND_TOKEN, TIER_TOKEN]
            .into_iter()
            .filter_map(|token| base.rfind(token).map(|pos| (pos, token)))
            .max_by_key(|(pos, _)| *pos)
        else {
            break;
        };
        let value = &base[pos + token.len()..];
        base = &base[..pos];
        if token == MIN_PRIORITY_TOKEN {
            let prio = value.trim().parse::<i64>().map_err(|_| {
                anyhow::anyhow!(
                    "invalid min_priority in worker URL entry {entry:?}: \
                     {value:?} is not an integer"
                )
            })?;
            caps.min_priority = Some(prio);
        } else if token == BACKEND_TOKEN {
            caps.backend = match value.trim() {
                "sglang" => WorkerBackend::Sglang,
                "vllm" => WorkerBackend::Vllm,
                other => {
                    return Err(anyhow::anyhow!(
                        "invalid backend in worker URL entry {entry:?}: \
                         {other:?} is not one of: sglang, vllm"
                    ));
                }
            };
        } else {
            caps.tier = match value.trim() {
                "default" => WorkerTier::Default,
                "bulk" => WorkerTier::Bulk,
                "shared" => WorkerTier::Shared,
                other => {
                    return Err(anyhow::anyhow!(
                        "invalid tier in worker URL entry {entry:?}: \
                         {other:?} is not one of: default, bulk, shared"
                    ));
                }
            };
        }
    }
    Ok((base.to_string(), caps))
}

/// Normalize worker URLs for config-key matching. This mirrors
/// `Config::validate`: strip capability suffixes, parse as an HTTP URL, and
/// drop a trailing slash so `http://x:30000` and `http://x:30000/` match the
/// same bearer-key entry.
pub(crate) fn normalize_worker_url(entry: &str) -> Result<String> {
    let (base, _caps) = parse_worker_entry(entry)?;
    let parsed = url::Url::parse(&base)
        .map_err(|e| anyhow!("worker URL entry {entry:?} is not a valid URL: {e}"))?;
    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

/// Spawn the static-URLs producer task and return its `JoinHandle`.
///
/// Returns `Result` for parity with [`crate::discovery::k8s::spawn`] (which
/// can fail to construct a `kube::Client`) AND because a malformed
/// `@min_priority=` suffix is rejected here rather than ignored.
pub async fn spawn(
    cfg: StaticUrlsDiscoveryConfig,
    tx: mpsc::Sender<DiscoveryEvent>,
) -> Result<tokio::task::JoinHandle<()>> {
    // Parse + validate every entry up front so a bad suffix fails startup
    // loudly instead of after the task is detached.
    let parsed: Vec<(String, WorkerCapabilities)> = cfg
        .urls
        .iter()
        .map(|e| parse_worker_entry(e))
        .collect::<Result<_>>()?;
    let bearer_keys: HashMap<String, String> = cfg
        .bearer_keys
        .iter()
        .map(|entry| {
            Ok((
                normalize_worker_url(&entry.worker_url)?,
                entry.bearer_token.clone(),
            ))
        })
        .collect::<Result<_>>()?;
    let handle = tokio::spawn(async move {
        for (url, caps) in parsed {
            let bearer_token = normalize_worker_url(&url)
                .ok()
                .and_then(|normalized| bearer_keys.get(&normalized).cloned());
            let spec = WorkerSpec {
                id: WorkerId(url.clone()),
                url,
                mode: WorkerMode::Plain,
                model_ids: Vec::new(),
                bootstrap_port: None,
                min_priority: caps.min_priority,
                bearer_token,
                backend: caps.backend,
                tier: caps.tier,
            };
            if tx.send(DiscoveryEvent::Added(spec)).await.is_err() {
                tracing::info!(
                    "static_urls discovery: event channel closed during fan-out; exiting"
                );
                return;
            }
        }
        tracing::debug!(
            "static_urls discovery: initial fan-out complete; parking until channel closes"
        );
        // After fan-out the static backend has no further work — but
        // `server::supervisor::supervise_critical_tasks` treats *any*
        // discovery exit as fatal and flips `/readyz` to 503. Park here
        // until the consumer drops the receiver. `tx.closed()` resolves
        // the moment every `Receiver` has been dropped; the supervisor's
        // normal-shutdown path aborts this task before that. So
        // reaching the `info!` below means either (a) we lost the abort
        // race during a clean shutdown, or (b) the worker manager exited
        // unexpectedly — in case (b) the supervisor will catch the
        // subsequent discovery-task exit and `error!` + mark unready,
        // and this breadcrumb gives operator triage a starting point.
        tx.closed().await;
        tracing::info!(
            "static_urls discovery: event channel closed by receiver \
             (worker manager dropped its end, or shutdown abort raced); exiting"
        );
    });
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_entry_plain_url_has_no_min_priority() {
        let (url, caps) = parse_worker_entry("http://w0:30000").unwrap();
        assert_eq!(url, "http://w0:30000");
        assert_eq!(caps.min_priority, None);
        assert_eq!(caps.backend, WorkerBackend::Sglang);
        assert_eq!(caps.tier, WorkerTier::Default);
    }

    #[test]
    fn parse_entry_extracts_min_priority_suffix() {
        let (url, caps) = parse_worker_entry("http://rtx-01:30000@min_priority=100").unwrap();
        assert_eq!(url, "http://rtx-01:30000");
        assert_eq!(caps.min_priority, Some(100));
        assert_eq!(caps.backend, WorkerBackend::Sglang);
        assert_eq!(caps.tier, WorkerTier::Default);
    }

    #[test]
    fn parse_entry_extracts_vllm_backend_suffix() {
        let (url, caps) = parse_worker_entry("http://h20-r0:8006@backend=vllm").unwrap();
        assert_eq!(url, "http://h20-r0:8006");
        assert_eq!(caps.backend, WorkerBackend::Vllm);
        assert_eq!(caps.min_priority, None);
        assert_eq!(caps.tier, WorkerTier::Default);
    }

    #[test]
    fn parse_entry_extracts_tier_suffix() {
        let (url, caps) = parse_worker_entry("http://h20-r0:8006@tier=bulk").unwrap();
        assert_eq!(url, "http://h20-r0:8006");
        assert_eq!(caps.tier, WorkerTier::Bulk);
        assert_eq!(caps.backend, WorkerBackend::Sglang);
        assert_eq!(caps.min_priority, None);
    }

    #[test]
    fn parse_entry_extracts_combined_suffixes_in_either_order() {
        let (url, caps) =
            parse_worker_entry("http://h20-r0:8006@backend=vllm@tier=bulk@min_priority=100")
                .unwrap();
        assert_eq!(url, "http://h20-r0:8006");
        assert_eq!(caps.backend, WorkerBackend::Vllm);
        assert_eq!(caps.tier, WorkerTier::Bulk);
        assert_eq!(caps.min_priority, Some(100));

        let (url, caps) =
            parse_worker_entry("http://h20-r0:8006@min_priority=100@tier=bulk@backend=vllm")
                .unwrap();
        assert_eq!(url, "http://h20-r0:8006");
        assert_eq!(caps.backend, WorkerBackend::Vllm);
        assert_eq!(caps.tier, WorkerTier::Bulk);
        assert_eq!(caps.min_priority, Some(100));
    }

    #[test]
    fn parse_entry_strips_tier_suffix() {
        let (url, caps) = parse_worker_entry("http://b200-01:10100@tier=shared").unwrap();
        assert_eq!(url, "http://b200-01:10100");
        assert_eq!(caps.min_priority, None);
        assert_eq!(caps.tier, WorkerTier::Shared);
        assert_eq!(caps.backend, WorkerBackend::Sglang);
    }

    #[test]
    fn parse_entry_strips_tier_after_min_priority() {
        let (url, caps) =
            parse_worker_entry("http://rtx-01:30000@min_priority=100@tier=shared").unwrap();
        assert_eq!(url, "http://rtx-01:30000");
        assert_eq!(caps.min_priority, Some(100));
        assert_eq!(caps.tier, WorkerTier::Shared);
        assert_eq!(caps.backend, WorkerBackend::Sglang);
    }

    #[test]
    fn parse_entry_rejects_empty_tier_suffix() {
        let err = parse_worker_entry("http://w:30000@tier=")
            .unwrap_err()
            .to_string();
        assert!(err.contains("tier"), "got: {err}");
    }

    #[test]
    fn parse_entry_rejects_non_integer_min_priority() {
        let err = parse_worker_entry("http://w:30000@min_priority=high")
            .unwrap_err()
            .to_string();
        assert!(err.contains("min_priority"), "got: {err}");
    }

    #[test]
    fn parse_entry_rejects_unknown_tier() {
        let err = parse_worker_entry("http://w:30000@tier=gold")
            .unwrap_err()
            .to_string();
        assert!(err.contains("tier"), "got: {err}");
    }

    #[test]
    fn parse_entry_rejects_unknown_backend() {
        let err = parse_worker_entry("http://w:30000@backend=trtllm")
            .unwrap_err()
            .to_string();
        assert!(err.contains("backend"), "got: {err}");
    }

    #[test]
    fn parse_entry_userinfo_at_is_not_mistaken_for_token() {
        // A bare `@` (here in a hypothetical userinfo position) must not be
        // treated as the capability token — only `@min_priority=` splits.
        let (url, caps) = parse_worker_entry("http://user@host:30000").unwrap();
        assert_eq!(url, "http://user@host:30000");
        assert_eq!(caps.min_priority, None);
        assert_eq!(caps.backend, WorkerBackend::Sglang);
    }

    /// Task exits cleanly when the consumer drops the receiver mid-fanout.
    /// Without this early exit, the producer would block forever on the
    /// closed channel and shutdown would have to abort it. Kept in-source
    /// (rather than as a component test) because it inspects the
    /// `send().is_err()` branch, which is an implementation detail of
    /// this module — fan-out and event-shape assertions live in
    /// `tests/component/discovery/static_urls.rs`.
    #[tokio::test]
    async fn exits_when_receiver_dropped() {
        let cfg = StaticUrlsDiscoveryConfig {
            urls: (0..10).map(|i| format!("http://w{i}:30000")).collect(),
            bearer_keys: Vec::new(),
        };
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let h = spawn(cfg, tx).await.unwrap();
        // No panic, no hang — task exits on the first send error.
        h.await.unwrap();
    }

    /// After fan-out the task must STAY ALIVE so the critical-task
    /// supervisor (`server::supervisor::supervise_critical_tasks`)
    /// doesn't treat the exit as a failure and flip `/readyz` to 503.
    /// The static_urls backend has no hot-reload, so the only reasons
    /// it should ever exit are (a) the consumer dropped the receiver,
    /// or (b) the supervisor aborted it on shutdown. A "natural" exit
    /// after fan-out used to be the third path, and was wrongly
    /// interpreted as a panic by the supervisor — pinned here so a
    /// regression to "exit after fan-out" can't sneak back in.
    #[tokio::test]
    async fn stays_alive_after_fanout_until_receiver_dropped() {
        use std::time::Duration;

        let cfg = StaticUrlsDiscoveryConfig {
            urls: vec!["http://w0:30000".into(), "http://w1:30000".into()],
            bearer_keys: Vec::new(),
        };
        let (tx, mut rx) = mpsc::channel(8);
        let h = spawn(cfg, tx).await.unwrap();

        // Drain the fan-out so the task is past the for-loop.
        for _ in 0..2 {
            let _ = rx.recv().await.expect("fan-out event");
        }

        // Now give the task a long-by-test-standards moment to exit
        // post-fanout. Pre-fix this would have completed in under a
        // millisecond; post-fix it must time out.
        let mut handle = h;
        let exited = tokio::time::timeout(Duration::from_millis(200), &mut handle).await;
        let still_running = exited.is_err();
        if !still_running {
            panic!(
                "static_urls task exited after fan-out (joined as {exited:?}); \
                 this trips `supervise_critical_tasks` → mark_unready and the pod \
                 becomes /readyz 503. The task must park until the receiver is dropped."
            );
        }
        // Clean shutdown: dropping the receiver closes the channel, which
        // the post-fix task uses as its "time to exit" signal. Pin both
        // halves of the contract — parks while the receiver is alive AND
        // exits cleanly once it's dropped — so a future refactor that
        // parks the task on the wrong signal (e.g., a sleep, a token that
        // never fires) is caught here rather than silently lingering.
        drop(rx);
        let joined = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("task must exit promptly after the receiver is dropped");
        joined.expect("task panicked during clean shutdown");
    }
}

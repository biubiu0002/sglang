// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Background worker-load poller.
//!
//! The router-side in-flight counter (`Worker::active_load`) is a poor
//! "how busy is this worker" signal for a mixed short/long workload: one
//! 200k-token request and one 2k request both count as load 1, yet load
//! the engine completely differently. This poller periodically asks each
//! worker for its REAL queue depth via the worker's `/get_load` endpoint
//! and stores it on `Worker::reported_load`, which the `cache_aware_zmq`
//! policy consumes (via `Worker::effective_load`) for its min-load /
//! imbalance / hit-load-guard decisions.
//!
//! Auth: `/get_load` is behind the worker's `--api-key` (401 without), so
//! the poller carries the same `worker_introspect_key` bearer the KV-event
//! discovery already uses. Unlike the ZMQ KV-event feed, `/get_load` is
//! plain HTTP on the worker's normal port — reachable over NAT/Vast public
//! mappings with no special port.
//!
//! Failure handling: any poll error (timeout, non-2xx, parse) writes the
//! `REPORTED_LOAD_FAILED` sentinel so the policy treats that worker as HIGH
//! load and never spills onto a possibly-dead worker.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::policies::active_load::JanitorHandle;
use crate::workers::worker::REPORTED_LOAD_FAILED;
use crate::workers::WorkerRegistry;

/// Per-`dp_rank` entry from a worker's `/get_load` response, e.g.
/// `[{"dp_rank":0,"num_reqs":0,"num_waiting_reqs":0,"num_tokens":0,...}, ...]`.
/// Unknown fields are ignored; we only need the waiting-request count.
#[derive(Debug, Deserialize)]
struct GetLoadEntry {
    #[serde(default)]
    num_waiting_reqs: i64,
}

/// Per-request timeout for the `/get_load` GET. Small: it is a tiny JSON
/// payload from the worker's HTTP server and we poll on a short interval,
/// so a slow worker should fail fast and be marked HIGH load rather than
/// stall the whole poll round.
const GET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// Sum `num_waiting_reqs` across all dp ranks reported by one worker.
/// Returns `None` if the body does not parse as the expected array.
fn parse_total_waiting(body: &str) -> Option<i64> {
    let entries: Vec<GetLoadEntry> = serde_json::from_str(body).ok()?;
    Some(entries.iter().map(|e| e.num_waiting_reqs.max(0)).sum())
}

/// Poll one worker's `/get_load` once and store the result (or the
/// failure sentinel) on the worker. Never panics; never returns an error.
async fn poll_one(client: &reqwest::Client, worker: &Arc<crate::workers::worker::Worker>) {
    let url = format!("{}/get_load", worker.url.trim_end_matches('/'));
    let outcome = async {
        let mut req = client.get(&url).timeout(GET_LOAD_TIMEOUT);
        if let Some(token) = worker.bearer_token() {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("worker bearer token must be a valid HTTP header value");
            value.set_sensitive(true);
            req = req.header(reqwest::header::AUTHORIZATION, value);
        }
        let resp = req.send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body = resp.text().await.ok()?;
        parse_total_waiting(&body)
    }
    .await;
    match outcome {
        Some(waiting) => worker.set_reported_load(waiting),
        None => {
            worker.set_reported_load(REPORTED_LOAD_FAILED);
            tracing::debug!(worker_url = %worker.url, "load-poller: /get_load failed; marking HIGH load");
        }
    }
}

/// One poll round: fan out `/get_load` to every registered worker
/// concurrently and update each worker's `reported_load`.
async fn poll_round(client: &reqwest::Client, registry: &Arc<WorkerRegistry>) {
    let workers = registry.all();
    let futs = workers.iter().map(|w| poll_one(client, w));
    futures::future::join_all(futs).await;
}

/// Spawn the background load poller. Mirrors `spawn_sweeper`'s lifecycle:
/// the returned [`JanitorHandle`] cancels the task on drop / `shutdown()`.
/// `bearer` is the worker introspect key (same one KV-event discovery
/// uses); `None` means `/get_load` is hit unauthenticated (workers with no
/// `--api-key`).
pub fn spawn_load_poller(
    registry: Arc<WorkerRegistry>,
    interval: Duration,
    bearer: Option<String>,
) -> JanitorHandle {
    let mut builder = reqwest::Client::builder().timeout(GET_LOAD_TIMEOUT);
    if let Some(token) = bearer.as_deref() {
        let mut headers = reqwest::header::HeaderMap::new();
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("worker introspect key must be a valid HTTP header value");
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    let client = builder.build().expect("load-poller http client builds");

    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancel_for_task.cancelled() => {
                    tracing::debug!("load-poller: shutdown requested");
                    return;
                }
                _ = ticker.tick() => {
                    poll_round(&client, &registry).await;
                }
            }
        }
    });
    JanitorHandle::from_parts(cancel, join)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sums_waiting_across_dp_ranks() {
        let body = r#"[
            {"dp_rank":0,"num_reqs":3,"num_waiting_reqs":2,"num_tokens":100},
            {"dp_rank":1,"num_reqs":1,"num_waiting_reqs":5,"num_tokens":50}
        ]"#;
        assert_eq!(parse_total_waiting(body), Some(7));
    }

    #[test]
    fn parse_single_rank() {
        let body = r#"[{"dp_rank":0,"num_reqs":0,"num_waiting_reqs":0,"num_tokens":0}]"#;
        assert_eq!(parse_total_waiting(body), Some(0));
    }

    #[test]
    fn parse_negative_clamped_to_zero() {
        // Defensive: a bogus negative waiting count must not underflow the sum.
        let body = r#"[{"num_waiting_reqs":-4},{"num_waiting_reqs":3}]"#;
        assert_eq!(parse_total_waiting(body), Some(3));
    }

    #[test]
    fn parse_rejects_non_array() {
        assert_eq!(parse_total_waiting("not json"), None);
        assert_eq!(parse_total_waiting(r#"{"num_waiting_reqs":1}"#), None);
    }
}

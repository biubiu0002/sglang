// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! OpenAI `/v1/responses` passthrough route.
//!
//! Forwards the OpenAI Responses request body to a selected SGLang worker at
//! `/v1/responses` without translating to/from chat completions — the worker
//! natively serves `/v1/responses`. The router only needs `model` and `stream`
//! from the body for worker selection and buffered-vs-SSE routing.
//!
//! Mirrors `messages.rs` exactly except for the forward path and the error
//! envelope: router-originated `ApiError`s use the default OpenAI-shaped
//! `IntoResponse` (`{"error":{"type","code","message"}}`) — the same shape the
//! `/v1/chat/completions` path returns — so OpenAI SDK / Codex clients parse
//! router-side failures uniformly. Worker-originated errors are forwarded
//! verbatim and are already OpenAI-shaped.
//!
//! Deliberately does NOT replicate chat.rs's `input_ids` forwarding, PD
//! bootstrap injection, or decode-peer resolution (see design.md). It DOES
//! register active-load + hold the per-worker LoadGuard so load-aware policies
//! (`power_of_two`, `cache_aware_zmq`) see accurate in-flight counts.

use crate::discovery::{ModelId, WorkerMode};
use crate::policies::registry::{filter_eligible, PdPoolResolver, PdResolveError};
use crate::policies::SelectionContext;
use crate::server::app_context::AppContext;
use crate::server::error::ApiError;
use crate::server::metrics::{PriorityFilterOutcome, RequestOutcome, WorkerModeLabel};
use crate::workers::LoadGuard;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Response};
use bytes::Bytes;
use serde::Deserialize;
use std::sync::Arc;

/// Per-route body cap, mirroring chat/messages. Same rationale: bound heap
/// allocation before forwarding while accommodating long contexts.
pub const MAX_RESPONSES_BODY_BYTES: usize = 5 << 20;

/// Minimal probe: `model` selects the worker, `stream` picks buffered vs SSE.
/// The worker is authoritative for the full Responses schema. `#[serde(default)]`
/// keeps it tolerant of optional fields — only `model` is required.
#[derive(Debug, Deserialize)]
struct ResponsesProbe {
    #[serde(default)]
    stream: Option<bool>,
    model: Option<String>,
    /// Request priority, captured as a raw JSON value so a malformed value
    /// is tolerated (treated as `0`) rather than rejected. Gates
    /// capacity-restricted workers (see [`filter_eligible`]).
    #[serde(default)]
    priority: Option<serde_json::Value>,
}

fn parse_probe(body: &Bytes) -> Result<ResponsesProbe, ApiError> {
    serde_json::from_slice(body)
        .map_err(|_| ApiError::BadRequest("invalid request: body must be a JSON object".into()))
}

/// POST /v1/responses — select a worker via the per-model policy and proxy the
/// raw Responses body to `<worker>/v1/responses`. Router-side failures map to
/// the default OpenAI error envelope via `ApiError`'s `IntoResponse`.
pub async fn responses(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
    responses_inner(State(ctx), headers, body).await
}

async fn responses_inner(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
    let start = std::time::Instant::now();
    let probe = parse_probe(&body)?;
    let streaming = probe.stream.unwrap_or(false);
    let model_str = probe
        .model
        .ok_or_else(|| ApiError::BadRequest("missing `model` field".into()))?;
    let model_id = ModelId(model_str.clone());

    // Same candidate set as chat (prefill pool for PD; full set for plain).
    let resolver = PdPoolResolver::new(Arc::clone(&ctx.registry));
    let workers = resolver
        .prefill_candidates(&model_id)
        .map_err(|e| match e {
            PdResolveError::NoHealthyWorkers => ApiError::NoHealthyWorkers {
                model: model_str.clone(),
            },
            PdResolveError::NoPrefillWorkersAvailable => ApiError::NoPrefillWorkersAvailable {
                model: model_str.clone(),
            },
            PdResolveError::NoDecodeWorkersAvailable => ApiError::NoDecodeWorkersAvailable {
                model: model_str.clone(),
            },
        })?;

    // Resolve the model's policy BEFORE priority filtering — see the
    // `/v1/chat/completions` path: an unknown model must 404 `ModelNotFound`
    // rather than be masked by a 503 from the eligibility filter emptying a
    // gated-but-policyless model's candidate set.
    let policy = ctx
        .policies
        .get(&model_id)
        .ok_or_else(|| ApiError::ModelNotFound(model_str.clone()))?;

    // PD-disaggregated mode is unsupported on this route (same rationale as
    // /v1/messages): this passthrough forwards to a single worker and does NOT
    // replicate chat.rs's decode-peer resolution + bootstrap body injection, so
    // silently forwarding to a prefill worker would hang. Reject (400) BEFORE
    // priority filtering so the honest "PD not supported" error surfaces rather
    // than a misleading 503 from the filter emptying the candidate set.
    if workers.iter().any(|w| w.mode() != WorkerMode::Plain) {
        return Err(ApiError::BadRequest(
            "/v1/responses passthrough does not support PD-disaggregated mode yet; use /v1/chat/completions".into(),
        ));
    }

    // Priority-eligibility filtering — identical semantics to the
    // `/v1/chat/completions` and `/v1/messages` paths: capacity-restricted
    // workers are removed for sub-threshold requests before policy selection.
    // Hard isolation: if filtering empties the candidate set, reject with 503
    // rather than spill the request onto a gated worker.
    let request_priority = crate::policies::priority_from_value(probe.priority.as_ref());
    let eligible = filter_eligible(&workers, request_priority);
    if eligible.excluded_all {
        tracing::warn!(
            model = %model_str,
            request_priority,
            healthy_workers = workers.len(),
            "priority filter removed all candidates; rejecting request (no eligible-capacity worker healthy for this priority)",
        );
        ctx.metrics
            .record_priority_filtered(PriorityFilterOutcome::EmptySetRejected);
        return Err(ApiError::NoHealthyWorkers {
            model: model_str.clone(),
        });
    } else if eligible.excluded_any {
        ctx.metrics
            .record_priority_filtered(PriorityFilterOutcome::WorkerExcluded);
    }
    let workers = eligible.workers;

    // Deliberately do NOT produce routing tokens for /v1/responses.
    //
    // Same rationale as /v1/messages: the worker's Responses serving path
    // tokenizes the `input` (and folds any instructions/system) itself before
    // generation, and the router never forwards input_ids on this route. The
    // router's chat-encoder tokenization would not match the engine's cached
    // blocks for a Responses body, so router-side hashing risks false-locality
    // hits. We skip router-side tokenization: cache_aware_zmq falls back to
    // min-load for /v1/responses (honest trade-off, costs cache affinity on
    // this route until the engine exposes its Responses block hashes).
    let request_tokens: Option<crate::policies::RequestTokens> = None;

    let routing_key = ctx
        .config
        .model
        .sticky
        .as_ref()
        .and_then(|s| headers.get(s.header_name.as_str()))
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());
    // Pass NO body to the selection context (see /v1/messages): with
    // `request_tokens = None` AND no body, CacheAwareZmqPolicy::select falls
    // back to min-load rather than tokenizing the body. `routing_key` is still
    // honored by the sticky policy (it reads headers, not body).
    let selection_ctx = SelectionContext::with_routing_key(&model_id, None, routing_key)
        .with_request_tokens(request_tokens.as_ref().map(|t| t.ids.as_slice()));
    let (worker, pending_guard) = {
        let _selection_guard = ctx.selection_lock.lock().await;
        let worker = policy.select(&workers, &selection_ctx).ok_or_else(|| {
            ApiError::PolicySelectionFailed {
                model: model_str.clone(),
            }
        })?;
        let pending_guard = worker.pending_guard();
        (worker, pending_guard)
    };

    // Hold the per-worker in-flight guard + register active load so load-aware
    // policies see this request. prefill_load uses the byte heuristic (we never
    // tokenize on this route), same as chat's fallback.
    let guard = worker.load_guard();
    let prefill_load = request_tokens
        .as_ref()
        .map(|t| t.ids.len().max(1))
        .unwrap_or_else(|| crate::server::routes::chat::estimate_prefill_tokens(&body));
    let active_guard =
        ctx.active_load
            .register(worker.id.clone(), worker.url.clone(), prefill_load, 0);
    let stale_token = active_guard.cancel_token().clone();
    let metrics_worker_url = worker.url.clone();
    let metrics_model = model_str.clone();
    let metrics_mode = match worker.mode() {
        WorkerMode::Prefill => WorkerModeLabel::Prefill,
        WorkerMode::Decode => WorkerModeLabel::Decode,
        WorkerMode::Plain => WorkerModeLabel::Plain,
    };

    let result = if streaming {
        // Mirror /v1/messages: skip chat's TTFT hook + streaming-duration RAII
        // guard (v1 measures header-time only). Guards move into stream_guards
        // so load stays accurate for the stream's lifetime.
        let stream_guards: Box<dyn Send + 'static> = Box::new((guard, active_guard, pending_guard));
        let fetch = ctx.proxy.forward_streaming_to(
            &worker.url,
            &worker.breaker,
            "/v1/responses",
            &headers,
            body,
            Some(stream_guards),
            None,
        );
        tokio::select! {
            biased;
            r = fetch => r,
            _ = stale_token.cancelled() => Err(ApiError::StaleRequestExpired { model: model_str }),
        }
    } else {
        let _holds: (LoadGuard, _, _) = (guard, active_guard, pending_guard);
        let fetch = ctx.proxy.forward_json_to(
            &worker.url,
            &worker.breaker,
            "/v1/responses",
            &headers,
            body,
        );
        tokio::select! {
            biased;
            r = fetch => r,
            _ = stale_token.cancelled() => Err(ApiError::StaleRequestExpired { model: model_str }),
        }
    };

    let outcome = match &result {
        Ok(_) => RequestOutcome::Success,
        Err(ApiError::StaleRequestExpired { .. }) => {
            ctx.metrics
                .record_stale_request(crate::server::metrics::StaleRequestOutcome::Expired);
            RequestOutcome::Cancelled
        }
        Err(_) => RequestOutcome::Error,
    };
    ctx.metrics
        .record_request(&metrics_worker_url, &metrics_model, metrics_mode, outcome);

    let elapsed = start.elapsed();
    if !streaming {
        ctx.metrics
            .observe_request_duration(&metrics_model, elapsed.as_secs_f64());
    }
    let http_status = match &result {
        Ok(resp) => resp.status().as_u16(),
        Err(e) => e.status_code().as_u16(),
    };
    ctx.metrics.record_response(http_status);
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    tracing::info!(
        request_id = %request_id,
        method = "POST",
        path = "/v1/responses",
        model = %metrics_model,
        worker = %metrics_worker_url,
        outcome = match outcome {
            RequestOutcome::Success => "success",
            RequestOutcome::Error => "error",
            RequestOutcome::Cancelled => "cancelled",
        },
        http_status,
        stream = streaming,
        latency_ms = elapsed.as_millis() as u64,
        "responses",
    );
    result
}

#[cfg(test)]
mod tests {
    use super::parse_probe;
    use bytes::Bytes;

    #[test]
    fn probe_reads_stream_and_model() {
        let b = Bytes::from(r#"{"model":"glm","stream":true,"input":"hi"}"#);
        let p = parse_probe(&b).unwrap();
        assert_eq!(p.stream, Some(true));
        assert_eq!(p.model.as_deref(), Some("glm"));
    }

    #[test]
    fn probe_stream_defaults_to_false() {
        let b = Bytes::from(r#"{"model":"glm","input":"hi"}"#);
        let p = parse_probe(&b).unwrap();
        assert_eq!(p.stream, None);
        assert_eq!(p.model.as_deref(), Some("glm"));
    }

    #[test]
    fn probe_rejects_non_object() {
        let b = Bytes::from(b"\"hi\"".as_ref());
        assert!(parse_probe(&b).is_err(), "string body must not parse");
    }

    #[test]
    fn probe_allows_responses_shape_without_stream() {
        // Real Responses body has input/max_output_tokens; only model is required here.
        let b = Bytes::from(
            r#"{"model":"gpt","input":"hi","max_output_tokens":256,"reasoning":{"effort":"low"}}"#,
        );
        let p = parse_probe(&b).unwrap();
        assert_eq!(p.model.as_deref(), Some("gpt"));
        assert_eq!(p.stream, None);
    }

    #[test]
    fn probe_missing_model_parses_then_handler_rejects() {
        // parse_probe only requires valid JSON object; `model` absence is
        // enforced in responses_inner (returns 400 missing `model`).
        let b = Bytes::from(r#"{"input":"hi"}"#);
        let p = parse_probe(&b).unwrap();
        assert_eq!(p.model, None);
    }
}

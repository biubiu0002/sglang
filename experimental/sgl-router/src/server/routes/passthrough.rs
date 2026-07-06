// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Generic OpenAI-compatible passthrough routes.
//!
//! These endpoints keep the worker-facing path and request schema unchanged,
//! while still applying router-side model selection, priority gating, active
//! load accounting, and alias fallback.

use crate::discovery::{ModelId, WorkerMode};
use crate::policies::registry::{filter_eligible, PdPoolResolver, PdResolveError};
use crate::policies::SelectionContext;
use crate::policies::{request_tokens_for, RequestTokens};
use crate::server::app_context::AppContext;
use crate::server::error::ApiError;
use crate::server::metrics::{PriorityFilterOutcome, RequestOutcome, WorkerModeLabel};
use crate::server::routes::alias_fallback::{
    fallback_reason_for_error, fallback_reason_for_response, forward_to_fallback, rewrite_model,
};
use crate::server::routes::chat::{make_client_disconnect_hook, reserve_pending_load};
use crate::server::routes::priority_override::apply_request_priority_override;
use crate::workers::LoadGuard;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Response};
use bytes::Bytes;
use serde::Deserialize;
use std::sync::Arc;

/// Same cap as chat/messages. These routes are raw passthrough, so the worker
/// remains authoritative for schema validation.
pub const MAX_PASSTHROUGH_BODY_BYTES: usize = 5 << 20;

#[derive(Debug, Deserialize)]
struct PassthroughProbe {
    #[serde(default)]
    stream: Option<bool>,
    model: Option<String>,
    #[serde(default)]
    priority: Option<serde_json::Value>,
}

fn parse_probe(body: &Bytes) -> Result<PassthroughProbe, ApiError> {
    serde_json::from_slice(body)
        .map_err(|_| ApiError::BadRequest("invalid request: body must be a JSON object".into()))
}

pub async fn completions(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
    passthrough(State(ctx), headers, body, "/v1/completions", "completions").await
}

pub async fn responses(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
    passthrough(State(ctx), headers, body, "/v1/responses", "responses").await
}

async fn passthrough(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
    path: &'static str,
    log_name: &'static str,
) -> Result<Response<Body>, ApiError> {
    let body = apply_request_priority_override(&ctx.config.priority_override, &headers, body)?;
    let probe = parse_probe(&body)?;
    let streaming = probe.stream.unwrap_or(false);
    let model_str = probe
        .model
        .ok_or_else(|| ApiError::BadRequest("missing `model` field".into()))?;
    let Some(cfg) = ctx
        .config
        .alias_fallback
        .as_ref()
        .filter(|cfg| cfg.alias_model_id == model_str)
        .cloned()
    else {
        return passthrough_primary(State(ctx), headers, body, path, log_name).await;
    };

    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    let primary_body = rewrite_model(&body, &cfg.primary_model_id)?;
    ctx.metrics
        .record_alias_route(&cfg.alias_model_id, "primary", "selected");
    tracing::info!(
        request_id = %request_id,
        alias = %cfg.alias_model_id,
        route = "primary",
        primary_model = %cfg.primary_model_id,
        path,
        "alias primary selected",
    );

    let primary = passthrough_primary(
        State(Arc::clone(&ctx)),
        headers.clone(),
        primary_body,
        path,
        log_name,
    )
    .await;
    match primary {
        Ok(resp) => {
            if let Some(reason) = fallback_reason_for_response(resp.status()) {
                forward_to_fallback(
                    &ctx, &cfg, &headers, &body, path, streaming, request_id, reason,
                )
                .await
            } else {
                Ok(resp)
            }
        }
        Err(e) => {
            if let Some(reason) = fallback_reason_for_error(&e) {
                forward_to_fallback(
                    &ctx, &cfg, &headers, &body, path, streaming, request_id, reason,
                )
                .await
            } else {
                Err(e)
            }
        }
    }
}

async fn passthrough_primary(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
    path: &'static str,
    log_name: &'static str,
) -> Result<Response<Body>, ApiError> {
    let start = std::time::Instant::now();
    let probe = parse_probe(&body)?;
    let streaming = probe.stream.unwrap_or(false);
    let model_str = probe
        .model
        .ok_or_else(|| ApiError::BadRequest("missing `model` field".into()))?;
    let model_id = ModelId(model_str.clone());

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

    let policy = ctx
        .policies
        .get(&model_id)
        .ok_or_else(|| ApiError::ModelNotFound(model_str.clone()))?;

    if workers.iter().any(|w| w.mode() != WorkerMode::Plain) {
        return Err(ApiError::BadRequest(format!(
            "{path} passthrough does not support PD-disaggregated mode yet; use /v1/chat/completions"
        )));
    }

    let request_priority = crate::policies::priority_from_value(probe.priority.as_ref());
    let eligible = filter_eligible(&workers, request_priority);
    if eligible.excluded_all {
        tracing::warn!(
            model = %model_str,
            request_priority,
            healthy_workers = workers.len(),
            path,
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

    // /v1/completions has an explicit raw `prompt`; feed those tokens to
    // cache-aware routing without changing the worker-facing passthrough body.
    // Other generic passthrough shapes stay min-load until their engine prompt
    // construction is replicated exactly enough for routing hashes.
    let request_tokens: Option<RequestTokens> = if path == "/v1/completions" {
        serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| request_tokens_for(&ctx.tokenizers, &model_id, &v))
    } else {
        None
    };

    let routing_key = ctx
        .config
        .model
        .sticky
        .as_ref()
        .and_then(|s| headers.get(s.header_name.as_str()))
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());
    let selection_ctx = SelectionContext::with_routing_key(&model_id, None, routing_key)
        .with_request_tokens(request_tokens.as_ref().map(|t| t.ids.as_slice()));
    let (worker, pending_guard) = {
        let _selection_guard = ctx.selection_lock.lock().await;
        let worker = policy.select(&workers, &selection_ctx).ok_or_else(|| {
            ApiError::PolicySelectionFailed {
                model: model_str.clone(),
            }
        })?;
        let pending_tokens = request_tokens
            .as_ref()
            .map(|t| t.ids.len().max(1))
            .unwrap_or(1);
        let pending_guard = reserve_pending_load(&ctx, &worker, pending_tokens);
        (worker, pending_guard)
    };
    let worker_headers = worker
        .headers_for(&headers)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("build worker auth header: {e}")))?;

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
        let stream_guards: Box<dyn Send + 'static> = Box::new((guard, active_guard, pending_guard));
        let fetch = ctx.proxy.forward_streaming_to(
            &worker.url,
            &worker.breaker,
            path,
            worker_headers.as_ref(),
            body,
            Some(stream_guards),
            None,
            Some(make_client_disconnect_hook(Arc::clone(&ctx.metrics))),
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
            path,
            worker_headers.as_ref(),
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
        .record_worker_request(&metrics_worker_url, &metrics_model, metrics_mode, outcome);

    let elapsed = start.elapsed();
    if !streaming {
        ctx.metrics
            .observe_request_duration(&metrics_model, elapsed.as_secs_f64());
    }
    let http_status = match &result {
        Ok(resp) => resp.status().as_u16(),
        Err(e) => e.status_code().as_u16(),
    };
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    tracing::info!(
        request_id = %request_id,
        method = "POST",
        path,
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
        "{log_name}",
    );
    result
}

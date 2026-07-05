// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Explicit model-alias fallback routing.
//!
//! This is intentionally narrow: it rewrites one public alias to a primary
//! model served by the current router and, only when that primary attempt fails
//! before the client sees response bytes, rewrites the original request to a
//! configured fallback model and proxies it to an external upstream router.

use crate::config::AliasFallbackConfig;
use crate::server::app_context::AppContext;
use crate::server::error::ApiError;
use crate::server::routes::chat::make_client_disconnect_hook;
use crate::server::trace::TraceContext;
use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, Response, StatusCode};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

const FALLBACK_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub enum AliasFallbackReason {
    NoHealthyWorkers,
    NoPrefillWorkers,
    NoDecodeWorkers,
    PolicySelectionFailed,
    BreakerOpen,
    WorkerMisconfigured,
    UpstreamUnreachable,
    UpstreamTimeout,
    StaleRequestExpired,
    UpstreamStatus(StatusCode),
    RetryableStatus(StatusCode),
}

impl AliasFallbackReason {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::NoHealthyWorkers => "no_healthy_workers",
            Self::NoPrefillWorkers => "no_prefill_workers",
            Self::NoDecodeWorkers => "no_decode_workers",
            Self::PolicySelectionFailed => "policy_selection_failed",
            Self::BreakerOpen => "breaker_open",
            Self::WorkerMisconfigured => "worker_misconfigured",
            Self::UpstreamUnreachable => "upstream_unreachable",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::StaleRequestExpired => "stale_request_expired",
            Self::UpstreamStatus(_) => "upstream_status",
            Self::RetryableStatus(_) => "retryable_status",
        }
    }

    fn status(self) -> Option<StatusCode> {
        match self {
            Self::UpstreamStatus(status) | Self::RetryableStatus(status) => Some(status),
            _ => None,
        }
    }
}

pub fn rewrite_model(body: &Bytes, model_id: &str) -> Result<Bytes, ApiError> {
    let mut value: serde_json::Value = serde_json::from_slice(body).map_err(|_| {
        ApiError::BadRequest("invalid request: body must be a JSON object".to_string())
    })?;
    let obj = value.as_object_mut().ok_or_else(|| {
        ApiError::BadRequest("invalid request: body must be a JSON object".to_string())
    })?;
    obj.insert(
        "model".to_string(),
        serde_json::Value::String(model_id.to_string()),
    );
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|e| ApiError::Internal(anyhow::Error::new(e).context("rewrite alias model")))
}

pub fn fallback_reason_for_error(error: &ApiError) -> Option<AliasFallbackReason> {
    match error {
        ApiError::NoHealthyWorkers { .. } => Some(AliasFallbackReason::NoHealthyWorkers),
        ApiError::NoPrefillWorkersAvailable { .. } => Some(AliasFallbackReason::NoPrefillWorkers),
        ApiError::NoDecodeWorkersAvailable { .. } => Some(AliasFallbackReason::NoDecodeWorkers),
        ApiError::PolicySelectionFailed { .. } => Some(AliasFallbackReason::PolicySelectionFailed),
        ApiError::BreakerOpen { .. } => Some(AliasFallbackReason::BreakerOpen),
        ApiError::WorkerMisconfigured { .. } => Some(AliasFallbackReason::WorkerMisconfigured),
        ApiError::UpstreamUnreachable { .. } => Some(AliasFallbackReason::UpstreamUnreachable),
        ApiError::UpstreamTimeout { .. } => Some(AliasFallbackReason::UpstreamTimeout),
        ApiError::StaleRequestExpired { .. } => Some(AliasFallbackReason::StaleRequestExpired),
        ApiError::UpstreamStatus { status } if retryable_status(*status) => {
            Some(AliasFallbackReason::UpstreamStatus(*status))
        }
        _ => None,
    }
}

pub fn fallback_reason_for_response(status: StatusCode) -> Option<AliasFallbackReason> {
    retryable_status(status).then_some(AliasFallbackReason::RetryableStatus(status))
}

fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::UNAUTHORIZED
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

pub async fn forward_to_fallback(
    ctx: &Arc<AppContext>,
    cfg: &AliasFallbackConfig,
    inbound_headers: &HeaderMap,
    original_body: &Bytes,
    path: &'static str,
    streaming: bool,
    request_id: &str,
    reason: AliasFallbackReason,
) -> Result<Response<Body>, ApiError> {
    let body = rewrite_model(original_body, &cfg.fallback_model_id)?;
    let mut headers = fallback_headers(inbound_headers, cfg.fallback_bearer_token.as_deref())?;
    let trace_ctx = TraceContext::new(
        &mut headers,
        "POST",
        path,
        Some(cfg.fallback_model_id.clone()),
        streaming,
        body.clone(),
    );
    let breaker = ctx.alias_fallback_breaker.as_ref().ok_or_else(|| {
        ApiError::Internal(anyhow::anyhow!(
            "alias fallback configured without fallback circuit breaker"
        ))
    })?;
    ctx.metrics
        .record_alias_route(&cfg.alias_model_id, "fallback", reason.as_label());
    tracing::warn!(
        request_id = %request_id,
        alias = %cfg.alias_model_id,
        route = "fallback",
        reason = reason.as_label(),
        status = reason.status().map(|s| s.as_u16()),
        fallback_model = %cfg.fallback_model_id,
        fallback_url = %cfg.fallback_base_url,
        "alias fallback selected",
    );
    let snapshot = breaker.snapshot();
    if snapshot.state_code == 0 {
        if !breaker.allow() {
            return Err(ApiError::BreakerOpen {
                worker: cfg.fallback_base_url.clone(),
            });
        }
    } else {
        if !breaker.allow() {
            return Err(ApiError::BreakerOpen {
                worker: cfg.fallback_base_url.clone(),
            });
        }
        tracing::warn!(
            request_id = %request_id,
            alias = %cfg.alias_model_id,
            fallback_url = %cfg.fallback_base_url,
            fallback_model = %cfg.fallback_model_id,
            breaker_state = snapshot.state_code,
            "alias fallback breaker probing with synthetic generation",
        );
        if let Err(err) = ctx
            .proxy
            .probe_chat_completion(
                &cfg.fallback_base_url,
                breaker,
                &headers,
                &cfg.fallback_model_id,
                FALLBACK_PROBE_TIMEOUT,
            )
            .await
        {
            tracing::warn!(
                request_id = %request_id,
                alias = %cfg.alias_model_id,
                fallback_url = %cfg.fallback_base_url,
                error = ?err,
                "alias fallback synthetic generation probe failed",
            );
            return Err(ApiError::BreakerOpen {
                worker: cfg.fallback_base_url.clone(),
            });
        }
        tracing::info!(
            request_id = %request_id,
            alias = %cfg.alias_model_id,
            fallback_url = %cfg.fallback_base_url,
            "alias fallback synthetic generation probe recovered breaker",
        );
    }
    if streaming {
        ctx.proxy
            .forward_streaming_to_without_admission_traced(
                &cfg.fallback_base_url,
                breaker,
                path,
                &headers,
                body,
                None,
                None,
                Some(make_client_disconnect_hook(Arc::clone(&ctx.metrics))),
                ctx.trace_sink.clone(),
                trace_ctx,
            )
            .await
    } else {
        ctx.proxy
            .forward_json_to_without_admission_traced(
                &cfg.fallback_base_url,
                breaker,
                path,
                &headers,
                body,
                ctx.trace_sink.clone(),
                trace_ctx,
            )
            .await
    }
}

fn fallback_headers(
    inbound_headers: &HeaderMap,
    bearer_token: Option<&str>,
) -> Result<HeaderMap, ApiError> {
    let mut headers = inbound_headers.clone();
    if let Some(token) = bearer_token {
        let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
            ApiError::Internal(anyhow::Error::new(e).context("build alias fallback auth header"))
        })?;
        value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, value);
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_request_expired_is_fallback_eligible() {
        let reason = fallback_reason_for_error(&ApiError::StaleRequestExpired {
            model: "macaron-0.6".to_string(),
        });

        assert_eq!(
            reason.map(AliasFallbackReason::as_label),
            Some("stale_request_expired")
        );
    }
}

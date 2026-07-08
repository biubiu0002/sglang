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
use crate::policies::{request_tokens_for, RequestTokens, SelectionContext};
use crate::server::app_context::AppContext;
use crate::server::error::ApiError;
use crate::server::metrics::{PriorityFilterOutcome, RequestOutcome, WorkerModeLabel};
use crate::server::routes::admission::enforce_external_queue_admission;
use crate::server::routes::alias_fallback::{
    fallback_reason_for_error, fallback_reason_for_response, forward_to_fallback, rewrite_model,
};
use crate::server::routes::chat::{make_client_disconnect_hook, reserve_pending_load};
use crate::server::routes::priority_override::apply_request_priority_override;
use crate::server::trace::TraceContext;
use crate::workers::LoadGuard;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Response};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Map, Value};
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
    priority: Option<Value>,
}

fn parse_probe(body: &Bytes) -> Result<ResponsesProbe, ApiError> {
    serde_json::from_slice(body)
        .map_err(|_| ApiError::BadRequest("invalid request: body must be a JSON object".into()))
}

fn normalize_response_content_part_for_chat(part: &Value) -> Option<Value> {
    let obj = part.as_object()?;
    match obj.get("type").and_then(|t| t.as_str()) {
        Some("input_text") | Some("output_text") => Some(serde_json::json!({
            "type": "text",
            "text": obj.get("text").and_then(|t| t.as_str()).unwrap_or(""),
        })),
        Some("text") => Some(part.clone()),
        // Image parts affect prompt bytes through the multimodal processor,
        // which the router does not reproduce. Avoid false route-history hits.
        Some("input_image") | Some("image_url") => None,
        _ => Some(part.clone()),
    }
}

fn compact_json_string(value: &Value) -> Option<String> {
    serde_json::to_string(value).ok()
}

fn coerce_function_arguments(raw: Option<&Value>) -> Option<String> {
    match raw {
        Some(Value::String(s)) => {
            if s.is_empty() {
                return Some("{}".to_string());
            }
            match serde_json::from_str::<Value>(s) {
                Ok(Value::Object(_)) => Some(s.clone()),
                _ => Some("{}".to_string()),
            }
        }
        Some(Value::Object(_)) => compact_json_string(raw?),
        _ => Some("{}".to_string()),
    }
}

fn collect_response_text_parts(parts: Option<&Value>) -> Vec<String> {
    parts
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|entry| entry.as_object()?.get("text")?.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_response_message_for_chat(message: &Value) -> Option<Option<Value>> {
    let obj = message.as_object()?;
    let msg_type = obj.get("type").and_then(|t| t.as_str());
    if msg_type == Some("function_call") {
        return Some(Some(serde_json::json!({
            "role": "assistant",
            "tool_calls": [{
                "id": obj.get("call_id").or_else(|| obj.get("id")).cloned().unwrap_or(Value::Null),
                "type": "function",
                "function": {
                    "name": obj.get("name").cloned().unwrap_or(Value::Null),
                    "arguments": coerce_function_arguments(obj.get("arguments"))?,
                },
            }],
        })));
    }
    if msg_type == Some("function_call_output") {
        return Some(Some(serde_json::json!({
            "role": "tool",
            "tool_call_id": obj.get("call_id").cloned().unwrap_or(Value::Null),
            "content": obj.get("output").cloned().unwrap_or_else(|| Value::String(String::new())),
        })));
    }
    if msg_type == Some("reasoning") {
        let mut text_parts = collect_response_text_parts(obj.get("summary"));
        if text_parts.is_empty() {
            text_parts = collect_response_text_parts(obj.get("content"));
        }
        if text_parts.is_empty() {
            return Some(None);
        }
        return Some(Some(serde_json::json!({
            "role": "assistant",
            "reasoning_content": text_parts.join("\n"),
        })));
    }
    if !matches!(msg_type, None | Some("message")) {
        return None;
    }

    let mut out = Map::new();
    for (k, v) in obj {
        if v.is_null() || matches!(k.as_str(), "id" | "status" | "type") {
            continue;
        }
        if k == "role" && v.as_str() == Some("developer") {
            out.insert(k.clone(), Value::String("system".to_string()));
        } else if k != "content" {
            out.insert(k.clone(), v.clone());
        }
    }
    match obj.get("content") {
        Some(Value::Array(parts)) => {
            let mut normalized = Vec::with_capacity(parts.len());
            for part in parts {
                normalized.push(normalize_response_content_part_for_chat(part)?);
            }
            out.insert("content".to_string(), Value::Array(normalized));
        }
        Some(content) => {
            out.insert("content".to_string(), content.clone());
        }
        None => {}
    }
    Some(Some(Value::Object(out)))
}

fn as_text_parts(content: &Value) -> Vec<Value> {
    match content {
        Value::Array(parts) => parts.clone(),
        Value::String(s) if !s.is_empty() => vec![serde_json::json!({"type":"text","text":s})],
        _ => Vec::new(),
    }
}

fn merge_consecutive_assistant_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::with_capacity(messages.len());
    for msg in messages {
        let merge = msg.get("role").and_then(|r| r.as_str()) == Some("assistant")
            && merged
                .last()
                .and_then(|m| m.get("role"))
                .and_then(|r| r.as_str())
                == Some("assistant");
        if !merge {
            merged.push(msg);
            continue;
        }

        let prev = merged.last_mut().and_then(|v| v.as_object_mut()).unwrap();
        if let Some(new_content) = msg.get("content").filter(|c| !c.is_null() && **c != "") {
            match prev.get("content") {
                None => {
                    prev.insert("content".to_string(), new_content.clone());
                }
                Some(Value::String(s)) if s.is_empty() => {
                    prev.insert("content".to_string(), new_content.clone());
                }
                Some(Value::String(prev_s)) if new_content.is_string() => {
                    let new_s = new_content.as_str().unwrap_or("");
                    let sep = if !prev_s.is_empty() && !new_s.is_empty() {
                        "\n\n"
                    } else {
                        ""
                    };
                    prev.insert(
                        "content".to_string(),
                        Value::String(format!("{prev_s}{sep}{new_s}")),
                    );
                }
                Some(prev_content) => {
                    let mut parts = as_text_parts(prev_content);
                    parts.extend(as_text_parts(new_content));
                    prev.insert("content".to_string(), Value::Array(parts));
                }
            }
        }
        if let Some(Value::Array(new_calls)) = msg.get("tool_calls") {
            let mut calls = prev
                .get("tool_calls")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            calls.extend(new_calls.clone());
            prev.insert("tool_calls".to_string(), Value::Array(calls));
        }
        if let Some(new_reasoning) = msg.get("reasoning_content").and_then(|r| r.as_str()) {
            let reasoning = prev
                .get("reasoning_content")
                .and_then(|r| r.as_str())
                .map(|prev_r| format!("{prev_r}\n{new_reasoning}"))
                .unwrap_or_else(|| new_reasoning.to_string());
            prev.insert("reasoning_content".to_string(), Value::String(reasoning));
        }
    }
    merged
}

fn coalesce_system_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut system_chunks = Vec::new();
    let mut others = Vec::new();
    for msg in messages {
        if msg.get("role").and_then(|r| r.as_str()) == Some("system") {
            match msg.get("content") {
                Some(Value::String(s)) => system_chunks.push(s.clone()),
                Some(Value::Array(parts)) => {
                    for part in parts {
                        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                            system_chunks.push(text.to_string());
                        }
                    }
                }
                _ => {}
            }
        } else {
            others.push(msg);
        }
    }
    if !system_chunks.is_empty() {
        let mut out = vec![serde_json::json!({
            "role": "system",
            "content": system_chunks.join("\n\n"),
        })];
        out.extend(others);
        out
    } else {
        others
    }
}

fn response_tools_for_chat(value: &Value) -> Option<Option<Value>> {
    let tools = value
        .get("tools")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();
    let mut chat_tools = Vec::new();
    for tool in tools {
        let obj = tool.as_object()?;
        if obj.get("type").and_then(|t| t.as_str()) != Some("function") {
            continue;
        }
        chat_tools.push(serde_json::json!({
            "type": "function",
            "function": {
                "name": obj.get("name").cloned().unwrap_or(Value::Null),
                "description": obj.get("description").cloned().unwrap_or(Value::Null),
                "parameters": obj.get("parameters").cloned().unwrap_or(Value::Null),
                "strict": obj.get("strict").cloned().unwrap_or(Value::Null),
            },
        }));
    }
    if chat_tools.is_empty() {
        return Some(None);
    }

    match value.get("tool_choice") {
        Some(Value::String(s)) if s == "none" => Some(None),
        Some(Value::Object(obj)) => {
            let selected = obj
                .get("function")
                .and_then(|f| f.get("name"))
                .or_else(|| obj.get("name"))
                .and_then(|n| n.as_str());
            if let Some(selected) = selected {
                let filtered: Vec<Value> = chat_tools
                    .into_iter()
                    .filter(|tool| {
                        tool.get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(|n| n.as_str())
                            == Some(selected)
                    })
                    .collect();
                return Some((!filtered.is_empty()).then(|| Value::Array(filtered)));
            }
            Some(Some(Value::Array(chat_tools)))
        }
        _ => Some(Some(Value::Array(chat_tools))),
    }
}

fn responses_routing_value(body: &Bytes) -> Option<Value> {
    let value: Value = serde_json::from_slice(body).ok()?;
    if value
        .get("previous_response_id")
        .is_some_and(|v| !v.is_null())
    {
        return None;
    }

    let mut messages = Vec::new();
    if let Some(instructions) = value.get("instructions").and_then(|i| i.as_str()) {
        if !instructions.is_empty() {
            messages.push(serde_json::json!({"role":"system","content":instructions}));
        }
    }

    match value.get("input")? {
        Value::String(s) => messages.push(serde_json::json!({"role":"user","content":s})),
        Value::Array(items) => {
            for item in items {
                if let Some(normalized) = normalize_response_message_for_chat(item)? {
                    messages.push(normalized);
                }
            }
        }
        _ => return None,
    }

    let messages = coalesce_system_messages(merge_consecutive_assistant_messages(messages));
    let mut out = Map::new();
    out.insert("messages".to_string(), Value::Array(messages));
    if let Some(tools) = response_tools_for_chat(&value)? {
        out.insert("tools".to_string(), tools);
    }
    Some(Value::Object(out))
}

/// POST /v1/responses — select a worker via the per-model policy and proxy the
/// raw Responses body to `<worker>/v1/responses`. Router-side failures map to
/// the default OpenAI error envelope via `ApiError`'s `IntoResponse`.
pub async fn responses(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
    let body = apply_request_priority_override(&ctx.config.priority_override, &headers, body)?;
    let probe = parse_probe(&body)?;
    let model_str = probe
        .model
        .clone()
        .ok_or_else(|| ApiError::BadRequest("missing `model` field".into()))?;
    let Some(cfg) = ctx
        .config
        .alias_fallback
        .as_ref()
        .filter(|cfg| cfg.alias_model_id == model_str)
        .cloned()
    else {
        return responses_inner(State(ctx), headers, body).await;
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
        path = "/v1/responses",
        "alias primary selected",
    );
    let primary = responses_inner(State(Arc::clone(&ctx)), headers.clone(), primary_body).await;
    match primary {
        Ok(resp) => {
            if let Some(reason) = fallback_reason_for_response(resp.status()) {
                forward_to_fallback(
                    &ctx,
                    &cfg,
                    &headers,
                    &body,
                    "/v1/responses",
                    probe.stream.unwrap_or(false),
                    request_id,
                    reason,
                )
                .await
            } else {
                Ok(resp)
            }
        }
        Err(e) => {
            if let Some(reason) = fallback_reason_for_error(&e) {
                forward_to_fallback(
                    &ctx,
                    &cfg,
                    &headers,
                    &body,
                    "/v1/responses",
                    probe.stream.unwrap_or(false),
                    request_id,
                    reason,
                )
                .await
            } else {
                Err(e)
            }
        }
    }
}

async fn responses_inner(
    State(ctx): State<Arc<AppContext>>,
    mut headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
    let start = std::time::Instant::now();
    let probe = parse_probe(&body)?;
    let streaming = probe.stream.unwrap_or(false);
    let model_str = probe
        .model
        .ok_or_else(|| ApiError::BadRequest("missing `model` field".into()))?;
    let trace_ctx = TraceContext::new(
        &mut headers,
        "POST",
        "/v1/responses",
        Some(model_str.clone()),
        streaming,
        body.clone(),
    );
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
    enforce_external_queue_admission(&ctx, &model_str, &workers)?;

    // Produce routing-only tokens for stateless /v1/responses generation
    // requests. The worker still receives the original native Responses body;
    // this normalized chat-shaped value exists only so cache-aware routing can
    // hash the same instruction/input/tool prefix that the worker's non-harmony
    // Responses path passes to chat prompt processing. Stateful
    // previous_response_id and unsupported multimodal/renderer cases return
    // None, so route-history is not polluted with approximate prefixes.
    let request_tokens: Option<RequestTokens> = responses_routing_value(&body)
        .as_ref()
        .and_then(|v| request_tokens_for(&ctx.tokenizers, &model_id, v));

    let routing_key = ctx
        .config
        .model
        .sticky
        .as_ref()
        .and_then(|s| headers.get(s.header_name.as_str()))
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());
    // Pass NO body to the selection context (see /v1/messages): if
    // routing-only tokenization is unavailable, CacheAwareZmqPolicy::select
    // falls back to min-load rather than tokenizing the native Responses body.
    // `routing_key` is still honored by the sticky policy (it reads headers,
    // not body).
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

    // Hold the per-worker in-flight guard + register active load so load-aware
    // policies see this request. prefill_load uses the routing token count when
    // available, else the byte heuristic, same as chat's fallback.
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
        let fetch = ctx.proxy.forward_streaming_to_traced(
            &worker.url,
            &worker.breaker,
            "/v1/responses",
            worker_headers.as_ref(),
            body,
            Some(stream_guards),
            None,
            Some(make_client_disconnect_hook(Arc::clone(&ctx.metrics))),
            ctx.trace_sink.clone(),
            trace_ctx.clone(),
        );
        tokio::select! {
            biased;
            r = fetch => r,
            _ = stale_token.cancelled() => Err(ApiError::StaleRequestExpired { model: model_str }),
        }
    } else {
        let _holds: (LoadGuard, _, _) = (guard, active_guard, pending_guard);
        let fetch = ctx.proxy.forward_json_to_traced(
            &worker.url,
            &worker.breaker,
            "/v1/responses",
            worker_headers.as_ref(),
            body,
            ctx.trace_sink.clone(),
            trace_ctx.clone(),
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
    use super::{parse_probe, responses_routing_value};
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

    #[test]
    fn routing_value_builds_instructions_and_text_input() {
        let b = Bytes::from(
            r#"{
                "model":"gpt",
                "instructions":"be brief",
                "input":"hello",
                "metadata":{"session":"a"},
                "priority":100
            }"#,
        );

        assert_eq!(
            responses_routing_value(&b).expect("routing value"),
            serde_json::json!({
                "messages":[
                    {"role":"system","content":"be brief"},
                    {"role":"user","content":"hello"}
                ]
            })
        );
    }

    #[test]
    fn routing_value_normalizes_function_tools_calls_and_outputs() {
        let b = Bytes::from(
            r#"{
                "model":"gpt",
                "instructions":"root",
                "tools":[
                    {"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object"},"strict":true},
                    {"type":"web_search_preview","name":"web_search"}
                ],
                "tool_choice":{"type":"function","function":{"name":"lookup"}},
                "input":[
                    {"role":"developer","content":"dev"},
                    {"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},
                    {"type":"function_call","call_id":"call_1","name":"lookup","arguments":{"q":"x"}},
                    {"type":"function_call","call_id":"call_2","name":"lookup","arguments":"not-json"},
                    {"type":"function_call_output","call_id":"call_1","output":"result"}
                ]
            }"#,
        );

        assert_eq!(
            responses_routing_value(&b).expect("routing value"),
            serde_json::json!({
                "messages":[
                    {"role":"system","content":"root\n\ndev"},
                    {"role":"user","content":[{"type":"text","text":"hi"}]},
                    {
                        "role":"assistant",
                        "tool_calls":[
                            {"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"x\"}"}},
                            {"id":"call_2","type":"function","function":{"name":"lookup","arguments":"{}"}}
                        ]
                    },
                    {"role":"tool","tool_call_id":"call_1","content":"result"}
                ],
                "tools":[{
                    "type":"function",
                    "function":{
                        "name":"lookup",
                        "description":"Look up",
                        "parameters":{"type":"object"},
                        "strict":true
                    }
                }]
            })
        );
    }

    #[test]
    fn routing_value_rejects_previous_response_and_images() {
        let previous =
            Bytes::from(r#"{"model":"gpt","previous_response_id":"resp_1","input":"hello"}"#);
        assert!(responses_routing_value(&previous).is_none());

        let image = Bytes::from(
            r#"{"model":"gpt","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,abc"}]}]}"#,
        );
        assert!(responses_routing_value(&image).is_none());
    }
}

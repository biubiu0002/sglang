// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Anthropic `/v1/messages` passthrough route.
//!
//! Forwards the Anthropic request body to a selected SGLang worker at
//! `/v1/messages` without translating to/from OpenAI chat completions — the
//! worker natively serves `/v1/messages`. The router only needs `model` and
//! `stream` from the body for worker selection and buffered-vs-SSE routing.
//!
//! Deliberately does NOT replicate chat.rs's `input_ids` forwarding, PD
//! bootstrap injection, or decode-peer resolution (see design.md). It DOES
//! register active-load + hold the per-worker LoadGuard so load-aware
//! policies (`power_of_two`, `cache_aware_zmq`) see accurate in-flight
//! counts — without this the LB scheme would route on stale load.

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

/// Per-route body cap, mirroring chat. Same rationale: bound heap allocation
/// before forwarding while accommodating long contexts.
pub const MAX_MESSAGES_BODY_BYTES: usize = 5 << 20;

/// Minimal probe: `model` selects the worker, `stream` picks buffered vs SSE.
/// The worker is authoritative for the full Anthropic schema. `#[serde(default)]`
/// keeps it tolerant of optional fields — only `model` is required.
#[derive(Debug, Deserialize)]
struct MessagesProbe {
    #[serde(default)]
    stream: Option<bool>,
    model: Option<String>,
    /// Request priority, captured as a raw JSON value so a malformed value
    /// is tolerated (treated as `0`) rather than rejected. Gates
    /// capacity-restricted workers (see [`filter_eligible`]).
    #[serde(default)]
    priority: Option<Value>,
}

fn parse_probe(body: &Bytes) -> Result<MessagesProbe, ApiError> {
    serde_json::from_slice(body)
        .map_err(|_| ApiError::BadRequest("invalid request: body must be a JSON object".into()))
}

fn text_from_anthropic_content(content: &Value) -> Option<String> {
    match content {
        Value::String(s) => {
            let s = s.trim();
            (!s.is_empty()).then(|| s.to_string())
        }
        Value::Array(blocks) => {
            let texts: Vec<&str> = blocks
                .iter()
                .filter_map(|block| {
                    let text = block
                        .as_object()
                        .filter(|o| o.get("type").and_then(|t| t.as_str()) == Some("text"))?
                        .get("text")?
                        .as_str()?
                        .trim();
                    (!text.is_empty()).then_some(text)
                })
                .collect();
            (!texts.is_empty()).then(|| texts.join("\n"))
        }
        _ => None,
    }
}

fn collapse_content_parts(parts: &[Value]) -> Value {
    if parts.len() == 1 && parts[0].get("type").and_then(|t| t.as_str()) == Some("text") {
        Value::String(
            parts[0]
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string(),
        )
    } else {
        Value::Array(parts.to_vec())
    }
}

fn anthropic_tool_result_content(content: Option<&Value>) -> Option<(Value, String)> {
    match content {
        Some(Value::String(s)) => Some((Value::String(s.clone()), s.clone())),
        None | Some(Value::Null) => Some((Value::String(String::new()), String::new())),
        Some(Value::Array(blocks)) => {
            let mut parts = Vec::new();
            let mut text_parts = Vec::new();
            for block in blocks {
                let obj = block.as_object()?;
                match obj.get("type").and_then(|t| t.as_str()) {
                    Some("text") => {
                        let text = obj.get("text").and_then(|t| t.as_str()).unwrap_or("");
                        if !text.is_empty() {
                            text_parts.push(text.to_string());
                        }
                        parts.push(serde_json::json!({"type":"text","text":text}));
                    }
                    _ => return None,
                }
            }
            let joined = text_parts.join("\n");
            if parts.len() == 1 && parts[0].get("type").and_then(|t| t.as_str()) == Some("text") {
                let text = parts[0]
                    .get("text")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                Some((Value::String(text), joined))
            } else {
                Some((Value::Array(parts), joined))
            }
        }
        _ => None,
    }
}

fn emit_user_message(openai_messages: &mut Vec<Value>, parts: &mut Vec<Value>) {
    if parts.is_empty() {
        return;
    }
    openai_messages.push(serde_json::json!({
        "role": "user",
        "content": collapse_content_parts(parts),
    }));
    parts.clear();
}

fn anthropic_tools_for_chat(value: &Value) -> Option<Option<Value>> {
    let Some(tools) = value.get("tools").and_then(|t| t.as_array()) else {
        return Some(None);
    };
    let mut converted = Vec::new();
    for tool in tools {
        let obj = tool.as_object()?;
        let typ = obj.get("type").and_then(|t| t.as_str()).unwrap_or("custom");
        if typ.starts_with("web_search_")
            || typ.starts_with("computer_")
            || typ.starts_with("bash_")
            || typ.starts_with("text_editor_")
        {
            continue;
        }
        let name = obj.get("name").and_then(|n| n.as_str())?;
        let parameters = obj.get("input_schema")?.clone();
        let mut out = Map::new();
        out.insert("type".to_string(), Value::String("function".to_string()));
        if let Some(v) = obj.get("defer_loading") {
            out.insert("defer_loading".to_string(), v.clone());
        }
        out.insert(
            "function".to_string(),
            serde_json::json!({
                "name": name,
                "description": obj.get("description").and_then(|d| d.as_str()).unwrap_or(""),
                "parameters": parameters,
            }),
        );
        converted.push(Value::Object(out));
    }

    let Some(choice) = value.get("tool_choice").and_then(|c| c.as_object()) else {
        return Some((!converted.is_empty()).then(|| Value::Array(converted)));
    };
    match choice.get("type").and_then(|t| t.as_str()) {
        Some("none") => Some(None),
        Some("tool") => {
            let selected = choice.get("name").and_then(|n| n.as_str())?;
            let filtered: Vec<Value> = converted
                .into_iter()
                .filter(|tool| {
                    tool.get("function")
                        .and_then(|f| f.get("name"))
                        .and_then(|n| n.as_str())
                        == Some(selected)
                })
                .collect();
            if filtered.is_empty() {
                None
            } else {
                Some(Some(Value::Array(filtered)))
            }
        }
        Some("auto") | Some("any") | None => {
            Some((!converted.is_empty()).then(|| Value::Array(converted)))
        }
        _ => None,
    }
}

fn anthropic_message_to_chat(
    msg: &Value,
    system_parts: &mut Vec<String>,
    openai_messages: &mut Vec<Value>,
) -> Option<()> {
    let obj = msg.as_object()?;
    let role = obj.get("role").and_then(|r| r.as_str())?;
    let content = obj.get("content")?;
    if role == "system" {
        if let Some(text) = text_from_anthropic_content(content) {
            system_parts.push(text);
        }
        return Some(());
    }
    if matches!(content, Value::String(_)) {
        openai_messages.push(serde_json::json!({"role": role, "content": content}));
        return Some(());
    }
    let blocks = content.as_array()?;
    let mut content_parts = Vec::new();
    let mut tool_calls = Vec::new();

    for block in blocks {
        let block_obj = block.as_object()?;
        match block_obj.get("type").and_then(|t| t.as_str()) {
            Some("text") => content_parts.push(serde_json::json!({
                "type": "text",
                "text": block_obj.get("text").and_then(|t| t.as_str()).unwrap_or(""),
            })),
            Some("tool_use") => {
                if role != "assistant" {
                    return None;
                }
                let id = block_obj.get("id").and_then(|id| id.as_str())?;
                let name = block_obj
                    .get("name")
                    .and_then(|name| name.as_str())
                    .unwrap_or("");
                let input = block_obj
                    .get("input")
                    .filter(|input| input.is_object())
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let arguments = serde_json::to_string(&input).ok()?;
                tool_calls.push(serde_json::json!({
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": arguments,
                    },
                }));
            }
            Some("tool_result") => {
                let (tool_content, tool_text) =
                    anthropic_tool_result_content(block_obj.get("content"))?;
                let tool_call_id = block_obj
                    .get("tool_use_id")
                    .or_else(|| block_obj.get("id"))
                    .and_then(|id| id.as_str())
                    .unwrap_or("");
                if role == "user" {
                    emit_user_message(openai_messages, &mut content_parts);
                    openai_messages.push(serde_json::json!({
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": tool_content,
                    }));
                } else {
                    content_parts.push(serde_json::json!({
                        "type": "text",
                        "text": format!("Tool result: {tool_text}"),
                    }));
                }
            }
            Some("image")
            | Some("search_result")
            | Some("tool_reference")
            | Some("thinking")
            | Some("redacted_thinking") => return None,
            _ => return None,
        }
    }

    if role == "user" {
        emit_user_message(openai_messages, &mut content_parts);
        return Some(());
    }

    let mut openai_msg = Map::new();
    openai_msg.insert("role".to_string(), Value::String(role.to_string()));
    let has_tool_calls = !tool_calls.is_empty();
    if has_tool_calls {
        openai_msg.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }
    if !content_parts.is_empty() {
        openai_msg.insert(
            "content".to_string(),
            collapse_content_parts(&content_parts),
        );
    } else if !has_tool_calls {
        openai_msg.insert("content".to_string(), Value::String(String::new()));
    }
    openai_messages.push(Value::Object(openai_msg));
    Some(())
}

/// Build a routing-only OpenAI-chat-shaped view of an Anthropic request.
///
/// The worker is still authoritative for Anthropic validation/conversion, and
/// the original body is forwarded unchanged. This view exists only to let
/// cache-aware routing hash the same chat-shaped prompt that the worker builds
/// before tokenization. It intentionally omits request metadata, headers,
/// priority, and other transport fields.
fn anthropic_routing_value(body: &Bytes) -> Option<Value> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let messages = value.get("messages")?.as_array()?;

    let mut system_parts = Vec::new();
    if let Some(system) = value.get("system").and_then(text_from_anthropic_content) {
        system_parts.push(system);
    }

    let mut routed_messages = Vec::new();
    for msg in messages {
        anthropic_message_to_chat(msg, &mut system_parts, &mut routed_messages)?;
    }

    if !system_parts.is_empty() {
        routed_messages.insert(
            0,
            serde_json::json!({
                "role": "system",
                "content": system_parts.join("\n"),
            }),
        );
    }

    let mut out = Map::new();
    out.insert("messages".to_string(), Value::Array(routed_messages));
    if let Some(tools) = anthropic_tools_for_chat(&value)? {
        out.insert("tools".to_string(), tools);
    }
    Some(Value::Object(out))
}

/// POST /v1/messages — select a worker via the per-model policy and proxy the
/// raw Anthropic body to `<worker>/v1/messages`.
pub async fn messages(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let result = async {
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
            return messages_inner(State(ctx), headers, body, "/v1/messages").await;
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
            path = "/v1/messages",
            "alias primary selected",
        );
        let primary = messages_inner(
            State(Arc::clone(&ctx)),
            headers.clone(),
            primary_body,
            "/v1/messages",
        )
        .await;
        match primary {
            Ok(resp) => {
                if let Some(reason) = fallback_reason_for_response(resp.status()) {
                    forward_to_fallback(
                        &ctx,
                        &cfg,
                        &headers,
                        &body,
                        "/v1/messages",
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
                        "/v1/messages",
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
    .await;
    match result {
        Ok(resp) => resp,
        Err(e) => anthropic_error_response(e),
    }
}

/// POST /v1/messages/count_tokens — select a worker via the same per-model
/// policy as `/v1/messages` and proxy the raw Anthropic body to
/// `<worker>/v1/messages/count_tokens`. The worker natively serves this
/// endpoint (returns `{"input_tokens": N}`). Always buffered: the count_tokens
/// body carries no `stream` field, so the probe yields `streaming = false` and
/// the request takes the buffered forward path. Claude Code calls this before
/// each turn to size context, so it MUST be served once claude-proxy is retired.
pub async fn count_tokens(
    State(ctx): State<Arc<AppContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let result = async {
        let body = apply_request_priority_override(&ctx.config.priority_override, &headers, body)?;
        messages_inner(State(ctx), headers, body, "/v1/messages/count_tokens").await
    }
    .await;
    match result {
        Ok(resp) => resp,
        Err(e) => anthropic_error_response(e),
    }
}

/// Map a router-originated `ApiError` to an Anthropic Messages error envelope
/// `{"type":"error","error":{"type":...,"message":...}}` so Anthropic SDK clients
/// parse router-side failures (missing `model`, PD-reject, no healthy worker,
/// breaker open, stale). Worker-originated errors are forwarded verbatim by the
/// proxy and are already Anthropic-shaped, so they never reach here.
///
/// `message` comes from `ApiError::client_message()` — the same sanitized string
/// the OpenAI envelope uses, so no worker URL / anyhow chain leaks.
fn anthropic_error_response(e: ApiError) -> Response<Body> {
    use serde::Serialize;
    #[derive(Serialize)]
    struct AnthropicErr {
        #[serde(rename = "type")]
        typ: &'static str,
        message: String,
    }
    #[derive(Serialize)]
    struct Envelope {
        #[serde(rename = "type")]
        typ: &'static str,
        error: AnthropicErr,
    }
    let status = e.status_code();
    let typ = match status.as_u16() {
        400 => "invalid_request_error",
        404 => "not_found_error",
        503 => "overloaded_error",
        _ => "api_error",
    };
    let message = e.client_message();
    let body = serde_json::to_vec(&Envelope {
        typ: "error",
        error: AnthropicErr { typ, message },
    })
    .unwrap_or_else(|_| {
        b"{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"internal error\"}}"
            .to_vec()
    });
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = status;
    r.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    r
}

async fn messages_inner(
    State(ctx): State<Arc<AppContext>>,
    mut headers: HeaderMap,
    body: Bytes,
    forward_path: &'static str,
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
        forward_path,
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

    // PD-disaggregated mode is unsupported on this route, and that is a
    // permanent property of the model/route — NOT a transient capacity
    // condition. Reject it (400) BEFORE priority filtering so a PD model
    // whose only prefill candidate is priority-gated surfaces the honest
    // "PD not supported" error rather than a misleading 503 from the filter
    // emptying the candidate set. A model's workers are homogeneous in mode
    // (`prefill_candidates` yields prefill/non-Plain workers only for PD
    // deployments), so any non-Plain candidate marks a PD topology. This
    // passthrough forwards to a single worker and does NOT replicate
    // chat.rs's decode-peer resolution + bootstrap body injection (see
    // design.md); in PD mode the final response comes from the decode side,
    // so silently forwarding to a prefill worker would hang.
    if workers.iter().any(|w| w.mode() != WorkerMode::Plain) {
        return Err(ApiError::BadRequest(
            "/v1/messages passthrough does not support PD-disaggregated mode yet; use /v1/chat/completions".into(),
        ));
    }

    // Priority-eligibility filtering — identical semantics to the
    // `/v1/chat/completions` path: capacity-restricted workers are removed
    // for sub-threshold requests before policy selection. Hard isolation:
    // if filtering empties the candidate set, reject with 503 rather than
    // spill the request onto a gated worker.
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

    // Produce routing-only tokens for /v1/messages generation requests.
    //
    // The cache-aware-zmq policy hashes the request to find a worker that
    // already holds the prefix in its KV cache. For chat completions the
    // router's chat-encoder tokenization matches the engine's cached blocks.
    // Anthropic bodies need one extra normalization step before that is useful:
    // the worker folds top-level `system` into the prompt before tokenizing.
    // Hashing raw `messages` would miss that prefix and create false locality
    // between requests with different systems. Build a routing-only chat-shaped
    // value that includes the folded system and feed that to the existing chat
    // encoder. The original Anthropic body is still forwarded unchanged, and we
    // never inject `input_ids` on this route, so request semantics remain
    // entirely worker-owned.
    let request_tokens: Option<RequestTokens> = if forward_path == "/v1/messages" {
        let routing_value = anthropic_routing_value(&body);
        routing_value
            .as_ref()
            .and_then(|v| request_tokens_for(&ctx.tokenizers, &model_id, v))
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
    // Pass NO body to the selection context. If the routing-only tokenization
    // above fails, CacheAwareZmqPolicy::select falls back to min-load instead of
    // tokenizing the raw Anthropic body and losing the top-level `system`.
    // `routing_key` is still honored by the sticky policy (it reads headers, not body).
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
    // policies see this request. prefill_load uses the real token count when we
    // tokenized, else the byte heuristic (same as chat).
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
        // ponytail: skip chat's TTFT hook + streaming-duration RAII guard —
        // v1 measures header-time only; end-to-end streaming latency is a
        // follow-up. Guards move into stream_guards so load stays accurate.
        let stream_guards: Box<dyn Send + 'static> = Box::new((guard, active_guard, pending_guard));
        let fetch = ctx.proxy.forward_streaming_to_traced(
            &worker.url,
            &worker.breaker,
            forward_path,
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
            forward_path,
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
        path = forward_path,
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
        "messages",
    );
    result
}

#[cfg(test)]
mod tests {
    use super::{anthropic_routing_value, parse_probe};
    use crate::config::{
        ActiveLoadConfig, Config, DiscoveryBackend, ModelConfig, ObservabilityConfig, PolicyKind,
        ProxyConfig, ServerConfig, StaticUrlsDiscoveryConfig,
    };
    use crate::discovery::ModelId;
    use crate::policies::request_tokens_for;
    use crate::tokenizer::TokenizerRegistry;
    use bytes::Bytes;

    #[test]
    fn probe_reads_stream_and_model() {
        let b = Bytes::from(r#"{"model":"glm","stream":true,"messages":[]}"#);
        let p = parse_probe(&b).unwrap();
        assert_eq!(p.stream, Some(true));
        assert_eq!(p.model.as_deref(), Some("glm"));
    }

    #[test]
    fn probe_stream_defaults_to_false() {
        let b = Bytes::from(r#"{"model":"glm","messages":[]}"#);
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
    fn probe_allows_anthropic_shape_without_stream() {
        // Real Anthropic body has system/messages/max_tokens; only model is required here.
        let b = Bytes::from(
            r#"{"model":"claude-3","max_tokens":256,"system":"s","messages":[{"role":"user","content":"hi"}]}"#,
        );
        let p = parse_probe(&b).unwrap();
        assert_eq!(p.model.as_deref(), Some("claude-3"));
        assert_eq!(p.stream, None);
    }

    #[test]
    fn routing_value_folds_system_into_chat_messages() {
        let b = Bytes::from(
            r#"{
                "model":"claude-3",
                "max_tokens":256,
                "metadata":{"user_id":"session-a"},
                "priority":0,
                "system":[
                    {"type":"text","text":"top system"},
                    {"type":"text","text":"second"}
                ],
                "messages":[
                    {"role":"system","content":"mid system"},
                    {"role":"user","content":"hi"}
                ]
            }"#,
        );

        let routed = anthropic_routing_value(&b).expect("routing value");
        assert_eq!(
            routed,
            serde_json::json!({
                "messages": [
                    {"role":"system","content":"top system\nsecond\nmid system"},
                    {"role":"user","content":"hi"}
                ]
            })
        );
    }

    #[test]
    fn routing_value_maps_anthropic_tools_and_tool_blocks() {
        let b = Bytes::from(
            r#"{
                "model":"claude-3",
                "tools":[{
                    "name":"lookup",
                    "description":"Look up data",
                    "input_schema":{"type":"object","properties":{"q":{"type":"string"}}}
                }],
                "tool_choice":{"type":"tool","name":"lookup"},
                "messages":[
                    {"role":"user","content":"find x"},
                    {"role":"assistant","content":[
                        {"type":"text","text":"checking"},
                        {"type":"tool_use","id":"call_1","name":"lookup","input":{"q":"x"}}
                    ]},
                    {"role":"user","content":[
                        {"type":"tool_result","tool_use_id":"call_1","content":"result x"},
                        {"type":"text","text":"thanks"}
                    ]}
                ]
            }"#,
        );

        let routed = anthropic_routing_value(&b).expect("routing value");
        assert_eq!(
            routed,
            serde_json::json!({
                "messages": [
                    {"role":"user","content":"find x"},
                    {
                        "role":"assistant",
                        "tool_calls":[{
                            "id":"call_1",
                            "type":"function",
                            "function":{"name":"lookup","arguments":"{\"q\":\"x\"}"}
                        }],
                        "content":"checking"
                    },
                    {"role":"tool","tool_call_id":"call_1","content":"result x"},
                    {"role":"user","content":"thanks"}
                ],
                "tools":[{
                    "type":"function",
                    "function":{
                        "name":"lookup",
                        "description":"Look up data",
                        "parameters":{"type":"object","properties":{"q":{"type":"string"}}}
                    }
                }]
            })
        );
    }

    #[test]
    fn routing_value_keeps_text_blocks_but_rejects_thinking_blocks() {
        let text_only = Bytes::from(
            r#"{
                "model":"claude-3",
                "system":[{"type":"text","text":"be terse"}],
                "messages":[{"role":"user","content":[
                    {"type":"text","text":"hello"},
                    {"type":"text","text":"world"}
                ]}]
            }"#,
        );
        let routed = anthropic_routing_value(&text_only).expect("text routing value");
        assert_eq!(
            routed,
            serde_json::json!({
                "messages": [
                    {"role":"system","content":"be terse"},
                    {"role":"user","content":[
                        {"type":"text","text":"hello"},
                        {"type":"text","text":"world"}
                    ]}
                ]
            })
        );

        let thinking = Bytes::from(
            r#"{
                "model":"claude-3",
                "messages":[{"role":"assistant","content":[
                    {"type":"text","text":"answer"},
                    {"type":"thinking","thinking":"reasoning","signature":"sig"}
                ]}]
            }"#,
        );
        assert!(
            anthropic_routing_value(&thinking).is_none(),
            "thinking blocks are worker-owned Anthropic state; do not approximate a routing prompt"
        );
    }

    #[test]
    fn routing_value_rejects_redacted_thinking_and_bare_tool_result() {
        let redacted = Bytes::from(
            r#"{
                "model":"claude-3",
                "messages":[{"role":"assistant","content":[
                    {"type":"redacted_thinking","data":"opaque"}
                ]}]
            }"#,
        );
        assert!(
            anthropic_routing_value(&redacted).is_none(),
            "redacted thinking must not contribute to routing text"
        );

        let bare_tool_result = Bytes::from(
            r#"{
                "model":"claude-3",
                "messages":[{"role":"user","content":[
                    {"type":"tool_result","tool_use_id":"call_1","content":"42"}
                ]}]
            }"#,
        );
        assert_eq!(
            anthropic_routing_value(&bare_tool_result),
            Some(serde_json::json!({
                "messages": [
                    {"role":"tool","tool_call_id":"call_1","content":"42"}
                ]
            })),
            "tool_result text is represented as a tool message, not folded into user text"
        );
    }

    #[test]
    fn routing_value_rejects_unsupported_multimodal_messages() {
        let b = Bytes::from(
            r#"{
                "model":"claude-3",
                "messages":[{"role":"user","content":[
                    {"type":"image","source":{"type":"base64","media_type":"image/png","data":"abc"}}
                ]}]
            }"#,
        );

        assert!(
            anthropic_routing_value(&b).is_none(),
            "unsupported multimodal content must not create approximate route-history prefixes"
        );
    }

    #[test]
    fn routing_value_ignores_transport_metadata() {
        let a = Bytes::from(
            r#"{
                "model":"claude-3",
                "max_tokens":256,
                "metadata":{"user_id":"session-a"},
                "priority":0,
                "system":"same system",
                "messages":[{"role":"user","content":"hi"}]
            }"#,
        );
        let b = Bytes::from(
            r#"{
                "model":"claude-3",
                "max_tokens":256,
                "metadata":{"user_id":"session-b"},
                "priority":100,
                "system":"same system",
                "messages":[{"role":"user","content":"hi"}]
            }"#,
        );

        assert_eq!(
            anthropic_routing_value(&a),
            anthropic_routing_value(&b),
            "session metadata and priority must not perturb routing prompt"
        );
    }

    #[test]
    fn routing_value_changes_when_system_changes() {
        let a = Bytes::from(
            r#"{"model":"claude-3","max_tokens":256,"system":"system-a","messages":[{"role":"user","content":"hi"}]}"#,
        );
        let b = Bytes::from(
            r#"{"model":"claude-3","max_tokens":256,"system":"system-b","messages":[{"role":"user","content":"hi"}]}"#,
        );

        assert_ne!(
            anthropic_routing_value(&a),
            anthropic_routing_value(&b),
            "routing prompt must include top-level system"
        );
    }

    #[test]
    fn routing_tokens_include_system_but_ignore_metadata() {
        let cfg = Config {
            runtime_mode: crate::config::RuntimeMode::Gateway,
            server: ServerConfig {
                host: "0".into(),
                port: 0,
            },
            observability: ObservabilityConfig::default(),
            model: ModelConfig {
                id: "tiny".into(),
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
            proxy: ProxyConfig::default(),
            active_load: ActiveLoadConfig::default(),
            trace: crate::config::TraceConfig::default(),
            priority_override: crate::config::PriorityOverrideConfig::default(),
            worker_introspect_key: None,
            load_poll_interval_secs: None,
            cache_tree_page_size: None,
            cache_tree_bigram: false,
            cache_tree_max_nodes: 1_000_000,
            cache_state_url: None,
            cache_state_timeout_ms: 20,
            alias_fallback: None,
        };
        let registry = TokenizerRegistry::load_from_config(&cfg).unwrap();
        registry.attach_chat_template_for_test(
            "tiny",
            &serde_json::json!({
                "chat_template": "{{ bos_token }}{% for m in messages %}<|{{ m['role'] }}|>{{ m['content'] }}{% endfor %}",
                "bos_token": "<s>",
            }),
        );
        let model = ModelId("tiny".into());

        let session_a = Bytes::from(
            r#"{
                "model":"tiny",
                "max_tokens":256,
                "metadata":{"user_id":"session-a"},
                "priority":0,
                "system":"same system",
                "messages":[{"role":"user","content":"hi"}]
            }"#,
        );
        let session_b = Bytes::from(
            r#"{
                "model":"tiny",
                "max_tokens":256,
                "metadata":{"user_id":"session-b"},
                "priority":100,
                "system":"same system",
                "messages":[{"role":"user","content":"hi"}]
            }"#,
        );
        let different_system = Bytes::from(
            r#"{
                "model":"tiny",
                "max_tokens":256,
                "metadata":{"user_id":"session-b"},
                "priority":100,
                "system":"different system",
                "messages":[{"role":"user","content":"hi"}]
            }"#,
        );

        let ids_a = request_tokens_for(
            &registry,
            &model,
            &anthropic_routing_value(&session_a).unwrap(),
        )
        .expect("session a tokens")
        .ids;
        let ids_b = request_tokens_for(
            &registry,
            &model,
            &anthropic_routing_value(&session_b).unwrap(),
        )
        .expect("session b tokens")
        .ids;
        let ids_system = request_tokens_for(
            &registry,
            &model,
            &anthropic_routing_value(&different_system).unwrap(),
        )
        .expect("different system tokens")
        .ids;

        assert_eq!(
            ids_a, ids_b,
            "metadata/priority must not perturb cache-aware routing tokens"
        );
        assert_ne!(
            ids_a, ids_system,
            "top-level system must perturb cache-aware routing tokens"
        );
    }
}

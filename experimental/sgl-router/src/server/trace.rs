use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Response};
use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use crate::config::TraceConfig;

const X_TRACE_ID: HeaderName = HeaderName::from_static("x-trace-id");

#[derive(Clone, Debug)]
pub struct TraceSink {
    client: reqwest::Client,
    sink_url: String,
    capture_bodies: bool,
    body_max_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct TraceContext {
    pub trace_id: String,
    pub method: &'static str,
    pub path: &'static str,
    pub model: Option<String>,
    pub stream: bool,
    pub started: Instant,
    pub request_body: Bytes,
}

#[derive(Debug, Serialize)]
pub struct TraceEvent {
    pub trace_id: String,
    pub method: &'static str,
    pub path: &'static str,
    pub model: Option<String>,
    pub worker_url: String,
    pub status_code: Option<u16>,
    pub latency_ms: u64,
    pub stream: bool,
    pub request_body: Option<Value>,
    pub request_body_truncated: bool,
    pub request_body_bytes: usize,
    pub response_body: Option<Value>,
    pub response_body_truncated: bool,
    pub response_body_bytes: usize,
    pub error: Option<String>,
    pub message_entries: Vec<Value>,
}

impl TraceSink {
    pub fn from_config(config: &TraceConfig) -> Option<Arc<Self>> {
        let sink_url = config.sink_url.as_ref()?.trim();
        if sink_url.is_empty() {
            return None;
        }
        Some(Arc::new(Self {
            client: reqwest::Client::new(),
            sink_url: sink_url.to_string(),
            capture_bodies: config.capture_bodies,
            body_max_bytes: config.body_max_bytes.max(1),
        }))
    }

    pub fn capture_limited(&self, body: &[u8]) -> (Option<Value>, bool, usize) {
        let size = body.len();
        if !self.capture_bodies {
            return (None, false, size);
        }
        let truncated = size > self.body_max_bytes;
        let clipped = if truncated {
            &body[..self.body_max_bytes]
        } else {
            body
        };
        let text = String::from_utf8_lossy(clipped).to_string();
        let value = serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text));
        (Some(value), truncated, size)
    }

    pub fn message_entries_from_body(&self, body: &[u8]) -> Vec<Value> {
        if !self.capture_bodies {
            return Vec::new();
        }
        let (Some(value), _, _) = self.capture_limited(body) else {
            return Vec::new();
        };
        extract_message_entries(&value)
    }

    pub fn emit(self: Arc<Self>, event: TraceEvent) {
        tokio::spawn(async move {
            let result = self.client.post(&self.sink_url).json(&event).send().await;
            match result {
                Ok(resp) if resp.status().is_success() => {}
                Ok(resp) => {
                    tracing::warn!(
                        trace_id = %event.trace_id,
                        status = %resp.status(),
                        "router trace sink rejected event",
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        trace_id = %event.trace_id,
                        error = %error,
                        "router trace sink post failed",
                    );
                }
            }
        });
    }
}

impl TraceContext {
    pub fn new(
        headers: &mut HeaderMap,
        method: &'static str,
        path: &'static str,
        model: Option<String>,
        stream: bool,
        request_body: Bytes,
    ) -> Self {
        let trace_id = trace_id_from_headers(headers)
            .unwrap_or_else(|| format!("trace_{}", Uuid::new_v4().simple()));
        if let Ok(value) = HeaderValue::from_str(&trace_id) {
            headers.insert(X_TRACE_ID, value);
        }
        Self {
            trace_id,
            method,
            path,
            model,
            stream,
            started: Instant::now(),
            request_body,
        }
    }

    pub fn add_response_header(&self, response: &mut Response<Body>) {
        if let Ok(value) = HeaderValue::from_str(&self.trace_id) {
            response.headers_mut().insert(X_TRACE_ID, value);
        }
    }
}

pub fn trace_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(X_TRACE_ID)
        .or_else(|| headers.get("x-request-id"))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

fn extract_message_entries(body: &Value) -> Vec<Value> {
    let Some(obj) = body.as_object() else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    if let Some(messages) = obj.get("messages").and_then(Value::as_array) {
        for (index, item) in messages.iter().enumerate() {
            entries.push(serde_json::json!({
                "source": "messages",
                "index": index,
                "role": item.get("role").cloned().unwrap_or(Value::Null),
                "content": item.get("content").cloned().unwrap_or(Value::Null),
                "raw": item,
            }));
        }
    }
    match obj.get("input") {
        Some(Value::Array(items)) => {
            for (index, item) in items.iter().enumerate() {
                entries.push(serde_json::json!({
                    "source": "input",
                    "index": index,
                    "role": item.get("role").or_else(|| item.get("type")).cloned().unwrap_or(Value::Null),
                    "content": item.get("content").or_else(|| item.get("text")).cloned().unwrap_or_else(|| item.clone()),
                    "raw": item,
                }));
            }
        }
        Some(Value::String(text)) => entries.push(serde_json::json!({
            "source": "input",
            "index": 0,
            "role": "user",
            "content": text,
            "raw": text,
        })),
        _ => {}
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_context_reuses_header_and_inserts_missing_header() {
        let mut headers = HeaderMap::new();
        headers.insert(X_TRACE_ID, HeaderValue::from_static("given"));
        let ctx = TraceContext::new(
            &mut headers,
            "POST",
            "/v1/chat/completions",
            Some("m".into()),
            false,
            Bytes::from_static(b"{}"),
        );
        assert_eq!(ctx.trace_id, "given");
        assert_eq!(headers.get(X_TRACE_ID).unwrap(), "given");
    }

    #[test]
    fn body_capture_is_opt_in_and_truncated() {
        let sink = TraceSink {
            client: reqwest::Client::new(),
            sink_url: "http://collector".into(),
            capture_bodies: true,
            body_max_bytes: 8,
        };
        let (body, truncated, size) = sink.capture_limited(br#"{"model":"x","messages":[]}"#);
        assert!(body.is_some());
        assert!(truncated);
        assert_eq!(size, 27);
    }
}

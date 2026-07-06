// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use crate::config::PriorityOverrideConfig;
use crate::server::error::ApiError;
use axum::http::{HeaderMap, HeaderName};
use bytes::Bytes;
use serde_json::{Number, Value};

pub(crate) fn apply_request_priority_override(
    config: &PriorityOverrideConfig,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Bytes, ApiError> {
    let Some(priority) = effective_priority_override(config, headers) else {
        return Ok(body);
    };

    let mut value: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError::BadRequest("invalid request: body must be a JSON object".into()))?;
    let object = value.as_object_mut().ok_or_else(|| {
        ApiError::BadRequest("invalid request: body must be a JSON object".into())
    })?;
    object.insert(
        "priority".to_string(),
        Value::Number(Number::from(priority)),
    );
    let body = serde_json::to_vec(&value)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("serialize priority override: {e}")))?;
    Ok(Bytes::from(body))
}

fn effective_priority_override(
    config: &PriorityOverrideConfig,
    headers: &HeaderMap,
) -> Option<i64> {
    trusted_priority(config, headers).or(config.force_request_priority)
}

fn trusted_priority(config: &PriorityOverrideConfig, headers: &HeaderMap) -> Option<i64> {
    let priority_header = HeaderName::try_from(config.trusted_priority_header.as_deref()?).ok()?;
    let secret_header =
        HeaderName::try_from(config.trusted_priority_secret_header.as_deref()?).ok()?;
    let expected_secret = config.trusted_priority_secret.as_deref()?;

    let actual_secret = headers.get(secret_header)?.to_str().ok()?;
    if actual_secret != expected_secret {
        return None;
    }

    headers
        .get(priority_header)?
        .to_str()
        .ok()?
        .trim()
        .parse::<i64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(force: Option<i64>) -> PriorityOverrideConfig {
        PriorityOverrideConfig {
            force_request_priority: force,
            trusted_priority_header: None,
            trusted_priority_secret_header: None,
            trusted_priority_secret: None,
        }
    }

    fn trusted_cfg(force: Option<i64>) -> PriorityOverrideConfig {
        PriorityOverrideConfig {
            force_request_priority: force,
            trusted_priority_header: Some("x-internal-priority".into()),
            trusted_priority_secret_header: Some("x-internal-priority-secret".into()),
            trusted_priority_secret: Some("secret".into()),
        }
    }

    fn body_priority(body: Bytes) -> i64 {
        serde_json::from_slice::<Value>(&body).unwrap()["priority"]
            .as_i64()
            .unwrap()
    }

    #[test]
    fn leaves_body_unchanged_without_override() {
        let input = Bytes::from_static(br#"{"model":"m"}"#);
        let output =
            apply_request_priority_override(&cfg(None), &HeaderMap::new(), input.clone()).unwrap();
        assert_eq!(output, input);
    }

    #[test]
    fn applies_forced_priority() {
        let output = apply_request_priority_override(
            &cfg(Some(0)),
            &HeaderMap::new(),
            Bytes::from_static(br#"{"model":"m","priority":100}"#),
        )
        .unwrap();
        assert_eq!(body_priority(output), 0);
    }

    #[test]
    fn trusted_header_overrides_force() {
        let mut headers = HeaderMap::new();
        headers.insert("x-internal-priority", "100".parse().unwrap());
        headers.insert("x-internal-priority-secret", "secret".parse().unwrap());
        let output = apply_request_priority_override(
            &trusted_cfg(Some(0)),
            &headers,
            Bytes::from_static(br#"{"model":"m"}"#),
        )
        .unwrap();
        assert_eq!(body_priority(output), 100);
    }

    #[test]
    fn bad_secret_falls_back_to_force() {
        let mut headers = HeaderMap::new();
        headers.insert("x-internal-priority", "100".parse().unwrap());
        headers.insert("x-internal-priority-secret", "wrong".parse().unwrap());
        let output = apply_request_priority_override(
            &trusted_cfg(Some(0)),
            &headers,
            Bytes::from_static(br#"{"model":"m"}"#),
        )
        .unwrap();
        assert_eq!(body_priority(output), 0);
    }

    #[test]
    fn invalid_trusted_priority_falls_back_to_force() {
        let mut headers = HeaderMap::new();
        headers.insert("x-internal-priority", "high".parse().unwrap());
        headers.insert("x-internal-priority-secret", "secret".parse().unwrap());
        let output = apply_request_priority_override(
            &trusted_cfg(Some(0)),
            &headers,
            Bytes::from_static(br#"{"model":"m"}"#),
        )
        .unwrap();
        assert_eq!(body_priority(output), 0);
    }

    #[test]
    fn rejects_non_object_json_when_override_is_active() {
        let err = apply_request_priority_override(
            &cfg(Some(0)),
            &HeaderMap::new(),
            Bytes::from_static(br#"[]"#),
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("JSON object"));
    }

    #[test]
    fn rewrite_preserves_other_fields() {
        let output = apply_request_priority_override(
            &cfg(Some(0)),
            &HeaderMap::new(),
            Bytes::from(
                serde_json::to_vec(&json!({"model":"m","messages":[{"role":"user"}]})).unwrap(),
            ),
        )
        .unwrap();
        let parsed: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(parsed["model"], "m");
        assert_eq!(parsed["priority"], 0);
    }
}

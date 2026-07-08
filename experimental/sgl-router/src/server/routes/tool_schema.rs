// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use serde_json::Value;

/// Normalize client-generated JSON Schemas in tool definitions.
///
/// Some upstream schema generators serialize "no required properties" as
/// `required: null`. JSON Schema requires `required` to be an array when it is
/// present, and SGLang's worker validator rejects the null form. Dropping the
/// null field preserves the intended optional-property semantics while keeping
/// other malformed schemas visible to the worker.
pub(crate) fn normalize_tool_schema(schema: &mut Value) {
    match schema {
        Value::Object(map) => {
            if map.get("required").is_some_and(Value::is_null) {
                map.remove("required");
            }
            for child in map.values_mut() {
                normalize_tool_schema(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_tool_schema(item);
            }
        }
        _ => {}
    }
}

pub(crate) fn normalize_chat_tool_schemas(value: &mut Value) {
    let Some(tools) = value.get_mut("tools").and_then(Value::as_array_mut) else {
        return;
    };
    for tool in tools {
        if tool.get("type").and_then(Value::as_str) != Some("function") {
            continue;
        }
        if let Some(parameters) = tool
            .get_mut("function")
            .and_then(Value::as_object_mut)
            .and_then(|function| function.get_mut("parameters"))
        {
            normalize_tool_schema(parameters);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_tool_schema_removes_nested_required_null() {
        let mut schema = serde_json::json!({
            "type": "object",
            "required": null,
            "properties": {
                "query": {"type": "string"},
                "filters": {
                    "type": "object",
                    "required": null,
                    "properties": {
                        "tag": {"type": "string"}
                    }
                },
                "items": {
                    "type": "array",
                    "items": {"type": "object", "required": null}
                }
            },
            "anyOf": [
                {"type": "object", "required": null},
                {"type": "object", "required": ["query"]}
            ]
        });

        normalize_tool_schema(&mut schema);

        assert_eq!(
            schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "filters": {
                        "type": "object",
                        "properties": {
                            "tag": {"type": "string"}
                        }
                    },
                    "items": {
                        "type": "array",
                        "items": {"type": "object"}
                    }
                },
                "anyOf": [
                    {"type": "object"},
                    {"type": "object", "required": ["query"]}
                ]
            })
        );
    }

    #[test]
    fn normalize_chat_tool_schemas_only_touches_function_parameters() {
        let mut body = serde_json::json!({
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "lookup",
                        "parameters": {"type": "object", "required": null}
                    }
                },
                {"type": "web_search_preview", "required": null},
                {"type": "function", "function": {"name": "no_params"}}
            ]
        });

        normalize_chat_tool_schemas(&mut body);

        assert_eq!(
            body,
            serde_json::json!({
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "parameters": {"type": "object"}
                        }
                    },
                    {"type": "web_search_preview", "required": null},
                    {"type": "function", "function": {"name": "no_params"}}
                ]
            })
        );
    }
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

const APP_CONFIG_SCOPE_RESOURCE: &str = "https://azconfig.io";
const MANAGED_IDENTITY_TOKEN_URL: &str = "http://169.254.169.254/metadata/identity/oauth2/token";

#[derive(Clone, Debug, Default)]
pub struct RegistrySource {
    pub inline_json: Option<String>,
    pub file: Option<String>,
    pub app_config: Option<AppConfigSource>,
}

#[derive(Clone, Debug)]
pub struct AppConfigSource {
    pub endpoint: String,
    pub key: String,
    pub label: Option<String>,
    pub managed_identity_client_id: Option<String>,
    pub timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct WorkerRegistry {
    pub schema: String,
    #[serde(default)]
    pub pools: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub workers: HashMap<String, RegistryWorker>,
}

#[derive(Debug, Deserialize)]
pub struct RegistryWorker {
    pub url: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub pool_url_suffixes: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ManagedIdentityToken {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct AppConfigKeyValue {
    value: String,
}

fn default_enabled() -> bool {
    true
}

impl RegistrySource {
    pub fn is_configured(&self) -> bool {
        self.inline_json.is_some() || self.file.is_some() || self.app_config.is_some()
    }

    pub async fn load(&self) -> Result<String> {
        let configured = self.inline_json.is_some() as u8
            + self.file.is_some() as u8
            + self.app_config.is_some() as u8;
        if configured > 1 {
            return Err(anyhow!(
                "configure only one worker registry source: inline JSON, file, or App Configuration"
            ));
        }
        if let Some(json) = &self.inline_json {
            return Ok(json.clone());
        }
        if let Some(path) = &self.file {
            return std::fs::read_to_string(path)
                .with_context(|| format!("read worker registry file {path}"));
        }
        if let Some(app_config) = &self.app_config {
            return app_config.fetch_value().await;
        }
        Err(anyhow!("worker registry source is not configured"))
    }
}

impl AppConfigSource {
    pub async fn fetch_value(&self) -> Result<String> {
        if self.timeout_secs == 0 {
            return Err(anyhow!("App Configuration timeout must be greater than 0"));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.timeout_secs))
            .build()
            .context("build App Configuration HTTP client")?;
        let token =
            fetch_managed_identity_token(&client, self.managed_identity_client_id.as_deref())
                .await?;
        let url = app_config_key_url(&self.endpoint, &self.key, self.label.as_deref())?;
        let response = client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .context("fetch worker registry from Azure App Configuration")?
            .error_for_status()
            .context("Azure App Configuration worker registry request failed")?
            .json::<AppConfigKeyValue>()
            .await
            .context("decode Azure App Configuration key/value response")?;
        Ok(response.value)
    }
}

pub fn parse_registry(raw_json: &str) -> Result<WorkerRegistry> {
    let registry: WorkerRegistry =
        serde_json::from_str(raw_json).context("parse worker registry JSON")?;
    if registry.schema != "macaron.worker_registry.v1" {
        return Err(anyhow!(
            "unsupported worker registry schema {:?}",
            registry.schema
        ));
    }
    Ok(registry)
}

pub fn worker_urls_for_pool(
    registry: &WorkerRegistry,
    pool: &str,
    default_url_suffix: Option<&str>,
) -> Result<Vec<String>> {
    let names = registry
        .pools
        .get(pool)
        .with_context(|| format!("worker registry pool {pool:?} is missing"))?;
    if names.is_empty() {
        return Err(anyhow!("worker registry pool {pool:?} is empty"));
    }

    let mut urls = Vec::with_capacity(names.len());
    for name in names {
        let worker = registry.workers.get(name).with_context(|| {
            format!("worker registry pool {pool:?} references missing worker {name:?}")
        })?;
        if !worker.enabled {
            return Err(anyhow!(
                "worker registry pool {pool:?} references disabled worker {name:?}"
            ));
        }
        let url = worker
            .url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .with_context(|| {
                format!("worker registry worker {name:?} in pool {pool:?} has no url")
            })?;
        let suffix = worker
            .pool_url_suffixes
            .get(pool)
            .map(String::as_str)
            .or(default_url_suffix)
            .unwrap_or("");
        urls.push(format!("{url}{suffix}"));
    }
    Ok(urls)
}

fn app_config_key_url(endpoint: &str, key: &str, label: Option<&str>) -> Result<Url> {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.is_empty() {
        return Err(anyhow!("App Configuration endpoint is empty"));
    }
    if key.trim().is_empty() {
        return Err(anyhow!("App Configuration key is empty"));
    }
    let encoded_key: String = url::form_urlencoded::byte_serialize(key.as_bytes()).collect();
    let mut url = Url::parse(&format!("{endpoint}/kv/{encoded_key}"))
        .with_context(|| format!("parse App Configuration endpoint {endpoint:?}"))?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("api-version", "1.0");
        if let Some(label) = label.filter(|label| !label.trim().is_empty()) {
            query.append_pair("label", label);
        }
    }
    Ok(url)
}

async fn fetch_managed_identity_token(
    client: &reqwest::Client,
    client_id: Option<&str>,
) -> Result<String> {
    let mut url = Url::parse(MANAGED_IDENTITY_TOKEN_URL).expect("valid IMDS token URL");
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("api-version", "2018-02-01");
        query.append_pair("resource", APP_CONFIG_SCOPE_RESOURCE);
        if let Some(client_id) = client_id.filter(|value| !value.trim().is_empty()) {
            query.append_pair("client_id", client_id);
        }
    }
    let token = client
        .get(url)
        .header("Metadata", "true")
        .send()
        .await
        .context("fetch managed identity token for Azure App Configuration")?
        .error_for_status()
        .context("managed identity token request failed")?
        .json::<ManagedIdentityToken>()
        .await
        .context("decode managed identity token response")?;
    if token.access_token.trim().is_empty() {
        return Err(anyhow!("managed identity returned an empty access token"));
    }
    Ok(token.access_token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_registry() -> WorkerRegistry {
        parse_registry(
            r#"{
              "schema": "macaron.worker_registry.v1",
              "model": "zai-org/GLM-5.2-FP8",
              "pools": {
                "glm52-main": ["b200-01", "b200-02"],
                "internal-low": ["b200-01"]
              },
              "workers": {
                "b200-01": {
                  "url": "http://10.0.0.1:30000",
                  "enabled": true,
                  "pool_url_suffixes": {"internal-low": "@tier=shared"}
                },
                "b200-02": {"url": "http://10.0.0.2:30000", "enabled": true}
              }
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn builds_worker_urls_for_pool_in_order() {
        let registry = sample_registry();
        let urls = worker_urls_for_pool(&registry, "glm52-main", None).unwrap();
        assert_eq!(urls, vec!["http://10.0.0.1:30000", "http://10.0.0.2:30000"]);
    }

    #[test]
    fn applies_pool_specific_suffix_before_default_suffix() {
        let registry = sample_registry();
        let urls = worker_urls_for_pool(&registry, "internal-low", Some("@tier=bulk")).unwrap();
        assert_eq!(urls, vec!["http://10.0.0.1:30000@tier=shared"]);
    }

    #[test]
    fn rejects_unknown_schema() {
        let err = parse_registry(r#"{"schema":"other","pools":{},"workers":{}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported worker registry schema"), "{err}");
    }

    #[test]
    fn app_config_url_encodes_slash_key_and_label() {
        let url = app_config_key_url(
            "https://macaron-llm-deploy-prod.azconfig.io/",
            "macaron/prod/worker-registry/glm52/current",
            Some("prod-candidate"),
        )
        .unwrap();
        let rendered = url.as_str();
        assert!(rendered.contains("/kv/macaron%2Fprod%2Fworker-registry%2Fglm52%2Fcurrent"));
        assert!(rendered.contains("api-version=1.0"));
        assert!(rendered.contains("label=prod-candidate"));
    }
}

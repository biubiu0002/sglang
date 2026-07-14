//! SLS (阿里云日志服务) direct-push log layer for tracing.
//!
//! Implements a `tracing_subscriber::Layer` that batches log events and
//! pushes them to阿里云 SLS via the PutLogs REST API (HMAC-SHA1 signed).
//! No Logtail agent required — works on Azure / Railway / any platform.
//!
//! Enabled when env vars `SLS_ENDPOINT` + `SLS_ACCESS_KEY_ID` +
//! `SLS_ACCESS_KEY_SECRET` are all set. Falls back to no-op otherwise.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use md5::compute as md5_compute;
use reqwest::blocking::Client as BlockingClient;
use sha1::Sha1;
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{layer::Context, Layer};

type HmacSha1 = Hmac<Sha1>;

/// Configuration for the SLS direct-push layer.
#[derive(Clone, Debug)]
pub struct SlsLayerConfig {
    pub endpoint: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub project: String,
    pub logstore: String,
    pub service_name: String,
}

impl SlsLayerConfig {
    /// Build config from environment variables. Returns None if any required
    /// credential is missing (handler stays inert — logs go to stdout only).
    pub fn from_env(service_name: &str) -> Option<Self> {
        let endpoint = std::env::var("SLS_ENDPOINT").unwrap_or_default().trim().to_string();
        let ak = std::env::var("SLS_ACCESS_KEY_ID").unwrap_or_default().trim().to_string();
        let sk = std::env::var("SLS_ACCESS_KEY_SECRET").unwrap_or_default().trim().to_string();
        if endpoint.is_empty() || ak.is_empty() || sk.is_empty() {
            return None;
        }
        let project = std::env::var("SLS_PROJECT")
            .unwrap_or_else(|_| "macaron-log".to_string())
            .trim()
            .to_string();
        let logstore = std::env::var("SLS_LOGSTORE")
            .unwrap_or_else(|_| "sglang-router".to_string())
            .trim()
            .to_string();
        Some(Self {
            endpoint,
            access_key_id: ak,
            access_key_secret: sk,
            project,
            logstore,
            service_name: service_name.to_string(),
        })
    }
}

/// A single log entry to be batched and pushed to SLS.
struct SlsLogEntry {
    timestamp: u32,
    contents: Vec<(String, String)>,
}

/// Batching buffer that accumulates log entries and flushes to SLS.
struct SlsBatcher {
    config: SlsLayerConfig,
    batch: Vec<SlsLogEntry>,
    batch_size: usize,
    client: BlockingClient,
}

impl SlsBatcher {
    fn new(config: SlsLayerConfig) -> Self {
        let client = BlockingClient::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest blocking client");
        Self {
            config,
            batch: Vec::new(),
            batch_size: 200,
            client,
        }
    }

    fn push(&mut self, entry: SlsLogEntry) {
        self.batch.push(entry);
        if self.batch.len() >= self.batch_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let entries = std::mem::take(&mut self.batch);
        if let Err(e) = self.send_batch(entries) {
            eprintln!("[sls-log-layer] flush error: {}", e);
        }
    }

    /// Send a batch of log entries to SLS via PutLogs REST API.
    fn send_batch(&self, entries: Vec<SlsLogEntry>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = encode_log_group_pb(&entries);
        let content_md5 = format!("{:X}", md5_compute(&body));
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let resource = format!("/logstores/{}/shards/lb", self.config.logstore);

        let content_type = "application/x-protobuf";
        let body_raw_size = body.len().to_string();

        // Build headers for signing
        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert("Content-Type".to_string(), content_type.to_string());
        headers.insert("Content-MD5".to_string(), content_md5.clone());
        headers.insert("x-log-bodyrawsize".to_string(), body_raw_size.clone());
        headers.insert("x-log-apiversion".to_string(), "0.6.0".to_string());
        headers.insert("x-log-signaturemethod".to_string(), "hmac-sha1".to_string());
        headers.insert("Date".to_string(), date.clone());

        // Canonicalized log headers: sorted x-log-* and x-acs-* headers
        let mut sign_headers: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| {
                let lk = k.to_lowercase();
                lk.starts_with("x-acs-") || (lk.starts_with("x-log-") && !lk.starts_with("x-log-meta-"))
            })
            .map(|(k, v)| (k.to_lowercase(), v.clone()))
            .collect();
        sign_headers.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_log_headers: String = sign_headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        // Build string to sign
        let sign_content = format!(
            "POST\n{}\n{}\n{}\n{}{}",
            content_md5, content_type, date, canonical_log_headers, resource
        );

        // HMAC-SHA1 signature
        let mut mac = HmacSha1::new_from_slice(self.config.access_key_secret.as_bytes())?;
        mac.update(sign_content.as_bytes());
        let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        let url = format!("https://{}.{}{}", self.config.project, self.config.endpoint, resource);

        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", content_type)
            .header("Content-MD5", &content_md5)
            .header("x-log-bodyrawsize", &body_raw_size)
            .header("x-log-apiversion", "0.6.0")
            .header("x-log-signaturemethod", "hmac-sha1")
            .header("Date", &date)
            .header("x-log-date", &date)
            .header(
                "Authorization",
                format!("LOG {}:{}", self.config.access_key_id, signature),
            )
            .body(body);

        // Add x-log-* / x-acs-* headers that are in the signature
        for (k, v) in &sign_headers {
            request = request.header(k, v);
        }

        let resp = request.send()?;
        if !resp.status().is_success() {
            return Err(format!("SLS PutLogs returned {}", resp.status()).into());
        }
        Ok(())
    }
}

/// Visitor that collects tracing field values into a HashMap.
struct FieldVisitor {
    fields: HashMap<String, String>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{:?}", value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

/// Tracing layer that pushes log events to阿里云 SLS via direct HTTP API.
pub struct SlsLogLayer {
    batcher: Arc<Mutex<SlsBatcher>>,
}

impl SlsLogLayer {
    /// Create a new SLS log layer from config. If config is None, returns None
    /// (caller should skip adding this layer).
    pub fn new(config: SlsLayerConfig) -> Self {
        let batcher = SlsBatcher::new(config);
        Self {
            batcher: Arc::new(Mutex::new(batcher)),
        }
    }

    /// Flush any pending log entries. Should be called on shutdown.
    pub fn flush(&self) {
        if let Ok(mut b) = self.batcher.lock() {
            b.flush();
        }
    }
}

impl<S> Layer<S> for SlsLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Collect metadata and fields
        let metadata = event.metadata();

        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        // Build log contents
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);

        let mut contents: Vec<(String, String)> = Vec::with_capacity(visitor.fields.len() + 5);
        contents.push(("level".to_string(), metadata.level().to_string().to_lowercase()));
        contents.push(("target".to_string(), metadata.target().to_string()));
        contents.push(("message".to_string(), format!("{}", metadata.name())));
        contents.push(("service_name".to_string(), self.service_name()));
        contents.push((
            "timestamp".to_string(),
            Utc::now().to_rfc3339(),
        ));

        // Add tracing fields (trace_id, request_id, etc.)
        for (k, v) in &visitor.fields {
            if k != "message" {
                contents.push((k.clone(), v.clone()));
            }
        }

        let entry = SlsLogEntry { timestamp, contents };

        if let Ok(mut b) = self.batcher.lock() {
            b.push(entry);
        }
    }
}

impl SlsLogLayer {
    fn service_name(&self) -> String {
        // Access the batcher's config to get service_name
        self.batcher
            .lock()
            .map(|b| b.config.service_name.clone())
            .unwrap_or_else(|_| "sglang-router".to_string())
    }
}

// --- Protobuf encoding (manual wire format, no prost dependency) ---
// LogGroup { Logs: [Log], Topic: string, Source: string }
// Log { Time: uint32, Contents: [Content] }
// Content { Key: string, Value: string }

fn encode_varint(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut v = value;
    while v >= 0x80 {
        result.push((v as u8) | 0x80);
        v >>= 7;
    }
    result.push(v as u8);
    result
}

fn encode_tag(field_number: u32, wire_type: u32) -> Vec<u8> {
    encode_varint(((field_number as u64) << 3) | (wire_type as u64))
}

fn encode_string(field_number: u32, value: &str) -> Vec<u8> {
    let mut result = encode_tag(field_number, 2); // length-delimited
    result.extend(encode_varint(value.len() as u64));
    result.extend_from_slice(value.as_bytes());
    result
}

fn encode_uint32(field_number: u32, value: u32) -> Vec<u8> {
    let mut result = encode_tag(field_number, 0); // varint
    result.extend(encode_varint(value as u64));
    result
}

fn encode_content(key: &str, value: &str) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend(encode_string(1, key)); // Key: field=1
    result.extend(encode_string(2, value)); // Value: field=2
    result
}

fn encode_log(entry: &SlsLogEntry) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend(encode_uint32(1, entry.timestamp)); // Time: field=1
    for (key, value) in &entry.contents {
        let content_bytes = encode_content(key, value);
        result.extend(encode_tag(2, 2)); // Contents: field=2, length-delimited
        result.extend(encode_varint(content_bytes.len() as u64));
        result.extend(&content_bytes);
    }
    result
}

fn encode_log_group_pb(entries: &[SlsLogEntry]) -> Vec<u8> {
    let mut result = Vec::new();
    for entry in entries {
        let log_bytes = encode_log(entry);
        result.extend(encode_tag(1, 2)); // Logs: field=1, length-delimited
        result.extend(encode_varint(log_bytes.len() as u64));
        result.extend(&log_bytes);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_varint() {
        assert_eq!(encode_varint(0), vec![0x00]);
        assert_eq!(encode_varint(1), vec![0x01]);
        assert_eq!(encode_varint(127), vec![0x7f]);
        assert_eq!(encode_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_varint(300), vec![0xac, 0x02]);
    }

    #[test]
    fn test_encode_string() {
        let encoded = encode_string(1, "hello");
        // tag(1, 2) = 0x0a, length=5, "hello"
        assert_eq!(encoded, vec![0x0a, 0x05, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_encode_content() {
        let encoded = encode_content("key", "val");
        // Key: tag=0x0a, len=3, "key"
        // Value: tag=0x12, len=3, "val"
        assert_eq!(
            encoded,
            vec![0x0a, 0x03, b'k', b'e', b'y', 0x12, 0x03, b'v', b'a', b'l']
        );
    }

    #[test]
    fn test_encode_log_group_not_empty() {
        let entry = SlsLogEntry {
            timestamp: 1234567890,
            contents: vec![("level".to_string(), "info".to_string())],
        };
        let encoded = encode_log_group_pb(&[entry]);
        assert!(!encoded.is_empty());
        // Should start with Logs field tag (1, length-delimited = 0x0a)
        assert_eq!(encoded[0], 0x0a);
    }

    #[test]
    fn test_config_from_env_missing() {
        std::env::remove_var("SLS_ENDPOINT");
        std::env::remove_var("SLS_ACCESS_KEY_ID");
        std::env::remove_var("SLS_ACCESS_KEY_SECRET");
        assert!(SlsLayerConfig::from_env("sglang-router").is_none());
    }

    #[test]
    fn test_config_from_env_present() {
        std::env::set_var("SLS_ENDPOINT", "ap-southeast-1.log.aliyuncs.com");
        std::env::set_var("SLS_ACCESS_KEY_ID", "fake_ak");
        std::env::set_var("SLS_ACCESS_KEY_SECRET", "fake_sk");
        std::env::set_var("SLS_PROJECT", "macaron-log");
        std::env::set_var("SLS_LOGSTORE", "sglang-router");
        let config = SlsLayerConfig::from_env("sglang-router").expect("config should be created");
        assert_eq!(config.endpoint, "ap-southeast-1.log.aliyuncs.com");
        assert_eq!(config.access_key_id, "fake_ak");
        assert_eq!(config.project, "macaron-log");
        assert_eq!(config.logstore, "sglang-router");
        assert_eq!(config.service_name, "sglang-router");
    }
}

//! SLS (Alibaba Cloud Log Service) direct-push log layer for tracing.
//!
//! The tracing callback only enqueues structured events into a bounded channel.
//! A dedicated worker thread batches and pushes them to SLS, keeping network I/O
//! off request-serving threads. No Logtail agent is required.

use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use reqwest::{
    blocking::Client as BlockingClient,
    header::{HeaderValue, AUTHORIZATION},
};
use sha1::Sha1;
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{layer::Context, Layer};

const SLS_BATCH_SIZE: usize = 200;
const SLS_QUEUE_CAPACITY: usize = 10_000;
const SLS_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const SLS_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type HmacSha1 = Hmac<Sha1>;

/// Configuration for the SLS direct-push layer.
#[derive(Clone)]
pub struct SlsLayerConfig {
    pub endpoint: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub project: String,
    pub logstore: String,
    pub service_name: String,
}

impl std::fmt::Debug for SlsLayerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlsLayerConfig")
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"<redacted>")
            .field("access_key_secret", &"<redacted>")
            .field("project", &self.project)
            .field("logstore", &self.logstore)
            .field("service_name", &self.service_name)
            .finish()
    }
}

impl SlsLayerConfig {
    /// Build config from environment variables. Missing credentials disable SLS.
    pub fn from_env(service_name: &str) -> Option<Self> {
        let endpoint = std::env::var("SLS_ENDPOINT")
            .unwrap_or_default()
            .trim()
            .to_string();
        let access_key_id = std::env::var("SLS_ACCESS_KEY_ID")
            .unwrap_or_default()
            .trim()
            .to_string();
        let access_key_secret = std::env::var("SLS_ACCESS_KEY_SECRET")
            .unwrap_or_default()
            .trim()
            .to_string();
        if endpoint.is_empty() || access_key_id.is_empty() || access_key_secret.is_empty() {
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
            access_key_id,
            access_key_secret,
            project,
            logstore,
            service_name: service_name.to_string(),
        })
    }
}

#[derive(Debug)]
struct SlsLogEntry {
    timestamp: u32,
    contents: Vec<(String, String)>,
}

enum SlsCommand {
    Entry(SlsLogEntry),
    Shutdown,
}

trait SlsBatchSink: Send + 'static {
    fn send_batch(&mut self, entries: Vec<SlsLogEntry>) -> Result<(), BoxError>;
}

struct SlsHttpSink {
    config: SlsLayerConfig,
    client: BlockingClient,
}

impl SlsHttpSink {
    fn new(config: SlsLayerConfig) -> Result<Self, reqwest::Error> {
        let client = BlockingClient::builder()
            .timeout(SLS_HTTP_TIMEOUT)
            .build()?;
        Ok(Self { config, client })
    }

    fn build_request(
        &self,
        entries: Vec<SlsLogEntry>,
    ) -> Result<reqwest::blocking::Request, BoxError> {
        let body = encode_log_group_pb(&entries);
        let content_md5 = format!("{:X}", Md5::digest(&body));
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let resource = format!("/logstores/{}/shards/lb", self.config.logstore);

        let content_type = "application/x-protobuf";
        let body_raw_size = body.len().to_string();
        let mut sign_headers = vec![
            ("x-log-apiversion", "0.6.0".to_string()),
            ("x-log-bodyrawsize", body_raw_size),
            ("x-log-signaturemethod", "hmac-sha1".to_string()),
        ];
        sign_headers.sort_by(|a, b| a.0.cmp(b.0));
        let canonical_log_headers: String = sign_headers
            .iter()
            .map(|(key, value)| format!("{}:{}\n", key, value))
            .collect();

        let sign_content = format!(
            "POST\n{}\n{}\n{}\n{}{}",
            content_md5, content_type, date, canonical_log_headers, resource
        );
        let mut mac = HmacSha1::new_from_slice(self.config.access_key_secret.as_bytes())?;
        mac.update(sign_content.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let mut authorization =
            HeaderValue::from_str(&format!("LOG {}:{}", self.config.access_key_id, signature))?;
        authorization.set_sensitive(true);

        let url = format!(
            "https://{}.{}{}",
            self.config.project, self.config.endpoint, resource
        );
        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", content_type)
            .header("Content-MD5", &content_md5)
            .header("Date", &date)
            // AuthV1 adds x-log-date after signing so proxies can strip Date.
            .header("x-log-date", &date)
            .header(AUTHORIZATION, authorization)
            .body(body);

        for (key, value) in sign_headers {
            request = request.header(key, value);
        }

        Ok(request.build()?)
    }
}

impl SlsBatchSink for SlsHttpSink {
    fn send_batch(&mut self, entries: Vec<SlsLogEntry>) -> Result<(), BoxError> {
        let request = self.build_request(entries)?;
        let response = self.client.execute(request)?;
        if !response.status().is_success() {
            return Err(format!("SLS PutLogs returned {}", response.status()).into());
        }
        Ok(())
    }
}

/// Stops the SLS worker and flushes queued logs when logging shuts down.
pub struct SlsWorkerGuard {
    shutdown_tx: Option<SyncSender<SlsCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for SlsWorkerGuard {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(SlsCommand::Shutdown);
        }
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                eprintln!("[sls-log-layer] worker thread panicked");
            }
        }
    }
}

fn spawn_sls_worker<S: SlsBatchSink>(
    sink: S,
    queue_capacity: usize,
    batch_size: usize,
    flush_interval: Duration,
) -> Result<(SyncSender<SlsCommand>, SlsWorkerGuard), std::io::Error> {
    let (tx, rx) = mpsc::sync_channel(queue_capacity);
    let shutdown_tx = tx.clone();
    let worker = thread::Builder::new()
        .name("sls-log-pusher".to_string())
        .spawn(move || run_sls_worker(rx, sink, batch_size, flush_interval))?;

    Ok((
        tx,
        SlsWorkerGuard {
            shutdown_tx: Some(shutdown_tx),
            worker: Some(worker),
        },
    ))
}

fn run_sls_worker<S: SlsBatchSink>(
    rx: Receiver<SlsCommand>,
    mut sink: S,
    batch_size: usize,
    flush_interval: Duration,
) {
    let mut batch = Vec::with_capacity(batch_size);
    let mut flush_deadline = Instant::now() + flush_interval;

    loop {
        let timeout = flush_deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(timeout) {
            Ok(SlsCommand::Entry(entry)) => {
                batch.push(entry);
                if batch.len() >= batch_size {
                    flush_batch(&mut sink, &mut batch, batch_size);
                    flush_deadline = Instant::now() + flush_interval;
                }
            }
            Ok(SlsCommand::Shutdown) => {
                drain_and_flush(&rx, &mut sink, &mut batch, batch_size);
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                flush_batch(&mut sink, &mut batch, batch_size);
                flush_deadline = Instant::now() + flush_interval;
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(&mut sink, &mut batch, batch_size);
                return;
            }
        }
    }
}

fn drain_and_flush<S: SlsBatchSink>(
    rx: &Receiver<SlsCommand>,
    sink: &mut S,
    batch: &mut Vec<SlsLogEntry>,
    batch_size: usize,
) {
    loop {
        match rx.try_recv() {
            Ok(SlsCommand::Entry(entry)) => {
                batch.push(entry);
                if batch.len() >= batch_size {
                    flush_batch(sink, batch, batch_size);
                }
            }
            Ok(SlsCommand::Shutdown) => {}
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
    flush_batch(sink, batch, batch_size);
}

fn flush_batch<S: SlsBatchSink>(sink: &mut S, batch: &mut Vec<SlsLogEntry>, batch_size: usize) {
    if batch.is_empty() {
        return;
    }

    let entry_count = batch.len();
    let entries = std::mem::replace(batch, Vec::with_capacity(batch_size));
    match sink.send_batch(entries) {
        Ok(()) => {
            metrics::counter!("smg_sls_log_batches_total", "result" => "success").increment(1);
            metrics::counter!("smg_sls_log_entries_sent_total").increment(entry_count as u64);
        }
        Err(error) => {
            metrics::counter!("smg_sls_log_batches_total", "result" => "error").increment(1);
            metrics::counter!("smg_sls_log_entries_dropped_total", "reason" => "send_error")
                .increment(entry_count as u64);
            eprintln!("[sls-log-layer] flush error: {}", error);
        }
    }
}

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
        self.fields
            .insert(field.name().to_string(), value.to_string());
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

/// Tracing layer that enqueues events for the SLS worker thread.
pub struct SlsLogLayer {
    tx: SyncSender<SlsCommand>,
    service_name: String,
}

impl SlsLogLayer {
    pub fn new(config: SlsLayerConfig) -> Result<(Self, SlsWorkerGuard), BoxError> {
        let service_name = config.service_name.clone();
        let sink = SlsHttpSink::new(config)?;
        let (tx, guard) =
            spawn_sls_worker(sink, SLS_QUEUE_CAPACITY, SLS_BATCH_SIZE, SLS_FLUSH_INTERVAL)?;
        Ok((Self { tx, service_name }, guard))
    }

    fn enqueue(&self, entry: SlsLogEntry) {
        match self.tx.try_send(SlsCommand::Entry(entry)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                metrics::counter!("smg_sls_log_entries_dropped_total", "reason" => "queue_full")
                    .increment(1);
            }
            Err(TrySendError::Disconnected(_)) => {
                metrics::counter!("smg_sls_log_entries_dropped_total", "reason" => "worker_stopped")
                    .increment(1);
            }
        }
    }
}

impl<S> Layer<S> for SlsLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as u32)
            .unwrap_or(0);
        let message = visitor
            .fields
            .remove("message")
            .unwrap_or_else(|| metadata.name().to_string());

        let mut contents = Vec::with_capacity(visitor.fields.len() + 5);
        contents.push((
            "level".to_string(),
            metadata.level().to_string().to_lowercase(),
        ));
        contents.push(("target".to_string(), metadata.target().to_string()));
        contents.push(("message".to_string(), message));
        contents.push(("service_name".to_string(), self.service_name.clone()));
        contents.push(("timestamp".to_string(), Utc::now().to_rfc3339()));
        contents.extend(visitor.fields);

        self.enqueue(SlsLogEntry {
            timestamp,
            contents,
        });
    }
}

// Protobuf wire encoding for LogGroup { Logs: [Log] }, Log, and Content.

fn encode_varint(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut value = value;
    while value >= 0x80 {
        result.push((value as u8) | 0x80);
        value >>= 7;
    }
    result.push(value as u8);
    result
}

fn encode_tag(field_number: u32, wire_type: u32) -> Vec<u8> {
    encode_varint(((field_number as u64) << 3) | wire_type as u64)
}

fn encode_string(field_number: u32, value: &str) -> Vec<u8> {
    let mut result = encode_tag(field_number, 2);
    result.extend(encode_varint(value.len() as u64));
    result.extend_from_slice(value.as_bytes());
    result
}

fn encode_uint32(field_number: u32, value: u32) -> Vec<u8> {
    let mut result = encode_tag(field_number, 0);
    result.extend(encode_varint(value as u64));
    result
}

fn encode_content(key: &str, value: &str) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend(encode_string(1, key));
    result.extend(encode_string(2, value));
    result
}

fn encode_log(entry: &SlsLogEntry) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend(encode_uint32(1, entry.timestamp));
    for (key, value) in &entry.contents {
        let content_bytes = encode_content(key, value);
        result.extend(encode_tag(2, 2));
        result.extend(encode_varint(content_bytes.len() as u64));
        result.extend(content_bytes);
    }
    result
}

fn encode_log_group_pb(entries: &[SlsLogEntry]) -> Vec<u8> {
    let mut result = Vec::new();
    for entry in entries {
        let log_bytes = encode_log(entry);
        result.extend(encode_tag(1, 2));
        result.extend(encode_varint(log_bytes.len() as u64));
        result.extend(log_bytes);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tracing_subscriber::layer::SubscriberExt;

    struct RecordingSink {
        batches: mpsc::Sender<Vec<String>>,
    }

    impl SlsBatchSink for RecordingSink {
        fn send_batch(&mut self, entries: Vec<SlsLogEntry>) -> Result<(), BoxError> {
            let messages = entries
                .into_iter()
                .map(|entry| {
                    entry
                        .contents
                        .into_iter()
                        .find_map(|(key, value)| (key == "message").then_some(value))
                        .unwrap_or_default()
                })
                .collect();
            self.batches.send(messages)?;
            Ok(())
        }
    }

    fn test_entry(message: &str) -> SlsLogEntry {
        SlsLogEntry {
            timestamp: 1,
            contents: vec![("message".to_string(), message.to_string())],
        }
    }

    #[test]
    fn test_worker_flushes_at_batch_size() {
        let (batch_tx, batch_rx) = mpsc::channel();
        let (tx, guard) = spawn_sls_worker(
            RecordingSink { batches: batch_tx },
            8,
            2,
            Duration::from_secs(30),
        )
        .unwrap();

        tx.send(SlsCommand::Entry(test_entry("one"))).unwrap();
        tx.send(SlsCommand::Entry(test_entry("two"))).unwrap();

        assert_eq!(
            batch_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            vec!["one", "two"]
        );
        drop(guard);
    }

    #[test]
    fn test_worker_flushes_on_interval() {
        let (batch_tx, batch_rx) = mpsc::channel();
        let (tx, guard) = spawn_sls_worker(
            RecordingSink { batches: batch_tx },
            8,
            10,
            Duration::from_millis(20),
        )
        .unwrap();

        tx.send(SlsCommand::Entry(test_entry("one"))).unwrap();

        assert_eq!(
            batch_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            vec!["one"]
        );
        drop(guard);
    }

    #[test]
    fn test_worker_drains_on_shutdown() {
        let (batch_tx, batch_rx) = mpsc::channel();
        let (tx, guard) = spawn_sls_worker(
            RecordingSink { batches: batch_tx },
            8,
            10,
            Duration::from_secs(30),
        )
        .unwrap();

        tx.send(SlsCommand::Entry(test_entry("one"))).unwrap();
        tx.send(SlsCommand::Entry(test_entry("two"))).unwrap();
        drop(guard);

        assert_eq!(
            batch_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            vec!["one", "two"]
        );
    }

    #[test]
    fn test_layer_drops_when_queue_is_full() {
        let (tx, rx) = mpsc::sync_channel(1);
        let layer = SlsLogLayer {
            tx,
            service_name: "sglang-router".to_string(),
        };

        layer.enqueue(test_entry("queued"));
        layer.enqueue(test_entry("dropped"));

        let SlsCommand::Entry(entry) = rx.try_recv().unwrap() else {
            panic!("expected a log entry");
        };
        assert_eq!(entry.contents[0].1, "queued");
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_layer_preserves_event_message() {
        let (tx, rx) = mpsc::sync_channel(1);
        let layer = SlsLogLayer {
            tx,
            service_name: "sglang-router".to_string(),
        };
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(trace_id = "trace-1", "hello {}", "world");
        });

        let SlsCommand::Entry(entry) = rx.recv_timeout(Duration::from_secs(1)).unwrap() else {
            panic!("expected a log entry");
        };
        let fields: HashMap<_, _> = entry.contents.into_iter().collect();
        assert_eq!(fields.get("message").unwrap(), "hello world");
        assert_eq!(fields.get("trace_id").unwrap(), "trace-1");
    }

    #[test]
    fn test_request_has_one_copy_of_each_signed_header() {
        let sink = SlsHttpSink::new(SlsLayerConfig {
            endpoint: "example.log.aliyuncs.com".to_string(),
            access_key_id: "fake_access_key".to_string(),
            access_key_secret: "fake_secret".to_string(),
            project: "test-project".to_string(),
            logstore: "test-logstore".to_string(),
            service_name: "sglang-router".to_string(),
        })
        .unwrap();

        let request = sink.build_request(vec![test_entry("hello")]).unwrap();
        for header_name in [
            "x-log-apiversion",
            "x-log-bodyrawsize",
            "x-log-signaturemethod",
        ] {
            assert_eq!(
                request.headers().get_all(header_name).iter().count(),
                1,
                "duplicate {header_name} header"
            );
        }
        assert!(request.headers()[AUTHORIZATION].is_sensitive());
    }

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
        assert_eq!(
            encode_string(1, "hello"),
            vec![0x0a, 0x05, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn test_encode_content() {
        assert_eq!(
            encode_content("key", "val"),
            vec![0x0a, 0x03, b'k', b'e', b'y', 0x12, 0x03, b'v', b'a', b'l']
        );
    }

    #[test]
    fn test_encode_log_group_not_empty() {
        let encoded = encode_log_group_pb(&[test_entry("hello")]);
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0x0a);
    }

    #[test]
    #[serial]
    fn test_config_from_env_missing() {
        std::env::remove_var("SLS_ENDPOINT");
        std::env::remove_var("SLS_ACCESS_KEY_ID");
        std::env::remove_var("SLS_ACCESS_KEY_SECRET");
        assert!(SlsLayerConfig::from_env("sglang-router").is_none());
    }

    #[test]
    #[serial]
    fn test_config_from_env_present_and_debug_redacted() {
        std::env::set_var("SLS_ENDPOINT", "example.log.aliyuncs.com");
        std::env::set_var("SLS_ACCESS_KEY_ID", "fake_access_key");
        std::env::set_var("SLS_ACCESS_KEY_SECRET", "fake_secret");
        std::env::set_var("SLS_PROJECT", "test-project");
        std::env::set_var("SLS_LOGSTORE", "sglang-router");

        let config = SlsLayerConfig::from_env("sglang-router").unwrap();
        assert_eq!(config.endpoint, "example.log.aliyuncs.com");
        assert_eq!(config.project, "test-project");
        assert_eq!(config.logstore, "sglang-router");
        let debug = format!("{:?}", config);
        assert!(!debug.contains("fake_access_key"));
        assert!(!debug.contains("fake_secret"));

        std::env::remove_var("SLS_ENDPOINT");
        std::env::remove_var("SLS_ACCESS_KEY_ID");
        std::env::remove_var("SLS_ACCESS_KEY_SECRET");
        std::env::remove_var("SLS_PROJECT");
        std::env::remove_var("SLS_LOGSTORE");
    }
}

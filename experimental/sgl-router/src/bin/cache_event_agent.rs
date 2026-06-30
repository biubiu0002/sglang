use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reqwest::Client;
use sgl_router::cache_event_stream::{
    encode_base64, KafkaKvEventProducer, KafkaKvEventStreamConfig, KvEventStreamRecord,
    LocalKvEventStream, LocalKvEventStreamConfig,
};
use sgl_router::cache_state::CacheStateKvEventsRequest;
use sgl_router::policies::kv_events::tree::KvWorkerId;
use sgl_router::policies::kv_events::wire::decode_event_batch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use zeromq::{Socket, SocketRecv, SubSocket};

const END_SEQ_SENTINEL: i64 = -1;

#[derive(Debug, Parser)]
#[command(about = "Forward local SGLang KV events to a remote cache-state service")]
struct Args {
    #[arg(
        long,
        env = "CACHE_EVENT_AGENT_CACHE_STATE_URL",
        required_unless_present_any = ["stream_path", "kafka_bootstrap_servers"]
    )]
    cache_state_url: Option<String>,

    #[arg(long, env = "CACHE_EVENT_AGENT_STREAM_PATH")]
    stream_path: Option<PathBuf>,

    #[arg(long, env = "CACHE_EVENT_AGENT_STREAM_RETENTION_SECS")]
    stream_retention_secs: Option<u64>,

    #[arg(long, env = "CACHE_EVENT_AGENT_STREAM_MAX_BYTES")]
    stream_max_bytes: Option<u64>,

    #[arg(long, env = "CACHE_EVENT_AGENT_KAFKA_BOOTSTRAP_SERVERS")]
    kafka_bootstrap_servers: Option<String>,

    #[arg(long, env = "CACHE_EVENT_AGENT_KAFKA_TOPIC")]
    kafka_topic: Option<String>,

    #[arg(long, env = "CACHE_EVENT_AGENT_KAFKA_USERNAME")]
    kafka_username: Option<String>,

    #[arg(long, env = "CACHE_EVENT_AGENT_KAFKA_PASSWORD")]
    kafka_password: Option<String>,

    #[arg(long, env = "CACHE_EVENT_AGENT_KAFKA_CLIENT_ID")]
    kafka_client_id: Option<String>,

    #[arg(long, env = "CACHE_EVENT_AGENT_WORKER_URL")]
    worker_url: String,

    #[arg(long, env = "CACHE_EVENT_AGENT_MODEL_ID")]
    model_id: String,

    #[arg(
        long,
        env = "CACHE_EVENT_AGENT_ENDPOINT_HOST",
        default_value = "127.0.0.1"
    )]
    endpoint_host: String,

    #[arg(long, env = "CACHE_EVENT_AGENT_PORT_BASE", default_value_t = 5557)]
    port_base: u16,

    #[arg(long, env = "CACHE_EVENT_AGENT_DP_SIZE", default_value_t = 1)]
    dp_size: u32,

    #[arg(long, env = "CACHE_EVENT_AGENT_TOPIC", default_value = "")]
    topic: String,

    #[arg(
        long,
        env = "CACHE_EVENT_AGENT_POST_TIMEOUT_MS",
        default_value_t = 2000
    )]
    post_timeout_ms: u64,

    #[arg(long, env = "CACHE_EVENT_AGENT_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .with_target(true)
        .json()
        .init();

    if args.dp_size == 0 {
        return Err(anyhow!("--dp-size must be greater than 0"));
    }
    let sinks = build_event_sinks(&args)?;
    let client = Client::builder()
        .timeout(Duration::from_millis(args.post_timeout_ms))
        .build()
        .context("build HTTP client")?;
    let cache_state_api_token = std::env::var("CACHE_STATE_API_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());
    let cancel = CancellationToken::new();
    install_signal_handlers(cancel.clone())?;

    info!(
        worker_url = %args.worker_url,
        model_id = %args.model_id,
        endpoint_host = %args.endpoint_host,
        port_base = args.port_base,
        dp_size = args.dp_size,
        sinks = %sinks.iter().map(EventSink::name).collect::<Vec<_>>().join(","),
        "cache-event-agent starting",
    );

    let mut handles = Vec::new();
    for dp_rank in 0..args.dp_size {
        let Some(port) = (u32::from(args.port_base) + dp_rank)
            .try_into()
            .ok()
            .map(|p: u16| p)
        else {
            warn!(
                dp_rank,
                port_base = args.port_base,
                "skipping dp rank whose ZMQ port overflows u16"
            );
            continue;
        };
        let task = AgentTask {
            client: client.clone(),
            sinks: sinks.clone(),
            worker_url: args.worker_url.clone(),
            model_id: args.model_id.clone(),
            endpoint: format!("tcp://{}:{}", args.endpoint_host, port),
            topic: args.topic.clone(),
            dp_rank,
            cache_state_api_token: cache_state_api_token.clone(),
            cancel: cancel.clone(),
        };
        handles.push(tokio::spawn(task.run()));
    }

    if handles.is_empty() {
        return Err(anyhow!("no subscriber tasks were started"));
    }
    for handle in handles {
        if let Err(err) = handle.await {
            error!(error = %err, "subscriber task panicked");
        }
    }
    info!("cache-event-agent stopped");
    Ok(())
}

#[derive(Clone)]
enum EventSink {
    Http {
        ingest_url: String,
        display_url: String,
    },
    Stream(LocalKvEventStream),
    Kafka(KafkaKvEventProducer),
}

impl EventSink {
    fn name(&self) -> &str {
        match self {
            Self::Http { display_url, .. } => display_url,
            Self::Stream(_) => "local-event-stream",
            Self::Kafka(producer) => producer.topic(),
        }
    }
}

fn build_event_sinks(args: &Args) -> Result<Vec<EventSink>> {
    let mut sinks = Vec::new();
    if let Some(base_url) = args
        .cache_state_url
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let base_url = base_url.trim_end_matches('/').to_string();
        sinks.push(EventSink::Http {
            ingest_url: format!("{base_url}/v1/cache_state/kv_events"),
            display_url: base_url,
        });
    }
    if let Some(stream_path) = args.stream_path.clone() {
        sinks.push(EventSink::Stream(LocalKvEventStream::new(
            LocalKvEventStreamConfig {
                path: stream_path,
                retention_secs: args.stream_retention_secs,
                max_bytes: args.stream_max_bytes,
            },
        )));
    }
    if let Some(bootstrap_servers) = args
        .kafka_bootstrap_servers
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let topic = args
            .kafka_topic
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("--kafka-topic is required when Kafka is enabled"))?
            .to_string();
        let producer = KafkaKvEventProducer::new(KafkaKvEventStreamConfig {
            bootstrap_servers: bootstrap_servers.to_string(),
            topic,
            username: args.kafka_username.clone(),
            password: args.kafka_password.clone(),
            client_id: args.kafka_client_id.clone(),
            consumer_group: None,
            auto_offset_reset: "latest".to_string(),
        })?;
        sinks.push(EventSink::Kafka(producer));
    }
    if sinks.is_empty() {
        return Err(anyhow!(
            "configure at least one sink: cache-state URL, stream path, or Kafka"
        ));
    }
    Ok(sinks)
}

struct AgentTask {
    client: Client,
    sinks: Vec<EventSink>,
    worker_url: String,
    model_id: String,
    endpoint: String,
    topic: String,
    dp_rank: u32,
    cache_state_api_token: Option<String>,
    cancel: CancellationToken,
}

impl AgentTask {
    async fn run(self) {
        let worker = KvWorkerId::new(self.worker_url.clone(), self.dp_rank);
        loop {
            if self.cancel.is_cancelled() {
                return;
            }
            match self.connect().await {
                Some(mut sub) => self.recv_loop(&worker, &mut sub).await,
                None => return,
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }

    async fn connect(&self) -> Option<SubSocket> {
        loop {
            let mut sub = SubSocket::new();
            info!(
                endpoint = %self.endpoint,
                dp_rank = self.dp_rank,
                "connecting to local KV event publisher"
            );
            let connect_result = tokio::select! {
                _ = self.cancel.cancelled() => return None,
                res = sub.connect(&self.endpoint) => res,
            };
            if let Err(err) = connect_result {
                warn!(
                    endpoint = %self.endpoint,
                    dp_rank = self.dp_rank,
                    error = %err,
                    "connect failed; retrying"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            let subscribe_result = tokio::select! {
                _ = self.cancel.cancelled() => return None,
                res = sub.subscribe(&self.topic) => res,
            };
            match subscribe_result {
                Ok(()) => return Some(sub),
                Err(err) => {
                    warn!(
                        endpoint = %self.endpoint,
                        dp_rank = self.dp_rank,
                        error = %err,
                        "subscribe failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn recv_loop(&self, worker: &KvWorkerId, sub: &mut SubSocket) {
        loop {
            let msg = tokio::select! {
                _ = self.cancel.cancelled() => return,
                res = sub.recv() => match res {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!(
                            dp_rank = self.dp_rank,
                            error = %err,
                            "recv failed; reconnecting"
                        );
                        return;
                    }
                },
            };
            let Some((seq, payload)) = decode_zmq_message(&msg, self.dp_rank) else {
                continue;
            };
            if seq == END_SEQ_SENTINEL {
                info!(
                    dp_rank = self.dp_rank,
                    "publisher shutdown sentinel received"
                );
                continue;
            }
            let batch = match decode_event_batch(payload) {
                Ok(batch) => batch,
                Err(err) => {
                    warn!(
                        dp_rank = self.dp_rank,
                        seq,
                        error = %err,
                        "failed to decode KV event batch"
                    );
                    continue;
                }
            };
            let n_events = batch.events.len();
            for sink in &self.sinks {
                match sink {
                    EventSink::Http { ingest_url, .. } => {
                        let req = CacheStateKvEventsRequest {
                            model_id: self.model_id.clone(),
                            worker_url: worker.url.clone(),
                            dp_rank: worker.dp_rank,
                            seq,
                            payload_b64: encode_base64(payload),
                        };
                        let mut post = self.client.post(ingest_url);
                        if let Some(token) = self.cache_state_api_token.as_ref() {
                            post = post.bearer_auth(token);
                        }
                        match post.json(&req).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                debug!(
                                    dp_rank = self.dp_rank,
                                    seq, n_events, "forwarded KV event batch"
                                );
                            }
                            Ok(resp) => {
                                warn!(
                                    dp_rank = self.dp_rank,
                                    seq,
                                    n_events,
                                    status = %resp.status(),
                                    "cache-state rejected KV event batch"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    dp_rank = self.dp_rank,
                                    seq,
                                    n_events,
                                    error = %err,
                                    "failed to forward KV event batch"
                                );
                            }
                        }
                    }
                    EventSink::Stream(stream) => {
                        let record = KvEventStreamRecord::from_payload(
                            self.model_id.clone(),
                            worker.url.clone(),
                            worker.dp_rank,
                            seq,
                            payload,
                        );
                        match stream.append(&record) {
                            Ok(stats) => {
                                debug!(
                                    dp_rank = self.dp_rank,
                                    seq,
                                    n_events,
                                    stream_records = stats.records_after_compaction,
                                    stream_bytes = stats.bytes_after_compaction,
                                    "appended KV event batch to stream"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    dp_rank = self.dp_rank,
                                    seq,
                                    n_events,
                                    error = %err,
                                    "failed to append KV event batch to stream"
                                );
                            }
                        }
                    }
                    EventSink::Kafka(producer) => {
                        let record = KvEventStreamRecord::from_payload(
                            self.model_id.clone(),
                            worker.url.clone(),
                            worker.dp_rank,
                            seq,
                            payload,
                        );
                        match producer.send(&record).await {
                            Ok(()) => {
                                debug!(
                                    dp_rank = self.dp_rank,
                                    seq,
                                    n_events,
                                    topic = producer.topic(),
                                    "published KV event batch to Kafka stream"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    dp_rank = self.dp_rank,
                                    seq,
                                    n_events,
                                    topic = producer.topic(),
                                    error = %err,
                                    "failed to publish KV event batch to Kafka stream"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn decode_zmq_message(msg: &zeromq::ZmqMessage, dp_rank: u32) -> Option<(i64, &[u8])> {
    if msg.len() != 3 {
        warn!(
            dp_rank,
            frames = msg.len(),
            "dropping ZMQ message with unexpected frame count"
        );
        return None;
    }
    let seq_frame = msg.get(1)?;
    let payload = msg.get(2)?;
    let seq_bytes: [u8; 8] = match seq_frame.as_ref().try_into() {
        Ok(bytes) => bytes,
        Err(_) => {
            warn!(
                dp_rank,
                seq_len = seq_frame.len(),
                "dropping message with invalid seq frame"
            );
            return None;
        }
    };
    Some((i64::from_be_bytes(seq_bytes), payload.as_ref()))
}

fn install_signal_handlers(cancel: CancellationToken) -> Result<()> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("install SIGINT handler")?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        cancel.cancel();
    });
    Ok(())
}

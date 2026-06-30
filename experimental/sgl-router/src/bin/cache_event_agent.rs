use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reqwest::Client;
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
    #[arg(long, env = "CACHE_EVENT_AGENT_CACHE_STATE_URL")]
    cache_state_url: String,

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
    let base_url = args.cache_state_url.trim_end_matches('/').to_string();
    let ingest_url = format!("{base_url}/v1/cache_state/kv_events");
    let client = Client::builder()
        .timeout(Duration::from_millis(args.post_timeout_ms))
        .build()
        .context("build HTTP client")?;
    let cancel = CancellationToken::new();
    install_signal_handlers(cancel.clone())?;

    info!(
        worker_url = %args.worker_url,
        model_id = %args.model_id,
        endpoint_host = %args.endpoint_host,
        port_base = args.port_base,
        dp_size = args.dp_size,
        cache_state_url = %base_url,
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
            ingest_url: ingest_url.clone(),
            worker_url: args.worker_url.clone(),
            model_id: args.model_id.clone(),
            endpoint: format!("tcp://{}:{}", args.endpoint_host, port),
            topic: args.topic.clone(),
            dp_rank,
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

struct AgentTask {
    client: Client,
    ingest_url: String,
    worker_url: String,
    model_id: String,
    endpoint: String,
    topic: String,
    dp_rank: u32,
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
            let req = CacheStateKvEventsRequest {
                model_id: self.model_id.clone(),
                worker_url: worker.url.clone(),
                dp_rank: worker.dp_rank,
                seq,
                payload_b64: encode_base64(payload),
            };
            match self.client.post(&self.ingest_url).json(&req).send().await {
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

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() >= 2 {
            out.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() == 3 {
            out.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
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

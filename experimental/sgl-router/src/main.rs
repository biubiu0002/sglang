// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use clap::Parser;
use sgl_router::config::{Cli, LogFormat, RuntimeMode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio_util::sync::CancellationToken;

/// Install the global tracing subscriber.
///
/// Idempotent: a second call returns `Ok` without panicking. When
/// `try_init` errors, some other code has already installed a subscriber,
/// so the `tracing::debug!` below is delivered through THAT subscriber —
/// no recursive init.
///
/// `format` selects the output shape: `Json` emits one JSON record per
/// line (target for production / k8s log aggregators), `Text` is the
/// human-readable default. The `RUST_LOG` environment variable always
/// wins over `default_level`.
fn init_tracing(default_level: &str, format: LogFormat) -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    let install_result = match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .json()
            .try_init(),
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .try_init(),
    };
    if let Err(e) = install_result {
        // A second install attempt; the existing subscriber is fine.
        // Surface the attempted default level so an operator can see
        // what we tried.
        tracing::debug!(
            default_level = %default_level,
            ?format,
            error = %e,
            "tracing subscriber already installed; continuing"
        );
    }
    Ok(())
}

/// Install a minimal text-format subscriber BEFORE config resolution so a
/// config-resolution error has somewhere to surface. The real subscriber
/// (driven by `Config.observability`) is installed after; the second
/// `try_init` is a no-op because a subscriber is already present.
/// The bootstrap subscriber respects `RUST_LOG` so an operator can
/// debug startup with `RUST_LOG=debug` even when configuration resolution
/// fails.
fn install_bootstrap_subscriber() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

/// Install SIGTERM and SIGINT handlers up front so a failure here surfaces
/// before `axum::serve` starts. If installation fails (rare: container
/// without signal capability, seccomp policy), we return an error and the
/// process exits cleanly rather than running deaf to k8s termination.
fn install_signal_handlers() -> Result<(Signal, Signal)> {
    let sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    Ok((sigterm, sigint))
}

fn build_worker_metadata_client(worker_introspect_key: Option<&str>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(std::time::Duration::from_secs(2));
    if let Some(token) = worker_introspect_key {
        let mut headers = reqwest::header::HeaderMap::new();
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("worker introspect key must be a valid HTTP header value");
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    builder.build().expect("default http client builds")
}

fn kv_event_endpoint_overrides_from_env(
) -> Result<HashMap<String, sgl_router::policies::kv_events::KvEventEndpointOverride>> {
    match std::env::var("KV_EVENT_ENDPOINT_OVERRIDES") {
        Ok(raw) if !raw.trim().is_empty() => {
            sgl_router::policies::kv_events::parse_endpoint_overrides(&raw)
                .context("parse KV_EVENT_ENDPOINT_OVERRIDES")
        }
        _ => Ok(HashMap::new()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Bootstrap subscriber so a config-resolution error has structured
    // output. The configured-format subscriber installs after this and
    // becomes a no-op via try_init's idempotency.
    install_bootstrap_subscriber();
    let cfg = cli
        .into_config()
        .context("resolve configuration from CLI flags")?;

    init_tracing(&cfg.observability.log_level, cfg.observability.log_format)?;

    tracing::info!(
        "sgl-router {} starting on {}:{}",
        env!("CARGO_PKG_VERSION"),
        cfg.server.host,
        cfg.server.port
    );

    match cfg.runtime_mode {
        RuntimeMode::CacheState => return run_cache_state(cfg).await,
        RuntimeMode::RouterState => return run_router_state(cfg).await,
        RuntimeMode::Gateway => {}
    }

    let tokenizers = Arc::new(
        sgl_router::tokenizer::TokenizerRegistry::load_from_config(&cfg)
            .context("load tokenizers")?,
    );

    let registry = Arc::new(sgl_router::workers::WorkerRegistry::default());
    let router_state_url = std::env::var("ROUTER_STATE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let router_state_redis_url = std::env::var("ROUTER_STATE_REDIS_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let router_state_timeout_ms = env_u64("ROUTER_STATE_TIMEOUT_MS")?.unwrap_or(20).max(1);
    let router_state_snapshot_interval_ms = env_u64("ROUTER_STATE_SNAPSHOT_INTERVAL_MS")?
        .unwrap_or(200)
        .max(1);
    if router_state_url.is_some() && router_state_redis_url.is_some() {
        anyhow::bail!("ROUTER_STATE_URL and ROUTER_STATE_REDIS_URL are mutually exclusive");
    }
    let (router_state_client, router_state_overlay, router_state_poller_handle) =
        if let Some(redis_url) = router_state_redis_url {
            let key_prefix = std::env::var("ROUTER_STATE_REDIS_KEY_PREFIX")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "sgl-router:router-state".to_string());
            let client: Arc<dyn sgl_router::router_state::RouterStateClient> = Arc::new(
                sgl_router::router_state::RedisRouterStateClient::new(
                    &redis_url,
                    &key_prefix,
                    std::time::Duration::from_millis(router_state_timeout_ms),
                )
                .context("build Redis router-state client")?,
            );
            let overlay = sgl_router::router_state::RouterStateLoadOverlay::new();
            registry.attach_router_state_overlay(Arc::clone(&overlay));
            let handle = sgl_router::router_state::spawn_router_state_snapshot_poller(
                Arc::clone(&client),
                Arc::clone(&overlay),
                std::time::Duration::from_millis(router_state_snapshot_interval_ms),
            );
            tracing::info!(
                redis_url_configured = true,
                key_prefix = %key_prefix,
                timeout_ms = router_state_timeout_ms,
                snapshot_interval_ms = router_state_snapshot_interval_ms,
                "Redis router-state active-load overlay enabled"
            );
            (Some(client), Some(overlay), Some(handle))
        } else if let Some(url) = router_state_url {
            let client: Arc<dyn sgl_router::router_state::RouterStateClient> =
                Arc::new(sgl_router::router_state::RemoteRouterStateClient::new(
                    url.clone(),
                    std::time::Duration::from_millis(router_state_timeout_ms),
                ));
            let overlay = sgl_router::router_state::RouterStateLoadOverlay::new();
            registry.attach_router_state_overlay(Arc::clone(&overlay));
            let handle = sgl_router::router_state::spawn_router_state_snapshot_poller(
                Arc::clone(&client),
                Arc::clone(&overlay),
                std::time::Duration::from_millis(router_state_snapshot_interval_ms),
            );
            tracing::info!(
                router_state_url = %url,
                timeout_ms = router_state_timeout_ms,
                snapshot_interval_ms = router_state_snapshot_interval_ms,
                "router-state active-load overlay enabled"
            );
            (Some(client), Some(overlay), Some(handle))
        } else {
            (None, None, None)
        };

    // Build the KV-event index up front so the cache-aware-zmq policy can
    // share its `HashTree` handle + `BlockSizeOracle`. Only ZMQ-backed
    // cache-aware routing attaches subscribers to worker KV event ports.
    let block_size_oracle = sgl_router::policies::kv_events::BlockSizeOracle::new();
    let cache_tree_source = cfg.model.cache_aware.as_ref().map(|c| c.tree_source);

    // Route-history tree mode: the router feeds the prefix tree from its own
    // routing decisions instead of subscribing to worker ZMQ KV-events. In
    // that mode there is no worker introspection to seed the block-size
    // oracle, so seed it from --cache-tree-page-size / --cache-tree-bigram,
    // and we deliberately DON'T attach ZMQ subscribers (no worker ZMQ port
    // needed — works over NAT/Vast public mappings).
    let route_history = matches!(
        cache_tree_source,
        Some(sgl_router::config::CacheTreeSource::RouteHistory)
    );
    let uses_zmq_cache_events = matches!(
        cache_tree_source,
        Some(sgl_router::config::CacheTreeSource::Zmq)
    );
    if route_history {
        if let Some(ps) = cfg.cache_tree_page_size {
            match block_size_oracle.try_set(ps) {
                Ok(v) => tracing::info!(
                    page_size = v,
                    bigram = cfg.cache_tree_bigram,
                    "route-history tree: seeded block-size oracle"
                ),
                Err(e) => {
                    tracing::error!(error = ?e, "route-history tree: failed to seed block size")
                }
            }
            block_size_oracle.set_bigram(cfg.cache_tree_bigram);
        } else {
            tracing::error!("route-history tree-source but --cache-tree-page-size unset; cache routing will fall back to min-load");
        }
    }

    // The KV-event discovery client hits each worker's key-protected
    // `/server_info` (same endpoint as worker introspection), so it must
    // carry the pool's shared worker key as a default Authorization header
    // when one is configured — otherwise discovery gets 401, no ZMQ
    // subscriber is attached, and cache_aware_zmq degrades to min-load.
    let kv_discovery_client = build_worker_metadata_client(cfg.worker_introspect_key.as_deref());
    let endpoint_overrides = kv_event_endpoint_overrides_from_env()?;
    if !endpoint_overrides.is_empty() {
        tracing::info!(
            count = endpoint_overrides.len(),
            "kv-events: endpoint overrides configured"
        );
    }
    let kv_index =
        sgl_router::policies::kv_events::KvEventIndex::new_with_http_oracle_and_endpoint_overrides(
            kv_discovery_client,
            Arc::clone(&block_size_oracle),
            endpoint_overrides,
        );
    let policies = Arc::new(
        sgl_router::policies::factory::build_registry(
            &cfg,
            kv_index.tree(),
            Arc::clone(&tokenizers),
            Arc::clone(&block_size_oracle),
        )
        .context("build policy registry")?,
    );

    // Shared ActiveLoadRegistry + janitor task. The janitor reaps
    // request entries whose lifetime exceeded `stale_request_timeout`,
    // so a leaked guard (proxy task panic, etc.) does not inflate a
    // worker's load forever. The registry is built BEFORE the manager
    // is spawned so the manager can call `forget_worker` on
    // `DiscoveryEvent::Removed`.
    let stale_timeout = std::time::Duration::from_secs(cfg.active_load.stale_request_timeout_secs);
    let active_load = sgl_router::policies::active_load::ActiveLoadRegistry::new(
        Arc::new(sgl_router::policies::active_load::SystemTimeClock),
        stale_timeout,
    );
    // Sweep cadence is 1/10 of the configured timeout, clamped to
    // [1 s, 60 s]. A short timeout (test setting) needs frequent
    // sweeps to fire within the test's window; a long timeout
    // (production) doesn't need sub-minute checks.
    let sweep_interval = std::time::Duration::from_secs(
        (cfg.active_load.stale_request_timeout_secs / 10).clamp(1, 60),
    );
    let janitor_handle =
        sgl_router::policies::active_load::spawn_janitor(Arc::clone(&active_load), sweep_interval);

    // Route-history tree eviction: the tree is fed by routing decisions and
    // has no worker-driven BlockRemoved events to bound it, so periodically
    // LRU-evict down to --cache-tree-max-nodes. (zmq mode evicts via worker
    // events + its own cap, so this task is route-history-only.)
    let tree_evict_handle = if route_history {
        let tree = kv_index.tree();
        let max_nodes = cfg.cache_tree_max_nodes;
        tracing::info!(
            max_nodes,
            "route-history tree: spawning LRU eviction sweeper"
        );
        Some(sgl_router::policies::active_load::spawn_sweeper(
            move || tree.evict_lru(max_nodes),
            std::time::Duration::from_secs(10),
            "route-history-tree",
        ))
    } else {
        None
    };

    // Optional background load poller: when a load poll interval is set,
    // poll each worker's /get_load for its real queue depth and feed it to
    // cache_aware_zmq (instead of the router-side in-flight count). Reuses the
    // worker introspect key for auth. None => not spawned (in-flight count).
    let load_poller_handle = cfg.load_poll_interval_secs.map(|secs| {
        tracing::info!(
            interval_secs = secs,
            "spawning worker load poller (/get_load)"
        );
        sgl_router::policies::load_poller::spawn_load_poller(
            Arc::clone(&registry),
            std::time::Duration::from_secs(secs),
            cfg.worker_introspect_key.clone(),
        )
    });

    // Spawn discovery + manager tasks.
    let (event_rx, discovery_handle) = sgl_router::discovery::spawn_discovery(&cfg)
        .await
        .context("spawn discovery")?;
    // Only ZMQ cache-aware mode attaches the KV-event index to the manager.
    // Route-history feeds the tree from routing decisions, and non-cache
    // policies such as tiered_spillover should not introspect /server_info or
    // subscribe to worker KV event ports.
    let kv_index_opt: Option<Arc<sgl_router::policies::kv_events::KvEventIndex>> =
        if uses_zmq_cache_events {
            Some(Arc::clone(&kv_index))
        } else {
            None
        };
    let manager_handle = tokio::spawn(sgl_router::workers::manager::run_with_config(
        event_rx,
        registry.clone(),
        Some(Arc::new(cfg.clone())),
        kv_index_opt,
        Some(Arc::clone(&active_load)),
    ));

    let proxy = Arc::new(
        sgl_router::proxy::Proxy::new(std::time::Duration::from_secs(
            cfg.proxy.request_timeout_secs,
        ))
        .context("build proxy client")?,
    );

    let ctx = Arc::new(
        sgl_router::server::app_context::AppContext::with_active_load_and_router_state(
            cfg.clone(),
            tokenizers,
            proxy,
            registry,
            policies,
            active_load,
            router_state_client,
            router_state_overlay,
        ),
    );
    ctx.mark_ready();

    let app = sgl_router::server::app::build_router(ctx.clone());

    let bind = format!("{}:{}", cfg.server.host, cfg.server.port);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    tracing::info!("listening on {bind}");

    let (sigterm, sigint) = install_signal_handlers()?;

    let serve = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal(sigterm, sigint));
    let server_result = serve.await.context("axum serve");

    // Best-effort: cancel discovery + manager + janitor on shutdown.
    // The janitor handle's drop signals cancellation; we additionally
    // await `shutdown` so the task joins cleanly before the process
    // exits — useful for tracing tail logs.
    discovery_handle.abort();
    manager_handle.abort();
    janitor_handle.shutdown().await;
    if let Some(h) = tree_evict_handle {
        h.shutdown().await;
    }
    if let Some(h) = load_poller_handle {
        h.shutdown().await;
    }
    if let Some(h) = router_state_poller_handle {
        h.shutdown().await;
    }
    server_result
}

async fn run_router_state(cfg: sgl_router::config::Config) -> Result<()> {
    let service = Arc::new(sgl_router::router_state::RouterStateService::new());
    let router_state_api_token = std::env::var("ROUTER_STATE_API_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());
    if router_state_api_token.is_some() {
        tracing::info!("router-state HTTP API bearer token auth enabled");
    }
    let app = service.router_with_api_token(router_state_api_token);
    let bind = format!("{}:{}", cfg.server.host, cfg.server.port);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    tracing::info!("router-state service listening on {bind}");
    let (sigterm, sigint) = install_signal_handlers()?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(sigterm, sigint))
        .await
        .context("axum serve router-state")
}

async fn run_cache_state(cfg: sgl_router::config::Config) -> Result<()> {
    let block_size_oracle = sgl_router::policies::kv_events::BlockSizeOracle::new();
    let endpoint_overrides = kv_event_endpoint_overrides_from_env()?;
    if !endpoint_overrides.is_empty() {
        tracing::info!(
            count = endpoint_overrides.len(),
            "cache-state kv-events: endpoint overrides configured"
        );
    }
    let kv_index =
        sgl_router::policies::kv_events::KvEventIndex::new_with_http_oracle_and_endpoint_overrides(
            build_worker_metadata_client(cfg.worker_introspect_key.as_deref()),
            Arc::clone(&block_size_oracle),
            endpoint_overrides,
        );
    let service = Arc::new(sgl_router::cache_state::CacheStateService::new(
        kv_index.tree(),
    ));
    let cache_state_api_token = std::env::var("CACHE_STATE_API_TOKEN")
        .ok()
        .filter(|s| !s.is_empty());
    if cache_state_api_token.is_some() {
        tracing::info!("cache-state HTTP API bearer token auth enabled");
    }
    let stream_consumer_handle = spawn_cache_state_stream_consumer_if_configured(
        Arc::clone(&service),
        std::env::var("CACHE_STATE_EVENT_STREAM_PATH")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from),
        env_u64("CACHE_STATE_EVENT_STREAM_RETENTION_SECS")?,
        env_u64("CACHE_STATE_EVENT_STREAM_MAX_BYTES")?,
        env_u64("CACHE_STATE_EVENT_STREAM_POLL_MS")?.unwrap_or(1000),
    )?;
    let kafka_consumer_handle =
        spawn_cache_state_kafka_consumer_if_configured(Arc::clone(&service))?;
    let app = service.router_with_api_token(cache_state_api_token);
    let bind = format!("{}:{}", cfg.server.host, cfg.server.port);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    let discovery_configured = match &cfg.discovery {
        sgl_router::config::DiscoveryBackend::StaticUrls(s) => !s.urls.is_empty(),
        sgl_router::config::DiscoveryBackend::K8s(_) => true,
    };
    let cache_state_manager = if discovery_configured {
        let registry = Arc::new(sgl_router::workers::WorkerRegistry::default());
        let (event_rx, discovery_handle) = sgl_router::discovery::spawn_discovery(&cfg)
            .await
            .context("spawn cache-state discovery")?;
        let manager_handle = tokio::spawn(sgl_router::workers::manager::run_with_config(
            event_rx,
            registry,
            Some(Arc::new(cfg.clone())),
            Some(Arc::clone(&kv_index)),
            None,
        ));
        tracing::info!("cache-state service subscribed to worker KV events");
        Some((discovery_handle, manager_handle))
    } else {
        tracing::info!("cache-state service starting with empty tree and HTTP insert API only");
        None
    };
    tracing::info!("cache-state service listening on {bind}");
    let (sigterm, sigint) = install_signal_handlers()?;
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(sigterm, sigint))
        .await
        .context("axum serve cache-state");
    if let Some((discovery_handle, manager_handle)) = cache_state_manager {
        discovery_handle.abort();
        manager_handle.abort();
    }
    if let Some(handle) = stream_consumer_handle {
        handle.shutdown().await;
    }
    if let Some(handle) = kafka_consumer_handle {
        handle.shutdown().await;
    }
    server_result
}

struct StreamConsumerHandle {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl StreamConsumerHandle {
    async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

fn spawn_cache_state_stream_consumer_if_configured(
    service: Arc<sgl_router::cache_state::CacheStateService>,
    path: Option<PathBuf>,
    retention_secs: Option<u64>,
    max_bytes: Option<u64>,
    poll_ms: u64,
) -> Result<Option<StreamConsumerHandle>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let stream = sgl_router::cache_event_stream::LocalKvEventStream::new(
        sgl_router::cache_event_stream::LocalKvEventStreamConfig {
            path: path.clone(),
            retention_secs,
            max_bytes,
        },
    );
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let join = tokio::spawn(async move {
        let mut last_applied_key: Option<String> = None;
        let poll = std::time::Duration::from_millis(poll_ms.max(100));
        tracing::info!(
            path = %path.display(),
            poll_ms,
            retention_secs,
            max_bytes,
            "cache-state event-stream consumer starting"
        );
        loop {
            if task_cancel.is_cancelled() {
                break;
            }
            match stream.read_all() {
                Ok(records) => {
                    let start = last_applied_key
                        .as_ref()
                        .and_then(|key| {
                            records
                                .iter()
                                .rposition(|record| record.dedupe_key() == *key)
                                .map(|idx| idx + 1)
                        })
                        .unwrap_or(0);
                    if start < records.len() {
                        let to_apply = &records[start..];
                        match service.apply_stream_records(to_apply) {
                            Ok(resp) => {
                                last_applied_key =
                                    to_apply.last().map(|record| record.dedupe_key());
                                tracing::debug!(
                                    records = to_apply.len(),
                                    applied_events = resp.applied_events,
                                    "applied cache-state event-stream records"
                                );
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = ?err,
                                    records = to_apply.len(),
                                    "failed to apply cache-state event-stream records"
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to read cache-state event stream");
                }
            }
            tokio::select! {
                _ = task_cancel.cancelled() => break,
                _ = tokio::time::sleep(poll) => {}
            }
        }
        tracing::info!("cache-state event-stream consumer stopped");
    });
    Ok(Some(StreamConsumerHandle { cancel, join }))
}

fn spawn_cache_state_kafka_consumer_if_configured(
    service: Arc<sgl_router::cache_state::CacheStateService>,
) -> Result<Option<StreamConsumerHandle>> {
    let Some(config) =
        sgl_router::cache_event_stream::KafkaKvEventStreamConfig::consumer_from_env("CACHE_STATE")?
    else {
        return Ok(None);
    };
    let consumer = sgl_router::cache_event_stream::KafkaKvEventConsumer::new(config)?;
    let topic = consumer.topic().to_string();
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let join = tokio::spawn(async move {
        tracing::info!(topic = %topic, "cache-state Kafka event-stream consumer starting");
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => break,
                recv = consumer.recv() => {
                    match recv {
                        Ok(record) => {
                            match service.apply_stream_records(std::slice::from_ref(&record)) {
                                Ok(resp) => {
                                    tracing::debug!(
                                        worker_url = %record.worker_url,
                                        dp_rank = record.dp_rank,
                                        seq = record.seq,
                                        applied_events = resp.applied_events,
                                        "applied cache-state Kafka event-stream record"
                                    );
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        worker_url = %record.worker_url,
                                        dp_rank = record.dp_rank,
                                        seq = record.seq,
                                        error = ?err,
                                        "failed to apply cache-state Kafka event-stream record"
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "failed to receive cache-state Kafka event-stream record"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
        tracing::info!("cache-state Kafka event-stream consumer stopped");
    });
    Ok(Some(StreamConsumerHandle { cancel, join }))
}

fn env_u64(name: &str) -> Result<Option<u64>> {
    let Some(raw) = std::env::var(name).ok().filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    raw.parse::<u64>()
        .with_context(|| format!("parse {name}={raw:?} as u64"))
        .map(Some)
}

async fn shutdown_signal(mut sigterm: Signal, mut sigint: Signal) {
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("got SIGTERM, shutting down"),
        _ = sigint.recv()  => tracing::info!("got SIGINT, shutting down"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn install_signal_handlers_returns_both() {
        // Pins the contract that handler installation works on a standard
        // tokio runtime. If this fails on a sandboxed runner, the real
        // service would also fail to install — which is the point.
        assert!(install_signal_handlers().is_ok());
    }

    #[test]
    fn init_tracing_is_idempotent() {
        let _ = init_tracing("info", LogFormat::Text);
        let _ = init_tracing("info", LogFormat::Text);
    }

    #[test]
    fn init_tracing_accepts_json_format() {
        // Doesn't matter whether we win or lose the race against another
        // subscriber install — the function must return Ok either way.
        assert!(init_tracing("info", LogFormat::Json).is_ok());
    }
}

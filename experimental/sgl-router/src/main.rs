// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use clap::Parser;
use sgl_router::config::{Cli, LogFormat, RuntimeMode};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::signal::unix::{signal, Signal, SignalKind};

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

    if cfg.runtime_mode == RuntimeMode::CacheState {
        return run_cache_state(cfg).await;
    }

    let tokenizers = Arc::new(
        sgl_router::tokenizer::TokenizerRegistry::load_from_config(&cfg)
            .context("load tokenizers")?,
    );

    let registry = Arc::new(sgl_router::workers::WorkerRegistry::default());

    // Build the KV-event index up front so the cache-aware-zmq policy can
    // share its `HashTree` handle + `BlockSizeOracle`. When no model uses
    // `cache_aware_zmq`, the index is still constructed (cheap) but no
    // subscribers are ever added.
    let block_size_oracle = sgl_router::policies::kv_events::BlockSizeOracle::new();

    // Route-history tree mode: the router feeds the prefix tree from its own
    // routing decisions instead of subscribing to worker ZMQ KV-events. In
    // that mode there is no worker introspection to seed the block-size
    // oracle, so seed it from --cache-tree-page-size / --cache-tree-bigram,
    // and we deliberately DON'T attach ZMQ subscribers (no worker ZMQ port
    // needed — works over NAT/Vast public mappings).
    let route_history = matches!(
        cfg.model.cache_aware.as_ref().map(|c| c.tree_source),
        Some(sgl_router::config::CacheTreeSource::RouteHistory)
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

    // Optional background load poller: when --load-poll-interval-secs is set,
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
    // In route-history mode, do NOT attach the KV-event index to the manager:
    // that path introspects each worker's /server_info and spawns ZMQ
    // subscribers, which is exactly what route-history avoids. The policy still
    // shares the same tree handle (built above) — it's just fed by routing
    // decisions instead of ZMQ events.
    let kv_index_opt: Option<Arc<sgl_router::policies::kv_events::KvEventIndex>> = if route_history
    {
        None
    } else {
        Some(Arc::clone(&kv_index))
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
        sgl_router::server::app_context::AppContext::with_active_load(
            cfg.clone(),
            tokenizers,
            proxy,
            registry,
            policies,
            active_load,
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
    server_result
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
    server_result
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

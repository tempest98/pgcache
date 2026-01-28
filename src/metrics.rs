use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;

/// Metric names as constants for consistency
pub mod names {
    // Counter metrics
    pub const QUERIES_TOTAL: &str = "pgcache.queries.total";
    pub const QUERIES_CACHEABLE: &str = "pgcache.queries.cacheable";
    pub const QUERIES_UNCACHEABLE: &str = "pgcache.queries.uncacheable";
    pub const QUERIES_UNSUPPORTED: &str = "pgcache.queries.unsupported";
    pub const QUERIES_INVALID: &str = "pgcache.queries.invalid";
    pub const QUERIES_CACHE_HIT: &str = "pgcache.queries.cache_hit";
    pub const QUERIES_CACHE_MISS: &str = "pgcache.queries.cache_miss";
    pub const QUERIES_CACHE_ERROR: &str = "pgcache.queries.cache_error";

    // Histogram metrics (latency in seconds per Prometheus convention)
    pub const QUERY_LATENCY_SECONDS: &str = "pgcache.query.latency_seconds";
    pub const CACHE_LOOKUP_LATENCY_SECONDS: &str = "pgcache.cache.lookup_latency_seconds";
    pub const ORIGIN_LATENCY_SECONDS: &str = "pgcache.origin.latency_seconds";
    pub const QUERY_REGISTRATION_LATENCY_SECONDS: &str =
        "pgcache.query.registration_latency_seconds";

    // Connection metrics
    pub const CONNECTIONS_TOTAL: &str = "pgcache.connections.total";
    pub const CONNECTIONS_ACTIVE: &str = "pgcache.connections.active";
    pub const CONNECTIONS_ERRORS: &str = "pgcache.connections.errors";

    // CDC/Replication metrics
    pub const CDC_EVENTS_PROCESSED: &str = "pgcache.cdc.events_processed";
    pub const CDC_INSERTS: &str = "pgcache.cdc.inserts";
    pub const CDC_UPDATES: &str = "pgcache.cdc.updates";
    pub const CDC_DELETES: &str = "pgcache.cdc.deletes";
    pub const CDC_LAG_BYTES: &str = "pgcache.cdc.lag_bytes";
    pub const CDC_LAG_SECONDS: &str = "pgcache.cdc.lag_seconds";

    // Cache performance metrics
    pub const CACHE_INVALIDATIONS: &str = "pgcache.cache.invalidations";
    pub const CACHE_EVICTIONS: &str = "pgcache.cache.evictions";

    // CDC handler metrics (counters)
    pub const CACHE_HANDLE_INSERTS: &str = "pgcache.cache.handle_inserts";
    pub const CACHE_HANDLE_UPDATES: &str = "pgcache.cache.handle_updates";
    pub const CACHE_HANDLE_DELETES: &str = "pgcache.cache.handle_deletes";

    // CDC handler latency (histograms)
    pub const CACHE_HANDLE_INSERT_SECONDS: &str = "pgcache.cache.handle_insert_seconds";
    pub const CACHE_HANDLE_UPDATE_SECONDS: &str = "pgcache.cache.handle_update_seconds";
    pub const CACHE_HANDLE_DELETE_SECONDS: &str = "pgcache.cache.handle_delete_seconds";

    // Cache state metrics
    pub const CACHE_QUERIES_REGISTERED: &str = "pgcache.cache.queries_registered";
    pub const CACHE_QUERIES_LOADING: &str = "pgcache.cache.queries_loading";
    pub const CACHE_SIZE_BYTES: &str = "pgcache.cache.size_bytes";
    pub const CACHE_SIZE_LIMIT_BYTES: &str = "pgcache.cache.size_limit_bytes";
    pub const CACHE_GENERATION: &str = "pgcache.cache.generation";
    pub const CACHE_TABLES_TRACKED: &str = "pgcache.cache.tables_tracked";

    // Extended protocol metrics
    pub const PROTOCOL_SIMPLE_QUERIES: &str = "pgcache.protocol.simple_queries";
    pub const PROTOCOL_EXTENDED_QUERIES: &str = "pgcache.protocol.extended_queries";
    pub const PROTOCOL_PREPARED_STATEMENTS: &str = "pgcache.protocol.prepared_statements";

    // Per-stage timing histograms
    pub const QUERY_STAGE_PARSE_SECONDS: &str = "pgcache.query.stage.parse_seconds";
    pub const QUERY_STAGE_DISPATCH_SECONDS: &str = "pgcache.query.stage.dispatch_seconds";
    pub const QUERY_STAGE_LOOKUP_SECONDS: &str = "pgcache.query.stage.lookup_seconds";
    pub const QUERY_STAGE_ROUTING_SECONDS: &str = "pgcache.query.stage.routing_seconds";
    pub const QUERY_STAGE_WORKER_EXEC_SECONDS: &str = "pgcache.query.stage.worker_exec_seconds";
    pub const QUERY_STAGE_RESPONSE_WRITE_SECONDS: &str =
        "pgcache.query.stage.response_write_seconds";
    pub const QUERY_STAGE_TOTAL_SECONDS: &str = "pgcache.query.stage.total_seconds";
}

/// Install the Prometheus metrics recorder.
///
/// If `metrics_socket` is provided, starts an HTTP server on that address
/// serving Prometheus metrics at `/metrics`.
pub fn prometheus_install(metrics_socket: Option<SocketAddr>) -> Result<(), &'static str> {
    use std::sync::mpsc;

    // Configure Prometheus with quantiles for histograms
    let mut builder = PrometheusBuilder::new()
        .set_quantiles(&[0.5, 0.95, 0.99])
        .expect("configure prometheus quantiles");

    // Optionally bind HTTP listener
    if let Some(socket) = metrics_socket {
        builder = builder.with_http_listener(socket);
    }

    // build() requires a tokio runtime context, so we create the runtime
    // in a background thread, build the recorder there, and send it back
    let (tx, rx) = mpsc::sync_channel(1);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create tokio runtime for metrics exporter");

        // Enter runtime context so build() can set up the HTTP listener
        let _guard = rt.enter();

        let (prometheus, exporter_future) = match builder.build() {
            Ok(result) => result,
            Err(e) => {
                let _ = tx.send(Err(format!("failed to build prometheus: {e:?}")));
                return;
            }
        };

        let _ = tx.send(Ok(prometheus));

        // Run the HTTP exporter (blocks until shutdown)
        if let Err(e) = rt.block_on(exporter_future) {
            tracing::error!("prometheus exporter failed: {e:?}");
        }
    });

    let prometheus = rx
        .recv()
        .map_err(|_| "metrics thread failed to start")?
        .map_err(|_| "failed to build prometheus recorder")?;

    metrics::set_global_recorder(prometheus).map_err(|_| "failed to install metrics recorder")?;

    Ok(())
}

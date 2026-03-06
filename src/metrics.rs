use std::fmt;
use std::net::SocketAddr;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use rootcause::Report;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

/// Metrics subsystem errors.
#[derive(Debug)]
pub enum MetricsError {
    /// Prometheus recorder build failed.
    Build(String),
    /// A global metrics recorder was already installed.
    RecorderInstall,
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricsError::Build(msg) => write!(f, "{msg}"),
            MetricsError::RecorderInstall => {
                write!(f, "global metrics recorder already installed")
            }
        }
    }
}

impl std::error::Error for MetricsError {}

pub type MetricsResult<T> = Result<T, Report<MetricsError>>;

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
    pub const QUERIES_ALLOWLIST_SKIPPED: &str = "pgcache.queries.allowlist_skipped";

    // Histogram metrics (latency in seconds per Prometheus convention)
    /// End-to-end latency for cache hits: client message received → response written to client.
    pub const CACHE_QUERY_LATENCY_SECONDS: &str = "pgcache.query.cache_latency_seconds";
    /// End-to-end latency for origin queries: client message received → ReadyForQuery forwarded to client.
    pub const ORIGIN_QUERY_LATENCY_SECONDS: &str = "pgcache.query.origin_latency_seconds";
    /// Pure origin execution time: forward decision made → ReadyForQuery received (excludes proxy overhead).
    pub const ORIGIN_EXECUTION_SECONDS: &str = "pgcache.origin.execution_seconds";
    pub const CACHE_LOOKUP_LATENCY_SECONDS: &str = "pgcache.cache.lookup_latency_seconds";
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
    pub const CDC_FLUSH_STALENESS_SECONDS: &str = "pgcache.cdc.flush_staleness_seconds";

    // Cache performance metrics
    pub const CACHE_INVALIDATIONS: &str = "pgcache.cache.invalidations";
    pub const CACHE_FRESHNESS_HITS: &str = "pgcache.cache.freshness_hits";
    pub const CACHE_EVICTIONS: &str = "pgcache.cache.evictions";
    pub const CACHE_READMISSIONS: &str = "pgcache.cache.readmissions";
    pub const CACHE_SUBSUMPTIONS: &str = "pgcache.cache.subsumptions";
    pub const CACHE_SUBSUMPTION_LATENCY_SECONDS: &str = "pgcache.cache.subsumption_latency_seconds";

    // Cache state metrics (admission/eviction policy)
    pub const CACHE_QUERIES_PENDING: &str = "pgcache.cache.queries_pending";
    pub const CACHE_QUERIES_INVALIDATED: &str = "pgcache.cache.queries_invalidated";

    // Queue depth gauges
    pub const CACHE_WRITER_QUERY_QUEUE: &str = "pgcache.cache.writer_query_queue";
    pub const CACHE_WRITER_CDC_QUEUE: &str = "pgcache.cache.writer_cdc_queue";
    pub const CACHE_WRITER_INTERNAL_QUEUE: &str = "pgcache.cache.writer_internal_queue";
    pub const CACHE_WORKER_QUEUE: &str = "pgcache.cache.worker_queue";
    pub const CACHE_POPULATION_WORKER_QUEUE: &str = "pgcache.cache.population_worker_queue";
    pub const CACHE_PROXY_MESSAGE_QUEUE: &str = "pgcache.cache.proxy_message_queue";
    pub const PROXY_WORKER_QUEUE: &str = "pgcache.proxy.worker_queue";
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

    // Worker wait time histograms
    pub const CACHE_WORKER_WAIT_SECONDS: &str = "pgcache.cache.worker_wait_seconds";
    pub const CACHE_WORKER_CONN_WAIT_SECONDS: &str = "pgcache.cache.worker_conn_wait_seconds";
}

/// Install the Prometheus metrics recorder.
///
/// If `metrics_socket` is provided, spawns an HTTP server on that address
/// serving Prometheus metrics at `/metrics` with CORS headers enabled.
pub fn prometheus_install(metrics_socket: Option<SocketAddr>) -> MetricsResult<()> {
    let recorder = PrometheusBuilder::new()
        .set_quantiles(&[0.5, 0.95, 0.99])
        .map_err(|e| MetricsError::Build(e.to_string()))?
        .build_recorder();

    let handle = recorder.handle();

    metrics::set_global_recorder(recorder)
        .map_err(|_| Report::new(MetricsError::RecorderInstall))?;

    if let Some(socket) = metrics_socket {
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("metrics tokio runtime");
            rt.block_on(metrics_server_run(socket, handle));
        });
    }

    Ok(())
}

/// Minimal HTTP server that serves Prometheus metrics with CORS headers.
async fn metrics_server_run(addr: SocketAddr, handle: PrometheusHandle) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("metrics server bind failed on {addr}: {e}");
            return;
        }
    };

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("metrics server accept error: {e}");
                continue;
            }
        };

        let handle = handle.clone();
        tokio::spawn(async move {
            // Read enough of the request to determine method and path.
            // We don't need to parse the full request for this simple server.
            let mut buf = [0u8; 1024];
            let n = match tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };

            let request = String::from_utf8_lossy(buf.get(..n).unwrap_or(&[]));
            let first_line = request.lines().next().unwrap_or("");

            let response = if first_line.starts_with("OPTIONS ") {
                // CORS preflight
                "HTTP/1.1 204 No Content\r\n\
                 Access-Control-Allow-Origin: *\r\n\
                 Access-Control-Allow-Methods: GET, OPTIONS\r\n\
                 Access-Control-Allow-Headers: *\r\n\
                 Content-Length: 0\r\n\
                 Connection: close\r\n\r\n"
                    .to_owned()
            } else if first_line.starts_with("GET /health") {
                "HTTP/1.1 200 OK\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Content-Type: text/plain\r\n\
                     Content-Length: 2\r\n\
                     Connection: close\r\n\r\nOK"
                    .to_owned()
            } else if first_line.starts_with("GET ") {
                let body = handle.render();
                format!(
                    "HTTP/1.1 200 OK\r\n\
                     Access-Control-Allow-Origin: *\r\n\
                     Content-Type: text/plain; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                )
            } else {
                "HTTP/1.1 405 Method Not Allowed\r\n\
                 Content-Length: 0\r\n\
                 Connection: close\r\n\r\n"
                    .to_owned()
            };

            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

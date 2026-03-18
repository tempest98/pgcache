use serde::Serialize;
use tokio::sync::oneshot;

/// Request for current cache status, sent from the admin HTTP handler.
pub struct StatusRequest {
    pub reply_tx: oneshot::Sender<StatusResponse>,
}

/// Full status response assembled by the cache writer.
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub cache: CacheStatusData,
    pub cdc: CdcStatusData,
    pub queries: Vec<QueryStatusData>,
}

/// Cache subsystem state.
#[derive(Debug, Serialize)]
pub struct CacheStatusData {
    pub size_bytes: usize,
    pub size_limit_bytes: Option<usize>,
    pub generation: u64,
    pub tables_tracked: usize,
    pub policy: String,
}

/// CDC / logical replication state.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CdcStatusData {
    pub tables: Vec<String>,
    pub last_received_lsn: u64,
    pub last_flushed_lsn: u64,
    pub lag_bytes: u64,
}

/// Per-query status for a cached query.
#[derive(Debug, Serialize)]
pub struct QueryStatusData {
    pub fingerprint: u64,
    pub sql_preview: String,
    pub tables: Vec<String>,
    pub state: String,
    pub cached_bytes: usize,
    pub max_limit: Option<u64>,
    pub pinned: bool,
    // Per-query operational metrics
    pub hit_count: u64,
    pub miss_count: u64,
    /// Milliseconds since last cache hit (None = no hits yet)
    pub idle_duration_ms: Option<u64>,
    /// Milliseconds since query was first seen by the cache
    pub registered_duration_ms: Option<u64>,
    /// Milliseconds since query last became Ready (None = not currently cached)
    pub cached_duration_ms: Option<u64>,
    pub invalidation_count: u64,
    pub readmission_count: u64,
    pub eviction_count: u64,
    pub subsumption_count: u64,
    pub population_count: u64,
    /// Milliseconds (None = not set)
    pub last_population_duration_ms: Option<u64>,
    pub total_bytes_served: u64,
    pub population_row_count: u64,
    pub cache_hit_latency: Option<LatencyStats>,
}

/// Summary statistics from the cache-hit latency histogram.
#[derive(Debug, Serialize)]
pub struct LatencyStats {
    pub count: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub min_us: u64,
    pub max_us: u64,
}

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
}

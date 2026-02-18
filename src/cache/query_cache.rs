use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tokio_util::bytes::BytesMut;
use tracing::{error, instrument, trace};

use crate::metrics::names;

use crate::query::ast::{LimitClause, query_expr_fingerprint};
use crate::query::resolved::ResolvedQueryExpr;
use crate::settings::Settings;
use crate::timing::QueryTiming;

use super::{
    CacheError, CacheResult,
    messages::{CacheReply, QueryCommand},
    query::{CacheableQuery, limit_is_sufficient, limit_rows_needed},
    types::{CacheStateView, CachedQueryState, CachedQueryView},
};
use crate::proxy::ClientSocket;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Simple,
    Extended,
}

pub struct QueryRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub cacheable_query: Box<CacheableQuery>,
    pub result_formats: Vec<i16>,
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    /// Resolved search_path for schema resolution
    pub search_path: Vec<String>,
    /// Per-query timing data
    pub timing: QueryTiming,
}

/// Request sent to cache worker for executing cached queries.
/// Contains the resolved AST with schema-qualified table names.
pub struct WorkerRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: ResolvedQueryExpr,
    /// Generation number for row tracking in pgcache_pgrx extension
    pub generation: u64,
    pub result_formats: Vec<i16>,
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Incoming query's LIMIT clause, appended to SQL at serve time
    pub limit: Option<LimitClause>,
}

/// Query cache coordinator - routes queries and delegates writes to the writer thread.
#[derive(Debug, Clone)]
pub struct QueryCache {
    query_tx: UnboundedSender<QueryCommand>,
    worker_tx: UnboundedSender<WorkerRequest>,
    state_view: Arc<RwLock<CacheStateView>>,
}

impl QueryCache {
    pub async fn new(
        _settings: &Settings,
        query_tx: UnboundedSender<QueryCommand>,
        worker_tx: UnboundedSender<WorkerRequest>,
        state_view: Arc<RwLock<CacheStateView>>,
    ) -> CacheResult<Self> {
        Ok(Self {
            query_tx,
            worker_tx,
            state_view,
        })
    }

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_dispatch(&mut self, msg: QueryRequest) -> CacheResult<()> {
        // Generate fingerprint from AST (excludes LIMIT/OFFSET)
        let fingerprint = query_expr_fingerprint(&msg.cacheable_query.query);
        trace!("{fingerprint}");

        // Compute rows needed for this query's LIMIT
        let rows_needed = limit_rows_needed(&msg.cacheable_query.query.limit);

        // Measure cache lookup latency
        let lookup_start = Instant::now();

        // Check cache state from shared view
        let cache_entry = self
            .state_view
            .read()
            .ok()
            .and_then(|view| view.cached_queries.get(&fingerprint).cloned());

        // Record cache lookup latency
        let lookup_duration = lookup_start.elapsed();
        metrics::histogram!(names::CACHE_LOOKUP_LATENCY_SECONDS)
            .record(lookup_duration.as_secs_f64());

        // Cache hit: Ready state with resolved query — check if cached rows are sufficient
        if let Some(CachedQueryView {
            state: CachedQueryState::Ready,
            generation,
            resolved: Some(resolved),
            max_limit,
        }) = &cache_entry
        {
            if limit_is_sufficient(*max_limit, rows_needed) {
                // Sufficient rows cached — serve from cache
                let timing = {
                    let mut t = msg.timing;
                    t.lookup_complete_at = Some(Instant::now());
                    t
                };

                let worker_request = WorkerRequest {
                    query_type: msg.query_type,
                    data: msg.data,
                    resolved: resolved.clone(),
                    generation: *generation,
                    result_formats: msg.result_formats,
                    client_socket: msg.client_socket,
                    reply_tx: msg.reply_tx,
                    timing,
                    limit: msg.cacheable_query.query.limit.clone(),
                };
                return self.worker_tx.send(worker_request).map_err(|e| {
                    error!("worker send {e} {fingerprint}");
                    CacheError::WorkerSend.into()
                });
            }

            // Insufficient rows — forward to origin and request re-population
            // with the higher limit
            trace!("limit bump {fingerprint} cached={max_limit:?} needed={rows_needed:?}");

            // Set state to Loading immediately to prevent duplicate LimitBump
            // commands from racing before the writer processes this one
            if let Ok(mut view) = self.state_view.write()
                && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
            {
                entry.state = CachedQueryState::Loading;
            }

            msg.reply_tx
                .send(CacheReply::Forward(msg.data))
                .map_err(|_| CacheError::Reply)?;

            self.query_tx
                .send(QueryCommand::LimitBump {
                    fingerprint,
                    max_limit: rows_needed,
                })
                .map_err(|_| CacheError::WorkerSend)?;

            return Ok(());
        }

        // Cache miss or Loading - forward to origin
        trace!("cache miss {fingerprint}");

        msg.reply_tx
            .send(CacheReply::Forward(msg.data))
            .map_err(|_| CacheError::Reply)?;

        trace!("cache_entry check {fingerprint}");

        // Register only if not already in cache (prevents duplicate registrations)
        if cache_entry.is_none() {
            // Insert Loading placeholder to prevent races
            if let Ok(mut view) = self.state_view.write() {
                view.cached_queries.insert(
                    fingerprint,
                    CachedQueryView {
                        state: CachedQueryState::Loading,
                        generation: 0,
                        resolved: None,
                        max_limit: None,
                    },
                );
            }

            trace!("send to writer {fingerprint}");

            self.query_tx
                .send(QueryCommand::Register {
                    fingerprint,
                    cacheable_query: msg.cacheable_query,
                    search_path: msg.search_path,
                    started_at: Instant::now(),
                })
                .map_err(|_| {
                    trace!("query registration error {fingerprint}");
                    CacheError::WorkerSend
                })?;

            trace!("sent to writer {fingerprint}");
        }

        Ok(())
    }
}

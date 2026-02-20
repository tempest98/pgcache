use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tokio_util::bytes::BytesMut;
use tracing::{error, instrument, trace};

use crate::metrics::names;

use crate::query::ast::{LimitClause, query_expr_fingerprint};
use crate::query::resolved::ResolvedQueryExpr;
use crate::settings::{CachePolicy, Settings};
use crate::timing::QueryTiming;

use super::{
    CacheError, CacheResult,
    messages::{CacheReply, PipelineContext, PipelineDescribe, QueryCommand},
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
    /// Pipeline context from the proxy (None for simple queries and cold-path extended)
    pub pipeline: Option<PipelineContext>,
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
    /// Whether a Sync was included in the buffered pipeline.
    /// When true, the worker appends ReadyForQuery to the response.
    pub has_sync: bool,
    /// Whether Parse was buffered in the pipeline.
    /// False for Bind-only pipelines (named statement re-execution without Parse).
    pub has_parse: bool,
    /// Whether Bind was buffered in the pipeline.
    /// False when Bind was flushed separately (e.g., JDBC Parse/Bind/Describe/Flush then Execute/Sync).
    pub has_bind: bool,
    /// Whether the pipeline includes a Describe message and which type.
    pub pipeline_describe: PipelineDescribe,
    /// Stored ParameterDescription bytes for Describe('S') responses in the pipeline.
    pub parameter_description: Option<BytesMut>,
    /// Buffered bytes for origin fallback on worker error.
    pub forward_bytes: Option<BytesMut>,
}

/// Query cache coordinator - routes queries and delegates writes to the writer thread.
#[derive(Debug, Clone)]
pub struct QueryCache {
    query_tx: UnboundedSender<QueryCommand>,
    worker_tx: UnboundedSender<WorkerRequest>,
    state_view: Arc<RwLock<CacheStateView>>,
    cache_policy: CachePolicy,
    admission_threshold: u32,
}

impl QueryCache {
    pub async fn new(
        settings: &Settings,
        query_tx: UnboundedSender<QueryCommand>,
        worker_tx: UnboundedSender<WorkerRequest>,
        state_view: Arc<RwLock<CacheStateView>>,
    ) -> CacheResult<Self> {
        Ok(Self {
            query_tx,
            worker_tx,
            state_view,
            cache_policy: settings.cache_policy,
            admission_threshold: settings.admission_threshold,
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

        match &cache_entry {
            // Cache hit: Ready with sufficient rows — serve from cache
            Some(CachedQueryView {
                state: CachedQueryState::Ready,
                generation,
                resolved: Some(resolved),
                max_limit,
                ..
            }) if limit_is_sufficient(*max_limit, rows_needed) => {
                // Set reference bit for CLOCK eviction (brief write lock)
                if self.cache_policy == CachePolicy::Clock
                    && let Ok(mut view) = self.state_view.write()
                    && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
                {
                    entry.referenced = true;
                }

                let timing = {
                    let mut t = msg.timing;
                    t.lookup_complete_at = Some(Instant::now());
                    t
                };

                // Extract pipeline fields for the worker
                let (
                    has_sync,
                    has_parse,
                    has_bind,
                    pipeline_describe,
                    parameter_description,
                    forward_bytes,
                ) = match msg.pipeline {
                    Some(pipeline) => (
                        true,
                        pipeline.has_parse,
                        pipeline.has_bind,
                        pipeline.describe,
                        pipeline.parameter_description,
                        Some(pipeline.buffered_bytes),
                    ),
                    None => (false, false, false, PipelineDescribe::None, None, None),
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
                    has_sync,
                    has_parse,
                    has_bind,
                    pipeline_describe,
                    parameter_description,
                    forward_bytes,
                };
                return self.worker_tx.send(worker_request).map_err(|e| {
                    error!("worker send {e} {fingerprint}");
                    CacheError::WorkerSend.into()
                });
            }

            // Cache hit: Ready but insufficient rows — forward and request limit bump
            Some(CachedQueryView {
                state: CachedQueryState::Ready,
                max_limit,
                ..
            }) => {
                trace!("limit bump {fingerprint} cached={max_limit:?} needed={rows_needed:?}");

                // Set state to Loading immediately to prevent duplicate LimitBump
                // commands from racing before the writer processes this one
                if let Ok(mut view) = self.state_view.write()
                    && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
                {
                    entry.state = CachedQueryState::Loading;
                }

                msg.reply_tx
                    .send(CacheReply::Forward(forward_buf_assemble(
                        msg.pipeline,
                        msg.data,
                    )))
                    .map_err(|_| CacheError::Reply)?;

                self.query_tx
                    .send(QueryCommand::LimitBump {
                        fingerprint,
                        max_limit: rows_needed,
                    })
                    .map_err(|_| CacheError::WorkerSend)?;

                return Ok(());
            }

            // Loading — forward to origin, don't re-register
            Some(CachedQueryView {
                state: CachedQueryState::Loading,
                ..
            }) => {
                trace!("cache loading {fingerprint}");
                msg.reply_tx
                    .send(CacheReply::Forward(forward_buf_assemble(
                        msg.pipeline,
                        msg.data,
                    )))
                    .map_err(|_| CacheError::Reply)?;
                return Ok(());
            }

            // Pending — forward to origin, increment hit count, admit if threshold reached
            Some(CachedQueryView {
                state: CachedQueryState::Pending(hit_count),
                ..
            }) => {
                let new_count = hit_count + 1;
                trace!("pending {fingerprint} count={new_count}");

                msg.reply_tx
                    .send(CacheReply::Forward(forward_buf_assemble(
                        msg.pipeline,
                        msg.data,
                    )))
                    .map_err(|_| CacheError::Reply)?;

                if new_count >= self.admission_threshold {
                    // Promote to Loading and register
                    if let Ok(mut view) = self.state_view.write()
                        && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
                    {
                        entry.state = CachedQueryState::Loading;
                    }

                    self.query_tx
                        .send(QueryCommand::Register {
                            fingerprint,
                            cacheable_query: msg.cacheable_query,
                            search_path: msg.search_path,
                            started_at: Instant::now(),
                        })
                        .map_err(|_| CacheError::WorkerSend)?;
                } else {
                    // Increment hit count
                    if let Ok(mut view) = self.state_view.write()
                        && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
                    {
                        entry.state = CachedQueryState::Pending(new_count);
                    }
                }

                return Ok(());
            }

            // Invalidated — forward to origin, fast-readmit (skip admission gate)
            Some(CachedQueryView {
                state: CachedQueryState::Invalidated,
                ..
            }) => {
                trace!("invalidated readmit {fingerprint}");

                msg.reply_tx
                    .send(CacheReply::Forward(forward_buf_assemble(
                        msg.pipeline,
                        msg.data,
                    )))
                    .map_err(|_| CacheError::Reply)?;

                // Transition to Loading and send Register for fast readmission
                if let Ok(mut view) = self.state_view.write()
                    && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
                {
                    entry.state = CachedQueryState::Loading;
                }

                self.query_tx
                    .send(QueryCommand::Register {
                        fingerprint,
                        cacheable_query: msg.cacheable_query,
                        search_path: msg.search_path,
                        started_at: Instant::now(),
                    })
                    .map_err(|_| CacheError::WorkerSend)?;

                return Ok(());
            }

            // Not found — new query
            None => {}
        }

        // Not found: first miss for this query
        trace!("cache miss {fingerprint}");

        msg.reply_tx
            .send(CacheReply::Forward(forward_buf_assemble(
                msg.pipeline,
                msg.data,
            )))
            .map_err(|_| CacheError::Reply)?;

        // Determine initial state based on policy
        let should_register =
            self.cache_policy == CachePolicy::Fifo || self.admission_threshold <= 1;

        if should_register {
            // FIFO or threshold=1: admit immediately
            if let Ok(mut view) = self.state_view.write() {
                view.cached_queries.insert(
                    fingerprint,
                    CachedQueryView {
                        state: CachedQueryState::Loading,
                        generation: 0,
                        resolved: None,
                        max_limit: None,
                        referenced: false,
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
        } else {
            // CLOCK with threshold > 1: start in Pending state
            if let Ok(mut view) = self.state_view.write() {
                view.cached_queries.insert(
                    fingerprint,
                    CachedQueryView {
                        state: CachedQueryState::Pending(1),
                        generation: 0,
                        resolved: None,
                        max_limit: None,
                        referenced: false,
                    },
                );
            }
            trace!("pending new {fingerprint}");
        }

        Ok(())
    }
}

/// Assemble a forward buffer from pipeline context or raw data.
///
/// Pipeline path: buffered_bytes already includes Parse+Bind+Execute+Sync.
/// Simple query path: data is the raw query bytes.
fn forward_buf_assemble(pipeline: Option<PipelineContext>, data: BytesMut) -> BytesMut {
    match pipeline {
        Some(pipeline) => pipeline.buffered_bytes,
        None => data,
    }
}

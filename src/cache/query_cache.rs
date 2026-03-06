use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tokio_util::bytes::BytesMut;
use tracing::{error, info, instrument, trace};

use crate::metrics::names;

use crate::query::ast::{LimitClause, QueryExpr, TableNode, query_expr_fingerprint};
use crate::settings::{CachePolicy, Settings};
use crate::timing::QueryTiming;

use super::{
    CacheError, CacheResult,
    messages::{
        AdmitAction, CacheReply, PipelineContext, PipelineDescribe, QueryCommand, SubsumptionResult,
    },
    query::{CacheableQuery, limit_is_sufficient, limit_rows_needed},
    types::{CacheStateView, CachedQueryState, CachedQueryView, SharedResolved},
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
    pub cacheable_query: Arc<CacheableQuery>,
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
    pub resolved: SharedResolved,
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

/// Parsed allowlist entry: (optional schema, table name), both lowercased.
type AllowlistEntry = (Option<String>, String);

/// Parsed and ready-to-match allowlist. None = all tables cacheable.
type Allowlist = Option<Vec<AllowlistEntry>>;

/// Parse an allowlist entry string into (optional schema, table name).
/// Supports "table" and "schema.table" forms.
fn allowlist_entry_parse(entry: &str) -> AllowlistEntry {
    let entry = entry.trim();
    match entry.rsplit_once('.') {
        Some((schema, table)) => (Some(schema.to_lowercase()), table.to_lowercase()),
        None => (None, entry.to_lowercase()),
    }
}

/// Parse config strings into a ready-to-match allowlist.
fn allowlist_parse(tables: &Option<Vec<String>>) -> Allowlist {
    tables
        .as_ref()
        .filter(|v| !v.is_empty())
        .map(|entries| entries.iter().map(|e| allowlist_entry_parse(e)).collect())
}

/// Query cache coordinator - routes queries and delegates writes to the writer thread.
#[derive(Debug, Clone)]
pub struct QueryCache {
    query_tx: UnboundedSender<QueryCommand>,
    worker_tx: UnboundedSender<WorkerRequest>,
    state_view: Arc<RwLock<CacheStateView>>,
    cache_policy: CachePolicy,
    admission_threshold: u32,
    allowed_tables: Allowlist,
}

impl QueryCache {
    pub async fn new(
        settings: &Settings,
        query_tx: UnboundedSender<QueryCommand>,
        worker_tx: UnboundedSender<WorkerRequest>,
        state_view: Arc<RwLock<CacheStateView>>,
    ) -> CacheResult<Self> {
        let allowed_tables = allowlist_parse(&settings.allowed_tables);
        match &allowed_tables {
            Some(entries) => {
                let names: Vec<&str> = settings
                    .allowed_tables
                    .as_ref()
                    .map(|v| v.iter().map(String::as_str).collect())
                    .unwrap_or_default();
                info!("table allowlist enabled: {names:?}");
                let _ = entries; // silence unused warning
            }
            None => info!("table allowlist disabled, all tables cacheable"),
        }

        Ok(Self {
            query_tx,
            worker_tx,
            state_view,
            cache_policy: settings.cache_policy,
            admission_threshold: settings.admission_threshold,
            allowed_tables,
        })
    }

    /// Check whether all tables in the query are in the allowlist.
    /// Returns true if no allowlist is configured (all tables allowed).
    fn query_allowlist_check(&self, query: &QueryExpr) -> bool {
        let Some(entries) = &self.allowed_tables else {
            return true;
        };
        query.nodes::<TableNode>().all(|t| {
            let table_name = t.name.to_lowercase();
            let table_schema = t.schema.as_ref().map(|s| s.to_lowercase());
            entries.iter().any(|(ws, wt)| {
                *wt == table_name
                    && match ws {
                        Some(ws) => table_schema.as_deref() == Some(ws.as_str()),
                        None => true,
                    }
            })
        })
    }

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_dispatch(&mut self, msg: QueryRequest) -> CacheResult<()> {
        if !self.query_allowlist_check(&msg.cacheable_query.query) {
            metrics::counter!(names::QUERIES_ALLOWLIST_SKIPPED).increment(1);
            return reply_forward(msg.reply_tx, msg.pipeline, msg.data);
        }

        let fingerprint = query_expr_fingerprint(&msg.cacheable_query.query);
        trace!("{fingerprint}");

        let rows_needed = limit_rows_needed(&msg.cacheable_query.query.limit);

        let lookup_start = Instant::now();
        let cache_entry = self
            .state_view
            .read()
            .ok()
            .and_then(|view| view.cached_queries.get(&fingerprint).cloned());
        metrics::histogram!(names::CACHE_LOOKUP_LATENCY_SECONDS)
            .record(lookup_start.elapsed().as_secs_f64());

        match &cache_entry {
            // Cache hit: Ready with sufficient rows — serve from cache
            Some(CachedQueryView {
                state: CachedQueryState::Ready,
                generation,
                resolved: Some(resolved),
                max_limit,
                ..
            }) if limit_is_sufficient(*max_limit, rows_needed) => {
                self.clock_reference_set(&fingerprint);
                self.worker_request_send(msg, Arc::clone(resolved), *generation)
            }

            // Cache hit: Ready but insufficient rows — forward and request limit bump
            Some(CachedQueryView {
                state: CachedQueryState::Ready,
                max_limit,
                ..
            }) => {
                trace!("limit bump {fingerprint} cached={max_limit:?} needed={rows_needed:?}");
                // Set Loading immediately to prevent duplicate LimitBump commands from racing
                self.cached_query_state_set(&fingerprint, CachedQueryState::Loading);
                reply_forward(msg.reply_tx, msg.pipeline, msg.data)?;
                self.query_tx
                    .send(QueryCommand::LimitBump {
                        fingerprint,
                        max_limit: rows_needed,
                    })
                    .map_err(|_| CacheError::WorkerSend)?;
                Ok(())
            }

            // Loading — forward to origin, don't re-register
            Some(CachedQueryView {
                state: CachedQueryState::Loading,
                ..
            }) => {
                trace!("cache loading {fingerprint}");
                reply_forward(msg.reply_tx, msg.pipeline, msg.data)
            }

            // Pending — hold request, check subsumption, increment hit count, admit if threshold reached
            Some(CachedQueryView {
                state: CachedQueryState::Pending(hit_count),
                ..
            }) => {
                let new_count = hit_count + 1;
                trace!("pending {fingerprint} count={new_count}");

                if new_count >= self.admission_threshold {
                    self.cached_query_state_set(&fingerprint, CachedQueryState::Loading);
                    self.subsumption_await(msg, fingerprint, AdmitAction::Admit)
                        .await
                } else {
                    self.cached_query_state_set(&fingerprint, CachedQueryState::Pending(new_count));
                    self.subsumption_await(msg, fingerprint, AdmitAction::CheckOnly)
                        .await
                }
            }

            // Invalidated — hold request, check subsumption, fast-readmit (skip admission gate)
            Some(CachedQueryView {
                state: CachedQueryState::Invalidated,
                ..
            }) => {
                trace!("invalidated readmit {fingerprint}");
                self.cached_query_state_set(&fingerprint, CachedQueryState::Loading);
                self.subsumption_await(msg, fingerprint, AdmitAction::Admit)
                    .await
            }

            // Cache miss — hold request, check subsumption
            None => {
                trace!("cache miss {fingerprint}");
                self.query_first_miss_handle(fingerprint, msg).await
            }
        }
    }

    /// Set the CLOCK reference bit for eviction tracking.
    fn clock_reference_set(&self, fingerprint: &u64) {
        if self.cache_policy == CachePolicy::Clock
            && let Ok(mut view) = self.state_view.write()
            && let Some(entry) = view.cached_queries.get_mut(fingerprint)
        {
            entry.referenced = true;
        }
    }

    /// Update a cached query's state in the shared view.
    fn cached_query_state_set(&self, fingerprint: &u64, state: CachedQueryState) {
        if let Ok(mut view) = self.state_view.write()
            && let Some(entry) = view.cached_queries.get_mut(fingerprint)
        {
            entry.state = state;
        }
    }

    /// Build and send a WorkerRequest for serving a query from cache.
    fn worker_request_send(
        &self,
        msg: QueryRequest,
        resolved: SharedResolved,
        generation: u64,
    ) -> CacheResult<()> {
        let mut timing = msg.timing;
        timing.lookup_complete_at = Some(Instant::now());

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

        self.worker_tx
            .send(WorkerRequest {
                query_type: msg.query_type,
                data: msg.data,
                resolved,
                generation,
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
            })
            .map_err(|e| {
                error!("worker send {e}");
                CacheError::WorkerSend.into()
            })
    }

    /// Register pinned queries at startup by sending Register commands with `pinned: true`.
    pub fn pinned_queries_register(&self, pinned: &[crate::cache::PinnedQuery]) -> CacheResult<()> {
        for pq in pinned {
            // Set Loading state in CacheStateView
            if let Ok(mut view) = self.state_view.write() {
                view.cached_queries.insert(
                    pq.fingerprint,
                    CachedQueryView {
                        state: CachedQueryState::Loading,
                        generation: 0,
                        resolved: None,
                        max_limit: None,
                        referenced: false,
                    },
                );
            }

            let (subsumption_tx, _subsumption_rx) = oneshot::channel();
            self.query_tx
                .send(QueryCommand::Register {
                    fingerprint: pq.fingerprint,
                    cacheable_query: Arc::clone(&pq.cacheable_query),
                    search_path: vec!["public".to_owned()],
                    started_at: Instant::now(),
                    subsumption_tx,
                    admit_action: AdmitAction::Admit,
                    pinned: true,
                })
                .map_err(|_| CacheError::WorkerSend)?;
        }
        Ok(())
    }

    /// Send a Register command to the writer thread with a subsumption oneshot.
    fn query_register_send(
        &self,
        fingerprint: u64,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Vec<String>,
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        admit_action: AdmitAction,
    ) -> CacheResult<()> {
        self.query_tx
            .send(QueryCommand::Register {
                fingerprint,
                cacheable_query,
                search_path,
                started_at: Instant::now(),
                subsumption_tx,
                admit_action,
                pinned: false,
            })
            .map_err(|_| CacheError::WorkerSend.into())
    }

    /// Hold a request, send Register with subsumption oneshot, and route
    /// based on the writer's response. Subsumed → serve from cache,
    /// NotSubsumed → forward to origin.
    async fn subsumption_await(
        &self,
        msg: QueryRequest,
        fingerprint: u64,
        admit_action: AdmitAction,
    ) -> CacheResult<()> {
        let (subsumption_tx, subsumption_rx) = oneshot::channel();

        self.query_register_send(
            fingerprint,
            Arc::clone(&msg.cacheable_query),
            msg.search_path.clone(),
            subsumption_tx,
            admit_action,
        )?;

        match subsumption_rx.await {
            Ok(SubsumptionResult::Subsumed {
                generation,
                resolved,
                ..
            }) => self.worker_request_send(msg, resolved, generation),
            Ok(SubsumptionResult::NotSubsumed) | Err(_) => {
                reply_forward(msg.reply_tx, msg.pipeline, msg.data)
            }
        }
    }

    /// Handle first cache miss: hold request, check subsumption.
    /// Register immediately (FIFO/threshold≤1) or start Pending.
    async fn query_first_miss_handle(
        &self,
        fingerprint: u64,
        msg: QueryRequest,
    ) -> CacheResult<()> {
        let immediate_admit =
            self.cache_policy == CachePolicy::Fifo || self.admission_threshold <= 1;

        let initial_state = if immediate_admit {
            CachedQueryState::Loading
        } else {
            CachedQueryState::Pending(1)
        };

        if let Ok(mut view) = self.state_view.write() {
            view.cached_queries.insert(
                fingerprint,
                CachedQueryView {
                    state: initial_state,
                    generation: 0,
                    resolved: None,
                    max_limit: None,
                    referenced: false,
                },
            );
        }

        if immediate_admit {
            trace!("send to writer {fingerprint}");
            self.subsumption_await(msg, fingerprint, AdmitAction::Admit)
                .await
        } else {
            trace!("pending new {fingerprint}");
            self.subsumption_await(msg, fingerprint, AdmitAction::CheckOnly)
                .await
        }
    }
}

/// Forward a query to origin by sending the reply through the oneshot channel.
fn reply_forward(
    reply_tx: oneshot::Sender<CacheReply>,
    pipeline: Option<PipelineContext>,
    data: BytesMut,
) -> CacheResult<()> {
    let buf = match pipeline {
        Some(pipeline) => pipeline.buffered_bytes,
        None => data,
    };
    reply_tx
        .send(CacheReply::Forward(buf))
        .map_err(|_| CacheError::Reply.into())
}

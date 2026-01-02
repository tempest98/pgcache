use std::sync::{Arc, RwLock};

use tokio::net::TcpStream;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::bytes::BytesMut;
use tracing::{error, instrument, trace};

use crate::query::ast::ast_query_fingerprint;
use crate::query::resolved::ResolvedSelectStatement;
use crate::settings::Settings;

use super::{
    CacheError,
    messages::{CacheReply, WriterCommand},
    query::CacheableQuery,
    types::{CacheStateView, CachedQueryState},
};

#[derive(Debug, PartialEq, Eq)]
pub enum QueryType {
    Simple,
    Extended,
}

#[derive(Debug)]
pub struct QueryRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub cacheable_query: Box<CacheableQuery>,
    pub result_formats: Vec<i16>,
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
    /// Resolved search_path for schema resolution
    pub search_path: Vec<String>,
}

/// Request sent to cache worker for executing cached queries.
/// Contains the resolved AST with schema-qualified table names.
#[derive(Debug)]
pub struct WorkerRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: ResolvedSelectStatement,
    /// Generation number for row tracking in pgcache_pgrx extension
    pub generation: u64,
    pub result_formats: Vec<i16>,
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
}

/// Query cache coordinator - routes queries and delegates writes to the writer thread.
#[derive(Debug, Clone)]
pub struct QueryCache {
    writer_tx: UnboundedSender<WriterCommand>,
    worker_tx: UnboundedSender<WorkerRequest>,
    state_view: Arc<RwLock<CacheStateView>>,
}

impl QueryCache {
    pub async fn new(
        _settings: &Settings,
        writer_tx: UnboundedSender<WriterCommand>,
        worker_tx: UnboundedSender<WorkerRequest>,
        state_view: Arc<RwLock<CacheStateView>>,
    ) -> Result<Self, CacheError> {
        Ok(Self {
            writer_tx,
            worker_tx,
            state_view,
        })
    }

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_dispatch(&mut self, msg: QueryRequest) -> Result<(), CacheError> {
        // Generate fingerprint from AST
        let stmt = msg.cacheable_query.statement();
        let fingerprint = ast_query_fingerprint(stmt);

        // Check cache state from shared view
        let cache_state = self
            .state_view
            .read()
            .ok()
            .and_then(|view| {
                view.cached_queries
                    .get(&fingerprint)
                    .map(|q| (q.state, q.resolved.clone(), q.generation))
            });

        if let Some((CachedQueryState::Ready, resolved, generation)) = cache_state {
            // Cache hit - send to worker
            let worker_request = WorkerRequest {
                query_type: msg.query_type,
                data: msg.data,
                resolved,
                generation,
                result_formats: msg.result_formats,
                client_socket: msg.client_socket,
                reply_tx: msg.reply_tx,
            };
            self.worker_tx.send(worker_request).map_err(|e| {
                error!("worker send {e}");
                CacheError::WorkerSend
            })
        } else {
            // Forward query to origin and load cache
            msg.reply_tx
                .send(CacheReply::Forward(msg.data))
                .await
                .map_err(|_| CacheError::Reply)?;

            // Only register if not already in cache (state was None, not Loading)
            if cache_state.is_none() {
                // Send QueryRegisterAndPopulate to writer and wait for response
                let (response_tx, response_rx) = oneshot::channel();
                self.writer_tx
                    .send(WriterCommand::QueryRegisterAndPopulate {
                        fingerprint,
                        cacheable_query: msg.cacheable_query,
                        search_path: msg.search_path,
                        response_tx,
                    })
                    .map_err(|_| CacheError::WorkerSend)?;

                // Wait for the writer to complete registration and population
                let result = response_rx.await.map_err(|_| CacheError::Reply)??;

                trace!(
                    "cached query ready, generation={}, tables={}",
                    result.generation,
                    result.relation_oids.len()
                );
            }

            Ok(())
        }
    }
}

use std::time::Instant;

use tokio::sync::oneshot;
use tokio_util::bytes::{Bytes, BytesMut};

use super::{CacheError, Report, query::CacheableQuery, query_cache::QueryType};
use crate::catalog::TableMetadata;
use crate::proxy::ClientSocket;
use crate::timing::QueryTiming;

/// Whether the pipeline includes a Describe and which type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDescribe {
    /// No Describe in the pipeline
    None,
    /// Describe('S') — worker should include ParameterDescription + RowDescription
    Statement,
    /// Describe('P') — worker should include RowDescription only
    Portal,
}

/// Pipeline context for atomic extended query dispatch.
/// Contains the raw Parse/Bind/Describe bytes buffered by the proxy,
/// used for origin fallback on cache miss.
pub struct PipelineContext {
    /// All buffered messages (Parse + Bind + optional Describe) concatenated.
    /// Forwarded to origin on cache miss (Forward reply).
    pub buffered_bytes: BytesMut,
    /// Whether the pipeline includes a Describe message.
    pub describe: PipelineDescribe,
    /// Stored ParameterDescription bytes for Describe('S') responses.
    pub parameter_description: Option<BytesMut>,
    /// Whether Parse was buffered in this pipeline.
    /// False for Bind-only pipelines (named statement re-execution without Parse).
    pub has_parse: bool,
    /// Whether Bind was buffered in this pipeline.
    /// False when Bind was flushed separately (e.g., JDBC Parse/Bind/Describe/Flush then Execute/Sync).
    pub has_bind: bool,
}

/// Converted query data ready for processing
pub struct QueryData {
    pub data: BytesMut,
    pub cacheable_query: Box<CacheableQuery>,
    pub query_type: QueryType,
    pub result_formats: Vec<i16>,
}

/// Parameters passed into an extended query
#[derive(Debug)]
pub struct QueryParameters {
    pub values: Vec<Option<Bytes>>,
    pub formats: Vec<i16>,
    pub oids: Vec<u32>,
}

impl QueryParameters {
    pub fn get(&self, index: usize) -> Option<QueryParameter> {
        let value = self.values.get(index)?;

        // Per the extended query protocol, format codes and OIDs may have fewer
        // entries than there are parameters:
        //   0 entries  → apply the default (text format / unspecified OID) to all
        //   1 entry    → apply that single value to all parameters
        //   N entries  → one entry per parameter
        let format = match self.formats.as_slice() {
            [] => 0,
            [single] => *single,
            codes => *codes.get(index)?,
        };
        let oid = match self.oids.as_slice() {
            [] => 0,
            [single] => *single,
            oids => *oids.get(index)?,
        };

        Some(QueryParameter {
            value: value.clone(),
            format,
            oid,
        })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Debug)]
pub struct QueryParameter {
    pub value: Option<Bytes>,
    pub format: i16,
    pub oid: u32,
}

/// Message types for communication between proxy and cache
#[derive(Debug)]
pub enum CacheMessage {
    Query(BytesMut, Box<CacheableQuery>),
    QueryParameterized(BytesMut, Box<CacheableQuery>, QueryParameters, Vec<i16>),
}

impl CacheMessage {
    /// Extracts the raw query data buffer, discarding the parsed query information.
    pub fn into_data(self) -> BytesMut {
        match self {
            CacheMessage::Query(data, _) | CacheMessage::QueryParameterized(data, _, _, _) => data,
        }
    }

    /// Converts the cache message into query data ready for processing.
    /// For parameterized queries, this performs parameter replacement in the AST.
    ///
    /// On error, returns the original data buffer so it can be forwarded to the origin.
    pub fn into_query_data(self) -> Result<QueryData, (Report<CacheError>, BytesMut)> {
        match self {
            CacheMessage::Query(data, cacheable_query) => Ok(QueryData {
                data,
                cacheable_query,
                query_type: QueryType::Simple,
                result_formats: Vec::new(),
            }),
            CacheMessage::QueryParameterized(
                data,
                mut cacheable_query,
                parameters,
                result_formats,
            ) => {
                // Replace parameters in AST
                if let Err(e) = cacheable_query.parameters_replace(&parameters) {
                    return Err((e.context_transform(CacheError::from), data));
                }
                Ok(QueryData {
                    data,
                    cacheable_query,
                    query_type: QueryType::Extended,
                    result_formats,
                })
            }
        }
    }
}

/// State of data stream processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataStreamState {
    Incomplete,
    Complete,
}

/// Reply messages sent from cache back to proxy
#[derive(Debug)]
pub enum CacheReply {
    /// Query completed successfully. Worker wrote the full response to the client.
    Complete(Option<QueryTiming>),
    /// Query should be forwarded to origin (cache miss or not cacheable).
    /// Contains buffered bytes for origin (or just execute_data if no pipeline).
    Forward(BytesMut),
    /// Query execution failed. Contains buffered bytes for origin fallback.
    Error(BytesMut),
}

/// Message from proxy containing query and connection details
pub struct ProxyMessage {
    pub message: CacheMessage,
    /// Socket for sending response data directly to the client
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    /// Resolved search_path for this connection (with $user expanded to session_user)
    pub search_path: Vec<String>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context for atomic extended query dispatch.
    /// None for simple queries and cold-path extended queries (no pipeline active).
    pub pipeline: Option<PipelineContext>,
}

/// Commands for query registration lifecycle, sent to the writer thread
#[derive(Debug)]
pub enum QueryCommand {
    /// Register a new query and spawn background population (fire-and-forget)
    Register {
        fingerprint: u64,
        cacheable_query: Box<CacheableQuery>,
        search_path: Vec<String>,
        started_at: Instant,
    },

    /// Query population completed successfully
    Ready {
        fingerprint: u64,
        cached_bytes: usize,
    },

    /// Query population failed
    Failed { fingerprint: u64 },

    /// Bump the max_limit for a cached query and re-populate with higher limit.
    /// Sent when an incoming query needs more rows than currently cached.
    LimitBump {
        fingerprint: u64,
        /// New max_limit value (None = unlimited)
        max_limit: Option<u64>,
    },
}

/// Commands for CDC mutations and relation tracking, sent to the writer thread
#[derive(Debug)]
pub enum CdcCommand {
    /// Register table metadata from CDC
    TableRegister(TableMetadata),

    /// CDC Insert operation
    Insert {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },

    /// CDC Update operation
    Update {
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        row_data: Vec<Option<String>>,
    },

    /// CDC Delete operation
    Delete {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },

    /// CDC Truncate operation
    Truncate { relation_oids: Vec<u32> },
}

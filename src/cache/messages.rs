use tokio::sync::{mpsc::Sender, oneshot};
use tokio_util::bytes::BytesMut;

use super::{CacheError, query::CacheableQuery, query_cache::QueryType};
use crate::catalog::TableMetadata;
use crate::proxy::ClientSocket;

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
    pub values: Vec<Option<Vec<u8>>>,
    pub formats: Vec<i16>,
    pub oids: Vec<u32>,
}

impl QueryParameters {
    pub fn get(&self, index: usize) -> Option<QueryParameter> {
        if let Some(value) = self.values.get(index)
            && let Some(&format) = self.formats.get(index)
            && let Some(&oid) = self.oids.get(index)
        {
            Some(QueryParameter {
                value: value.clone(),
                format,
                oid,
            })
        } else {
            None
        }
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
    pub value: Option<Vec<u8>>,
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
    /// Converts the cache message into query data ready for processing.
    /// For parameterized queries, this performs parameter replacement in the AST.
    ///
    /// On error, returns the original data buffer so it can be forwarded to the origin.
    pub fn into_query_data(self) -> Result<QueryData, (CacheError, BytesMut)> {
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
                    return Err((e.into(), data));
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
    /// Data chunk to write to client (cache worker sends multiple of these)
    Data(BytesMut),
    /// Query completed successfully (final message after all Data chunks)
    Complete(BytesMut),
    /// Query should be forwarded to origin (cache miss or not cacheable)
    Forward(BytesMut),
    /// Query execution failed
    Error(BytesMut),
}

/// CDC update message containing both key and row data
#[derive(Debug)]
pub struct CdcMessageUpdate {
    pub relation_oid: u32,
    pub key_data: Vec<Option<String>>,
    pub row_data: Vec<Option<String>>,
}

/// Messages for CDC (Change Data Capture) processing
#[derive(Debug)]
pub(crate) enum CdcMessage {
    Register(crate::catalog::TableMetadata),
    Insert(u32, Vec<Option<String>>),
    Update(CdcMessageUpdate),
    Delete(u32, Vec<Option<String>>),
    Truncate(Vec<u32>),
    RelationCheck(u32, oneshot::Sender<bool>),
}

/// Message from proxy containing query and connection details
pub struct ProxyMessage {
    pub message: CacheMessage,
    /// Socket for sending response data directly to the client
    pub client_socket: ClientSocket,
    pub reply_tx: Sender<CacheReply>,
    /// Resolved search_path for this connection (with $user expanded to session_user)
    pub search_path: Vec<String>,
}

/// Commands sent to the cache writer thread for serialized cache mutations
#[derive(Debug)]
pub enum WriterCommand {
    /// Register a new query and spawn background population (fire-and-forget)
    QueryRegister {
        fingerprint: u64,
        cacheable_query: Box<CacheableQuery>,
        search_path: Vec<String>,
    },

    /// Query population completed successfully
    QueryReady {
        fingerprint: u64,
        cached_bytes: usize,
    },

    /// Query population failed
    QueryFailed { fingerprint: u64 },

    /// Register table metadata from CDC
    TableRegister(TableMetadata),

    /// CDC Insert operation
    CdcInsert {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },

    /// CDC Update operation
    CdcUpdate {
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        row_data: Vec<Option<String>>,
    },

    /// CDC Delete operation
    CdcDelete {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },

    /// CDC Truncate operation
    CdcTruncate { relation_oids: Vec<u32> },

    /// Check if any cached queries reference a relation (for CDC filtering)
    RelationCheck {
        relation_oid: u32,
        response_tx: oneshot::Sender<bool>,
    },
}

use tokio::net::TcpStream;
use tokio::sync::{mpsc::Sender, oneshot};
use tokio_util::bytes::BytesMut;

use super::{CacheError, query::CacheableQuery, query_cache::QueryType};

/// Converted query data ready for processing
pub struct QueryData {
    pub data: BytesMut,
    pub cacheable_query: Box<CacheableQuery>,
    pub query_type: QueryType,
}

/// Message types for communication between proxy and cache
#[derive(Debug)]
pub enum CacheMessage {
    Query(BytesMut, Box<CacheableQuery>),
    QueryParameterized(BytesMut, Box<CacheableQuery>, Vec<Option<Vec<u8>>>),
}

impl CacheMessage {
    /// Converts the cache message into query data ready for processing.
    /// For parameterized queries, this performs parameter replacement in the AST.
    pub fn into_query_data(self) -> Result<QueryData, CacheError> {
        match self {
            CacheMessage::Query(data, cacheable_query) => Ok(QueryData {
                data,
                cacheable_query,
                query_type: QueryType::Simple,
            }),
            CacheMessage::QueryParameterized(data, mut cacheable_query, parameters) => {
                // Replace parameters in AST
                cacheable_query.parameters_replace(&parameters)?;
                Ok(QueryData {
                    data,
                    cacheable_query,
                    query_type: QueryType::Extended,
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
    Complete(BytesMut),
    Forward(BytesMut),
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
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
}

/// Unified stream source for multiplexing proxy and CDC messages
pub(crate) enum StreamSource {
    Proxy(ProxyMessage),
    Cdc(CdcMessage),
}

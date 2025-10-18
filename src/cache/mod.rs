use std::{io, thread};

use error_set::error_set;
use iddqd::{BiHashMap, IdHashItem, IdHashMap, id_upcast};
use tokio::{
    net::TcpStream,
    runtime::Builder,
    sync::{
        mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::{LocalSet, spawn_local},
};
use tokio_postgres::Error;
use tokio_stream::{
    StreamExt,
    wrappers::{ReceiverStream, UnboundedReceiverStream},
};
use tokio_util::bytes::BytesMut;
use tracing::{debug, error, instrument};

use crate::cache::worker::CacheWorker;
use crate::{
    cache::cdc::CdcProcessor,
    cache::query::CacheableQuery,
    catalog::TableMetadata,
    query::ast::SelectStatement,
    query::constraints::QueryConstraints,
    query::resolved::{ResolveError, ResolvedSelectStatement},
    query::transform::AstTransformError,
};
use crate::{
    cache::query_cache::{QueryCache, QueryRequest},
    settings::Settings,
};

mod cdc;
pub(crate) mod query;
mod query_cache;
mod worker;

error_set! {
    CacheError := WriteError || ReadError || DbError || ParseError || TableError || SendError || QueryResolutionError

    ReadError := {
        IoError(io::Error),
        InvalidMessage,
    }

    DbError := {
        NoConnection,
        PgError(Error),
        CdcFailure,
        TooManyModifiedRows,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error),
        AstTransformError(AstTransformError),
        Other,
    }

    SendError := {
        WorkerSend,
        Reply,
    }

    WriteError := {
        Write,
    }


    TableError := {
        #[display("Oid: {oid:?} Name {name:?}")]
        UnknownTable {
            oid: Option<u32>,
            name: Option<String>,
        },
        UnknownColumn,
        UnknownSchema,
        NoPrimaryKey,
    }

    QueryResolutionError := {
        ResolveError(ResolveError),
    }
}

#[derive(Debug)]
pub enum CacheMessage {
    Query(BytesMut, Box<CacheableQuery>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataStreamState {
    Incomplete,
    Complete,
}

#[derive(Debug)]
pub enum CacheReply {
    Forward(BytesMut),
    Error(BytesMut),
}

pub struct CdcMessageUpdate {
    relation_oid: u32,
    key_data: Vec<Option<String>>,
    row_data: Vec<Option<String>>,
}

enum CdcMessage {
    Register(TableMetadata),
    Insert(u32, Vec<Option<String>>),
    Update(CdcMessageUpdate),
    Delete(u32, Vec<Option<String>>),
    Truncate(Vec<u32>),
    RelationCheck(u32, oneshot::Sender<bool>),
}

pub struct ProxyMessage {
    pub message: CacheMessage,
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
}

enum StreamSource {
    Proxy(ProxyMessage),
    Cdc(CdcMessage),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedQueryState {
    Ready,
    Loading,
}

#[derive(Debug)]
pub struct CachedQuery {
    pub state: CachedQueryState,
    pub fingerprint: u64,
    pub relation_oids: Vec<u32>,
    pub select_statement: SelectStatement,
    pub resolved: ResolvedSelectStatement,
    pub constraints: QueryConstraints,
}

impl IdHashItem for CachedQuery {
    type Key<'a> = u64;

    fn key(&self) -> Self::Key<'_> {
        self.fingerprint
    }

    id_upcast!();
}

#[derive(Debug, Clone)]
pub struct UpdateQuery {
    pub fingerprint: u64, //fingerprint of cached query that generated this update query
    pub query: SelectStatement,
}

#[derive(Debug)]
pub struct UpdateQueries {
    pub relation_oid: u32,
    pub queries: Vec<UpdateQuery>,
}

impl IdHashItem for UpdateQueries {
    type Key<'a> = u32;

    fn key(&self) -> Self::Key<'_> {
        self.relation_oid
    }

    id_upcast!();
}

#[derive(Debug)]
pub struct Cache {
    pub tables: BiHashMap<TableMetadata>,
    pub update_queries: IdHashMap<UpdateQueries>,
    pub cached_queries: IdHashMap<CachedQuery>,
}

#[instrument(skip_all)]
pub fn cache_run(settings: &Settings, cache_rx: Receiver<ProxyMessage>) -> Result<(), CacheError> {
    thread::scope(|scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;
        let cache = Cache {
            tables: BiHashMap::new(),
            update_queries: IdHashMap::new(),
            cached_queries: IdHashMap::new(),
        };

        let (worker_tx, worker_rx) = mpsc::unbounded_channel();
        let _worker_handle = thread::Builder::new()
            .name("cache worker".to_owned())
            .spawn_scoped(scope, || worker_run(settings, worker_rx))?;

        // let (writer_tx, writer_rx) = mpsc::unbounded_channel();
        // let writer_handle = thread::Builder::new()
        //     .name("cache writer".to_owned())
        //     .spawn_scoped(scope, || writer_run(&settings, writer_rx))?;

        let (cdc_tx, cdc_rx) = mpsc::unbounded_channel();
        let cdc_handle = thread::Builder::new()
            .name("cdc worker".to_owned())
            .spawn_scoped(scope, move || cdc_run(settings, cdc_tx))?;

        let cache_rx_mapped = ReceiverStream::new(cache_rx).map(StreamSource::Proxy);
        let cdc_rx_mapped = UnboundedReceiverStream::new(cdc_rx).map(StreamSource::Cdc);

        let mut stream = cache_rx_mapped.merge(cdc_rx_mapped);

        debug!("cache loop");
        rt.block_on(async {
            let qcache = QueryCache::new(settings, cache, worker_tx).await?;

            LocalSet::new()
                .run_until(async move {
                    while let Some(src) = stream.next().await {
                        let mut qcache = qcache.clone();
                        spawn_local(async move {
                            match src {
                                StreamSource::Proxy(proxy_msg) => match proxy_msg.message {
                                    CacheMessage::Query(data, cacheable_query) => {
                                        let msg = QueryRequest {
                                            data,
                                            cacheable_query,
                                            client_socket: proxy_msg.client_socket,
                                            reply_tx: proxy_msg.reply_tx,
                                        };
                                        match qcache.query_dispatch(msg).await {
                                            Ok(_) => (),
                                            Err(e) => {
                                                error!("query_dispatch error {e}");
                                            }
                                        }
                                    }
                                },
                                StreamSource::Cdc(msg) => match msg {
                                    CdcMessage::Register(table_metadata) => {
                                        let _ = qcache.cache_table_register(table_metadata).await;
                                    }
                                    CdcMessage::Insert(relation_oid, row_data) => {
                                        let _ = qcache.handle_insert(relation_oid, row_data).await;
                                    }
                                    CdcMessage::Update(update) => {
                                        let _ = qcache
                                            .handle_update(
                                                update.relation_oid,
                                                update.key_data,
                                                update.row_data,
                                            )
                                            .await;
                                    }
                                    CdcMessage::Delete(relation_oid, row_data) => {
                                        let _ = qcache.handle_delete(relation_oid, row_data).await;
                                    }
                                    CdcMessage::Truncate(relation_oids) => {
                                        let _ = qcache.handle_truncate(&relation_oids).await;
                                    }
                                    CdcMessage::RelationCheck(relation_oid, reply_tx) => {
                                        let exists =
                                            qcache.cached_queries_exist(relation_oid).await;
                                        let _ = reply_tx.send(exists);
                                    }
                                },
                            }
                        });

                        if cdc_handle.is_finished() {
                            return Err(CacheError::CdcFailure);
                        }
                    }

                    Ok(())
                })
                .await
        })
    })
}

fn worker_run(
    settings: &Settings,
    mut worker_rx: UnboundedReceiver<QueryRequest>,
) -> Result<(), CacheError> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    debug!("worker loop");
    rt.block_on(async {
        let worker = CacheWorker::new(settings).await?;

        LocalSet::new()
            .run_until(async move {
                while let Some(mut msg) = worker_rx.recv().await {
                    let worker = worker.clone();
                    spawn_local(async move {
                        debug!("cache worker task spawn");
                        match worker.handle_cached_query(&mut msg).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!("handle_cached_query failed {e}");
                                if msg
                                    .reply_tx
                                    .send(CacheReply::Error(msg.data))
                                    .await
                                    .is_err()
                                {
                                    error!("no receiver");
                                }
                            }
                        }
                        debug!("cache worker task done");
                    });
                }

                Ok(())
            })
            .await
    })
}

fn cdc_run(settings: &Settings, cdc_tx: UnboundedSender<CdcMessage>) -> Result<(), CacheError> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    debug!("cdc loop");
    rt.block_on(async {
        let mut cdc = CdcProcessor::new(settings, cdc_tx).await?;
        if let Err(e) = cdc.run().await {
            error!("cdc.run() failed {e}");
        }

        Err(CacheError::CdcFailure)
    })
}

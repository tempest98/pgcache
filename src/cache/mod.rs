use std::collections::HashMap;
use std::{io, sync::Arc, thread};

use error_set::error_set;
use iddqd::{BiHashItem, BiHashMap, IdHashItem, IdHashMap, bi_upcast, id_upcast};
use pg_query::ParseResult;
use tokio::sync::RwLock;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, Receiver, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::{LocalSet, spawn_local},
};
use tokio_postgres::{Error, types::Type};
use tokio_stream::{
    StreamExt,
    wrappers::{ReceiverStream, UnboundedReceiverStream},
};
use tokio_util::bytes::BytesMut;
use tracing::{debug, error, instrument};

use crate::cache::cdc::CdcProcessor;
use crate::cache::worker::CacheWorker;
use crate::{
    cache::query_cache::{QueryCache, QueryRequest},
    query::parse::*,
    settings::Settings,
};

mod cdc;
pub(crate) mod query;
mod query_cache;
mod worker;

error_set! {
    CacheError = ReadError || DbError || ParseError || TableError || SendError;

    ReadError = {
        IoError(io::Error),
        InvalidMessage,
    };

    DbError = {
        NoConnection,
        PgError(Error),
        CdcFailure,
    };

    ParseError = {
        InvalidUtf8,
        Parse(pg_query::Error),
        Other(),
    };

    SendError = {
        WorkerSend,
        Reply,
    };

    TableError = {
        UnknownTable,
        UnknownColumn,
    };
}

#[derive(Debug)]
pub enum CacheMessage {
    Query(BytesMut, ParseResult),
}

#[derive(Debug)]
pub enum CacheReply {
    Data(BytesMut),
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
}

enum StreamSource {
    Proxy((CacheMessage, oneshot::Sender<CacheReply>)),
    Cdc(CdcMessage),
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub relation_oid: u32,
    pub name: String,
    pub schema: String,
    pub primary_key_columns: Vec<String>,
    pub columns: HashMap<String, ColumnMetadata>,
    // pub last_updated: std::time::SystemTime,
    // pub estimated_row_count: Option<i64>,
}

impl BiHashItem for TableMetadata {
    type K1<'a> = u32;
    type K2<'a> = &'a str;

    fn key1(&self) -> Self::K1<'_> {
        self.relation_oid
    }

    fn key2(&self) -> Self::K2<'_> {
        self.name.as_str()
    }

    bi_upcast!();
}

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub position: i16,
    pub type_oid: u32,
    pub data_type: Type,
    pub type_name: String,
    pub is_primary_key: bool,
    // pub is_nullable: bool,
    // pub max_length: Option<i32>,
    // pub default_value: Option<String>,
}

impl BiHashItem for ColumnMetadata {
    type K1<'a> = i16;
    type K2<'a> = &'a str;

    fn key1(&self) -> Self::K1<'_> {
        self.position
    }

    fn key2(&self) -> Self::K2<'_> {
        self.name.as_str()
    }

    bi_upcast!();
}

#[derive(Debug, Clone)]
pub struct CachedQuery {
    pub fingerprint: u64,
    pub table_name: String,
    pub relation_oid: u32,
    pub filter_expr: Option<WhereExpr>,
}

impl IdHashItem for CachedQuery {
    type Key<'a> = u64;

    fn key(&self) -> Self::Key<'_> {
        self.fingerprint
    }

    id_upcast!();
}

#[derive(Debug, Clone)]
pub struct Cache {
    pub tables: Arc<RwLock<BiHashMap<TableMetadata>>>,
    pub queries: Arc<RwLock<IdHashMap<CachedQuery>>>,
}

#[instrument]
pub fn cache_run(
    settings: &Settings,
    cache_rx: Receiver<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<(), CacheError> {
    thread::scope(|scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;
        let cache = Cache {
            tables: Arc::new(RwLock::new(BiHashMap::new())),
            queries: Arc::new(RwLock::new(IdHashMap::new())),
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
        let cache_clone = cache.clone();
        let cdc_handle = thread::Builder::new()
            .name("cdc worker".to_owned())
            .spawn_scoped(scope, move || cdc_run(settings, cache_clone, cdc_tx))?;

        let cache_rx_mapped = ReceiverStream::new(cache_rx).map(StreamSource::Proxy);
        let cdc_rx_mapped = UnboundedReceiverStream::new(cdc_rx).map(StreamSource::Cdc);

        let mut stream = cache_rx_mapped.merge(cdc_rx_mapped);

        debug!("cache loop");
        rt.block_on(async {
            let qcache = QueryCache::new(settings, cache.clone(), worker_tx).await?;

            LocalSet::new()
                .run_until(async move {
                    while let Some(src) = stream.next().await {
                        let mut qcache = qcache.clone();
                        spawn_local(async move {
                            match src {
                                StreamSource::Proxy((msg, reply_tx)) => match msg {
                                    CacheMessage::Query(data, ast) => {
                                        let msg = QueryRequest {
                                            data,
                                            ast,
                                            reply_tx,
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
                while let Some(msg) = worker_rx.recv().await {
                    let worker = worker.clone();
                    spawn_local(async move {
                        debug!("cache worker task spawn");
                        match worker.handle_cached_query(&msg.data, &msg.ast).await {
                            Ok(buf) => {
                                if msg.reply_tx.send(CacheReply::Data(buf)).is_err() {
                                    error!("no receiver");
                                }
                            }
                            Err(e) => {
                                error!("handle_cached_query failed {e}");
                                if msg.reply_tx.send(CacheReply::Error(msg.data)).is_err() {
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

fn cdc_run(
    settings: &Settings,
    cache: Cache,
    cdc_tx: UnboundedSender<CdcMessage>,
) -> Result<(), CacheError> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    debug!("cdc loop");
    rt.block_on(async {
        let mut cdc = CdcProcessor::new(settings, cache, cdc_tx).await?;
        if let Err(e) = cdc.run().await {
            error!("cdc.run() failed {e}");
        }

        Err(CacheError::CdcFailure)
    })
}

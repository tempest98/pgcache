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
use tokio_postgres::{Error, SimpleColumn, SimpleQueryRow, types::Type};
use tokio_stream::{
    StreamExt,
    wrappers::{ReceiverStream, UnboundedReceiverStream},
};
use tokio_util::bytes::{BufMut, BytesMut};
use tracing::{debug, error, instrument};

use crate::cache::cdc::CdcProcessor;
use crate::query::evaluate::where_expr_evaluate;
use crate::{
    cache::query_cache::{CacheWorker, QueryCache, QueryRequest},
    pg::protocol::backend::{
        COMMAND_COMPLETE_TAG, DATA_ROW_TAG, READY_FOR_QUERY_TAG, ROW_DESCRIPTION_TAG,
    },
    query::parse::*,
    settings::Settings,
};

mod cdc;
mod query_cache;

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

fn row_description_encode(desc: &Arc<[SimpleColumn]>, buf: &mut BytesMut) {
    let cnt = desc.len() as i16;
    let string_len = desc.iter().fold(0, |acc, col| acc + col.name().len() + 1);

    buf.put_u8(ROW_DESCRIPTION_TAG);
    buf.put_i32(6 + (18 * cnt as i32) + string_len as i32);
    buf.put_i16(cnt);
    for col in desc.iter() {
        buf.put_slice(col.name().as_bytes());
        buf.put_u8(0);
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i32(0);
        buf.put_i16(-1);
        buf.put_i32(-1);
        buf.put_i16(0);
    }
}

fn simple_query_row_encode(row: &SimpleQueryRow, buf: &mut BytesMut) {
    let cnt = row.len() as i16;
    let mut value_len = 0;
    for i in 0..cnt {
        let value = row.get(i as usize).unwrap_or_default();
        value_len += value.len();
    }

    buf.put_u8(DATA_ROW_TAG);
    buf.put_i32(6 + (4 * cnt as i32) + value_len as i32);
    buf.put_i16(cnt);
    for i in 0..cnt {
        let data = row.get(i as usize).unwrap_or_default().as_bytes();
        buf.put_i32(data.len() as i32);
        buf.put_slice(data);
    }
}

fn command_complete_encode(cnt: u64, buf: &mut BytesMut) {
    let msg = format!("SELECT {cnt}");

    buf.put_u8(COMMAND_COMPLETE_TAG);
    buf.put_i32((4 + msg.len() + 1) as i32);
    buf.put_slice(msg.as_bytes());
    buf.put_u8(0);
}

fn ready_for_query_encode(buf: &mut BytesMut) {
    buf.put_u8(READY_FOR_QUERY_TAG);
    buf.put_i32(5);
    buf.put_u8(b'I');
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
            let mut qcache = QueryCache::new(settings, cache.clone(), worker_tx).await?;

            while let Some(src) = stream.next().await {
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

                if cdc_handle.is_finished() {
                    return Err(CacheError::CdcFailure);
                }
            }

            Ok(())
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

pub fn is_cacheable(ast: &ParseResult) -> bool {
    ast.statement_types().contains(&"SelectStmt")
        && ast.select_tables().len() == 1
        && !query_select_has_sublink(ast)
        && has_cacheable_where_clause(ast)
}

/// Check if the WHERE clause can be efficiently cached.
/// Currently supports: simple equality, AND of equalities, OR of equalities.
fn has_cacheable_where_clause(ast: &ParseResult) -> bool {
    match query_where_clause_parse(ast) {
        Ok(Some(expr)) => is_cacheable_expr(&expr),
        Ok(None) => true, // No WHERE clause is always cacheable
        Err(_) => false,  // Can't parse WHERE clause, not cacheable
    }
}

/// Determine if a WHERE expression can be efficiently cached.
/// Step 2: Support simple equality, AND of equalities, OR of equalities.
fn is_cacheable_expr(expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Binary(binary_expr) => {
            match binary_expr.op {
                WhereOp::Equal => {
                    // Simple equality: column = value
                    is_simple_equality(binary_expr)
                }
                WhereOp::And => {
                    // AND: both sides must be cacheable
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                WhereOp::Or => {
                    // OR: both sides must be cacheable
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                _ => false, // Other operators not supported yet
            }
        }
        _ => false, // Other expression types not supported yet
    }
}

/// Check if a binary expression is a simple equality (column = value).
fn is_simple_equality(binary_expr: &BinaryExpr) -> bool {
    matches!(
        (binary_expr.lexpr.as_ref(), binary_expr.rexpr.as_ref()),
        (WhereExpr::Column(_), WhereExpr::Value(_)) | (WhereExpr::Value(_), WhereExpr::Column(_))
    )
}

/// Check if a row matches the filter conditions of a cached query.
fn cache_query_row_matches(
    query: &CachedQuery,
    row_data: &[Option<String>],
    table_metadata: &TableMetadata,
) -> bool {
    // Guard clause: if no filter expression, all rows match
    // dbg!(&query.filter_expr);
    // dbg!(&row_data);
    // dbg!(&table_metadata);

    match &query.filter_expr {
        Some(expr) => where_expr_evaluate(expr, row_data, table_metadata),
        None => true, // No filter means all rows match
    }
}

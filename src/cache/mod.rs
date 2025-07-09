use std::{io, sync::Arc, thread};

use error_set::error_set;
use pg_query::ParseResult;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, Receiver, UnboundedReceiver},
        oneshot,
    },
    task::{LocalSet, spawn_local},
};
use tokio_postgres::{Error, SimpleColumn, SimpleQueryRow};
use tokio_util::bytes::{BufMut, BytesMut};
use tracing::{debug, error, instrument};

use crate::{
    cache::query_cache::{CacheWorker, QueryCache, QueryRequest},
    pg::protocol::backend::{
        COMMAND_COMPLETE_TAG, DATA_ROW_TAG, READY_FOR_QUERY_TAG, ROW_DESCRIPTION_TAG,
    },
    query::parse::*,
    settings::Settings,
};

mod query_cache;

error_set! {
    CacheError = ReadError || DbError || ParseError || TableError || SendError;

    ReadError = {
        IoError(io::Error),
        InvalidMessage,
    };

    DbError = {
        NoConnection,
        PgError(Error)
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
    mut cache_rx: Receiver<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<(), CacheError> {
    thread::scope(|scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;

        let (worker_tx, worker_rx) = mpsc::unbounded_channel();
        // let (writer_tx, writer_rx) = mpsc::unbounded_channel();

        let _worker_handle = thread::Builder::new()
            .name("cache worker".to_owned())
            .spawn_scoped(scope, || worker_run(settings, worker_rx))?;

        // let writer_handle = thread::Builder::new()
        //     .name("cache writer".to_owned())
        //     .spawn_scoped(scope, || writer_run(&settings, writer_rx))?;

        debug!("cache loop");
        rt.block_on(async {
            let mut cache = QueryCache::new(settings, worker_tx).await?;
            while let Some((msg, reply_tx)) = cache_rx.recv().await {
                match msg {
                    CacheMessage::Query(data, ast) => {
                        let msg = QueryRequest {
                            data,
                            ast,
                            reply_tx,
                        };
                        match cache.query_dispatch(msg).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!("query_dispatch error {e}");
                            }
                        }
                    }
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

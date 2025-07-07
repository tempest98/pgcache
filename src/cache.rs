use std::{io, sync::Arc, thread};

use crate::{
    pg::protocol::backend::{
        COMMAND_COMPLETE_TAG, DATA_ROW_TAG, READY_FOR_QUERY_TAG, ROW_DESCRIPTION_TAG,
    },
    query::parse::*,
    settings::Settings,
};

use error_set::error_set;
use pg_query::ParseResult;
use tokio::{
    runtime::Builder,
    sync::{mpsc::Receiver, oneshot},
};
use tokio_postgres::{
    Client, Config, Error, NoTls, SimpleColumn, SimpleQueryMessage, SimpleQueryRow,
};
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tracing::{debug, error, instrument};

error_set! {
    CacheError = ConnectError || ReadError || ParseError;

    ReadError = {
        IoError(io::Error),
        InvalidMessage,
    };

    ConnectError = {
        NoConnection,
        PgError(Error)
    };

    ParseError = {
        InvalidUtf8,
        Parse(pg_query::Error)
    };

}

#[derive(Debug)]
pub enum CacheMessage {
    Query(BytesMut, ParseResult),
}

#[derive(Debug)]
pub enum CacheReply {
    Data(BytesMut),
}

struct QueryCache {
    db_cache: Client,
    db_origin: Client,
}

impl QueryCache {
    async fn new(settings: &Settings) -> Result<Self, CacheError> {
        let (cache_client, cache_connection) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await?;

        let (origin_client, origin_connection) = Config::new()
            .host(&settings.origin.host)
            .port(settings.origin.port)
            .user(&settings.origin.user)
            .dbname(&settings.origin.database)
            .connect(NoTls)
            .await?;

        //task to process connection to cache pg db
        tokio::spawn(async move {
            if let Err(e) = cache_connection.await {
                eprintln!("connection error: {e}");
            }
        });

        //task to process connection to origin pg db
        tokio::spawn(async move {
            if let Err(e) = origin_connection.await {
                eprintln!("connection error: {e}");
            }
        });

        Ok(Self {
            db_cache: cache_client,
            db_origin: origin_client,
        })
    }

    async fn handle_query(
        &self,
        data: BytesMut,
        ast: &ParseResult,
    ) -> Result<BytesMut, CacheError> {
        // todo check for cache hit and store data in cache on miss
        // just run the query and return the results for now

        let cache_hit = false;
        // let cache_hit = self
        //     .query_cache_check(&ast)
        //     .await
        //     .is_ok_and(|is_cached| is_cached);

        let query_target = if cache_hit {
            &self.db_cache
        } else {
            &self.db_origin
        };

        let msg_len = (&data[1..5]).get_u32() as usize;
        let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;
        // let stmt = query_target.prepare(query).await.unwrap();
        let res = query_target.simple_query(query).await?;
        // let res = query_target.query(query, &[]).await;
        // dbg!(&res);

        let mut buf = BytesMut::new();
        let SimpleQueryMessage::RowDescription(desc) = &res[0] else {
            return Err(CacheError::InvalidMessage);
        };
        row_description_encode(desc, &mut buf);
        for msg in &res[1..] {
            match msg {
                SimpleQueryMessage::Row(row) => {
                    simple_query_row_encode(row, &mut buf);
                }
                SimpleQueryMessage::CommandComplete(cnt) => {
                    command_complete_encode(*cnt, &mut buf);
                }
                SimpleQueryMessage::RowDescription(_) => return Err(CacheError::InvalidMessage),
                _ => return Err(CacheError::InvalidMessage),
            }
        }
        ready_for_query_encode(&mut buf);

        // if !cache_hit {
        //     // Create cache table and store results
        //     self.query_register(self.origin.clone(), &ast).await?;

        //     if let Ok(rows) = &res {
        //         self.query_cache_results(&ast.select_tables()[0], rows)
        //             .await?;
        //     }
        // }

        Ok(buf)
    }
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
    thread::scope(|_scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;

        debug!("cache loop");
        rt.block_on(async {
            let cache = QueryCache::new(settings).await?;

            while let Some((msg, reply_tx)) = cache_rx.recv().await {
                match msg {
                    CacheMessage::Query(data, ast) => {
                        let buf = cache.handle_query(data, &ast).await?;
                        // todo check for cache hit and store data in cache on miss
                        // just run the query and return the results for now

                        if reply_tx.send(CacheReply::Data(buf)).is_err() {
                            error!("no receiver");
                        }
                    }
                }
            }
            Ok(())
        })
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

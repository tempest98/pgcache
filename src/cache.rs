use std::{io, thread};

use crate::{query::parse::*, settings::Settings};

use error_set::error_set;
use pg_query::ParseResult;
use tokio::{
    runtime::Builder,
    sync::{mpsc::Receiver, oneshot},
};
use tracing::{debug, error, instrument};

error_set! {
    ConnectionError = ConnectError || ReadError;

    ReadError = {
        IoError(io::Error),
    };

    ConnectError = {
        NoConnection,
    };
}

#[derive(Debug)]
pub enum CacheMessage {
    Query(ParseResult),
}

#[derive(Debug)]
pub enum CacheReply {
    CacheMiss(ParseResult),
}

#[instrument]
pub fn cache_run(
    settings: &Settings,
    mut cache_rx: Receiver<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<(), ConnectionError> {
    thread::scope(|_scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;

        debug!("cache loop");
        rt.block_on(async {
            while let Some((msg, reply_tx)) = cache_rx.recv().await {
                match msg {
                    CacheMessage::Query(ast) => {
                        // todo check for cache hit and store data in cache on miss
                        // just run the query and return the results for now
                        if reply_tx.send(CacheReply::CacheMiss(ast)).is_err() {
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

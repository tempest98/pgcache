use crate::query::{
    ast::{ExprOp, SelectStatement, SqlQuery, Statement, WhereExpr},
    evaluate::{is_simple_comparison, where_expr_evaluate},
};

use super::*;

pub fn is_cacheable_ast(sql_query: &SqlQuery) -> Option<&SelectStatement> {
    match &sql_query.statement {
        Statement::Select(select) => {
            if select.is_single_table() && !select.has_sublink() && is_cacheable_select(select) {
                Some(select)
            } else {
                None
            }
        } // _ => None, // Only SELECT statements are cacheable
    }
}

/// Check if a SELECT statement can be efficiently cached.
/// Currently supports: simple equality, AND of equalities, OR of equalities.
fn is_cacheable_select(select: &SelectStatement) -> bool {
    match &select.where_clause {
        Some(where_expr) => is_cacheable_expr(where_expr),
        None => true, // No WHERE clause is always cacheable
    }
}

/// Determine if a WHERE expression can be efficiently cached.
/// Step 2: Support simple equality, AND of equalities, OR of equalities.
fn is_cacheable_expr(expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Binary(binary_expr) => {
            match binary_expr.op {
                ExprOp::Equal
                | ExprOp::NotEqual
                | ExprOp::LessThan
                | ExprOp::LessThanOrEqual
                | ExprOp::GreaterThan
                | ExprOp::GreaterThanOrEqual => {
                    // Simple comparison: column op value
                    is_simple_comparison(binary_expr)
                }
                ExprOp::And => {
                    // AND: both sides must be cacheable
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                ExprOp::Or => {
                    // OR: both sides must be cacheable
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                _ => false, // Other operators not supported yet
            }
        }
        _ => false, // Other expression types not supported yet
    }
}

/// Check if a row matches the filter conditions of a cached query.
pub fn cache_query_row_matches(
    query: &CachedQuery,
    row_data: &[Option<String>],
    table_metadata: &TableMetadata,
) -> bool {
    match &query.select_statement.where_clause {
        Some(expr) => where_expr_evaluate(expr, row_data, table_metadata),
        None => true, // No filter means all rows match
    }
}

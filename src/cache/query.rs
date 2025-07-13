use pg_query::ParseResult;

use crate::query::{
    evaluate::{is_simple_equality, where_expr_evaluate},
    parse::{WhereExpr, WhereOp, query_select_has_sublink, query_where_clause_parse},
};

use super::*;

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

/// Check if a row matches the filter conditions of a cached query.
pub fn cache_query_row_matches(
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

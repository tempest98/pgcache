use crate::query::ast::SelectColumns;
use crate::query::{
    ast::{SelectStatement, SqlQuery, Statement, WhereExpr, WhereOp},
    evaluate::{is_simple_comparison, where_expr_evaluate},
};

use super::*;

pub fn is_cacheable_ast(sql_query: &SqlQuery) -> bool {
    match &sql_query.statement {
        Statement::Select(select) => {
            // Must be single table
            select.is_single_table() && !select.has_sublink() && is_cacheable_select(select)
        } // _ => false, // Only SELECT statements are cacheable
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
                WhereOp::Equal
                | WhereOp::NotEqual
                | WhereOp::LessThan
                | WhereOp::LessThanOrEqual
                | WhereOp::GreaterThan
                | WhereOp::GreaterThanOrEqual => {
                    // Simple comparison: column op value
                    is_simple_comparison(binary_expr)
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
    // Get the WHERE clause from the SQL query AST
    let where_clause = match &query.sql_query.statement {
        Statement::Select(select) => &select.where_clause,
        // _ => return false, // Non-SELECT queries don't match rows
    };

    match where_clause {
        Some(expr) => where_expr_evaluate(expr, row_data, table_metadata),
        None => true, // No filter means all rows match
    }
}

pub fn query_select_replace(ast: &SqlQuery) -> SqlQuery {
    // Only replace if this is a SELECT statement at the top level
    // (not an INSERT, UPDATE, etc. that contains an embedded SELECT)

    if !ast.is_single_table() {
        // Return original if not a pure SELECT statement
        return ast.clone();
    }

    let mut new_ast = ast.clone();
    let Statement::Select(select) = &mut new_ast.statement;
    select.columns = SelectColumns::All;

    new_ast
}

#[cfg(test)]
mod tests {
    use crate::query::ast::{Deparse, sql_query_convert};

    use super::*;

    #[test]
    fn test_query_select_replace() {
        // Test replacing specific columns with SELECT *
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let result = query_select_replace(&sql_query);

        // Deparse to verify the transformation
        let mut buf = String::new();
        let new_sql = result.deparse(&mut buf);

        // Should now be SELECT * FROM users WHERE id = 1
        assert!(
            new_sql.contains("SELECT *"),
            "Query should contain SELECT *"
        );
        assert!(
            new_sql.contains("FROM users"),
            "Query should preserve FROM clause"
        );
        assert!(
            new_sql.contains("WHERE id = 1"),
            "Query should preserve WHERE clause"
        );
        assert!(
            !new_sql.contains("id, name, email"),
            "Query should not contain original columns"
        );
    }

    #[test]
    fn test_query_select_replace_with_complex_where() {
        // Test with more complex query
        let original_query =
            "SELECT a.id, b.data FROM table_a a WHERE a.status = 'active' AND b.enabled = true";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let result = query_select_replace(&sql_query);

        // Deparse to verify the transformation
        let mut buf = String::new();
        let new_sql = result.deparse(&mut buf);

        // Should now contain SELECT *
        assert!(
            new_sql.contains("SELECT *"),
            "Query should contain SELECT *"
        );

        // Verify FROM clause is preserved
        assert!(
            new_sql.contains("FROM table_a a"),
            "Query should preserve FROM clause with alias: {new_sql}"
        );

        // Verify complete WHERE clause is preserved
        assert!(
            new_sql.contains("WHERE a.status = 'active' AND b.enabled = true"),
            "Query should preserve complete WHERE clause: {new_sql}"
        );

        // Verify the original column list is NOT present
        assert!(
            !new_sql.contains("a.id, b.data"),
            "Query should not contain original column list: {new_sql}"
        );

        // Verify the complete expected structure
        let expected_pattern =
            "SELECT * FROM table_a a WHERE a.status = 'active' AND b.enabled = true";
        assert_eq!(
            new_sql, expected_pattern,
            "Query should match expected pattern"
        );
    }
}

use pg_query::ParseResult;
use pg_query::protobuf::{
    AStar, ColumnRef, Node, RawStmt, ResTarget, SelectStmt, node::Node as NodeEnum,
};

use crate::query::{
    ast::{SelectStatement, SqlQuery, Statement, WhereExpr, WhereOp, sql_query_convert},
    evaluate::{is_simple_comparison, where_expr_evaluate},
};

use super::*;

pub fn is_cacheable(ast: &ParseResult) -> bool {
    // Try to convert to our custom AST - if conversion fails, not cacheable
    let sql_query = match sql_query_convert(ast) {
        Ok(query) => query,
        Err(_) => return false,
    };

    is_cacheable_ast(&sql_query)
}

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

pub fn query_select_replace(ast: &ParseResult) -> ParseResult {
    // Only replace if this is a SELECT statement at the top level
    // (not an INSERT, UPDATE, etc. that contains an embedded SELECT)
    if !ast.statement_types().contains(&"SelectStmt") || ast.statement_types().len() != 1 {
        // Return original if not a pure SELECT statement
        return ParseResult {
            protobuf: ast.protobuf.clone(),
            warnings: ast.warnings.clone(),
            tables: ast.tables.clone(),
            aliases: ast.aliases.clone(),
            cte_names: ast.cte_names.clone(),
            functions: ast.functions.clone(),
            filter_columns: ast.filter_columns.clone(),
        };
    }

    // Find the SelectStmt in the AST
    let nodes = ast.protobuf.nodes();
    let original_select = nodes.iter().find_map(|(node, _depth, _context, _)| {
        if let pg_query::NodeRef::SelectStmt(select_stmt) = node {
            Some(select_stmt)
        } else {
            None
        }
    });

    let Some(original_select) = original_select else {
        // No SelectStmt found, return the original unchanged
        return ParseResult {
            protobuf: ast.protobuf.clone(),
            warnings: ast.warnings.clone(),
            tables: ast.tables.clone(),
            aliases: ast.aliases.clone(),
            cte_names: ast.cte_names.clone(),
            functions: ast.functions.clone(),
            filter_columns: ast.filter_columns.clone(),
        };
    };

    // Create a SELECT * target (ResTarget -> ColumnRef -> AStar)
    let select_star_target = Node {
        node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
            name: "".to_string(),
            location: 7,
            indirection: vec![],
            val: Some(Box::new(Node {
                node: Some(NodeEnum::ColumnRef(ColumnRef {
                    fields: vec![Node {
                        node: Some(NodeEnum::AStar(AStar {})),
                    }],
                    location: 7,
                })),
            })),
        }))),
    };

    // Create a new SelectStmt with SELECT * but same FROM, WHERE, etc.
    let new_select_stmt = SelectStmt {
        distinct_clause: original_select.distinct_clause.clone(),
        into_clause: original_select.into_clause.clone(),
        target_list: vec![select_star_target], // Replace with SELECT *
        from_clause: original_select.from_clause.clone(),
        where_clause: original_select.where_clause.clone(),
        group_clause: original_select.group_clause.clone(),
        having_clause: original_select.having_clause.clone(),
        window_clause: original_select.window_clause.clone(),
        values_lists: original_select.values_lists.clone(),
        sort_clause: original_select.sort_clause.clone(),
        limit_offset: original_select.limit_offset.clone(),
        limit_count: original_select.limit_count.clone(),
        limit_option: original_select.limit_option,
        locking_clause: original_select.locking_clause.clone(),
        with_clause: original_select.with_clause.clone(),
        op: original_select.op,
        all: original_select.all,
        larg: original_select.larg.clone(),
        rarg: original_select.rarg.clone(),
        group_distinct: original_select.group_distinct,
    };

    // Create new ParseResult with the modified SelectStmt
    let new_stmt = Node {
        node: Some(NodeEnum::SelectStmt(Box::new(new_select_stmt))),
    };

    let raw_stmt = RawStmt {
        stmt: Some(Box::new(new_stmt)),
        stmt_location: 0,
        stmt_len: 0,
    };

    ParseResult {
        protobuf: pg_query::protobuf::ParseResult {
            version: ast.protobuf.version,
            stmts: vec![raw_stmt],
        },
        warnings: ast.warnings.clone(),
        tables: ast.tables.clone(),
        aliases: ast.aliases.clone(),
        cte_names: ast.cte_names.clone(),
        functions: ast.functions.clone(),
        filter_columns: ast.filter_columns.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_select_replace() {
        // Test replacing specific columns with SELECT *
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");

        let result = query_select_replace(&ast);

        // Deparse to verify the transformation
        let new_sql = result.deparse().expect("to deparse result");

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
        // Test with more complex query including JOIN and WHERE clause
        let original_query = "SELECT a.id, b.data FROM table_a a JOIN table_b b ON a.id = b.id WHERE a.status = 'active' AND b.enabled = true";
        let ast = pg_query::parse(original_query).expect("to parse query");

        let result = query_select_replace(&ast);

        // Deparse to verify the transformation
        let new_sql = result.deparse().expect("to deparse result");

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

        // Verify JOIN clause is preserved
        assert!(
            new_sql.contains("JOIN table_b b ON a.id = b.id"),
            "Query should preserve JOIN clause: {new_sql}"
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
        let expected_pattern = "SELECT * FROM table_a a JOIN table_b b ON a.id = b.id WHERE a.status = 'active' AND b.enabled = true";
        assert_eq!(
            new_sql, expected_pattern,
            "Query should match expected pattern"
        );
    }

    #[test]
    fn test_query_select_replace_with_all_clauses() {
        // Test with query containing multiple SQL clauses
        let original_query = "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id WHERE u.active = true GROUP BY u.id, u.name HAVING COUNT(p.id) > 0 ORDER BY u.name ASC LIMIT 10";
        let ast = pg_query::parse(original_query).expect("to parse query");

        let result = query_select_replace(&ast);
        let new_sql = result.deparse().expect("to deparse result");

        // Should contain SELECT *
        assert!(
            new_sql.contains("SELECT *"),
            "Query should contain SELECT *: {new_sql}"
        );

        // Verify all clauses are preserved
        assert!(
            new_sql.contains("FROM users u"),
            "Query should preserve FROM clause: {new_sql}"
        );

        assert!(
            new_sql.contains("LEFT JOIN posts p ON u.id = p.user_id"),
            "Query should preserve LEFT JOIN clause: {new_sql}"
        );

        assert!(
            new_sql.contains("WHERE u.active = true"),
            "Query should preserve WHERE clause: {new_sql}"
        );

        assert!(
            new_sql.contains("GROUP BY u.id, u.name"),
            "Query should preserve GROUP BY clause: {new_sql}"
        );

        assert!(
            new_sql.contains("HAVING count(p.id) > 0"),
            "Query should preserve HAVING clause: {new_sql}"
        );

        assert!(
            new_sql.contains("ORDER BY u.name"),
            "Query should preserve ORDER BY clause: {new_sql}"
        );

        assert!(
            new_sql.contains("LIMIT 10"),
            "Query should preserve LIMIT clause: {new_sql}"
        );

        // Verify original columns are NOT present
        assert!(
            !new_sql.contains("u.name, p.title"),
            "Query should not contain original column list: {new_sql}"
        );
    }

    #[test]
    fn test_query_select_replace_insert() {
        // Test with non-SELECT statement
        let original_query = "INSERT INTO users (name) VALUES ('test')";
        let ast = pg_query::parse(original_query).expect("to parse query");

        let result = query_select_replace(&ast);

        // Should return the original unchanged
        let new_sql = result.deparse().expect("to deparse result");

        assert_eq!(
            new_sql, original_query,
            "Non-SELECT queries should be unchanged"
        );
    }
}

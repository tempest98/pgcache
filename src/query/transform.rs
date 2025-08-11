use crate::query::ast::{SelectColumns, SelectStatement};

pub fn query_select_replace(select_statement: &SelectStatement) -> SelectStatement {
    if !select_statement.is_single_table() {
        // Return original if not a pure SELECT statement
        return select_statement.clone();
    }

    let mut new_stmt = select_statement.clone();
    new_stmt.columns = SelectColumns::All;

    new_stmt
}

#[cfg(test)]
mod tests {
    use crate::query::ast::{Deparse, Statement, sql_query_convert};

    use super::*;

    #[test]
    fn test_query_select_replace() {
        // Test replacing specific columns with SELECT *
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_select_replace(stmt);

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

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_select_replace(stmt);

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

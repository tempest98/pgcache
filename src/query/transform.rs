use error_set::error_set;

use crate::{
    cache::query::CacheableQuery,
    catalog::TableMetadata,
    query::ast::{
        ColumnExpr, LiteralValue, SelectColumn, SelectColumns, SelectStatement, TableAlias,
        TableNode, TableSource, TableSubqueryNode, WhereExpr,
    },
};

error_set! {
    AstTransformError := {
        MissingTable,
        #[display("Parameter index {index} out of bounds (have {count} parameters)")]
        ParameterOutOfBounds { index: usize, count: usize },
        #[display("Invalid parameter placeholder: {placeholder}")]
        InvalidParameterPlaceholder { placeholder: String },
        #[display("Invalid UTF-8 in parameter value")]
        InvalidUtf8,
    }
}

/// Replace parameter placeholders ($1, $2, etc.) in a SelectStatement with actual values.
///
/// This function traverses the AST and replaces all `LiteralValue::Parameter` nodes
/// with appropriate typed `LiteralValue` nodes based on the provided parameter values.
///
/// # Arguments
/// * `select_statement` - The SELECT statement containing parameter placeholders
/// * `parameters` - The parameter values, indexed from 0 (for $1, $2, etc.)
///
/// # Returns
/// A new `SelectStatement` with parameters replaced by literal values
///
/// # Errors
/// Returns `AstTransformError` if:
/// - A parameter index is out of bounds
/// - A parameter placeholder format is invalid
/// - Parameter value contains invalid UTF-8
pub fn ast_parameters_replace(
    select_statement: &SelectStatement,
    parameters: &[Option<Vec<u8>>],
) -> Result<SelectStatement, AstTransformError> {
    let mut new_stmt = select_statement.clone();

    // Replace parameters in WHERE clause
    if let Some(where_clause) = &mut new_stmt.where_clause {
        where_expr_parameters_replace(where_clause, parameters)?;
    }

    // Replace parameters in HAVING clause
    if let Some(having) = &mut new_stmt.having {
        where_expr_parameters_replace(having, parameters)?;
    }

    // Replace parameters in JOIN conditions
    for table_source in &mut new_stmt.from {
        table_source_parameters_replace(table_source, parameters)?;
    }

    Ok(new_stmt)
}

/// Replace parameters in a TableSource (recursively handles JOINs)
fn table_source_parameters_replace(
    table_source: &mut TableSource,
    parameters: &[Option<Vec<u8>>],
) -> Result<(), AstTransformError> {
    match table_source {
        TableSource::Join(join) => {
            // Replace in JOIN condition
            if let Some(condition) = &mut join.condition {
                where_expr_parameters_replace(condition, parameters)?;
            }
            // Recursively handle nested joins
            table_source_parameters_replace(&mut join.left, parameters)?;
            table_source_parameters_replace(&mut join.right, parameters)?;
        }
        TableSource::Subquery(subquery) => {
            // Replace parameters in subquery
            *subquery.select = ast_parameters_replace(&subquery.select, parameters)?;
        }
        TableSource::Table(_) => {
            // No parameters in simple table references
        }
    }
    Ok(())
}

/// Replace parameters in a WhereExpr tree (mutates in place)
fn where_expr_parameters_replace(
    expr: &mut WhereExpr,
    parameters: &[Option<Vec<u8>>],
) -> Result<(), AstTransformError> {
    match expr {
        WhereExpr::Value(literal) => {
            // Only replace if this is a parameter placeholder
            if let LiteralValue::Parameter(placeholder) = literal {
                // Parse parameter index from placeholder (e.g., "$1" -> 0)
                let index = parameter_index_parse(placeholder)?;

                // Get parameter value
                let param_value =
                    parameters
                        .get(index)
                        .ok_or(AstTransformError::ParameterOutOfBounds {
                            index,
                            count: parameters.len(),
                        })?;

                // Replace the LiteralValue in place
                *literal = parameter_to_literal(param_value)?;
            }
        }
        WhereExpr::Column(_) => {
            // No parameters in column references
        }
        WhereExpr::Unary(unary) => {
            where_expr_parameters_replace(&mut unary.expr, parameters)?;
        }
        WhereExpr::Binary(binary) => {
            where_expr_parameters_replace(&mut binary.lexpr, parameters)?;
            where_expr_parameters_replace(&mut binary.rexpr, parameters)?;
        }
        WhereExpr::Multi(multi) => {
            for expr in &mut multi.exprs {
                where_expr_parameters_replace(expr, parameters)?;
            }
        }
        WhereExpr::Function { args, .. } => {
            for arg in args {
                where_expr_parameters_replace(arg, parameters)?;
            }
        }
        WhereExpr::Subquery { .. } => {
            // Subqueries would need their own parameter replacement
            // but we don't currently support subqueries with parameters
        }
    }
    Ok(())
}

/// Parse parameter index from placeholder string (e.g., "$1" -> 0, "$2" -> 1)
fn parameter_index_parse(placeholder: &str) -> Result<usize, AstTransformError> {
    if !placeholder.starts_with('$') {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_string(),
        });
    }

    let index_str = &placeholder[1..];
    let param_num =
        index_str
            .parse::<usize>()
            .map_err(|_| AstTransformError::InvalidParameterPlaceholder {
                placeholder: placeholder.to_string(),
            })?;

    if param_num == 0 {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_string(),
        });
    }

    Ok(param_num - 1) // Convert 1-indexed to 0-indexed
}

/// Convert a parameter value (bytes) to a LiteralValue
fn parameter_to_literal(param: &Option<Vec<u8>>) -> Result<LiteralValue, AstTransformError> {
    match param {
        None => Ok(LiteralValue::Null),
        Some(bytes) => {
            // Convert bytes to string
            let s = String::from_utf8(bytes.clone()).map_err(|_| AstTransformError::InvalidUtf8)?;

            // Try to infer type from the string value
            // For now, use String type - could be enhanced with type hints later
            Ok(LiteralValue::String(s))
        }
    }
}

pub fn query_select_replace(
    select_statement: &SelectStatement,
    columns: SelectColumns,
) -> SelectStatement {
    let mut new_stmt = select_statement.clone();
    new_stmt.columns = columns;

    new_stmt
}

//generate queries used to check if a dml statement applies to a given table
pub fn query_table_update_queries(
    cacheable_query: &CacheableQuery,
) -> Vec<(&TableNode, SelectStatement)> {
    let select = cacheable_query.statement();
    let tables = select.nodes::<TableNode>().collect::<Vec<_>>();

    let column = SelectColumn {
        expr: ColumnExpr::Literal(LiteralValue::Boolean(true)),
        alias: None,
    };

    let select_list = SelectColumns::Columns(vec![column]);

    let mut queries = Vec::new();
    if tables.len() == 1 {
        queries.push((tables[0], query_select_replace(select, select_list)));
    } else if tables.len() == 2 {
        //can use same query for both tables (CacheableQuery guarantees is_supported_from)
        queries.push((tables[0], query_select_replace(select, select_list.clone())));
        queries.push((tables[1], query_select_replace(select, select_list)));
    }

    queries
}

pub fn query_table_replace_with_values(
    select: &SelectStatement,
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> Result<SelectStatement, AstTransformError> {
    let mut select_new = select.clone();

    //find first matching table source
    let mut frontier = vec![&mut select_new.from[0]];
    let mut source_node: Option<&mut TableSource> = None;
    while let Some(cur) = frontier.pop() {
        match cur {
            TableSource::Join(join) => {
                frontier.push(&mut join.left);
                frontier.push(&mut join.right);
            }
            TableSource::Table(table) => {
                if table.name == table_metadata.name {
                    source_node = Some(cur);
                    break;
                }
            }
            _ => (),
        }
    }

    let Some(source_node) = source_node else {
        return Err(AstTransformError::MissingTable);
    };
    let TableSource::Table(table_node) = source_node else {
        return Err(AstTransformError::MissingTable);
    };

    let mut column_names = Vec::new();
    let mut values = Vec::new();

    for column_meta in &table_metadata.columns {
        let position = column_meta.position as usize - 1;
        if position < row_data.len() {
            let value = row_data[position]
                .as_deref()
                .map_or(LiteralValue::Null, |v| {
                    //some ugly casting
                    LiteralValue::StringWithCast(v.to_owned(), column_meta.type_name.clone())
                });

            column_names.push(column_meta.name.as_str());
            values.push(value);
        }
    }

    let alias = if let Some(table_alias) = &table_node.alias {
        let mut alias = table_alias.clone();
        if table_alias.columns.is_empty() {
            for name in column_names {
                alias.columns.push(name.to_owned());
            }
        }
        alias
    } else {
        TableAlias {
            name: table_metadata.name.clone(),
            columns: Vec::new(),
        }
    };

    *source_node = TableSource::Subquery(TableSubqueryNode {
        lateral: false,
        select: Box::new(SelectStatement {
            values: vec![values],
            ..Default::default()
        }),
        alias: Some(alias),
    });

    Ok(select_new)
}

#[cfg(test)]
mod tests {
    use crate::query::ast::{Deparse, Statement, sql_query_convert};

    use super::*;

    #[test]
    fn test_ast_parameters_replace_simple() {
        let query = "SELECT id FROM users WHERE id = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with value "42"
        let params = vec![Some(b"42".to_vec())];
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE id = '42'");
    }

    #[test]
    fn test_ast_parameters_replace_multiple_params() {
        let query = "SELECT id FROM users WHERE id = $1 AND name = $2";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with "42", $2 with "alice"
        let params = vec![Some(b"42".to_vec()), Some(b"alice".to_vec())];
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id = '42' AND name = 'alice'"
        );
    }

    #[test]
    fn test_ast_parameters_replace_null() {
        let query = "SELECT id FROM users WHERE name = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with NULL
        let params = vec![None];
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE name = NULL");
    }

    #[test]
    fn test_ast_parameters_replace_out_of_bounds() {
        let query = "SELECT id FROM users WHERE id = $2";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Only provide 1 parameter, but query uses $2
        let params = vec![Some(b"42".to_vec())];
        let result = ast_parameters_replace(stmt, &params);

        assert!(result.is_err());
        match result {
            Err(AstTransformError::ParameterOutOfBounds { index, count }) => {
                assert_eq!(index, 1); // $2 -> index 1
                assert_eq!(count, 1); // Only 1 parameter provided
            }
            _ => panic!("Expected ParameterOutOfBounds error"),
        }
    }

    #[test]
    fn test_ast_parameters_replace_in_join() {
        let query = "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with "100"
        let params = vec![Some(b"100".to_vec())];
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert!(buf.contains("WHERE o.total > '100'"));
    }

    #[test]
    fn test_query_select_replace() {
        // Test replacing specific columns with SELECT *
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_select_replace(stmt, SelectColumns::All);

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
        let result = query_select_replace(stmt, SelectColumns::All);

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

    #[test]
    fn test_query_table_update_query_simple_select() {
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let cacheable_query = CacheableQuery::try_from(&sql_query).expect("query to be cacheable");
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 1);

        let mut buf = String::new();
        let new_sql = result[0].1.deparse(&mut buf);

        assert_eq!(new_sql, "SELECT true FROM users WHERE id = 1");
    }

    #[test]
    fn test_query_table_update_query_simple_join() {
        let original_query = "SELECT id, name, email FROM users \
                            JOIN location ON location.user_id = users.user_id \
                            WHERE users.user_id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let cacheable_query = CacheableQuery::try_from(&sql_query).expect("query to be cacheable");
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 2);

        let mut update1 = String::new();
        result[0].1.deparse(&mut update1);
        assert_eq!(
            update1,
            "SELECT true FROM users JOIN location ON location.user_id = users.user_id WHERE users.user_id = 1"
        );

        let mut update2 = String::new();
        result[1].1.deparse(&mut update2);
        assert_eq!(
            update2,
            "SELECT true FROM users JOIN location ON location.user_id = users.user_id WHERE users.user_id = 1"
        );
    }
}

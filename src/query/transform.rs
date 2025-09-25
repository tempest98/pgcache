use error_set::error_set;

use crate::{
    cache::TableMetadata,
    query::ast::{
        ColumnExpr, LiteralValue, SelectColumn, SelectColumns, SelectStatement, TableAlias,
        TableNode, TableSource, TableSubqueryNode,
    },
};

error_set! {
    AstTransformError = {
        MissingTable,
    };
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
pub fn query_table_update_queries(select: &SelectStatement) -> Vec<(&TableNode, SelectStatement)> {
    let tables = select.nodes::<TableNode>().collect::<Vec<_>>();

    let column = SelectColumn {
        expr: ColumnExpr::Literal(LiteralValue::Boolean(true)),
        alias: None,
    };

    let select_list = SelectColumns::Columns(vec![column]);

    let mut queries = Vec::new();
    if tables.len() == 1 {
        queries.push((tables[0], query_select_replace(select, select_list)));
    } else if tables.len() == 2 && select.is_supported_from() {
        //can use same query for both tables
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

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_table_update_queries(stmt);

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

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_table_update_queries(stmt);

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

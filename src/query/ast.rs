use std::collections::HashSet;

use error_set::error_set;
use pg_query::ParseResult;
use pg_query::protobuf::{
    ColumnRef as PgColumnRef, Node, RangeVar, SelectStmt, node::Node as NodeEnum,
};

use super::parse::{ColumnRef, LiteralValue, WhereExpr, WhereParseError, query_where_clause_parse};

error_set! {
    AstError = {
        #[display("Unsupported statement type: {statement_type}")]
        UnsupportedStatement { statement_type: String },
        #[display("Multiple statements not supported")]
        MultipleStatements,
        #[display("Missing statement")]
        MissingStatement,
        #[display("Unsupported SELECT feature: {feature}")]
        UnsupportedSelectFeature { feature: String },
        #[display("Invalid table reference")]
        InvalidTableRef,
        WhereParseError(WhereParseError),
    };
}

/// Simplified SQL AST focused on caching use cases
#[derive(Debug, Clone, PartialEq)]
pub struct SqlQuery {
    pub statement: Statement,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    // Future: Insert, Update, Delete for CDC
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub columns: SelectColumns,
    pub from: Vec<TableRef>,
    pub where_clause: Option<WhereExpr>,
    pub group_by: Vec<ColumnRef>,
    pub having: Option<WhereExpr>,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<LimitClause>,
    pub distinct: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumns {
    All,                        // SELECT *
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ...
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectColumn {
    pub expr: ColumnExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnExpr {
    Column(ColumnRef),      // column_name, table.column_name
    Function(FunctionCall), // COUNT(*), SUM(col), etc.
    Literal(LiteralValue),  // Constant values
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<ColumnExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    pub join: Option<JoinClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: Box<TableRef>,
    pub condition: Option<WhereExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub expr: ColumnExpr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub count: Option<i64>,
    pub offset: Option<i64>,
}

/// Convert a pg_query ParseResult into our simplified AST
pub fn sql_query_convert(ast: &ParseResult) -> Result<SqlQuery, AstError> {
    if ast.protobuf.stmts.len() != 1 {
        return Err(AstError::MultipleStatements);
    }

    let raw_stmt = &ast.protobuf.stmts[0];
    let stmt_node = raw_stmt.stmt.as_ref().ok_or(AstError::MissingStatement)?;

    match stmt_node.node.as_ref() {
        Some(NodeEnum::SelectStmt(select_stmt)) => {
            let statement = select_statement_convert(select_stmt, ast)?;
            Ok(SqlQuery {
                statement: Statement::Select(statement),
            })
        }
        Some(other) => Err(AstError::UnsupportedStatement {
            statement_type: format!("{other:?}"),
        }),
        None => Err(AstError::MissingStatement),
    }
}

fn select_statement_convert(
    select_stmt: &SelectStmt,
    ast: &ParseResult,
) -> Result<SelectStatement, AstError> {
    let columns = select_columns_convert(&select_stmt.target_list)?;
    let from = from_clause_convert(&select_stmt.from_clause)?;
    let where_clause = query_where_clause_parse(ast)?;

    // For now, only convert the features we need for basic caching
    // TODO: Add GROUP BY, HAVING, ORDER BY, LIMIT when needed

    Ok(SelectStatement {
        columns,
        from,
        where_clause,
        group_by: vec![], // TODO: Convert group_clause
        having: None,     // TODO: Convert having_clause
        order_by: vec![], // TODO: Convert sort_clause
        limit: None,      // TODO: Convert limit_count/limit_offset
        distinct: !select_stmt.distinct_clause.is_empty(),
    })
}

fn select_columns_convert(target_list: &[Node]) -> Result<SelectColumns, AstError> {
    if target_list.is_empty() {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "Empty target list".to_string(),
        });
    }

    let mut columns = Vec::new();
    let mut has_star = false;

    for target in target_list {
        if let Some(NodeEnum::ResTarget(res_target)) = &target.node {
            if let Some(val_node) = &res_target.val {
                match val_node.node.as_ref() {
                    Some(NodeEnum::ColumnRef(col_ref)) => {
                        // Check if this is SELECT *
                        if col_ref.fields.len() == 1 {
                            if let Some(NodeEnum::AStar(_)) = &col_ref.fields[0].node {
                                has_star = true;
                                continue;
                            }
                        }

                        // Regular column reference
                        let column_ref = column_ref_convert(col_ref)?;
                        let alias = if res_target.name.is_empty() {
                            None
                        } else {
                            Some(res_target.name.clone())
                        };

                        columns.push(SelectColumn {
                            expr: ColumnExpr::Column(column_ref),
                            alias,
                        });
                    }
                    // TODO: Add support for function calls, literals, etc.
                    other => {
                        return Err(AstError::UnsupportedSelectFeature {
                            feature: format!("Column expression: {other:?}"),
                        });
                    }
                }
            }
        }
    }

    if has_star && columns.is_empty() {
        Ok(SelectColumns::All)
    } else if !has_star && !columns.is_empty() {
        Ok(SelectColumns::Columns(columns))
    } else {
        Err(AstError::UnsupportedSelectFeature {
            feature: "Mixed * and column list".to_string(),
        })
    }
}

fn from_clause_convert(from_clause: &[Node]) -> Result<Vec<TableRef>, AstError> {
    let mut tables = Vec::new();

    for from_node in from_clause {
        if let Some(NodeEnum::RangeVar(range_var)) = &from_node.node {
            let table_ref = table_ref_convert(range_var)?;
            tables.push(table_ref);
        } else {
            return Err(AstError::UnsupportedSelectFeature {
                feature: format!("FROM clause type: {from_node:?}"),
            });
        }
    }

    Ok(tables)
}

fn table_ref_convert(range_var: &RangeVar) -> Result<TableRef, AstError> {
    let name = if range_var.schemaname.is_empty() {
        range_var.relname.clone()
    } else {
        format!("{}.{}", range_var.schemaname, range_var.relname)
    };

    let alias = range_var
        .alias
        .as_ref()
        .map(|alias_node| alias_node.aliasname.clone());

    Ok(TableRef {
        name,
        alias,
        join: None, // TODO: Convert JOIN clauses
    })
}

fn column_ref_convert(col_ref: &PgColumnRef) -> Result<ColumnRef, AstError> {
    if col_ref.fields.is_empty() {
        return Err(AstError::InvalidTableRef);
    }

    let mut table: Option<String> = None;
    let mut column: Option<String> = None;

    for field in &col_ref.fields {
        match field.node.as_ref() {
            Some(NodeEnum::String(s)) => {
                if column.is_none() {
                    column = Some(s.sval.clone());
                } else {
                    // If we already have a column, previous value becomes table
                    table = column.clone();
                    column = Some(s.sval.clone());
                }
            }
            _ => return Err(AstError::InvalidTableRef),
        }
    }

    let column = column.ok_or(AstError::InvalidTableRef)?;
    Ok(ColumnRef { table, column })
}

/// Helper functions for common query analysis tasks
impl SqlQuery {
    /// Get all table names referenced in the query
    pub fn tables(&self) -> HashSet<String> {
        match &self.statement {
            Statement::Select(select) => select.from.iter().map(|t| t.name.clone()).collect(),
        }
    }

    /// Check if query only references a single table
    pub fn is_single_table(&self) -> bool {
        self.tables().len() == 1
    }

    /// Check if query has a WHERE clause
    pub fn has_where_clause(&self) -> bool {
        match &self.statement {
            Statement::Select(select) => select.where_clause.is_some(),
        }
    }

    /// Get the WHERE clause if it exists
    pub fn where_clause(&self) -> Option<&WhereExpr> {
        match &self.statement {
            Statement::Select(select) => select.where_clause.as_ref(),
        }
    }

    /// Check if this is a SELECT * query
    pub fn is_select_star(&self) -> bool {
        match &self.statement {
            Statement::Select(select) => matches!(select.columns, SelectColumns::All),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_query_convert_simple_select() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        assert!(ast.is_single_table());
        assert!(ast.has_where_clause());
        assert!(!ast.is_select_star());
        assert_eq!(ast.tables(), HashSet::from(["users".to_string()]));
    }

    #[test]
    fn test_sql_query_convert_select_star() {
        let sql = "SELECT * FROM products";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        assert!(ast.is_single_table());
        assert!(!ast.has_where_clause());
        assert!(ast.is_select_star());
        assert_eq!(ast.tables(), HashSet::from(["products".to_string()]));
    }

    #[test]
    fn test_sql_query_convert_where_clause() {
        let sql = "SELECT * FROM users WHERE name = 'john' AND active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        assert!(ast.has_where_clause());
        let where_clause = ast.where_clause().unwrap();

        // Should convert the same WHERE clause as before
        // (reusing existing WhereExpr conversion)
        assert!(matches!(where_clause, WhereExpr::Binary(_)));
    }

    #[test]
    fn test_sql_query_convert_table_alias() {
        let sql = "SELECT u.id, u.name FROM users u WHERE u.active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Check table alias
        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.from.len(), 1);
        assert_eq!(select.from[0].name, "users");
        assert_eq!(select.from[0].alias, Some("u".to_string()));

        // Check column references
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns.len(), 2);

            // First column: u.id
            if let ColumnExpr::Column(col_ref) = &columns[0].expr {
                assert_eq!(col_ref.table, Some("u".to_string()));
                assert_eq!(col_ref.column, "id");
            }

            // Second column: u.name
            if let ColumnExpr::Column(col_ref) = &columns[1].expr {
                assert_eq!(col_ref.table, Some("u".to_string()));
                assert_eq!(col_ref.column, "name");
            }
        }
    }

    #[test]
    fn test_sql_query_convert_column_alias() {
        let sql = "SELECT id as user_id, name as full_name FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns.len(), 2);

            // First column: id as user_id
            assert_eq!(columns[0].alias, Some("user_id".to_string()));
            if let ColumnExpr::Column(col_ref) = &columns[0].expr {
                assert_eq!(col_ref.column, "id");
            }

            // Second column: name as full_name
            assert_eq!(columns[1].alias, Some("full_name".to_string()));
            if let ColumnExpr::Column(col_ref) = &columns[1].expr {
                assert_eq!(col_ref.column, "name");
            }
        }
    }

    #[test]
    fn test_sql_query_convert_no_alias() {
        let sql = "SELECT id, name FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        // Table should have no alias
        assert_eq!(select.from[0].alias, None);

        // Columns should have no aliases
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns[0].alias, None);
            assert_eq!(columns[1].alias, None);
        }
    }
}

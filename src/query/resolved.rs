use error_set::error_set;
use iddqd::BiHashMap;

use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::query::ast::{
    ColumnExpr, ColumnNode, ExprOp, JoinType, LiteralValue, OrderDirection, SelectColumns,
    SelectStatement, TableNode, TableSource, WhereExpr,
};

error_set! {
    ResolveError := {
        #[display("Table not found: {name}")]
        TableNotFound { name: String },

        #[display("Column '{column}' not found in table '{table}'")]
        ColumnNotFound { table: String, column: String },

        #[display("Ambiguous column reference: '{column}' could refer to multiple tables")]
        AmbiguousColumn { column: String },

        #[display("Schema '{schema}' not found")]
        SchemaNotFound { schema: String },

        #[display("Subquery alias '{alias}' not found")]
        SubqueryAliasNotFound { alias: String },

        #[display("Invalid table reference")]
        InvalidTableRef,
    }
}

/// Resolved table reference with complete metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTableNode {
    /// Full schema name (resolved from 'public' default if needed)
    pub schema: String,
    /// Table name
    pub name: String,
    /// Optional alias used in query
    pub alias: Option<String>,
    /// Relation OID from catalog
    pub relation_oid: u32,
}

/// Resolved column reference with type information
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedColumnNode {
    /// Schema name where the table is located
    pub schema: String,
    /// Table name (not alias) where column is defined
    pub table: String,
    /// Column name
    pub column: String,
    /// Column metadata from catalog (includes type info, position, primary key status, etc.)
    pub column_metadata: ColumnMetadata,
}

/// Resolved unary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedUnaryExpr {
    pub op: ExprOp,
    pub expr: Box<ResolvedWhereExpr>,
}

/// Resolved binary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedBinaryExpr {
    pub op: ExprOp,
    pub lexpr: Box<ResolvedWhereExpr>,
    pub rexpr: Box<ResolvedWhereExpr>,
}

/// Resolved multi-operand expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMultiExpr {
    pub op: ExprOp,
    pub exprs: Vec<ResolvedWhereExpr>,
}

/// Resolved WHERE expression with fully qualified references
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedWhereExpr {
    /// Literal value
    Value(LiteralValue),
    /// Fully qualified column reference
    Column(ResolvedColumnNode),
    /// Unary expression
    Unary(ResolvedUnaryExpr),
    /// Binary expression
    Binary(ResolvedBinaryExpr),
    /// Multi-operand expression
    Multi(ResolvedMultiExpr),
    /// Function call (for future support)
    Function {
        name: String,
        args: Vec<ResolvedWhereExpr>,
    },
    /// Subquery (for future support)
    Subquery { query: Box<ResolvedSelectStatement> },
}

/// Resolved column expression in SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedColumnExpr {
    /// Fully qualified column reference
    Column(ResolvedColumnNode),
    /// Function call (for future support)
    Function {
        name: String,
        args: Vec<ResolvedColumnExpr>,
    },
    /// Literal value
    Literal(LiteralValue),
    /// Subquery (for future support)
    Subquery(Box<ResolvedSelectStatement>),
}

/// Resolved SELECT column with optional alias
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectColumn {
    pub expr: ResolvedColumnExpr,
    pub alias: Option<String>,
}

/// Resolved SELECT columns list
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedSelectColumns {
    /// No columns (empty SELECT)
    None,
    /// SELECT * (expanded to all columns from all tables)
    All(Vec<ResolvedColumnNode>),
    /// Specific columns
    Columns(Vec<ResolvedSelectColumn>),
}

/// Resolved table source
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedTableSource {
    /// Direct table reference
    Table(ResolvedTableNode),
    /// Resolved subquery (for future support)
    Subquery {
        select: Box<ResolvedSelectStatement>,
        alias: String,
    },
    /// Resolved join
    Join(Box<ResolvedJoinNode>),
}

/// Resolved JOIN node
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedJoinNode {
    pub join_type: JoinType,
    pub left: ResolvedTableSource,
    pub right: ResolvedTableSource,
    pub condition: Option<ResolvedWhereExpr>,
}

/// Resolved ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOrderByClause {
    pub expr: ResolvedColumnExpr,
    pub direction: OrderDirection,
}

/// Resolved LIMIT clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLimitClause {
    pub count: Option<i64>,
    pub offset: Option<i64>,
}

/// Fully resolved SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectStatement {
    pub columns: ResolvedSelectColumns,
    pub from: Vec<ResolvedTableSource>,
    pub where_clause: Option<ResolvedWhereExpr>,
    pub group_by: Vec<ResolvedColumnNode>,
    pub having: Option<ResolvedWhereExpr>,
    pub order_by: Vec<ResolvedOrderByClause>,
    pub limit: Option<ResolvedLimitClause>,
    pub distinct: bool,
    pub values: Vec<Vec<LiteralValue>>,
}

impl Default for ResolvedSelectStatement {
    fn default() -> Self {
        Self {
            columns: ResolvedSelectColumns::None,
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            distinct: false,
            values: vec![vec![]],
        }
    }
}

/// Resolved statement (only SELECT for now)
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedStatement {
    Select(ResolvedSelectStatement),
}

/// Fully resolved SQL query
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSqlQuery {
    pub statement: ResolvedStatement,
}

/// Resolution scope tracking available tables and their aliases
#[derive(Debug)]
struct ResolutionScope<'a> {
    /// Tables available in this scope, indexed by alias (or table name if no alias)
    tables: Vec<(&'a TableMetadata, Option<&'a str>)>, // (metadata, alias)
}

impl<'a> ResolutionScope<'a> {
    fn new() -> Self {
        Self { tables: Vec::new() }
    }

    /// Add a table to the scope
    fn table_scope_add(&mut self, metadata: &'a TableMetadata, alias: Option<&'a str>) {
        self.tables.push((metadata, alias));
    }

    /// Find table metadata by name or alias
    fn table_scope_find(&self, name: &str) -> Option<(&'a TableMetadata, Option<&'a str>)> {
        self.tables
            .iter()
            .find(|(meta, alias)| {
                if let Some(alias_name) = alias {
                    *alias_name == name
                } else {
                    meta.name == name
                }
            })
            .copied()
    }
}

/// Resolve a table reference to a resolved table node
fn table_resolve(
    table_node: &TableNode,
    tables: &BiHashMap<TableMetadata>,
) -> Result<ResolvedTableNode, ResolveError> {
    let schema = table_node.schema.as_deref().unwrap_or("public");
    let table_name = &table_node.name;

    // Look up table in metadata
    let table_metadata =
        tables
            .get2(table_name.as_str())
            .ok_or_else(|| ResolveError::TableNotFound {
                name: table_name.clone(),
            })?;

    // Verify schema matches (if schema was explicitly specified)
    if table_node.schema.is_some() && table_metadata.schema != schema {
        return Err(ResolveError::SchemaNotFound {
            schema: schema.to_owned(),
        });
    }

    Ok(ResolvedTableNode {
        schema: table_metadata.schema.clone(),
        name: table_metadata.name.clone(),
        alias: table_node.alias.as_ref().map(|a| a.name.clone()),
        relation_oid: table_metadata.relation_oid,
    })
}

/// Resolve a column reference to a resolved column node
fn column_resolve<'a>(
    column_node: &ColumnNode,
    scope: &ResolutionScope<'a>,
) -> Result<ResolvedColumnNode, ResolveError> {
    let column_name = &column_node.column;

    // If column has table qualifier, resolve directly
    if let Some(table_qualifier) = &column_node.table {
        let (table_metadata, _) =
            scope
                .table_scope_find(table_qualifier)
                .ok_or_else(|| ResolveError::TableNotFound {
                    name: table_qualifier.clone(),
                })?;

        let column_metadata = table_metadata
            .columns
            .get1(column_name.as_str())
            .ok_or_else(|| ResolveError::ColumnNotFound {
                table: table_metadata.name.clone(),
                column: column_name.clone(),
            })?;

        return Ok(ResolvedColumnNode {
            schema: table_metadata.schema.clone(),
            table: table_metadata.name.clone(),
            column: column_metadata.name.clone(),
            column_metadata: column_metadata.clone(),
        });
    }

    // Unqualified column - search all tables in scope
    let mut matches = Vec::new();
    for (table_metadata, _) in &scope.tables {
        if let Some(column_metadata) = table_metadata.columns.get1(column_name.as_str()) {
            matches.push((table_metadata, column_metadata));
        }
    }

    match matches.len() {
        0 => Err(ResolveError::ColumnNotFound {
            table: "<unknown>".to_owned(),
            column: column_name.clone(),
        }),
        1 => {
            let (table_metadata, column_metadata) = matches[0];
            Ok(ResolvedColumnNode {
                schema: table_metadata.schema.clone(),
                table: table_metadata.name.clone(),
                column: column_metadata.name.clone(),
                column_metadata: column_metadata.clone(),
            })
        }
        _ => Err(ResolveError::AmbiguousColumn {
            column: column_name.clone(),
        }),
    }
}

/// Resolve a WHERE expression
fn where_expr_resolve(
    expr: &WhereExpr,
    scope: &ResolutionScope,
) -> Result<ResolvedWhereExpr, ResolveError> {
    match expr {
        WhereExpr::Value(lit) => Ok(ResolvedWhereExpr::Value(lit.clone())),
        WhereExpr::Column(col) => {
            let resolved = column_resolve(col, scope)?;
            Ok(ResolvedWhereExpr::Column(resolved))
        }
        WhereExpr::Unary(unary) => {
            let resolved_expr = where_expr_resolve(&unary.expr, scope)?;
            Ok(ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
                op: unary.op,
                expr: Box::new(resolved_expr),
            }))
        }
        WhereExpr::Binary(binary) => {
            let resolved_left = where_expr_resolve(&binary.lexpr, scope)?;
            let resolved_right = where_expr_resolve(&binary.rexpr, scope)?;
            Ok(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: binary.op,
                lexpr: Box::new(resolved_left),
                rexpr: Box::new(resolved_right),
            }))
        }
        WhereExpr::Multi(multi) => {
            let resolved_exprs = multi
                .exprs
                .iter()
                .map(|e| where_expr_resolve(e, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedWhereExpr::Multi(ResolvedMultiExpr {
                op: multi.op,
                exprs: resolved_exprs,
            }))
        }
        WhereExpr::Function { name, args } => {
            let resolved_args = args
                .iter()
                .map(|arg| where_expr_resolve(arg, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedWhereExpr::Function {
                name: name.clone(),
                args: resolved_args,
            })
        }
        WhereExpr::Subquery { .. } => {
            // Subqueries not yet supported
            Err(ResolveError::InvalidTableRef)
        }
    }
}

/// Resolve a table source (table, join, or subquery)
fn table_source_resolve<'a>(
    source: &'a TableSource,
    tables: &'a BiHashMap<TableMetadata>,
    scope: &mut ResolutionScope<'a>,
) -> Result<ResolvedTableSource, ResolveError> {
    match source {
        TableSource::Table(table_node) => {
            let resolved = table_resolve(table_node, tables)?;
            let table_metadata = tables
                .get2(table_node.name.as_str())
                .expect("table should exist after resolution");

            scope.table_scope_add(
                table_metadata,
                table_node.alias.as_ref().map(|a| a.name.as_str()),
            );

            Ok(ResolvedTableSource::Table(resolved))
        }
        TableSource::Join(join_node) => {
            // Resolve left side first and add to scope
            let resolved_left = table_source_resolve(&join_node.left, tables, scope)?;

            // Resolve right side and add to scope
            let resolved_right = table_source_resolve(&join_node.right, tables, scope)?;

            // Resolve join condition using the updated scope
            let resolved_condition = join_node
                .condition
                .as_ref()
                .map(|cond| where_expr_resolve(cond, scope))
                .transpose()?;

            Ok(ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
                join_type: join_node.join_type,
                left: resolved_left,
                right: resolved_right,
                condition: resolved_condition,
            })))
        }
        TableSource::Subquery(_) => {
            // Subqueries not yet supported
            Err(ResolveError::InvalidTableRef)
        }
    }
}

/// Resolve a column expression in SELECT list
fn column_expr_resolve(
    expr: &ColumnExpr,
    scope: &ResolutionScope,
) -> Result<ResolvedColumnExpr, ResolveError> {
    match expr {
        ColumnExpr::Column(col) => {
            let resolved = column_resolve(col, scope)?;
            Ok(ResolvedColumnExpr::Column(resolved))
        }
        ColumnExpr::Literal(lit) => Ok(ResolvedColumnExpr::Literal(lit.clone())),
        ColumnExpr::Function(func) => {
            // Recursively resolve function arguments
            let resolved_args = func
                .args
                .iter()
                .map(|arg| column_expr_resolve(arg, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedColumnExpr::Function {
                name: func.name.clone(),
                args: resolved_args,
            })
        }
        ColumnExpr::Subquery(_) => {
            // Subqueries not yet supported
            Err(ResolveError::InvalidTableRef)
        }
    }
}

/// Resolve SELECT columns
fn select_columns_resolve(
    columns: &SelectColumns,
    scope: &ResolutionScope,
) -> Result<ResolvedSelectColumns, ResolveError> {
    match columns {
        SelectColumns::None => Ok(ResolvedSelectColumns::None),
        SelectColumns::All => {
            // Expand SELECT * to all columns from all tables in scope
            let mut all_columns = Vec::new();
            for (table_metadata, _) in &scope.tables {
                for column_metadata in &table_metadata.columns {
                    all_columns.push(ResolvedColumnNode {
                        schema: table_metadata.schema.clone(),
                        table: table_metadata.name.clone(),
                        column: column_metadata.name.clone(),
                        column_metadata: column_metadata.clone(),
                    });
                }
            }
            Ok(ResolvedSelectColumns::All(all_columns))
        }
        SelectColumns::Columns(cols) => {
            let resolved_cols = cols
                .iter()
                .map(|col| {
                    let resolved_expr = column_expr_resolve(&col.expr, scope)?;
                    Ok(ResolvedSelectColumn {
                        expr: resolved_expr,
                        alias: col.alias.clone(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedSelectColumns::Columns(resolved_cols))
        }
    }
}

/// Resolve a SELECT statement
pub fn select_statement_resolve(
    stmt: &SelectStatement,
    tables: &BiHashMap<TableMetadata>,
) -> Result<ResolvedSelectStatement, ResolveError> {
    let mut scope = ResolutionScope::new();

    // First pass: resolve all table references and build scope
    let mut resolved_from = Vec::new();
    for table_source in &stmt.from {
        let resolved = table_source_resolve(table_source, tables, &mut scope)?;
        resolved_from.push(resolved);
    }

    // Resolve SELECT columns
    let resolved_columns = select_columns_resolve(&stmt.columns, &scope)?;

    // Resolve WHERE clause
    let resolved_where = stmt
        .where_clause
        .as_ref()
        .map(|w| where_expr_resolve(w, &scope))
        .transpose()?;

    // TODO: Resolve GROUP BY, HAVING, ORDER BY, LIMIT
    Ok(ResolvedSelectStatement {
        columns: resolved_columns,
        from: resolved_from,
        where_clause: resolved_where,
        group_by: Vec::new(), // TODO
        having: None,         // TODO
        order_by: Vec::new(), // TODO
        limit: None,          // TODO
        distinct: stmt.distinct,
        values: stmt.values.clone(),
    })
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::Type;

    use super::*;

    #[test]
    fn test_resolved_table_node_construction() {
        let table_node = ResolvedTableNode {
            schema: "public".to_owned(),
            name: "users".to_owned(),
            alias: Some("u".to_owned()),
            relation_oid: 12345,
        };

        assert_eq!(table_node.schema, "public");
        assert_eq!(table_node.name, "users");
        assert_eq!(table_node.alias, Some("u".to_owned()));
        assert_eq!(table_node.relation_oid, 12345);
    }

    #[test]
    fn test_resolved_column_node_construction() {
        let col_node = ResolvedColumnNode {
            schema: "public".to_owned(),
            table: "users".to_owned(),
            column: "id".to_owned(),
            column_metadata: ColumnMetadata {
                name: "id".to_owned(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".to_owned(),
                is_primary_key: true,
            },
        };

        assert_eq!(col_node.schema, "public");
        assert_eq!(col_node.table, "users");
        assert_eq!(col_node.column, "id");
        assert_eq!(col_node.column_metadata.type_name, "int4");
        assert_eq!(col_node.column_metadata.position, 1);
        assert!(col_node.column_metadata.is_primary_key);
    }

    #[test]
    fn test_resolved_select_statement_default() {
        let stmt = ResolvedSelectStatement::default();
        assert!(matches!(stmt.columns, ResolvedSelectColumns::None));
        assert!(stmt.from.is_empty());
        assert!(stmt.where_clause.is_none());
        assert!(stmt.group_by.is_empty());
        assert!(stmt.having.is_none());
        assert!(stmt.order_by.is_empty());
        assert!(stmt.limit.is_none());
        assert!(!stmt.distinct);
    }

    // Helper function to create test table metadata
    fn test_table_metadata(name: &str, relation_oid: u32) -> TableMetadata {
        let mut columns = BiHashMap::new();

        // Add id column
        columns.insert_overwrite(ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".to_owned(),
            is_primary_key: true,
        });

        // Add name column
        columns.insert_overwrite(ColumnMetadata {
            name: "name".to_owned(),
            position: 2,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".to_owned(),
            is_primary_key: false,
        });

        TableMetadata {
            relation_oid,
            name: name.to_owned(),
            schema: "public".to_owned(),
            primary_key_columns: vec!["id".to_owned()],
            columns,
        }
    }

    #[test]
    fn test_table_resolve_simple() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Table(table) = &resolved.from[0] {
            assert_eq!(table.schema, "public");
            assert_eq!(table.name, "users");
            assert_eq!(table.alias, None);
            assert_eq!(table.relation_oid, 1001);
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_table_resolve_with_alias() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users u";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Table(table) = &resolved.from[0] {
            assert_eq!(table.schema, "public");
            assert_eq!(table.name, "users");
            assert_eq!(table.alias, Some("u".to_owned()));
            assert_eq!(table.relation_oid, 1001);
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_table_resolve_not_found() {
        use crate::query::ast::{Statement, sql_query_convert};

        let tables = BiHashMap::new();

        let sql = "SELECT * FROM users";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let result = select_statement_resolve(stmt, &tables);

        assert!(matches!(result, Err(ResolveError::TableNotFound { .. })));
    }

    #[test]
    fn test_column_resolve_qualified() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE users.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check WHERE clause resolved correctly
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
            if let ResolvedWhereExpr::Column(col) = &*binary.lexpr {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "id");
                assert_eq!(col.column_metadata.type_name, "int4");
            } else {
                panic!("Expected column in binary expression");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_column_resolve_with_alias() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users u WHERE u.name = 'john'";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check WHERE clause resolved correctly
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
            if let ResolvedWhereExpr::Column(col) = &*binary.lexpr {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "name");
                assert_eq!(col.column_metadata.type_name, "text");
            } else {
                panic!("Expected column in binary expression");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_column_resolve_unqualified() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check WHERE clause resolved correctly
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
            if let ResolvedWhereExpr::Column(col) = &*binary.lexpr {
                assert_eq!(col.schema, "public");
                assert_eq!(col.table, "users");
                assert_eq!(col.column, "id");
            } else {
                panic!("Expected column in binary expression");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }

    #[test]
    fn test_column_resolve_ambiguous() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // Both tables have 'id' column, unqualified reference is ambiguous
        let sql = "SELECT * FROM users, orders WHERE id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let result = select_statement_resolve(stmt, &tables);

        assert!(matches!(result, Err(ResolveError::AmbiguousColumn { .. })));
    }

    #[test]
    fn test_select_star_expansion() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check that SELECT * was expanded to all columns
        if let ResolvedSelectColumns::All(cols) = &resolved.columns {
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0].column, "id");
            assert_eq!(cols[0].table, "users");
            assert_eq!(cols[1].column, "name");
            assert_eq!(cols[1].table, "users");
        } else {
            panic!("Expected All columns");
        }
    }

    #[test]
    fn test_select_specific_columns() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT id, name FROM users";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check that specific columns were resolved
        if let ResolvedSelectColumns::Columns(cols) = &resolved.columns {
            assert_eq!(cols.len(), 2);

            if let ResolvedColumnExpr::Column(col) = &cols[0].expr {
                assert_eq!(col.column, "id");
                assert_eq!(col.table, "users");
            } else {
                panic!("Expected column expression");
            }

            if let ResolvedColumnExpr::Column(col) = &cols[1].expr {
                assert_eq!(col.column, "name");
                assert_eq!(col.table, "users");
            } else {
                panic!("Expected column expression");
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_join_resolution() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.id";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check that JOIN was resolved
        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Join(join) = &resolved.from[0] {
            assert_eq!(join.join_type, JoinType::Inner);

            // Check left side
            if let ResolvedTableSource::Table(left) = &join.left {
                assert_eq!(left.name, "users");
            } else {
                panic!("Expected table on left side");
            }

            // Check right side
            if let ResolvedTableSource::Table(right) = &join.right {
                assert_eq!(right.name, "orders");
            } else {
                panic!("Expected table on right side");
            }

            // Check join condition
            if let Some(ResolvedWhereExpr::Binary(cond)) = &join.condition {
                if let ResolvedWhereExpr::Column(left_col) = &*cond.lexpr {
                    assert_eq!(left_col.table, "users");
                    assert_eq!(left_col.column, "id");
                }
                if let ResolvedWhereExpr::Column(right_col) = &*cond.rexpr {
                    assert_eq!(right_col.table, "orders");
                    assert_eq!(right_col.column, "id");
                }
            } else {
                panic!("Expected binary join condition");
            }
        } else {
            panic!("Expected join source");
        }
    }

    #[test]
    fn test_join_with_aliases() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.id";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check that JOIN with aliases was resolved
        if let ResolvedTableSource::Join(join) = &resolved.from[0] {
            // Check left side has alias
            if let ResolvedTableSource::Table(left) = &join.left {
                assert_eq!(left.name, "users");
                assert_eq!(left.alias, Some("u".to_owned()));
            } else {
                panic!("Expected table on left side");
            }

            // Check right side has alias
            if let ResolvedTableSource::Table(right) = &join.right {
                assert_eq!(right.name, "orders");
                assert_eq!(right.alias, Some("o".to_owned()));
            } else {
                panic!("Expected table on right side");
            }

            // Check join condition uses aliases
            if let Some(ResolvedWhereExpr::Binary(cond)) = &join.condition {
                if let ResolvedWhereExpr::Column(left_col) = &*cond.lexpr {
                    // Should resolve to 'users' table even though alias 'u' was used
                    assert_eq!(left_col.table, "users");
                    assert_eq!(left_col.column, "id");
                }
                if let ResolvedWhereExpr::Column(right_col) = &*cond.rexpr {
                    // Should resolve to 'orders' table even though alias 'o' was used
                    assert_eq!(right_col.table, "orders");
                    assert_eq!(right_col.column, "id");
                }
            }
        } else {
            panic!("Expected join source");
        }
    }

    #[test]
    fn test_where_expr_complex() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 AND name = 'john'";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        // Check that complex WHERE was resolved
        if let Some(ResolvedWhereExpr::Binary(and_expr)) = &resolved.where_clause {
            assert_eq!(and_expr.op, ExprOp::And);

            // Left side: id = 1
            if let ResolvedWhereExpr::Binary(left_binary) = &*and_expr.lexpr {
                assert_eq!(left_binary.op, ExprOp::Equal);
                if let ResolvedWhereExpr::Column(col) = &*left_binary.lexpr {
                    assert_eq!(col.column, "id");
                }
            } else {
                panic!("Expected binary expression on left");
            }

            // Right side: name = 'john'
            if let ResolvedWhereExpr::Binary(right_binary) = &*and_expr.rexpr {
                assert_eq!(right_binary.op, ExprOp::Equal);
                if let ResolvedWhereExpr::Column(col) = &*right_binary.lexpr {
                    assert_eq!(col.column, "name");
                }
            } else {
                panic!("Expected binary expression on right");
            }
        } else {
            panic!("Expected binary WHERE expression");
        }
    }
}

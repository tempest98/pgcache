use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use error_set::error_set;
use pg_query::ParseResult;
use pg_query::protobuf::{
    ColumnRef as PgColumnRef, Node, RangeVar, SelectStmt, node::Node as NodeEnum,
};

use super::parse::{WhereParseError, query_where_clause_parse};

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

// Core literal value types that can appear in SQL expressions
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Parameter(String), // For $1, $2, etc.
}

// Custom Hash implementation for LiteralValue to handle f64
impl std::hash::Hash for LiteralValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            LiteralValue::String(s) => {
                0u8.hash(state);
                s.hash(state);
            }
            LiteralValue::Integer(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            LiteralValue::Float(f) => {
                2u8.hash(state);
                // Convert f64 to bits for hashing to handle NaN/infinity consistently
                f.to_bits().hash(state);
            }
            LiteralValue::Boolean(b) => {
                3u8.hash(state);
                b.hash(state);
            }
            LiteralValue::Null => {
                4u8.hash(state);
            }
            LiteralValue::Parameter(p) => {
                5u8.hash(state);
                p.hash(state);
            }
        }
    }
}

// Column reference (potentially qualified: table.column)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

// Operators for WHERE expressions
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum WhereOp {
    // Logical operators
    And,
    Or,
    Not,

    // Comparison operators
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Pattern matching
    Like,
    ILike,
    NotLike,
    NotILike,

    // Set operations
    In,
    NotIn,

    // Range operations
    Between,
    NotBetween,

    // Null checks
    IsNull,
    IsNotNull,

    // Array operations
    Any,
    All,

    // Existence checks
    Exists,
    NotExists,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct UnaryExpr {
    pub op: WhereOp,
    pub expr: Box<WhereExpr>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BinaryExpr {
    pub op: WhereOp,
    pub lexpr: Box<WhereExpr>, // left expression
    pub rexpr: Box<WhereExpr>, // right expression
}

// Multi-operand expressions (for IN, BETWEEN, etc.)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct MultiExpr {
    pub op: WhereOp,
    pub exprs: Vec<WhereExpr>,
}

// WHERE expression tree - more abstract and flexible
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum WhereExpr {
    // Leaf nodes
    Value(LiteralValue),
    Column(ColumnRef),

    // Expression nodes
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Multi(MultiExpr),

    // Function calls (for extensibility)
    Function {
        name: String,
        args: Vec<WhereExpr>,
    },

    // Subqueries (for future support)
    Subquery {
        query: String, // Placeholder for now
    },
}

/// Simplified SQL AST focused on caching use cases
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SqlQuery {
    pub statement: Statement,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Statement {
    Select(SelectStatement),
    // Future: Insert, Update, Delete for CDC
}

#[derive(Debug, Clone, PartialEq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumns {
    All,                        // SELECT *
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ...
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectColumn {
    pub expr: ColumnExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ColumnExpr {
    Column(ColumnRef),      // column_name, table.column_name
    Function(FunctionCall), // COUNT(*), SUM(col), etc.
    Literal(LiteralValue),  // Constant values
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<ColumnExpr>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
    pub alias: Option<String>,
    pub join: Option<JoinClause>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: Box<TableRef>,
    pub condition: Option<WhereExpr>,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct OrderByClause {
    pub expr: ColumnExpr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Hash)]
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
    let schema = if range_var.schemaname.is_empty() {
        None
    } else {
        Some(range_var.schemaname.clone())
    };

    let name = range_var.relname.clone();

    let alias = range_var
        .alias
        .as_ref()
        .map(|alias_node| alias_node.aliasname.clone());

    Ok(TableRef {
        schema,
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

impl SelectStatement {
    /// Check if this SELECT statement references only a single table
    pub fn is_single_table(&self) -> bool {
        self.from.len() == 1 && self.from[0].join.is_none()
    }

    /// Check if this SELECT statement contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        // Check columns for subqueries
        if let SelectColumns::Columns(columns) = &self.columns {
            if columns
                .iter()
                .any(|col| self.column_expr_has_sublink(&col.expr))
            {
                return true;
            }
        }

        // Check WHERE clause for subqueries
        if let Some(where_clause) = &self.where_clause {
            if self.where_expr_has_sublink(where_clause) {
                return true;
            }
        }

        // Check HAVING clause for subqueries
        if let Some(having) = &self.having {
            if self.where_expr_has_sublink(having) {
                return true;
            }
        }

        false
    }

    #[allow(clippy::only_used_in_recursion)]
    fn column_expr_has_sublink(&self, expr: &ColumnExpr) -> bool {
        match expr {
            ColumnExpr::Function(func) => func
                .args
                .iter()
                .any(|arg| self.column_expr_has_sublink(arg)),
            _ => false, // Column references and literals don't contain sublinks
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn where_expr_has_sublink(&self, expr: &WhereExpr) -> bool {
        match expr {
            WhereExpr::Binary(binary) => {
                self.where_expr_has_sublink(&binary.lexpr)
                    || self.where_expr_has_sublink(&binary.rexpr)
            }
            WhereExpr::Unary(unary) => self.where_expr_has_sublink(&unary.expr),
            WhereExpr::Multi(multi) => multi.exprs.iter().any(|e| self.where_expr_has_sublink(e)),
            WhereExpr::Function { args, .. } => args.iter().any(|e| self.where_expr_has_sublink(e)),
            WhereExpr::Subquery { .. } => true, // Found a subquery!
            _ => false,                         // Value and Column don't contain sublinks
        }
    }
}

/// Create a fingerprint hash for SQL query AST.
/// This is faster and more normalized than string-based fingerprinting.
pub fn ast_query_fingerprint(sql_query: &SqlQuery) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql_query.hash(&mut hasher);
    hasher.finish()
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

use std::any::Any;

use error_set::error_set;
use iddqd::BiHashMap;

use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::query::ast::{
    ColumnExpr, ColumnNode, Deparse, ExprOp, JoinType, LiteralValue, OrderDirection, SelectColumns,
    SelectStatement, TableAlias, TableNode, TableSource, WhereExpr,
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

impl ResolvedTableNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for ResolvedTableNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        buf.push_str(&self.schema);
        buf.push('.');
        buf.push_str(&self.name);
        if let Some(alias) = &self.alias {
            buf.push(' ');
            buf.push_str(alias);
        }
        buf
    }
}

/// Resolved column reference with type information
///
/// Note: PartialEq and Hash are implemented manually to exclude `table_alias`
/// since aliases are for deparsing only and don't affect column identity.
#[derive(Debug, Clone, Eq)]
pub struct ResolvedColumnNode {
    /// Schema name where the table is located
    pub schema: String,
    /// Table name (not alias) where column is defined
    pub table: String,
    /// Table alias if one was used in the query (for deparsing only, not included in equality)
    pub table_alias: Option<String>,
    /// Column name
    pub column: String,
    /// Column metadata from catalog (includes type info, position, primary key status, etc.)
    pub column_metadata: ColumnMetadata,
}

impl PartialEq for ResolvedColumnNode {
    fn eq(&self, other: &Self) -> bool {
        // Exclude table_alias from equality - it's only for deparsing
        self.schema == other.schema
            && self.table == other.table
            && self.column == other.column
            && self.column_metadata == other.column_metadata
    }
}

impl std::hash::Hash for ResolvedColumnNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Exclude table_alias from hash - it's only for deparsing
        self.schema.hash(state);
        self.table.hash(state);
        self.column.hash(state);
        self.column_metadata.hash(state);
    }
}

impl ResolvedColumnNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for ResolvedColumnNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        // Use alias if available, otherwise use schema.table
        if let Some(alias) = &self.table_alias {
            buf.push_str(alias);
        } else {
            buf.push_str(&self.schema);
            buf.push('.');
            buf.push_str(&self.table);
        }
        buf.push('.');
        buf.push_str(&self.column);
        buf
    }
}

/// Resolved unary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedUnaryExpr {
    pub op: ExprOp,
    pub expr: Box<ResolvedWhereExpr>,
}

impl ResolvedUnaryExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

/// Resolved binary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedBinaryExpr {
    pub op: ExprOp,
    pub lexpr: Box<ResolvedWhereExpr>,
    pub rexpr: Box<ResolvedWhereExpr>,
}

impl ResolvedBinaryExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.lexpr.nodes().chain(self.rexpr.nodes());
        current.chain(children)
    }
}

/// Resolved multi-operand expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMultiExpr {
    pub op: ExprOp,
    pub exprs: Vec<ResolvedWhereExpr>,
}

impl ResolvedMultiExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.exprs.iter().flat_map(|expr| expr.nodes());
        current.chain(children)
    }
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

impl ResolvedWhereExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedWhereExpr::Value(lit) => Box::new(lit.nodes()),
            ResolvedWhereExpr::Column(col) => Box::new(col.nodes()),
            ResolvedWhereExpr::Unary(unary) => Box::new(unary.nodes()),
            ResolvedWhereExpr::Binary(binary) => Box::new(binary.nodes()),
            ResolvedWhereExpr::Multi(multi) => Box::new(multi.nodes()),
            ResolvedWhereExpr::Function { args, .. } => {
                Box::new(args.iter().flat_map(|arg| arg.nodes()))
            }
            ResolvedWhereExpr::Subquery { query } => Box::new(query.nodes()),
        };
        current.chain(children)
    }
}

impl Deparse for ResolvedWhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedWhereExpr::Value(lit) => lit.deparse(buf),
            ResolvedWhereExpr::Column(col) => col.deparse(buf),
            ResolvedWhereExpr::Unary(unary) => {
                unary.op.deparse(buf);
                buf.push(' ');
                unary.expr.deparse(buf);
                buf
            }
            ResolvedWhereExpr::Binary(binary) => {
                binary.lexpr.deparse(buf);
                buf.push(' ');
                binary.op.deparse(buf);
                buf.push(' ');
                binary.rexpr.deparse(buf);
                buf
            }
            #[allow(clippy::wildcard_enum_match_arm)]
            ResolvedWhereExpr::Multi(multi) => {
                let mut sep = "";
                for expr in &multi.exprs {
                    buf.push_str(sep);
                    expr.deparse(buf);
                    sep = match multi.op {
                        ExprOp::And => " AND ",
                        ExprOp::Or => " OR ",
                        _ => ", ",
                    };
                }
                buf
            }
            ResolvedWhereExpr::Function { name, args } => {
                buf.push_str(name);
                buf.push('(');
                let mut sep = "";
                for arg in args {
                    buf.push_str(sep);
                    arg.deparse(buf);
                    sep = ", ";
                }
                buf.push(')');
                buf
            }
            ResolvedWhereExpr::Subquery { query } => {
                buf.push('(');
                query.deparse(buf);
                buf.push(')');
                buf
            }
        }
    }
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

impl ResolvedColumnExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedColumnExpr::Column(col) => Box::new(col.nodes()),
            ResolvedColumnExpr::Literal(lit) => Box::new(lit.nodes()),
            ResolvedColumnExpr::Function { args, .. } => {
                Box::new(args.iter().flat_map(|arg| arg.nodes()))
            }
            ResolvedColumnExpr::Subquery(query) => Box::new(query.nodes()),
        };
        current.chain(children)
    }
}

impl Deparse for ResolvedColumnExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedColumnExpr::Column(col) => col.deparse(buf),
            ResolvedColumnExpr::Literal(lit) => lit.deparse(buf),
            ResolvedColumnExpr::Function { name, args } => {
                buf.push_str(name);
                buf.push('(');
                let mut sep = "";
                for arg in args {
                    buf.push_str(sep);
                    arg.deparse(buf);
                    sep = ", ";
                }
                buf.push(')');
                buf
            }
            ResolvedColumnExpr::Subquery(query) => {
                buf.push('(');
                query.deparse(buf);
                buf.push(')');
                buf
            }
        }
    }
}

/// Resolved SELECT column with optional alias
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectColumn {
    pub expr: ResolvedColumnExpr,
    pub alias: Option<String>,
}

impl ResolvedSelectColumn {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

impl Deparse for ResolvedSelectColumn {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.expr.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push_str(" AS ");
            buf.push_str(alias);
        }
        buf
    }
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

impl ResolvedSelectColumns {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedSelectColumns::None => Box::new(std::iter::empty()),
            ResolvedSelectColumns::All(cols) => Box::new(cols.iter().flat_map(|col| col.nodes())),
            ResolvedSelectColumns::Columns(cols) => {
                Box::new(cols.iter().flat_map(|col| col.nodes()))
            }
        };
        current.chain(children)
    }
}

impl Deparse for ResolvedSelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedSelectColumns::None => buf.push(' '),
            ResolvedSelectColumns::All(_) => {
                buf.push_str(" *");
            }
            ResolvedSelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ",";
                }
            }
        }
        buf
    }
}

/// Resolved table source
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedTableSource {
    /// Direct table reference
    Table(ResolvedTableNode),
    /// Resolved subquery
    Subquery(ResolvedTableSubqueryNode),
    /// Resolved join
    Join(Box<ResolvedJoinNode>),
}

impl ResolvedTableSource {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedTableSource::Table(table) => Box::new(table.nodes()),
            ResolvedTableSource::Subquery(subquery) => Box::new(subquery.nodes()),
            ResolvedTableSource::Join(join) => Box::new(join.nodes()),
        };
        current.chain(children)
    }
}

impl Deparse for ResolvedTableSource {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedTableSource::Table(table) => table.deparse(buf),
            ResolvedTableSource::Join(join) => join.deparse(buf),
            ResolvedTableSource::Subquery(subquery) => subquery.deparse(buf),
        }
    }
}

/// Resolved subquery table source
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTableSubqueryNode {
    pub select: Box<ResolvedSelectStatement>,
    pub alias: TableAlias,
}

impl ResolvedTableSubqueryNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.select.nodes();
        current.chain(children)
    }
}

impl Deparse for ResolvedTableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(" (");
        self.select.deparse(buf);
        buf.push_str(") ");
        self.alias.deparse(buf);
        buf
    }
}

/// Resolved JOIN node
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedJoinNode {
    pub join_type: JoinType,
    pub left: ResolvedTableSource,
    pub right: ResolvedTableSource,
    pub condition: Option<ResolvedWhereExpr>,
}

impl ResolvedJoinNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_nodes = self.left.nodes();
        let right_nodes = self.right.nodes();
        let condition_nodes = self.condition.iter().flat_map(|c| c.nodes());
        current
            .chain(left_nodes)
            .chain(right_nodes)
            .chain(condition_nodes)
    }
}

impl Deparse for ResolvedJoinNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);

        match self.join_type {
            JoinType::Inner => buf.push_str(" JOIN"),
            JoinType::Left => buf.push_str(" LEFT JOIN"),
            JoinType::Right => buf.push_str(" RIGHT JOIN"),
            JoinType::Full => buf.push_str(" FULL JOIN"),
        }

        self.right.deparse(buf);

        if let Some(condition) = &self.condition {
            buf.push_str(" ON ");
            condition.deparse(buf);
        }

        buf
    }
}

/// Resolved ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOrderByClause {
    pub expr: ResolvedColumnExpr,
    pub direction: OrderDirection,
}

impl ResolvedOrderByClause {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

impl Deparse for ResolvedOrderByClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.expr.deparse(buf);
        match self.direction {
            OrderDirection::Asc => buf.push_str(" ASC"),
            OrderDirection::Desc => buf.push_str(" DESC"),
        }
        buf
    }
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

impl ResolvedSelectStatement {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let columns_nodes = self.columns.nodes();
        let from_nodes = self.from.iter().flat_map(|t| t.nodes());
        let where_nodes = self.where_clause.iter().flat_map(|w| w.nodes());
        let group_by_nodes = self.group_by.iter().flat_map(|c| c.nodes());
        let having_nodes = self.having.iter().flat_map(|h| h.nodes());
        let order_by_nodes = self.order_by.iter().flat_map(|o| o.nodes());

        current
            .chain(columns_nodes)
            .chain(from_nodes)
            .chain(where_nodes)
            .chain(group_by_nodes)
            .chain(having_nodes)
            .chain(order_by_nodes)
    }

    /// Check if this SELECT statement references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [ResolvedTableSource::Table(_)])
    }
}

impl Deparse for ResolvedSelectStatement {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        // If values is present, output VALUES clause
        if !self.values.is_empty() {
            buf.push_str("VALUES ");
            let mut row_sep = "";
            buf.push('(');
            for row in &self.values {
                buf.push_str(row_sep);
                let mut sep = "";
                for value in row {
                    buf.push_str(sep);
                    value.deparse(buf);
                    sep = ", ";
                }
                row_sep = "), (";
            }
            buf.push(')');
        } else {
            buf.push_str("SELECT");
            if self.distinct {
                buf.push_str(" DISTINCT");
            }
            self.columns.deparse(buf);

            if !self.from.is_empty() {
                buf.push_str(" FROM");
                let mut sep = "";
                for table in &self.from {
                    buf.push_str(sep);
                    table.deparse(buf);
                    sep = ",";
                }
            }

            if let Some(expr) = &self.where_clause {
                buf.push_str(" WHERE ");
                expr.deparse(buf);
            }

            if !self.group_by.is_empty() {
                buf.push_str(" GROUP BY ");
                let mut sep = "";
                for col in &self.group_by {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ", ";
                }
            }

            if let Some(expr) = &self.having {
                buf.push_str(" HAVING ");
                expr.deparse(buf);
            }

            if !self.order_by.is_empty() {
                buf.push_str(" ORDER BY");
                let mut sep = "";
                for order in &self.order_by {
                    buf.push_str(sep);
                    buf.push(' ');
                    order.deparse(buf);
                    sep = ",";
                }
            }

            if let Some(limit) = &self.limit {
                if let Some(count) = limit.count {
                    buf.push_str(" LIMIT ");
                    buf.push_str(&count.to_string());
                }
                if let Some(offset) = limit.offset {
                    buf.push_str(" OFFSET ");
                    buf.push_str(&offset.to_string());
                }
            }
        }

        buf
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

impl Deparse for ResolvedSqlQuery {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match &self.statement {
            ResolvedStatement::Select(select) => select.deparse(buf),
        }
    }
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

/// Find table metadata for a table reference.
///
/// If the table has an explicit schema qualifier, use it directly.
/// Otherwise, search through the search_path schemas in order.
fn table_metadata_find<'map, 'node: 'map>(
    table_node: &'node TableNode,
    tables: &'map BiHashMap<TableMetadata>,
    search_path: &[&'map str],
) -> Option<&'map TableMetadata> {
    let table_name = table_node.name.as_str();

    // If table has explicit schema, use it directly
    if let Some(schema) = &table_node.schema {
        let table_metadata = tables.get2(&(schema.as_str(), table_name))?;
        return Some(table_metadata);
    }

    // Search through search_path schemas in order
    for schema in search_path {
        if let Some(table_metadata) = tables.get2(&(*schema, table_name)) {
            return Some(table_metadata);
        }
    }

    None
}

/// Resolve a column reference to a resolved column node
fn column_resolve<'a>(
    column_node: &ColumnNode,
    scope: &ResolutionScope<'a>,
) -> Result<ResolvedColumnNode, ResolveError> {
    let column_name = &column_node.column;

    // If column has table qualifier, resolve directly
    if let Some(table_qualifier) = &column_node.table {
        let (table_metadata, alias) =
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
            table_alias: alias.map(str::to_owned),
            column: column_metadata.name.clone(),
            column_metadata: column_metadata.clone(),
        });
    }

    // Unqualified column - search all tables in scope
    let mut matches = Vec::new();
    for (table_metadata, alias) in &scope.tables {
        if let Some(column_metadata) = table_metadata.columns.get1(column_name.as_str()) {
            matches.push((table_metadata, *alias, column_metadata));
        }
    }

    match matches.as_slice() {
        [] => Err(ResolveError::ColumnNotFound {
            table: "<unknown>".to_owned(),
            column: column_name.clone(),
        }),
        [(table_metadata, alias, column_metadata)] => Ok(ResolvedColumnNode {
            schema: table_metadata.schema.clone(),
            table: table_metadata.name.clone(),
            table_alias: alias.map(str::to_owned),
            column: column_metadata.name.clone(),
            column_metadata: (*column_metadata).clone(),
        }),
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
    search_path: &[&'a str],
) -> Result<ResolvedTableSource, ResolveError> {
    match source {
        TableSource::Table(table_node) => {
            // First find the table metadata (which gives us the schema)
            let table_metadata =
                table_metadata_find(table_node, tables, search_path).ok_or_else(|| {
                    ResolveError::TableNotFound {
                        name: table_node.name.clone(),
                    }
                })?;

            scope.table_scope_add(
                table_metadata,
                table_node.alias.as_ref().map(|a| a.name.as_str()),
            );

            let resolved = ResolvedTableNode {
                schema: table_metadata.schema.clone(),
                name: table_metadata.name.clone(),
                alias: table_node.alias.as_ref().map(|a| a.name.clone()),
                relation_oid: table_metadata.relation_oid,
            };

            Ok(ResolvedTableSource::Table(resolved))
        }
        TableSource::Join(join_node) => {
            // Resolve left side first and add to scope
            let resolved_left = table_source_resolve(&join_node.left, tables, scope, search_path)?;

            // Resolve right side and add to scope
            let resolved_right =
                table_source_resolve(&join_node.right, tables, scope, search_path)?;

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
            for (table_metadata, alias) in &scope.tables {
                for column_metadata in &table_metadata.columns {
                    all_columns.push(ResolvedColumnNode {
                        schema: table_metadata.schema.clone(),
                        table: table_metadata.name.clone(),
                        table_alias: alias.map(str::to_owned),
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

/// Resolve ORDER BY clauses
fn order_by_resolve(
    order_by: &[crate::query::ast::OrderByClause],
    scope: &ResolutionScope,
) -> Result<Vec<ResolvedOrderByClause>, ResolveError> {
    order_by
        .iter()
        .map(|clause| {
            let resolved_expr = column_expr_resolve(&clause.expr, scope)?;
            Ok(ResolvedOrderByClause {
                expr: resolved_expr,
                direction: clause.direction.clone(),
            })
        })
        .collect()
}

/// Resolve a SELECT statement
pub fn select_statement_resolve(
    stmt: &SelectStatement,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> Result<ResolvedSelectStatement, ResolveError> {
    let mut scope = ResolutionScope::new();

    // First pass: resolve all table references and build scope
    let mut resolved_from = Vec::new();
    for table_source in &stmt.from {
        let resolved = table_source_resolve(table_source, tables, &mut scope, search_path)?;
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

    // Resolve ORDER BY clause
    let resolved_order_by = order_by_resolve(&stmt.order_by, &scope)?;

    // TODO: Resolve GROUP BY, HAVING, LIMIT
    Ok(ResolvedSelectStatement {
        columns: resolved_columns,
        from: resolved_from,
        where_clause: resolved_where,
        group_by: Vec::new(), // TODO
        having: None,         // TODO
        order_by: resolved_order_by,
        limit: None, // TODO
        distinct: stmt.distinct,
        values: stmt.values.clone(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::unwrap_used)]

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
            table_alias: Some("u".to_owned()),
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
        assert_eq!(col_node.table_alias, Some("u".to_owned()));
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
            indexes: Vec::new(),
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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let result = select_statement_resolve(stmt, &tables, &["public"]);

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let result = select_statement_resolve(stmt, &tables, &["public"]);

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

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

    #[test]
    fn test_order_by_simple() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users ORDER BY name ASC";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        // Check ORDER BY was resolved
        assert_eq!(resolved.order_by.len(), 1);
        assert_eq!(resolved.order_by[0].direction, OrderDirection::Asc);

        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.schema, "public");
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
            assert_eq!(col.column_metadata.type_name, "text");
        } else {
            panic!("Expected column expression in ORDER BY");
        }
    }

    #[test]
    fn test_order_by_multiple_columns() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users ORDER BY name ASC, id DESC";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        // Check ORDER BY was resolved
        assert_eq!(resolved.order_by.len(), 2);

        // First column: name ASC
        assert_eq!(resolved.order_by[0].direction, OrderDirection::Asc);
        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.column, "name");
            assert_eq!(col.table, "users");
        } else {
            panic!("Expected column expression");
        }

        // Second column: id DESC
        assert_eq!(resolved.order_by[1].direction, OrderDirection::Desc);
        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[1].expr {
            assert_eq!(col.column, "id");
            assert_eq!(col.table, "users");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_qualified_column() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users u ORDER BY u.name DESC";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        // Check ORDER BY was resolved with qualified column
        assert_eq!(resolved.order_by.len(), 1);
        assert_eq!(resolved.order_by[0].direction, OrderDirection::Desc);

        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[0].expr {
            // Should resolve to actual table name, not alias
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
            assert_eq!(col.schema, "public");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_with_join() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql =
            "SELECT * FROM users u JOIN orders o ON u.id = o.id ORDER BY u.name ASC, o.id DESC";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        // Check ORDER BY was resolved across joined tables
        assert_eq!(resolved.order_by.len(), 2);

        // First: u.name ASC
        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column expression");
        }

        // Second: o.id DESC
        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[1].expr {
            assert_eq!(col.table, "orders");
            assert_eq!(col.column, "id");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_unqualified_column() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users ORDER BY name";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        // Check unqualified ORDER BY column was resolved
        assert_eq!(resolved.order_by.len(), 1);
        if let ResolvedColumnExpr::Column(col) = &resolved.order_by[0].expr {
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_column_not_found() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users ORDER BY nonexistent_column ASC";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let result = select_statement_resolve(stmt, &tables, &["public"]);

        // Should fail with column not found error
        assert!(matches!(result, Err(ResolveError::ColumnNotFound { .. })));
    }

    // ==================== Deparse Tests ====================

    fn id_column_metadata() -> ColumnMetadata {
        ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".to_owned(),
            is_primary_key: true,
        }
    }

    #[test]
    fn test_resolved_column_node_deparse_with_alias() {
        let mut buf = String::new();

        // Column with alias - should use alias
        ResolvedColumnNode {
            schema: "public".to_owned(),
            table: "users".to_owned(),
            table_alias: Some("u".to_owned()),
            column: "id".to_owned(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "u.id");
    }

    #[test]
    fn test_resolved_column_node_deparse_without_alias() {
        let mut buf = String::new();

        // Column without alias - should use schema.table
        ResolvedColumnNode {
            schema: "public".to_owned(),
            table: "users".to_owned(),
            table_alias: None,
            column: "id".to_owned(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "public.users.id");
    }

    #[test]
    fn test_resolved_table_node_deparse_with_alias() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "public".to_owned(),
            name: "users".to_owned(),
            alias: Some("u".to_owned()),
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users u");
    }

    #[test]
    fn test_resolved_table_node_deparse_without_alias() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "public".to_owned(),
            name: "users".to_owned(),
            alias: None,
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users");
    }

    #[test]
    fn test_resolved_select_deparse_with_where() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // SELECT * is preserved, table and column references are fully qualified
        assert_eq!(buf, "SELECT * FROM public.users WHERE public.users.id = 1");
    }

    #[test]
    fn test_resolved_select_deparse_with_alias() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT u.id, u.name FROM users u WHERE u.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // With alias, uses alias.column
        assert_eq!(
            buf,
            "SELECT u.id, u.name FROM public.users u WHERE u.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_join() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql = "SELECT u.id, o.name FROM users u JOIN orders o ON u.id = o.id WHERE u.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT u.id, o.name FROM public.users u JOIN public.orders o ON u.id = o.id WHERE u.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_order_by() {
        use crate::query::ast::{Statement, sql_query_convert};

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT id FROM users u ORDER BY name DESC";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();

        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables, &["public"]).unwrap();

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(buf, "SELECT u.id FROM public.users u ORDER BY u.name DESC");
    }

    #[test]
    fn test_resolved_column_equality_ignores_alias() {
        // Two columns with same schema/table/column but different aliases should be equal
        let col1 = ResolvedColumnNode {
            schema: "public".to_owned(),
            table: "users".to_owned(),
            table_alias: Some("u".to_owned()),
            column: "id".to_owned(),
            column_metadata: id_column_metadata(),
        };

        let col2 = ResolvedColumnNode {
            schema: "public".to_owned(),
            table: "users".to_owned(),
            table_alias: Some("u2".to_owned()), // Different alias
            column: "id".to_owned(),
            column_metadata: id_column_metadata(),
        };

        assert_eq!(col1, col2);

        // Hash should also be equal
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        col1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        col2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }
}

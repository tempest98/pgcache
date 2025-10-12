use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::convert::AsRef;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use error_set::error_set;
use ordered_float::NotNan;
use pg_query::ParseResult;
use pg_query::protobuf::{ColumnRef, Node, RangeVar, SelectStmt, node::Node as NodeEnum};
use pg_query::protobuf::{JoinExpr, RangeSubselect};
use postgres_protocol::escape;
use strum_macros::AsRefStr;

use crate::query::parse::{const_value_extract, node_convert_to_expr, select_stmt_parse_where};

use super::parse::WhereParseError;

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
        #[display("Unsupported feature: {feature}")]
        UnsupportedFeature { feature: String },
        #[display("Invalid table reference")]
        InvalidTableRef,
        UnsupportedJoinType,
        WhereParseError(WhereParseError),
    };
}

pub trait Deparse {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String;
}

// Core literal value types that can appear in SQL expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LiteralValue {
    String(String),
    StringWithCast(String, String),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    Null,
    Parameter(String), // For $1, $2, etc.
}

impl LiteralValue {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }

    /// Check if an Option<String> (from CDC row data) matches this LiteralValue.
    /// Parses the string value according to the type of this LiteralValue.
    pub fn matches(&self, row_value: &Option<String>) -> bool {
        match (row_value, self) {
            (None, LiteralValue::Null) => true,
            (None, _) => false,
            (Some(row_str), LiteralValue::String(constraint_str)) => row_str == constraint_str,
            (Some(row_str), LiteralValue::StringWithCast(constraint_str, _)) => {
                row_str == constraint_str
            }
            (Some(row_str), LiteralValue::Integer(constraint_int)) => {
                row_str.parse::<i64>().ok() == Some(*constraint_int)
            }
            (Some(row_str), LiteralValue::Float(constraint_float)) => {
                row_str
                    .parse::<f64>()
                    .ok()
                    .and_then(|f| NotNan::new(f).ok())
                    == Some(*constraint_float)
            }
            (Some(row_str), LiteralValue::Boolean(constraint_bool)) => {
                row_str.parse::<bool>().ok() == Some(*constraint_bool)
            }
            (Some(_), LiteralValue::Null) => false,
            (Some(_), LiteralValue::Parameter(_)) => false, // Parameters shouldn't appear in constraints
        }
    }
}

impl Deparse for LiteralValue {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            LiteralValue::String(s) => {
                let escaped = escape::escape_literal(s);
                // Remove leading space if escape_literal added one
                if escaped.starts_with(" E'") {
                    buf.push_str(&escaped[1..]); // Skip the first space
                } else {
                    buf.push_str(&escaped);
                }
            }
            LiteralValue::StringWithCast(s, cast) => {
                let escaped = escape::escape_literal(s);
                // Remove leading space if escape_literal added one
                if escaped.starts_with(" E'") {
                    buf.push_str(&escaped[1..]); // Skip the first space
                } else {
                    buf.push_str(&escaped);
                }
                buf.push_str("::");
                buf.push_str(cast);
            }
            LiteralValue::Integer(i) => {
                buf.push_str(i.to_string().as_str());
            }
            LiteralValue::Float(f) => {
                buf.push_str(f.into_inner().to_string().as_str());
            }
            LiteralValue::Boolean(b) => {
                buf.push_str(if *b { "true" } else { "false" });
            }
            LiteralValue::Null => {
                buf.push_str("NULL");
            }
            LiteralValue::Parameter(p) => {
                buf.push_str(p);
            }
        };

        buf
    }
}

// Column reference (potentially qualified: table.column)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ColumnNode {
    pub table: Option<String>,
    pub column: String,
}

impl ColumnNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for ColumnNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if let Some(table) = &self.table {
            buf.push_str(table);
            buf.push('.');
        }
        buf.push_str(&self.column);

        buf
    }
}

// Operators for WHERE expressions
#[derive(Debug, Clone, Copy, PartialEq, Hash, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ExprOp {
    // Logical operators
    And,
    Or,
    Not,

    // Comparison operators
    #[strum(to_string = "=")]
    Equal,
    #[strum(to_string = "!=")]
    NotEqual,
    #[strum(to_string = "<")]
    LessThan,
    #[strum(to_string = "<=")]
    LessThanOrEqual,
    #[strum(to_string = ">")]
    GreaterThan,
    #[strum(to_string = ">=")]
    GreaterThanOrEqual,

    // Pattern matching
    Like,
    ILike,
    #[strum(to_string = "NOT LIKE")]
    NotLike,
    #[strum(to_string = "NOT ILIKE")]
    NotILike,

    // Set operations
    In,
    #[strum(to_string = "NOT IN")]
    NotIn,

    // Range operations
    Between,
    #[strum(to_string = "NOT BETWEEN")]
    NotBetween,

    // Null checks
    #[strum(to_string = "IS NULL")]
    IsNull,
    #[strum(to_string = "IS NOT NULL")]
    IsNotNull,

    // Array operations
    Any,
    All,

    // Existence checks
    Exists,
    #[strum(to_string = "NOT EXISTS")]
    NotExists,
}

impl Deparse for ExprOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(self.as_ref());

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct UnaryExpr {
    pub op: ExprOp,
    pub expr: Box<WhereExpr>,
}

impl UnaryExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

impl Deparse for UnaryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.op.deparse(buf);
        buf.push(' ');
        self.expr.deparse(buf);

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BinaryExpr {
    pub op: ExprOp,
    pub lexpr: Box<WhereExpr>, // left expression
    pub rexpr: Box<WhereExpr>, // right expression
}

impl BinaryExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.lexpr.nodes().chain(self.rexpr.nodes());
        current.chain(children)
    }
}

impl Deparse for BinaryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.lexpr.deparse(buf);
        buf.push(' ');
        self.op.deparse(buf);
        buf.push(' ');
        self.rexpr.deparse(buf);

        buf
    }
}

// Multi-operand expressions (for IN, BETWEEN, etc.)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct MultiExpr {
    pub op: ExprOp,
    pub exprs: Vec<WhereExpr>,
}

impl MultiExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.exprs.iter().flat_map(|expr| expr.nodes());
        current.chain(children)
    }
}

impl Deparse for MultiExpr {
    fn deparse<'b>(&self, _buf: &'b mut String) -> &'b mut String {
        todo!();
    }
}

// WHERE expression tree - more abstract and flexible
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum WhereExpr {
    // Leaf nodes
    Value(LiteralValue),
    Column(ColumnNode),

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

type WhereExprNodeIter<'a, N> =
    std::iter::Chain<std::option::IntoIter<&'a N>, Box<dyn Iterator<Item = &'a N> + 'a>>;

impl WhereExpr {
    /// Get all nodes of the given type within this WhereExpr tree
    pub fn nodes<N: Any>(&self) -> WhereExprNodeIter<'_, N> {
        let current = ((self as &dyn Any)
            .downcast_ref::<N>()
            .or_else(|| match self {
                WhereExpr::Value(val) => (val as &dyn Any).downcast_ref::<N>(),
                WhereExpr::Column(col) => (col as &dyn Any).downcast_ref::<N>(),
                WhereExpr::Unary(unary) => (unary as &dyn Any).downcast_ref::<N>(),
                WhereExpr::Binary(binary) => (binary as &dyn Any).downcast_ref::<N>(),
                WhereExpr::Multi(multi) => (multi as &dyn Any).downcast_ref::<N>(),
                WhereExpr::Function { .. } => None,
                WhereExpr::Subquery { .. } => None,
            }))
        .into_iter();

        // Chain with child nodes
        let children = match self {
            WhereExpr::Unary(unary) => Box::new(unary.expr.nodes()) as Box<dyn Iterator<Item = &N>>,
            WhereExpr::Binary(binary) => Box::new(binary.lexpr.nodes().chain(binary.rexpr.nodes())),
            WhereExpr::Multi(multi) => Box::new(multi.exprs.iter().flat_map(|expr| expr.nodes())),
            WhereExpr::Function { args, .. } => Box::new(args.iter().flat_map(|expr| expr.nodes())),
            WhereExpr::Value(_) | WhereExpr::Column(_) | WhereExpr::Subquery { .. } => {
                Box::new(std::iter::empty())
            }
        };

        current.chain(children)
    }

    /// Check if this WHERE expression contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        match self {
            WhereExpr::Binary(binary) => binary.lexpr.has_sublink() || binary.rexpr.has_sublink(),
            WhereExpr::Unary(unary) => unary.expr.has_sublink(),
            WhereExpr::Multi(multi) => multi.exprs.iter().any(|e| e.has_sublink()),
            WhereExpr::Function { args, .. } => args.iter().any(|e| e.has_sublink()),
            WhereExpr::Subquery { .. } => true, // Found a subquery!
            _ => false,                         // Value and Column don't contain sublinks
        }
    }
}

impl Deparse for WhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            WhereExpr::Value(literal) => literal.deparse(buf),
            WhereExpr::Column(col) => col.deparse(buf),
            WhereExpr::Unary(expr) => expr.deparse(buf),
            WhereExpr::Binary(expr) => expr.deparse(buf),
            WhereExpr::Multi(expr) => expr.deparse(buf),
            WhereExpr::Function { .. } => todo!(),
            WhereExpr::Subquery { .. } => todo!(),
        };

        buf
    }
}

/// Simplified SQL AST focused on caching use cases
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SqlQuery {
    pub statement: Statement,
}

impl SqlQuery {
    /// Get all nodes of the given type
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        match &self.statement {
            Statement::Select(select) => select.nodes(),
        }
    }

    /// Check if query only references a single table
    pub fn is_single_table(&self) -> bool {
        self.nodes::<TableNode>().nth(1).is_none()
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

impl Deparse for SqlQuery {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.statement.deparse(buf)
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Statement {
    Select(SelectStatement),
    // Future: Insert, Update, Delete for CDC
}

impl Statement {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        match self {
            Statement::Select(select) => select.nodes(),
        }
    }
}

impl Deparse for Statement {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            Statement::Select(select) => select.deparse(buf),
        };

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectStatement {
    pub columns: SelectColumns,
    pub from: Vec<TableSource>,
    pub where_clause: Option<WhereExpr>,
    pub group_by: Vec<ColumnNode>,
    pub having: Option<WhereExpr>,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<LimitClause>,
    pub distinct: bool,
    pub values: Vec<Vec<LiteralValue>>,
}

impl Default for SelectStatement {
    fn default() -> Self {
        Self {
            columns: SelectColumns::None,
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

impl SelectStatement {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let columns_nodes = self.columns.nodes();

        let from_nodes = {
            let mut iter = self.from.iter();
            let table_iter = iter.next().map(|t| t.nodes());
            SelectStatementNodeIter {
                iter,
                table_iter,
                _phantom: PhantomData,
            }
        };

        let where_nodes = self.where_clause.iter().flat_map(|w| w.nodes());
        let group_by_nodes = self.group_by.iter().flat_map(|c| c.nodes());
        let having_nodes = self.having.iter().flat_map(|h| h.nodes());
        let values_nodes = self
            .values
            .iter()
            .flat_map(|row| row.iter().flat_map(|v| v.nodes()));

        columns_nodes
            .chain(from_nodes)
            .chain(where_nodes)
            .chain(group_by_nodes)
            .chain(having_nodes)
            .chain(values_nodes)
    }

    /// Check if this SELECT statement references only a single table
    pub fn is_single_table(&self) -> bool {
        self.from.len() == 1 && matches!(self.from[0], TableSource::Table(_))
    }

    /// Check if this SELECT statement contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        // Check columns for subqueries
        if let SelectColumns::Columns(columns) = &self.columns
            && columns
                .iter()
                .any(|col| self.column_expr_has_sublink(&col.expr))
        {
            return true;
        }

        // Check WHERE clause for subqueries
        if let Some(where_clause) = &self.where_clause
            && where_clause.has_sublink()
        {
            return true;
        }

        // Check HAVING clause for subqueries
        if let Some(having) = &self.having
            && having.has_sublink()
        {
            return true;
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
}

impl Deparse for SelectStatement {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        //if values is present, ignore the rest (would be nice to represent this in the type system)
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
                buf.push_str(" DISTINCT")
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
                todo!();
            }

            if let Some(expr) = &self.having {
                buf.push_str(" HAVING");
                expr.deparse(buf);
            }

            if !self.order_by.is_empty() {
                todo!();
            }

            if let Some(_limit) = &self.limit {
                todo!();
            }
        }

        buf
    }
}

struct SelectStatementNodeIter<'a, N> {
    iter: std::slice::Iter<'a, TableSource>,
    table_iter: Option<TableSourceNodeIter<'a, N>>,
    _phantom: PhantomData<N>,
}

impl<'a, N: Any> Iterator for SelectStatementNodeIter<'a, N> {
    type Item = &'a N;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.table_iter {
                Some(table_iter) => {
                    if let Some(node) = table_iter.next() {
                        return Some(node);
                    }
                    // Current iterator is exhausted, try the next table source
                    self.table_iter = self.iter.next().map(|t| t.nodes());
                }
                None => {
                    // No current iterator, start with the first table source
                    self.table_iter = self.iter.next().map(|t| t.nodes());
                    self.table_iter.as_ref()?;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumns {
    None,
    All,                        // SELECT *
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ...
}

impl SelectColumns {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();

        let children: Box<dyn Iterator<Item = &N> + '_> = match self {
            SelectColumns::None | SelectColumns::All => Box::new(std::iter::empty()),
            SelectColumns::Columns(columns) => Box::new(columns.iter().flat_map(|col| col.nodes())),
        };

        Box::new(current.chain(children))
    }
}

impl Deparse for SelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SelectColumns::None => buf.push(' '),
            SelectColumns::All => buf.push_str(" *"),
            SelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ",";
                }
            }
        };

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectColumn {
    pub expr: ColumnExpr,
    pub alias: Option<String>,
}

impl SelectColumn {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

impl Deparse for SelectColumn {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if let Some(alias) = &self.alias {
            buf.push_str(alias);
            buf.push('.');
        }
        self.expr.deparse(buf)
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ColumnExpr {
    Column(ColumnNode),     // column_name, table.column_name
    Function(FunctionCall), // COUNT(*), SUM(col), etc.
    Literal(LiteralValue),  // Constant values
    Subquery(SelectStatement),
}

impl ColumnExpr {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        Box::new(
            ((self as &dyn Any)
                .downcast_ref::<N>()
                .or_else(|| match self {
                    ColumnExpr::Column(col) => (col as &dyn Any).downcast_ref::<N>(),
                    ColumnExpr::Function(func) => (func as &dyn Any).downcast_ref::<N>(),
                    ColumnExpr::Literal(lit) => (lit as &dyn Any).downcast_ref::<N>(),
                    ColumnExpr::Subquery(select) => (select as &dyn Any).downcast_ref::<N>(),
                }))
            .into_iter(),
        )
    }
}

impl Deparse for ColumnExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ColumnExpr::Column(col) => col.deparse(buf),
            ColumnExpr::Function(func) => func.deparse(buf),
            ColumnExpr::Literal(lit) => lit.deparse(buf),
            ColumnExpr::Subquery(select) => {
                buf.push('(');
                select.deparse(buf);
                buf.push(')');
                buf
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<ColumnExpr>,
}

impl FunctionCall {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.args.iter().flat_map(|arg| arg.nodes());
        current.chain(children)
    }
}

impl Deparse for FunctionCall {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        buf.push_str(self.name.as_str());
        buf.push('(');
        let mut sep = "";
        for col in &self.args {
            buf.push_str(sep);
            col.deparse(buf);
            sep = ",";
        }
        buf.push(')');

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

impl Deparse for TableAlias {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(&self.name);
        if !self.columns.is_empty() {
            buf.push('(');
            buf.push_str(&self.columns.join(", "));
            buf.push(')');
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableSource {
    Table(TableNode),
    Subquery(TableSubqueryNode),
    Join(JoinNode),
}

impl TableSource {
    fn nodes<N: Any>(&self) -> TableSourceNodeIter<'_, N> {
        TableSourceNodeIter {
            source: Some(self),
            join_iter: None,
            _phantom: PhantomData,
        }
    }
}

impl Deparse for TableSource {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            TableSource::Table(table) => table.deparse(buf),
            TableSource::Subquery(subquery) => subquery.deparse(buf),
            TableSource::Join(join) => join.deparse(buf),
        }
    }
}

struct TableSourceNodeIter<'a, N> {
    source: Option<&'a TableSource>,
    join_iter: Option<Box<JoinNodeIter<'a, N>>>,
    _phantom: PhantomData<N>,
}

impl<'a, N: Any> Iterator for TableSourceNodeIter<'a, N> {
    type Item = &'a N;

    fn next(&mut self) -> Option<Self::Item> {
        match self.source {
            Some(TableSource::Table(table)) => {
                self.source = None;
                (table as &dyn Any).downcast_ref::<N>()
            }
            Some(TableSource::Subquery(_)) => {
                self.source = None;
                None // TODO: implement subquery node iteration if needed
            }
            Some(TableSource::Join(join)) => {
                if let Some(iter) = &mut self.join_iter {
                    // Continue with the existing join iterator
                    if let Some(node) = iter.next() {
                        return Some(node);
                    } else {
                        // Join iterator exhausted
                        self.source = None;
                        return None;
                    }
                }

                // First time processing this join
                // Check if we're looking for JoinNode types - if so, yield this join first
                if TypeId::of::<N>() == TypeId::of::<JoinNode>()
                    && let Some(this_join) = (join as &dyn Any).downcast_ref::<N>()
                {
                    // Yield this join, and set up iterator for children
                    self.join_iter = Some(Box::new(join.nodes()));
                    return Some(this_join);
                }

                // For other types (like TableNode), iterate through children
                let mut iter = join.nodes();
                if let Some(node) = iter.next() {
                    self.join_iter = Some(Box::new(iter));
                    Some(node)
                } else {
                    self.source = None;
                    None
                }
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableNode {
    pub schema: Option<String>,
    pub name: String,
    pub alias: Option<TableAlias>,
}

impl TableNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for TableNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if let Some(schema) = &self.schema {
            buf.push_str(schema);
            buf.push('.');
        }
        buf.push_str(self.name.as_str());
        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableSubqueryNode {
    pub lateral: bool,
    pub select: Box<SelectStatement>,
    pub alias: Option<TableAlias>,
}

impl TableSubqueryNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.select.nodes();
        current.chain(children)
    }
}

impl Deparse for TableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if self.lateral {
            buf.push_str("LATERAL ");
        }

        buf.push('(');
        self.select.deparse(buf);
        buf.push(')');

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct JoinNode {
    pub join_type: JoinType,
    pub left: Box<TableSource>,
    pub right: Box<TableSource>,
    pub condition: Option<WhereExpr>,
}

impl JoinNode {
    pub fn nodes<N: Any>(&self) -> JoinNodeIter<'_, N> {
        JoinNodeIter {
            left_iter: self.left.nodes(),
            right_iter: self.right.nodes(),
            condition_iter: self.condition.as_ref().map(|c| c.nodes()),
            _phantom: PhantomData,
        }
    }
}

impl Deparse for JoinNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        buf.push_str(" JOIN");
        self.right.deparse(buf);
        if let Some(condition) = &self.condition {
            buf.push_str(" ON ");
            condition.deparse(buf);
        }
        buf
    }
}

pub struct JoinNodeIter<'a, N> {
    left_iter: TableSourceNodeIter<'a, N>,
    right_iter: TableSourceNodeIter<'a, N>,
    condition_iter: Option<WhereExprNodeIter<'a, N>>,
    _phantom: PhantomData<N>,
}

impl<'a, N: Any> Iterator for JoinNodeIter<'a, N> {
    type Item = &'a N;

    fn next(&mut self) -> Option<Self::Item> {
        // Try left iterator first
        if let Some(item) = self.left_iter.next() {
            return Some(item);
        }

        // Then try right iterator
        if let Some(item) = self.right_iter.next() {
            return Some(item);
        }

        // Finally try condition iterator
        if let Some(ref mut condition_iter) = self.condition_iter {
            condition_iter.next()
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl TryFrom<pg_query::protobuf::JoinType> for JoinType {
    type Error = AstError;

    fn try_from(v: pg_query::protobuf::JoinType) -> Result<Self, Self::Error> {
        match v {
            pg_query::protobuf::JoinType::Undefined => Err(AstError::UnsupportedJoinType),
            pg_query::protobuf::JoinType::JoinInner => Ok(Self::Inner),
            pg_query::protobuf::JoinType::JoinLeft => Ok(Self::Left),
            pg_query::protobuf::JoinType::JoinFull => Ok(Self::Full),
            pg_query::protobuf::JoinType::JoinRight => Ok(Self::Right),
            pg_query::protobuf::JoinType::JoinSemi => Err(AstError::UnsupportedJoinType),
            pg_query::protobuf::JoinType::JoinAnti => Err(AstError::UnsupportedJoinType),
            pg_query::protobuf::JoinType::JoinRightAnti => Err(AstError::UnsupportedJoinType),
            pg_query::protobuf::JoinType::JoinUniqueOuter => Err(AstError::UnsupportedJoinType),
            pg_query::protobuf::JoinType::JoinUniqueInner => Err(AstError::UnsupportedJoinType),
        }
    }
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
            let statement = select_statement_convert(select_stmt)?;
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

fn select_statement_convert(select_stmt: &SelectStmt) -> Result<SelectStatement, AstError> {
    let columns = select_columns_convert(&select_stmt.target_list)?;
    let from = from_clause_convert(&select_stmt.from_clause)?;
    let where_clause = select_stmt_parse_where(select_stmt)?;
    let values = value_list_convert(&select_stmt.values_lists)?;

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
        values,
    })
}

fn value_list_convert(value_list: &[Node]) -> Result<Vec<Vec<LiteralValue>>, AstError> {
    let mut rv = Vec::new();
    for value in value_list {
        let mut row = Vec::new();
        if let Some(NodeEnum::List(node_list)) = &value.node {
            for item in &node_list.items {
                match &item.node {
                    Some(NodeEnum::AConst(const_val)) => {
                        row.push(const_value_extract(const_val)?);
                    }
                    other => {
                        return Err(AstError::UnsupportedFeature {
                            feature: format!("Value expression: {other:?}"),
                        });
                    }
                }
            }
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("Values: {value:?}"),
            });
        }
        rv.push(row);
    }

    Ok(rv)
}

fn select_columns_convert(target_list: &[Node]) -> Result<SelectColumns, AstError> {
    if target_list.is_empty() {
        return Ok(SelectColumns::None);
    }

    let mut columns = Vec::new();
    let mut has_star = false;

    for target in target_list {
        if let Some(NodeEnum::ResTarget(res_target)) = &target.node
            && let Some(val_node) = &res_target.val
        {
            let alias = if res_target.name.is_empty() {
                None
            } else {
                Some(res_target.name.clone())
            };

            match val_node.node.as_ref() {
                Some(NodeEnum::ColumnRef(col_ref)) => {
                    // Check if this is SELECT *
                    if col_ref.fields.len() == 1
                        && let Some(NodeEnum::AStar(_)) = &col_ref.fields[0].node
                    {
                        has_star = true;
                        continue;
                    }

                    // Regular column reference
                    let column_ref = column_ref_convert(col_ref)?;

                    columns.push(SelectColumn {
                        expr: ColumnExpr::Column(column_ref),
                        alias,
                    });
                }
                Some(NodeEnum::SubLink(sub_link)) => {
                    match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
                        Some(NodeEnum::SelectStmt(select_stmt)) => {
                            let statement = select_statement_convert(select_stmt)?;
                            columns.push(SelectColumn {
                                expr: ColumnExpr::Subquery(statement),
                                alias,
                            });
                        }
                        other => {
                            return Err(AstError::UnsupportedSelectFeature {
                                feature: format!("Column expression: {other:?}"),
                            });
                        }
                    }
                }
                // TODO: Add support for function calls, literals, etc.
                other => {
                    return Err(AstError::UnsupportedSelectFeature {
                        feature: format!("Column expression: {other:?}"),
                    });
                }
            }
        } else {
            return Err(AstError::UnsupportedSelectFeature {
                feature: format!("Target: {target:?}"),
            });
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

fn from_clause_convert(from_clause: &[Node]) -> Result<Vec<TableSource>, AstError> {
    let mut tables = Vec::new();

    for from_node in from_clause {
        match &from_node.node {
            Some(NodeEnum::RangeVar(range_var)) => {
                let table_node = table_node_convert(range_var)?;
                tables.push(table_node);
            }
            Some(NodeEnum::RangeSubselect(range_subselect)) => {
                let table_subquery_node = table_subquery_node_convert(range_subselect)?;
                tables.push(table_subquery_node);
            }
            Some(NodeEnum::JoinExpr(join_expr)) => {
                let join_node = join_expr_convert(join_expr)?;
                tables.push(join_node);
            }
            _ => {
                return Err(AstError::UnsupportedSelectFeature {
                    feature: format!("FROM clause type: {from_node:?}"),
                });
            }
        }
    }

    Ok(tables)
}

fn join_arg_convert(node: &Node, side: &str) -> Result<TableSource, AstError> {
    match &node.node {
        Some(NodeEnum::RangeVar(range_var)) => table_node_convert(range_var),
        Some(NodeEnum::RangeSubselect(range_subselect)) => {
            table_subquery_node_convert(range_subselect)
        }
        Some(NodeEnum::JoinExpr(nested_join)) => {
            // Recursively handle nested joins
            join_expr_convert(nested_join)
        }
        _ => Err(AstError::UnsupportedSelectFeature {
            feature: format!("join {side} argument: {node:?}"),
        }),
    }
}

fn join_expr_convert(join_expr: &JoinExpr) -> Result<TableSource, AstError> {
    // Convert left argument - can be a table, subquery, or another join
    let left_table = if let Some(larg_node) = &join_expr.larg {
        join_arg_convert(larg_node, "left")?
    } else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "join missing left argument".to_string(),
        });
    };

    // Convert right argument - can be a table, subquery, or another join
    let right_table = if let Some(rarg_node) = &join_expr.rarg {
        join_arg_convert(rarg_node, "right")?
    } else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "join missing right argument".to_string(),
        });
    };

    let clause = if let Some(clause) = &join_expr.quals {
        Some(node_convert_to_expr(clause)?)
    } else {
        None
    };

    Ok(TableSource::Join(JoinNode {
        join_type: JoinType::try_from(join_expr.jointype())?,
        left: Box::new(left_table),
        right: Box::new(right_table),
        condition: clause,
    }))
}

fn table_node_convert(range_var: &RangeVar) -> Result<TableSource, AstError> {
    let schema = if range_var.schemaname.is_empty() {
        None
    } else {
        Some(range_var.schemaname.clone())
    };

    let name = range_var.relname.clone();

    let alias = range_var.alias.as_ref().map(|alias_node| TableAlias {
        name: alias_node.aliasname.clone(),
        columns: alias_node
            .colnames
            .iter()
            .flat_map(|n| {
                if let Some(NodeEnum::String(string_node)) = &n.node {
                    Some(string_node.sval.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    });

    Ok(TableSource::Table(TableNode {
        schema,
        name,
        alias,
    }))
}

fn table_subquery_node_convert(range_subselect: &RangeSubselect) -> Result<TableSource, AstError> {
    let Some(NodeEnum::SelectStmt(select_stmt)) = range_subselect
        .subquery
        .as_ref()
        .and_then(|n| n.node.as_ref())
    else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: format!("{:?}", range_subselect.subquery),
        });
    };

    let select = select_statement_convert(select_stmt)?;

    let alias = range_subselect.alias.as_ref().map(|alias_node| TableAlias {
        name: alias_node.aliasname.clone(),
        columns: alias_node
            .colnames
            .iter()
            .flat_map(|n| {
                if let Some(NodeEnum::String(string_node)) = &n.node {
                    Some(string_node.sval.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    });

    Ok(TableSource::Subquery(TableSubqueryNode {
        lateral: range_subselect.lateral,
        select: Box::new(select),
        alias,
    }))
}

fn column_ref_convert(col_ref: &ColumnRef) -> Result<ColumnNode, AstError> {
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
    Ok(ColumnNode { table, column })
}

/// Create a fingerprint hash for SQL query AST.
pub fn ast_query_fingerprint(select_statement: &SelectStatement) -> u64 {
    let mut hasher = DefaultHasher::new();
    select_statement.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, panic};

    use super::*;

    #[test]
    fn test_sql_query_convert_simple_select() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        assert!(ast.is_single_table());
        assert!(ast.has_where_clause());
        assert!(!ast.is_select_star());
        assert_eq!(
            ast.nodes::<TableNode>()
                .map(|t| (t.schema.as_deref(), t.name.as_str()))
                .collect::<HashSet<_>>(),
            HashSet::<(Option<&str>, _)>::from([(None, "users")])
        );
    }

    #[test]
    fn test_sql_query_convert_select_star() {
        let sql = "SELECT * FROM products";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        assert!(ast.is_single_table());
        assert!(!ast.has_where_clause());
        assert!(ast.is_select_star());
        assert_eq!(
            ast.nodes::<TableNode>()
                .map(|t| (t.schema.as_deref(), t.name.as_str()))
                .collect::<HashSet<_>>(),
            HashSet::<(Option<&str>, _)>::from([(None, "products")])
        );
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
    fn test_sql_query_convert_table_schema() {
        let sql = "SELECT id, name FROM test.users WHERE active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Check table alias
        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.from.len(), 1);

        let TableSource::Table(table) = &select.from[0] else {
            panic!("exepected table");
        };

        assert_eq!(table.schema, Some("test".to_owned()));
        assert_eq!(table.name, "users");
        assert_eq!(table.alias, None);

        // Check column references
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns.len(), 2);

            // First column: id
            if let ColumnExpr::Column(col_ref) = &columns[0].expr {
                assert_eq!(col_ref.table, None);
                assert_eq!(col_ref.column, "id");
            }

            // Second column: name
            if let ColumnExpr::Column(col_ref) = &columns[1].expr {
                assert_eq!(col_ref.table, None);
                assert_eq!(col_ref.column, "name");
            }
        }
    }

    #[test]
    fn test_sql_query_convert_table_alias() {
        let sql = "SELECT u.id, u.name FROM users u WHERE u.active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Check table alias
        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.from.len(), 1);

        let TableSource::Table(table) = &select.from[0] else {
            panic!("exepected table");
        };

        assert_eq!(table.name, "users");
        assert_eq!(table.alias.as_ref().unwrap().name, "u".to_owned());
        assert!(table.alias.as_ref().unwrap().columns.is_empty());

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
        assert_eq!(select.from.len(), 1);

        let TableSource::Table(table) = &select.from[0] else {
            panic!("exepected table");
        };

        // Table should have no alias
        assert_eq!(table.alias, None);

        // Columns should have no aliases
        if let SelectColumns::Columns(columns) = &select.columns {
            assert_eq!(columns[0].alias, None);
            assert_eq!(columns[1].alias, None);
        }
    }

    #[test]
    fn test_sql_query_join() {
        let sql = "SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        assert_eq!(
            ast.nodes::<TableNode>()
                .map(|t| (t.schema.as_deref(), t.name.as_str(), t.alias.as_ref()))
                .collect::<HashSet<_>>(),
            HashSet::<(Option<&str>, _, _)>::from([
                (None, "invoice", None),
                (
                    None,
                    "product",
                    Some(&TableAlias {
                        name: "p".to_owned(),
                        columns: Vec::new()
                    })
                )
            ])
        );
    }

    #[test]
    fn test_sql_query_multiple_joins_two_tables() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Should parse successfully with three tables
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].name, "a");
        assert_eq!(tables[1].name, "b");
        assert_eq!(tables[2].name, "c");

        // Should have 2 join nodes (nested structure)
        let joins: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].join_type, JoinType::Inner);
        assert_eq!(joins[1].join_type, JoinType::Inner);

        // Verify each join's condition contains the expected columns
        // Note: We count each join's condition separately to avoid double-counting
        // columns from nested joins (since joins can be nested in the AST)
        let mut col_count = 0;
        for join in &joins {
            if let Some(condition) = &join.condition {
                col_count += condition.nodes::<ColumnNode>().count();
            }
        }
        assert_eq!(col_count, 4); // a.id, b.id from first join + b.id, c.id from second join
    }

    #[test]
    fn test_sql_query_multiple_joins_three_tables() {
        let sql =
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Should parse successfully with four tables
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 4);
        assert_eq!(tables[0].name, "a");
        assert_eq!(tables[1].name, "b");
        assert_eq!(tables[2].name, "c");
        assert_eq!(tables[3].name, "d");

        // Should have 3 join nodes (deeply nested structure)
        let joins: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(joins.len(), 3);
        assert!(joins.iter().all(|j| j.join_type == JoinType::Inner));
    }

    #[test]
    fn test_sql_query_mixed_join_types() {
        let sql = "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Should parse successfully with three tables
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[1].name, "orders");
        assert_eq!(tables[2].name, "payments");

        // Should have 2 join nodes with different types
        // Note: Joins are returned in traversal order (outer join first, then nested joins)
        // For this query, PostgreSQL creates: (users INNER JOIN orders) LEFT JOIN payments
        // So the outer LEFT join is returned first, then the inner INNER join
        let joins: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].join_type, JoinType::Left); // Outer join
        assert_eq!(joins[1].join_type, JoinType::Inner); // Nested join
    }

    #[test]
    fn test_sql_query_multiple_joins_deparse() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);

        // Parse the deparsed SQL to verify it's valid
        let pg_ast2 = pg_query::parse(&buf).unwrap();
        let ast2 = sql_query_convert(&pg_ast2).unwrap();

        // Should produce identical AST
        assert_eq!(ast, ast2);
    }

    #[test]
    fn test_sql_query_select_subquery() {
        let sql = "SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) as one FROM invoice";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!();
        };

        assert_eq!(
            columns[0],
            SelectColumn {
                expr: ColumnExpr::Column(ColumnNode {
                    table: Some("invoice".to_owned()),
                    column: "id".to_owned()
                }),
                alias: None,
            }
        );

        assert!(matches!(columns[1].expr, ColumnExpr::Subquery(_)));
        assert_eq!(columns[1].alias, Some("one".to_owned()));
    }

    #[test]
    fn test_sql_query_table_subquery() {
        let sql = "SELECT * FROM (SELECT * FROM invoice WHERE id = 2) inv";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.from.len(), 1);

        let TableSource::Subquery(subquery) = &select.from[0] else {
            panic!("exepected subquery");
        };

        assert!(!subquery.lateral);
        assert_eq!(subquery.alias.as_ref().unwrap().name, "inv".to_owned());
        assert!(subquery.alias.as_ref().unwrap().columns.is_empty());
    }

    #[test]
    fn test_sql_query_values() {
        let sql = "SELECT * FROM (VALUES(1, 2, 'test'), (3, 4, 'a')) v";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.from.len(), 1);

        let TableSource::Subquery(subquery) = &select.from[0] else {
            panic!("exepected subquery");
        };

        assert!(!subquery.lateral);
        assert_eq!(subquery.alias.as_ref().unwrap().name, "v".to_owned());

        assert_eq!(subquery.select.values.len(), 2);
        assert_eq!(subquery.select.values[0].len(), 3);
        assert_eq!(subquery.select.values[1].len(), 3);
    }

    #[test]
    fn test_sql_query_deparse_simple() {
        let sql = "SELECT id, name FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_literal_value_deparse() {
        let mut buf = String::new();

        // String literal
        LiteralValue::String("hello".to_string()).deparse(&mut buf);
        assert_eq!(buf, "'hello'");
        buf.clear();

        // Integer literal
        LiteralValue::Integer(42).deparse(&mut buf);
        assert_eq!(buf, "42");
        buf.clear();

        // Float literal
        LiteralValue::Float(NotNan::new(3.25).unwrap()).deparse(&mut buf);
        assert_eq!(buf, "3.25");
        buf.clear();

        // Boolean literals
        LiteralValue::Boolean(true).deparse(&mut buf);
        assert_eq!(buf, "true");
        buf.clear();

        LiteralValue::Boolean(false).deparse(&mut buf);
        assert_eq!(buf, "false");
        buf.clear();

        // NULL literal
        LiteralValue::Null.deparse(&mut buf);
        assert_eq!(buf, "NULL");
        buf.clear();

        // Parameter
        LiteralValue::Parameter("$1".to_string()).deparse(&mut buf);
        assert_eq!(buf, "$1");
    }

    #[test]
    fn test_column_ref_deparse() {
        let mut buf = String::new();

        // Simple column
        ColumnNode {
            table: None,
            column: "id".to_string(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "id");
        buf.clear();

        // Qualified column
        ColumnNode {
            table: Some("users".to_string()),
            column: "name".to_string(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "users.name");
    }

    #[test]
    fn test_expr_op_deparse() {
        let mut buf = String::new();

        // Comparison operators
        ExprOp::Equal.deparse(&mut buf);
        assert_eq!(buf, "=");
        buf.clear();

        ExprOp::NotEqual.deparse(&mut buf);
        assert_eq!(buf, "!=");
        buf.clear();

        ExprOp::LessThan.deparse(&mut buf);
        assert_eq!(buf, "<");
        buf.clear();

        ExprOp::GreaterThanOrEqual.deparse(&mut buf);
        assert_eq!(buf, ">=");
        buf.clear();

        // Null checks (test the fix)
        ExprOp::IsNull.deparse(&mut buf);
        assert_eq!(buf, "IS NULL");
        buf.clear();

        ExprOp::IsNotNull.deparse(&mut buf);
        assert_eq!(buf, "IS NOT NULL");
        buf.clear();

        // Pattern matching
        ExprOp::Like.deparse(&mut buf);
        assert_eq!(buf, "LIKE");
        buf.clear();

        ExprOp::NotLike.deparse(&mut buf);
        assert_eq!(buf, "NOT LIKE");
        buf.clear();

        // Logical operators
        ExprOp::And.deparse(&mut buf);
        assert_eq!(buf, "AND");
        buf.clear();

        ExprOp::Or.deparse(&mut buf);
        assert_eq!(buf, "OR");
    }

    #[test]
    fn test_binary_expr_deparse() {
        let mut buf = String::new();

        // Simple equality: id = 1
        let expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(1))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "id = 1");
        buf.clear();

        // Complex expression: users.name = 'john'
        let expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: Some("users".to_string()),
                column: "name".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_string()))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "users.name = 'john'");
    }

    #[test]
    fn test_unary_expr_deparse() {
        let mut buf = String::new();

        // NOT active
        let expr = UnaryExpr {
            op: ExprOp::Not,
            expr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "active".to_string(),
            })),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "NOT active");
    }

    #[test]
    fn test_select_deparse_with_where() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_distinct() {
        let sql = "SELECT DISTINCT name FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_multiple_tables() {
        let sql = "SELECT * FROM users, orders";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_schema_qualified() {
        let sql = "SELECT * FROM public.users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_join() {
        let sql = "SELECT first_name, last_name, film_id FROM actor a \
                JOIN film_actor fa ON a.actor_id = fa.actor_id \
                WHERE a.actor_id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_select_deparse_table_values() {
        let sql = "SELECT fa.actor_id \
            FROM (VALUES ('1', '2'), ('3', '4')) fa(actor_id, film_id) \
            WHERE a.actor_id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_round_trip() {
        fn round_trip(sql: &str) {
            // Parse original
            let pg_ast1 = pg_query::parse(sql).unwrap();
            let ast1 = sql_query_convert(&pg_ast1).unwrap();

            // Deparse to string
            let mut deparsed = String::with_capacity(1024);
            ast1.deparse(&mut deparsed);

            // Parse deparsed version
            let pg_ast2 = pg_query::parse(&deparsed).unwrap();
            let ast2 = sql_query_convert(&pg_ast2).unwrap();

            // Should be equivalent
            assert_eq!(ast1, ast2);
        }

        round_trip("SELECT id, name FROM users WHERE active = true");
        round_trip(
            "SELECT id, name \
            FROM users JOIN address a on a.is = users.address_id \
            WHERE active = true",
        );
        round_trip(
            "SELECT id, name \
            FROM (SELECT * FROM users) u \
            WHERE active = true",
        );
    }

    #[test]
    fn test_literal_empty_string() {
        let mut buf = String::new();
        LiteralValue::String("".to_string()).deparse(&mut buf);
        assert_eq!(buf, "''");
    }

    #[test]
    fn test_literal_string_with_quotes() {
        let mut buf = String::new();
        LiteralValue::String("test'quote".to_string()).deparse(&mut buf);
        // postgres-protocol should properly escape the quote
        assert_eq!(buf, "'test''quote'");
    }

    #[test]
    fn test_literal_string_with_backslashes() {
        let mut buf = String::new();
        LiteralValue::String("test\\path".to_string()).deparse(&mut buf);
        // postgres-protocol should use E'' syntax for backslashes
        assert_eq!(buf, "E'test\\\\path'");
    }

    #[test]
    fn test_nodes_table_extraction() {
        let sql = "SELECT first_name, last_name, film_id \
                    FROM actor a \
                    JOIN public.film_actor fa ON a.actor_id = fa.actor_id \
                    WHERE a.actor_id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test extracting TableNode instances using the generic nodes function
        let tables = ast.nodes::<TableNode>().collect::<Vec<_>>();

        assert_eq!(tables.len(), 2);

        assert_eq!(tables[0].schema, None);
        assert_eq!(tables[0].name, "actor");
        assert_eq!(
            tables[0].alias,
            Some(TableAlias {
                name: "a".to_owned(),
                columns: vec![]
            })
        );

        assert_eq!(tables[1].schema, Some("public".to_owned()));
        assert_eq!(tables[1].name, "film_actor");
        assert_eq!(
            tables[1].alias,
            Some(TableAlias {
                name: "fa".to_owned(),
                columns: vec![]
            })
        );
    }

    #[test]
    fn test_nodes_table_nodes() {
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test getting TableNode instances using the nodes function
        let table_nodes: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(table_nodes.len(), 2);

        assert_eq!(table_nodes[0].name, "users");
        assert_eq!(table_nodes[0].alias.as_ref().unwrap().name, "u");

        assert_eq!(table_nodes[1].name, "orders");
        assert_eq!(table_nodes[1].alias.as_ref().unwrap().name, "o");
    }

    #[test]
    fn test_nodes_join_nodes() {
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test getting JoinNode instances using the nodes function
        let join_nodes: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(join_nodes.len(), 1);

        assert_eq!(join_nodes[0].join_type, JoinType::Inner);
        assert!(join_nodes[0].condition.is_some());
    }

    #[test]
    fn test_nodes_mixed_types() {
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Should find both table nodes and join nodes independently
        let table_nodes: Vec<&TableNode> = ast.nodes().collect();
        let join_nodes: Vec<&JoinNode> = ast.nodes().collect();

        assert_eq!(table_nodes.len(), 2);
        assert_eq!(join_nodes.len(), 1);

        // Verify the types are different queries but same AST
        assert_eq!(table_nodes[0].name, "users");
        assert_eq!(table_nodes[1].name, "orders");
        assert_eq!(join_nodes[0].join_type, JoinType::Inner);
    }

    #[test]
    fn test_where_expr_nodes_column() {
        let sql = "SELECT * FROM users WHERE name = 'john' AND age > 25";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let where_clause = ast.where_clause().unwrap();
        let columns: Vec<&ColumnNode> = where_clause.nodes().collect();

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column, "name");
        assert_eq!(columns[1].column, "age");
    }

    #[test]
    fn test_where_expr_nodes_literal() {
        let sql = "SELECT * FROM users WHERE name = 'john' AND age > 25 AND active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();

        assert_eq!(literals.len(), 3);
        assert_eq!(literals[0], &LiteralValue::String("john".to_string()));
        assert_eq!(literals[1], &LiteralValue::Integer(25));
        assert_eq!(literals[2], &LiteralValue::Boolean(true));
    }

    #[test]
    fn test_where_expr_nodes_binary() {
        let sql = "SELECT * FROM users WHERE name = 'john' AND age > 25";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let where_clause = ast.where_clause().unwrap();
        let binary_exprs: Vec<&BinaryExpr> = where_clause.nodes().collect();

        assert_eq!(binary_exprs.len(), 3); // AND, =, >
        assert_eq!(binary_exprs[0].op, ExprOp::And);
        assert_eq!(binary_exprs[1].op, ExprOp::Equal);
        assert_eq!(binary_exprs[2].op, ExprOp::GreaterThan);
    }

    #[test]
    fn test_where_expr_nodes_whole_expr() {
        let sql = "SELECT * FROM users WHERE name = 'john'";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let where_clause = ast.where_clause().unwrap();
        let where_exprs: Vec<&WhereExpr> = where_clause.nodes().collect();

        // Should find the root expression plus all child expressions
        assert_eq!(where_exprs.len(), 3); // Binary(name = 'john'), Column(name), Value('john')
    }

    #[test]
    fn test_where_expr_nodes_nested() {
        let sql = "SELECT * FROM users WHERE (name = 'john' OR name = 'jane') AND age > 18";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let where_clause = ast.where_clause().unwrap();
        let columns: Vec<&ColumnNode> = where_clause.nodes().collect();

        // Should find all column references in nested structure
        assert_eq!(columns.len(), 3); // name, name, age
        assert_eq!(columns[0].column, "name");
        assert_eq!(columns[1].column, "name");
        assert_eq!(columns[2].column, "age");
    }

    #[test]
    fn test_join_condition_nodes() {
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Get the join node from the query
        let join_nodes: Vec<&JoinNode> = ast.nodes().collect();
        assert_eq!(join_nodes.len(), 1);

        let join_node = join_nodes[0];

        // Test that we can extract column nodes from the join condition
        let columns: Vec<&ColumnNode> = join_node.nodes().collect();

        // Should find only the condition columns (u.id, o.user_id)
        // since we're specifically collecting ColumnNode instances
        assert_eq!(columns.len(), 2);

        // Verify the condition columns
        assert_eq!(columns[0].table, Some("u".to_string()));
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].table, Some("o".to_string()));
        assert_eq!(columns[1].column, "user_id");
    }

    #[test]
    fn test_statement_nodes() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that Statement::nodes() delegates to SelectStatement
        let tables: Vec<&TableNode> = ast.statement.nodes().collect();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn test_select_columns_nodes() {
        let sql = "SELECT id, name FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that we can extract ColumnNode through SelectColumns
        let columns: Vec<&ColumnNode> = ast.nodes().collect();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].column, "name");
    }

    #[test]
    fn test_column_expr_nodes() {
        let sql = "SELECT id, name FROM users WHERE active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that ColumnExpr::nodes() can traverse through to ColumnNode
        let columns: Vec<&ColumnNode> = ast.nodes().collect();
        assert_eq!(columns.len(), 3); // id, name, active
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].column, "name");
        assert_eq!(columns[2].column, "active");
    }

    #[test]
    fn test_function_call_nodes() {
        // Note: FunctionCall support is limited in the current parser
        // This test verifies the nodes() implementation works for the structure
        let func = FunctionCall {
            name: "COUNT".to_string(),
            args: vec![ColumnExpr::Column(ColumnNode {
                table: None,
                column: "id".to_string(),
            })],
        };

        // Test that FunctionCall::nodes() can extract ColumnNode from args
        let columns: Vec<&ColumnNode> = func.nodes().collect();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column, "id");
    }

    #[test]
    fn test_table_node_nodes() {
        let sql = "SELECT * FROM public.users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that TableNode::nodes() returns itself as a leaf node
        let tables: Vec<&TableNode> = ast.nodes().collect();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].schema, Some("public".to_string()));
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn test_table_subquery_node_nodes() {
        let sql = "SELECT * FROM (SELECT id FROM users) sub";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Note: TableSource::Subquery iteration is not fully implemented yet
        // So we can't extract nodes from within subqueries via TableSource
        // This test verifies that TableSubqueryNode itself can be extracted
        let subqueries: Vec<&TableSubqueryNode> = ast.nodes().collect();
        assert_eq!(subqueries.len(), 0); // Currently not traversed via TableSourceNodeIter
    }

    #[test]
    fn test_unary_expr_nodes() {
        let sql = "SELECT * FROM users WHERE NOT active";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that UnaryExpr::nodes() returns itself
        let unary_exprs: Vec<&UnaryExpr> = ast.nodes().collect();
        assert_eq!(unary_exprs.len(), 1);
        assert_eq!(unary_exprs[0].op, ExprOp::Not);
    }

    #[test]
    fn test_binary_expr_nodes() {
        let sql = "SELECT * FROM users WHERE name = 'john'";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that BinaryExpr::nodes() returns itself
        let binary_exprs: Vec<&BinaryExpr> = ast.nodes().collect();
        assert_eq!(binary_exprs.len(), 1);
        assert_eq!(binary_exprs[0].op, ExprOp::Equal);
    }

    #[test]
    fn test_multi_expr_nodes() {
        // Test MultiExpr::nodes() with a manually constructed MultiExpr
        let multi = MultiExpr {
            op: ExprOp::In,
            exprs: vec![
                WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_string(),
                }),
                WhereExpr::Value(LiteralValue::Integer(1)),
                WhereExpr::Value(LiteralValue::Integer(2)),
            ],
        };

        // Test that MultiExpr::nodes() returns itself
        let multi_exprs: Vec<&MultiExpr> = multi.nodes().collect();
        assert_eq!(multi_exprs.len(), 1);
        assert_eq!(multi_exprs[0].op, ExprOp::In);

        // Test that it can extract child nodes
        let columns: Vec<&ColumnNode> = multi.nodes().collect();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column, "id");
    }
}

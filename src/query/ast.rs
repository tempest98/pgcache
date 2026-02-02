use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::convert::AsRef;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use error_set::error_set;
use ordered_float::NotNan;
use pg_query::ParseResult;
use pg_query::protobuf::{ColumnRef, Node, RangeVar, SelectStmt, node::Node as NodeEnum};
use pg_query::protobuf::{
    AExpr, AExprKind, CaseExpr as PgCaseExpr, CoalesceExpr, FuncCall, JoinExpr, MinMaxExpr,
    MinMaxOp, RangeSubselect, SortByDir,
};
use postgres_protocol::escape;
use strum_macros::AsRefStr;

use crate::pg::identifier_needs_quotes;
use crate::query::parse::{const_value_extract, node_convert_to_expr, select_stmt_parse_where};

use super::parse::WhereParseError;

error_set! {
    AstError := {
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
    }
}

pub trait Deparse {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String;
}

impl Deparse for String {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => {
                buf.push('"');
                buf.push_str(self);
                buf.push('"');
            }
            false => buf.push_str(self),
        };

        buf
    }
}

impl Deparse for &str {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => {
                buf.push('"');
                buf.push_str(self);
                buf.push('"');
            }
            false => buf.push_str(self),
        };

        buf
    }
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
            table.deparse(buf);
            buf.push('.');
        }
        self.column.deparse(buf);

        buf
    }
}

// Unary operators (prefix operators on single expression)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum UnaryOp {
    Not,
    #[strum(to_string = "IS NULL")]
    IsNull,
    #[strum(to_string = "IS NOT NULL")]
    IsNotNull,
    Exists,
    #[strum(to_string = "NOT EXISTS")]
    NotExists,
}

impl Deparse for UnaryOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(self.as_ref());
        buf
    }
}

// Binary operators (infix operators between two expressions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum BinaryOp {
    // Logical
    And,
    Or,
    // Comparison
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
}

impl Deparse for BinaryOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(self.as_ref());
        buf
    }
}

// Multi-operand operators (one subject with multiple values)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum MultiOp {
    In,
    #[strum(to_string = "NOT IN")]
    NotIn,
    Between,
    #[strum(to_string = "NOT BETWEEN")]
    NotBetween,
    Any,
    All,
}

impl Deparse for MultiOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(self.as_ref());
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct UnaryExpr {
    pub op: UnaryOp,
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
        match self.op {
            UnaryOp::IsNull | UnaryOp::IsNotNull => {
                // Postfix operators: expr IS NULL
                self.expr.deparse(buf);
                buf.push(' ');
                self.op.deparse(buf);
            }
            UnaryOp::Not | UnaryOp::Exists | UnaryOp::NotExists => {
                // Prefix operators: NOT expr
                self.op.deparse(buf);
                buf.push(' ');
                self.expr.deparse(buf);
            }
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BinaryExpr {
    pub op: BinaryOp,
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
    pub op: MultiOp,
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
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        // MultiExpr format: [column, value1, value2, ...]
        // Output: column IN (value1, value2, ...) or column NOT IN (...)
        let [first, rest @ ..] = self.exprs.as_slice() else {
            return buf;
        };

        // First expression is the column/left side
        first.deparse(buf);

        match self.op {
            MultiOp::In => buf.push_str(" IN ("),
            MultiOp::NotIn => buf.push_str(" NOT IN ("),
            MultiOp::Between | MultiOp::NotBetween | MultiOp::Any | MultiOp::All => {
                buf.push(' ');
                self.op.deparse(buf);
                buf.push_str(" (");
            }
        }

        // Remaining expressions are the values
        let mut sep = "";
        for expr in rest {
            buf.push_str(sep);
            expr.deparse(buf);
            sep = ", ";
        }
        buf.push(')');
        buf
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
            WhereExpr::Subquery { .. } => true,
            WhereExpr::Value(_) | WhereExpr::Column(_) => false,
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
        let order_by_nodes = self.order_by.iter().flat_map(|o| o.nodes());
        let values_nodes = self
            .values
            .iter()
            .flat_map(|row| row.iter().flat_map(|v| v.nodes()));

        columns_nodes
            .chain(from_nodes)
            .chain(where_nodes)
            .chain(group_by_nodes)
            .chain(having_nodes)
            .chain(order_by_nodes)
            .chain(values_nodes)
    }

    /// Check if this SELECT statement references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [TableSource::Table(_)])
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

        // Check FROM clause for subqueries
        if self.from.iter().any(TableSource::has_sublink) {
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

    fn column_expr_has_sublink(&self, expr: &ColumnExpr) -> bool {
        expr.has_sublink()
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
                buf.push_str(" ORDER BY");
                let mut sep = "";
                for order in &self.order_by {
                    buf.push_str(sep);
                    buf.push(' ');
                    order.deparse(buf);
                    sep = ",";
                }
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
        self.expr.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push_str(" AS ");
            alias.deparse(buf);
        }
        buf
    }
}

/// Arithmetic operators for expressions like `amount * 2`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
pub enum ArithmeticOp {
    #[strum(to_string = "+")]
    Add,
    #[strum(to_string = "-")]
    Subtract,
    #[strum(to_string = "*")]
    Multiply,
    #[strum(to_string = "/")]
    Divide,
}

/// Arithmetic expression: `left op right` (e.g., `amount * -1`)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ArithmeticExpr {
    pub left: Box<ColumnExpr>,
    pub op: ArithmeticOp,
    pub right: Box<ColumnExpr>,
}

impl ArithmeticExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_children = self.left.nodes();
        let right_children = self.right.nodes();
        current.chain(left_children).chain(right_children)
    }

    pub fn has_sublink(&self) -> bool {
        self.left.has_sublink() || self.right.has_sublink()
    }
}

impl Deparse for ArithmeticExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        self.left.deparse(buf);
        buf.push(' ');
        buf.push_str(self.op.as_ref());
        buf.push(' ');
        self.right.deparse(buf);
        buf.push(')');
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum ColumnExpr {
    Column(ColumnNode),         // column_name, table.column_name
    Function(FunctionCall),     // COUNT(*), SUM(col), etc.
    Literal(LiteralValue),      // Constant values
    Case(CaseExpr),             // CASE WHEN ... THEN ... END
    Arithmetic(ArithmeticExpr), // amount * -1, price + tax
    Subquery(Box<SelectStatement>),
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
                    ColumnExpr::Case(case) => (case as &dyn Any).downcast_ref::<N>(),
                    ColumnExpr::Arithmetic(arith) => (arith as &dyn Any).downcast_ref::<N>(),
                    ColumnExpr::Subquery(select) => (select as &dyn Any).downcast_ref::<N>(),
                }))
            .into_iter(),
        )
    }

    /// Check if this column expression contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        match self {
            ColumnExpr::Function(func) => func.has_sublink(),
            ColumnExpr::Case(case) => case.has_sublink(),
            ColumnExpr::Arithmetic(arith) => arith.has_sublink(),
            ColumnExpr::Subquery(_) => true,
            ColumnExpr::Column(_) | ColumnExpr::Literal(_) => false,
        }
    }
}

impl Deparse for ColumnExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ColumnExpr::Column(col) => col.deparse(buf),
            ColumnExpr::Function(func) => func.deparse(buf),
            ColumnExpr::Literal(lit) => lit.deparse(buf),
            ColumnExpr::Case(case) => case.deparse(buf),
            ColumnExpr::Arithmetic(arith) => arith.deparse(buf),
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
    pub agg_star: bool,     // COUNT(*)
    pub agg_distinct: bool, // COUNT(DISTINCT col)
    pub agg_order: Vec<OrderByClause>, // ORDER BY inside aggregate: string_agg(x, ',' ORDER BY x)
    pub over: Option<WindowSpec>, // Window function OVER clause
}

impl FunctionCall {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let arg_children = self.args.iter().flat_map(|arg| arg.nodes());
        let agg_order_children = self.agg_order.iter().flat_map(|o| o.nodes());
        let over_children = self.over.iter().flat_map(|w| w.nodes());
        current
            .chain(arg_children)
            .chain(agg_order_children)
            .chain(over_children)
    }

    /// Check if this function call contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        self.args.iter().any(|arg| arg.has_sublink())
            || self.agg_order.iter().any(|o| o.expr.has_sublink())
            || self.over.as_ref().is_some_and(|w| w.has_sublink())
    }
}

impl Deparse for FunctionCall {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(&self.name.to_uppercase());
        buf.push('(');
        if self.agg_distinct {
            buf.push_str("DISTINCT ");
        }
        if self.agg_star {
            buf.push('*');
        } else {
            let mut sep = "";
            for arg in &self.args {
                buf.push_str(sep);
                arg.deparse(buf);
                sep = ", ";
            }
        }
        if !self.agg_order.is_empty() {
            buf.push_str(" ORDER BY ");
            let mut sep = "";
            for clause in &self.agg_order {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        if let Some(over) = &self.over {
            buf.push_str(" OVER ");
            over.deparse(buf);
        }
        buf
    }
}

/// Window specification for OVER clause: OVER (PARTITION BY ... ORDER BY ...)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct WindowSpec {
    /// PARTITION BY columns
    pub partition_by: Vec<ColumnExpr>,
    /// ORDER BY clauses
    pub order_by: Vec<OrderByClause>,
}

impl WindowSpec {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let partition_children = self.partition_by.iter().flat_map(|p| p.nodes());
        let order_children = self.order_by.iter().flat_map(|o| o.nodes());
        current.chain(partition_children).chain(order_children)
    }

    /// Check if this window spec contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        self.partition_by.iter().any(|p| p.has_sublink())
            || self.order_by.iter().any(|o| o.expr.has_sublink())
    }
}

impl Deparse for WindowSpec {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        if !self.partition_by.is_empty() {
            buf.push_str("PARTITION BY ");
            let mut sep = "";
            for col in &self.partition_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                buf.push(' ');
            }
            buf.push_str("ORDER BY ");
            let mut sep = "";
            for clause in &self.order_by {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        buf
    }
}

/// CASE expression: CASE [arg] WHEN condition THEN result [...] [ELSE default] END
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CaseExpr {
    /// For simple CASE (CASE expr WHEN val...), holds the expression being tested.
    /// None for searched CASE (CASE WHEN condition...).
    pub arg: Option<Box<ColumnExpr>>,
    /// List of WHEN clauses
    pub whens: Vec<CaseWhen>,
    /// ELSE result (None means NULL if no WHEN matches)
    pub default: Option<Box<ColumnExpr>>,
}

impl CaseExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let arg_nodes = self.arg.iter().flat_map(|a| a.nodes());
        let when_nodes = self.whens.iter().flat_map(|w| w.nodes());
        let default_nodes = self.default.iter().flat_map(|d| d.nodes());
        current
            .chain(arg_nodes)
            .chain(when_nodes)
            .chain(default_nodes)
    }

    /// Check if this CASE expression contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        self.arg.as_ref().is_some_and(|a| a.has_sublink())
            || self.whens.iter().any(|w| w.has_sublink())
            || self.default.as_ref().is_some_and(|d| d.has_sublink())
    }
}

impl Deparse for CaseExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("CASE");
        if let Some(arg) = &self.arg {
            buf.push(' ');
            arg.deparse(buf);
        }
        for when in &self.whens {
            buf.push_str(" WHEN ");
            when.condition.deparse(buf);
            buf.push_str(" THEN ");
            when.result.deparse(buf);
        }
        if let Some(default) = &self.default {
            buf.push_str(" ELSE ");
            default.deparse(buf);
        }
        buf.push_str(" END");
        buf
    }
}

/// A single WHEN clause in a CASE expression
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CaseWhen {
    /// The condition (for searched CASE) or value (for simple CASE)
    pub condition: WhereExpr,
    /// The result if condition is true/matches
    pub result: ColumnExpr,
}

impl CaseWhen {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let condition_nodes = self.condition.nodes();
        let result_nodes = self.result.nodes();
        condition_nodes.chain(result_nodes)
    }

    pub fn has_sublink(&self) -> bool {
        self.condition.has_sublink() || self.result.has_sublink()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

impl Deparse for TableAlias {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.name.deparse(buf);
        if !self.columns.is_empty() {
            buf.push('(');
            let mut sep = "";
            for column in self.columns.iter().map(|c| c.as_str()) {
                buf.push_str(sep);
                column.deparse(buf);
                sep = ", ";
            }
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

    /// Check if this table source contains sublinks/subqueries
    pub fn has_sublink(&self) -> bool {
        match self {
            TableSource::Subquery(_) => true,
            TableSource::Table(_) => false,
            TableSource::Join(join) => {
                join.left.has_sublink()
                    || join.right.has_sublink()
                    || join.condition.as_ref().is_some_and(|c| c.has_sublink())
            }
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
            schema.deparse(buf);
            buf.push('.');
        }

        self.name.deparse(buf);

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

impl OrderByClause {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        Box::new(current.chain(children))
    }
}

impl Deparse for OrderByClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.expr.deparse(buf);
        buf.push(' ');
        self.direction.deparse(buf);
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl OrderDirection {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for OrderDirection {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            OrderDirection::Asc => buf.push_str("ASC"),
            OrderDirection::Desc => buf.push_str("DESC"),
        }
        buf
    }
}

impl TryFrom<SortByDir> for OrderDirection {
    type Error = AstError;

    fn try_from(dir: SortByDir) -> Result<Self, Self::Error> {
        match dir {
            SortByDir::SortbyAsc | SortByDir::SortbyDefault => Ok(OrderDirection::Asc),
            SortByDir::SortbyDesc => Ok(OrderDirection::Desc),
            SortByDir::Undefined | SortByDir::SortbyUsing => Err(AstError::UnsupportedFeature {
                feature: format!("ORDER BY direction: {dir:?}"),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct LimitClause {
    pub count: Option<LiteralValue>,
    pub offset: Option<LiteralValue>,
}

/// Convert a pg_query ParseResult into our simplified AST
pub fn sql_query_convert(ast: &ParseResult) -> Result<SqlQuery, AstError> {
    let [raw_stmt] = ast.protobuf.stmts.as_slice() else {
        return Err(AstError::MultipleStatements);
    };

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
    let order_by = order_by_clause_convert(&select_stmt.sort_clause)?;
    let group_by = group_by_clause_convert(&select_stmt.group_clause)?;
    let having = having_clause_convert(select_stmt.having_clause.as_deref())?;
    let limit = limit_clause_convert(
        select_stmt.limit_count.as_deref(),
        select_stmt.limit_offset.as_deref(),
    )?;

    Ok(SelectStatement {
        columns,
        from,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
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
                    if let [field] = col_ref.fields.as_slice()
                        && let Some(NodeEnum::AStar(_)) = &field.node
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
                                expr: ColumnExpr::Subquery(Box::new(statement)),
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
                Some(NodeEnum::FuncCall(func_call)) => {
                    let function = func_call_convert(func_call)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::AConst(const_val)) => {
                    let value = const_value_extract(const_val)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Literal(value),
                        alias,
                    });
                }
                Some(NodeEnum::CoalesceExpr(coalesce)) => {
                    let function = coalesce_expr_convert(coalesce)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::MinMaxExpr(minmax)) => {
                    let function = minmax_expr_convert(minmax)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::AExpr(aexpr))
                    if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprNullif) =>
                {
                    let function = aexpr_nullif_convert(aexpr)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Function(function),
                        alias,
                    });
                }
                Some(NodeEnum::AExpr(aexpr))
                    if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprOp) =>
                {
                    let arith = aexpr_arithmetic_convert(aexpr)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Arithmetic(arith),
                        alias,
                    });
                }
                Some(NodeEnum::CaseExpr(case_expr)) => {
                    let case = case_expr_convert(case_expr)?;
                    columns.push(SelectColumn {
                        expr: ColumnExpr::Case(case),
                        alias,
                    });
                }
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
            feature: "Mixed * and column list".to_owned(),
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
            feature: "join missing left argument".to_owned(),
        });
    };

    // Convert right argument - can be a table, subquery, or another join
    let right_table = if let Some(rarg_node) = &join_expr.rarg {
        join_arg_convert(rarg_node, "right")?
    } else {
        return Err(AstError::UnsupportedSelectFeature {
            feature: "join missing right argument".to_owned(),
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

fn node_convert_to_column_expr(node: &Node) -> Result<ColumnExpr, AstError> {
    match node.node.as_ref() {
        Some(NodeEnum::ColumnRef(col_ref)) => {
            let column = column_ref_convert(col_ref)?;
            Ok(ColumnExpr::Column(column))
        }
        Some(NodeEnum::AConst(const_val)) => {
            let value = const_value_extract(const_val)?;
            Ok(ColumnExpr::Literal(value))
        }
        Some(NodeEnum::SubLink(sub_link)) => {
            match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::SelectStmt(select_stmt)) => {
                    let statement = select_statement_convert(select_stmt)?;
                    Ok(ColumnExpr::Subquery(Box::new(statement)))
                }
                other => Err(AstError::UnsupportedFeature {
                    feature: format!("Sublink type: {other:?}"),
                }),
            }
        }
        Some(NodeEnum::FuncCall(func_call)) => {
            let function = func_call_convert(func_call)?;
            Ok(ColumnExpr::Function(function))
        }
        Some(NodeEnum::CoalesceExpr(coalesce)) => {
            let function = coalesce_expr_convert(coalesce)?;
            Ok(ColumnExpr::Function(function))
        }
        Some(NodeEnum::MinMaxExpr(minmax)) => {
            let function = minmax_expr_convert(minmax)?;
            Ok(ColumnExpr::Function(function))
        }
        Some(NodeEnum::AExpr(aexpr))
            if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprNullif) =>
        {
            let function = aexpr_nullif_convert(aexpr)?;
            Ok(ColumnExpr::Function(function))
        }
        Some(NodeEnum::AExpr(aexpr))
            if AExprKind::try_from(aexpr.kind) == Ok(AExprKind::AexprOp) =>
        {
            let arith = aexpr_arithmetic_convert(aexpr)?;
            Ok(ColumnExpr::Arithmetic(arith))
        }
        Some(NodeEnum::CaseExpr(case_expr)) => {
            let case = case_expr_convert(case_expr)?;
            Ok(ColumnExpr::Case(case))
        }
        other => Err(AstError::UnsupportedFeature {
            feature: format!("Column expression node: {other:?}"),
        }),
    }
}

fn func_call_convert(func_call: &FuncCall) -> Result<FunctionCall, AstError> {
    // Extract function name - last component of qualified name (e.g., "pg_catalog.count" -> "count")
    let name = func_call
        .funcname
        .iter()
        .filter_map(|n| match &n.node {
            Some(NodeEnum::String(s)) => Some(s.sval.clone()),
            _ => None,
        })
        .next_back()
        .ok_or_else(|| AstError::UnsupportedSelectFeature {
            feature: "function with no name".to_owned(),
        })?;

    // Handle COUNT(*) - agg_star means no explicit args
    let args = if func_call.agg_star {
        vec![]
    } else {
        func_call
            .args
            .iter()
            .map(node_convert_to_column_expr)
            .collect::<Result<Vec<_>, _>>()?
    };

    // Parse aggregate ORDER BY (same structure as window ORDER BY)
    let agg_order = window_order_by_convert(&func_call.agg_order)?;

    // Parse OVER clause for window functions
    let over = func_call
        .over
        .as_ref()
        .map(|win_def| window_def_convert(win_def))
        .transpose()?;

    Ok(FunctionCall {
        name,
        args,
        agg_star: func_call.agg_star,
        agg_distinct: func_call.agg_distinct,
        agg_order,
        over,
    })
}

/// Convert a pg_query WindowDef to our WindowSpec
fn window_def_convert(
    win_def: &pg_query::protobuf::WindowDef,
) -> Result<WindowSpec, AstError> {
    // Convert PARTITION BY columns
    let partition_by = win_def
        .partition_clause
        .iter()
        .map(node_convert_to_column_expr)
        .collect::<Result<Vec<_>, _>>()?;

    // Convert ORDER BY clauses
    let order_by = window_order_by_convert(&win_def.order_clause)?;

    Ok(WindowSpec {
        partition_by,
        order_by,
    })
}

/// Convert window ORDER BY clause (similar to regular ORDER BY but from window context)
fn window_order_by_convert(
    order_clause: &[pg_query::protobuf::Node],
) -> Result<Vec<OrderByClause>, AstError> {
    let mut order_by = Vec::new();

    for sort_node in order_clause {
        if let Some(NodeEnum::SortBy(sort_by)) = &sort_node.node {
            let expr = sort_by
                .node
                .as_ref()
                .ok_or_else(|| AstError::UnsupportedFeature {
                    feature: "ORDER BY with no expression".to_owned(),
                })
                .and_then(|n| node_convert_to_column_expr(n))?;

            let direction = match SortByDir::try_from(sort_by.sortby_dir) {
                Ok(SortByDir::SortbyAsc) => OrderDirection::Asc,
                Ok(SortByDir::SortbyDesc) => OrderDirection::Desc,
                Ok(SortByDir::SortbyDefault) => OrderDirection::Asc, // Default is ASC
                _ => {
                    return Err(AstError::UnsupportedFeature {
                        feature: format!("ORDER BY direction: {}", sort_by.sortby_dir),
                    })
                }
            };

            order_by.push(OrderByClause { expr, direction });
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("Window ORDER BY node type: {sort_node:?}"),
            });
        }
    }

    Ok(order_by)
}

fn coalesce_expr_convert(coalesce: &CoalesceExpr) -> Result<FunctionCall, AstError> {
    let args = coalesce
        .args
        .iter()
        .map(node_convert_to_column_expr)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FunctionCall {
        name: "coalesce".to_owned(),
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        over: None,
    })
}

fn minmax_expr_convert(minmax: &MinMaxExpr) -> Result<FunctionCall, AstError> {
    let name = match MinMaxOp::try_from(minmax.op) {
        Ok(MinMaxOp::IsGreatest) => "greatest",
        Ok(MinMaxOp::IsLeast) => "least",
        _ => {
            return Err(AstError::UnsupportedFeature {
                feature: format!("Unknown MinMaxOp: {}", minmax.op),
            })
        }
    };

    let args = minmax
        .args
        .iter()
        .map(node_convert_to_column_expr)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FunctionCall {
        name: name.to_owned(),
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        over: None,
    })
}

fn aexpr_nullif_convert(aexpr: &AExpr) -> Result<FunctionCall, AstError> {
    let mut args = Vec::new();

    if let Some(lexpr) = &aexpr.lexpr {
        args.push(node_convert_to_column_expr(lexpr)?);
    }
    if let Some(rexpr) = &aexpr.rexpr {
        args.push(node_convert_to_column_expr(rexpr)?);
    }

    Ok(FunctionCall {
        name: "nullif".to_owned(),
        args,
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        over: None,
    })
}

fn aexpr_arithmetic_convert(aexpr: &AExpr) -> Result<ArithmeticExpr, AstError> {
    // Extract operator from name field
    let op = arithmetic_op_extract(&aexpr.name)?;

    let left = aexpr
        .lexpr
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "arithmetic expression without left operand".to_owned(),
        })
        .and_then(|n| node_convert_to_column_expr(n))?;

    let right = aexpr
        .rexpr
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "arithmetic expression without right operand".to_owned(),
        })
        .and_then(|n| node_convert_to_column_expr(n))?;

    Ok(ArithmeticExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn arithmetic_op_extract(name_nodes: &[pg_query::Node]) -> Result<ArithmeticOp, AstError> {
    let [name_node] = name_nodes else {
        return Err(AstError::UnsupportedFeature {
            feature: "multi-part operator names in arithmetic".to_owned(),
        });
    };

    match name_node.node.as_ref() {
        Some(NodeEnum::String(s)) => match s.sval.as_str() {
            "+" => Ok(ArithmeticOp::Add),
            "-" => Ok(ArithmeticOp::Subtract),
            "*" => Ok(ArithmeticOp::Multiply),
            "/" => Ok(ArithmeticOp::Divide),
            op => Err(AstError::UnsupportedFeature {
                feature: format!("arithmetic operator: {op}"),
            }),
        },
        _ => Err(AstError::UnsupportedFeature {
            feature: "invalid operator name format".to_owned(),
        }),
    }
}

fn case_expr_convert(case_expr: &PgCaseExpr) -> Result<CaseExpr, AstError> {
    // Convert optional arg (for simple CASE: CASE expr WHEN val...)
    let arg = case_expr
        .arg
        .as_ref()
        .map(|n| node_convert_to_column_expr(n))
        .transpose()?
        .map(Box::new);

    // Convert WHEN clauses
    let whens = case_expr
        .args
        .iter()
        .map(case_when_convert)
        .collect::<Result<Vec<_>, _>>()?;

    // Convert ELSE clause
    let default = case_expr
        .defresult
        .as_ref()
        .map(|n| node_convert_to_column_expr(n))
        .transpose()?
        .map(Box::new);

    Ok(CaseExpr { arg, whens, default })
}

fn case_when_convert(node: &Node) -> Result<CaseWhen, AstError> {
    let Some(NodeEnum::CaseWhen(case_when)) = &node.node else {
        return Err(AstError::UnsupportedFeature {
            feature: format!("Expected CaseWhen, got: {:?}", node.node),
        });
    };

    let condition = case_when
        .expr
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "CASE WHEN without condition".to_owned(),
        })
        .and_then(|n| node_convert_to_expr(n).map_err(AstError::from))?;

    let result = case_when
        .result
        .as_ref()
        .ok_or_else(|| AstError::UnsupportedFeature {
            feature: "CASE WHEN without result".to_owned(),
        })
        .and_then(|n| node_convert_to_column_expr(n))?;

    Ok(CaseWhen { condition, result })
}

fn order_by_clause_convert(sort_clause: &[Node]) -> Result<Vec<OrderByClause>, AstError> {
    let mut order_by = Vec::new();

    for sort_node in sort_clause {
        if let Some(NodeEnum::SortBy(sort_by)) = &sort_node.node {
            let expr_node = sort_by.node.as_ref().ok_or(AstError::UnsupportedFeature {
                feature: "ORDER BY without expression".to_owned(),
            })?;

            let expr = node_convert_to_column_expr(expr_node)?;
            let direction =
                OrderDirection::try_from(SortByDir::try_from(sort_by.sortby_dir).map_err(
                    |_| AstError::UnsupportedFeature {
                        feature: format!("Invalid SortByDir value: {}", sort_by.sortby_dir),
                    },
                )?)?;

            order_by.push(OrderByClause { expr, direction });
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("ORDER BY node type: {sort_node:?}"),
            });
        }
    }

    Ok(order_by)
}

fn group_by_clause_convert(group_clause: &[Node]) -> Result<Vec<ColumnNode>, AstError> {
    let mut group_by = Vec::new();
    for node in group_clause {
        if let Some(NodeEnum::ColumnRef(col_ref)) = &node.node {
            group_by.push(column_ref_convert(col_ref)?);
        } else {
            return Err(AstError::UnsupportedFeature {
                feature: format!("GROUP BY expression: {node:?}"),
            });
        }
    }
    Ok(group_by)
}

fn having_clause_convert(having_clause: Option<&Node>) -> Result<Option<WhereExpr>, AstError> {
    match having_clause {
        Some(node) => Ok(Some(node_convert_to_expr(node)?)),
        None => Ok(None),
    }
}

fn limit_clause_convert(
    limit_count: Option<&Node>,
    limit_offset: Option<&Node>,
) -> Result<Option<LimitClause>, AstError> {
    let count = limit_node_extract(limit_count)?;
    let offset = limit_node_extract(limit_offset)?;

    if count.is_none() && offset.is_none() {
        return Ok(None);
    }

    Ok(Some(LimitClause { count, offset }))
}

fn limit_node_extract(node: Option<&Node>) -> Result<Option<LiteralValue>, AstError> {
    let Some(node) = node else { return Ok(None) };

    match &node.node {
        Some(NodeEnum::AConst(const_val)) => {
            let value = const_value_extract(const_val)?;
            match value {
                LiteralValue::Integer(_) => Ok(Some(value)),
                LiteralValue::String(_)
                | LiteralValue::StringWithCast(..)
                | LiteralValue::Float(_)
                | LiteralValue::Boolean(_)
                | LiteralValue::Null
                | LiteralValue::Parameter(_) => Err(AstError::UnsupportedFeature {
                    feature: format!("LIMIT/OFFSET value: {value:?}"),
                }),
            }
        }
        Some(NodeEnum::ParamRef(param_ref)) => Ok(Some(LiteralValue::Parameter(format!(
            "${}",
            param_ref.number
        )))),
        other => Err(AstError::UnsupportedFeature {
            feature: format!("LIMIT/OFFSET expression: {other:?}"),
        }),
    }
}

/// Create a fingerprint hash for SQL query AST.
pub fn ast_query_fingerprint(select_statement: &SelectStatement) -> u64 {
    let mut hasher = DefaultHasher::new();
    select_statement.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::unwrap_used)]

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
                assert_eq!(col_ref.table, Some("u".to_owned()));
                assert_eq!(col_ref.column, "id");
            }

            // Second column: u.name
            if let ColumnExpr::Column(col_ref) = &columns[1].expr {
                assert_eq!(col_ref.table, Some("u".to_owned()));
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
            assert_eq!(columns[0].alias, Some("user_id".to_owned()));
            if let ColumnExpr::Column(col_ref) = &columns[0].expr {
                assert_eq!(col_ref.column, "id");
            }

            // Second column: name as full_name
            assert_eq!(columns[1].alias, Some("full_name".to_owned()));
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
        LiteralValue::String("hello".to_owned()).deparse(&mut buf);
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
        LiteralValue::Parameter("$1".to_owned()).deparse(&mut buf);
        assert_eq!(buf, "$1");
    }

    #[test]
    fn test_table_node_deparse() {
        let mut buf = String::new();

        // Simple table
        TableNode {
            schema: None,
            name: "users".to_owned(),
            alias: None,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " users");
        buf.clear();

        // Qualified table with alias
        TableNode {
            schema: Some("public".to_owned()),
            name: "users".to_owned(),
            alias: Some(TableAlias {
                name: "alias".to_owned(),
                columns: vec![],
            }),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users alias");
        buf.clear();

        // table requires quoting
        TableNode {
            schema: Some("public".to_owned()),
            name: "userAccount".to_owned(),
            alias: Some(TableAlias {
                name: "usrAcc".to_owned(),
                columns: vec![],
            }),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.\"userAccount\" \"usrAcc\"");
    }

    #[test]
    fn test_column_ref_deparse() {
        let mut buf = String::new();

        // Simple column
        ColumnNode {
            table: None,
            column: "id".to_owned(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "id");
        buf.clear();

        // Qualified column
        ColumnNode {
            table: Some("users".to_owned()),
            column: "name".to_owned(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "users.name");
        buf.clear();

        // table and column require quoting
        ColumnNode {
            table: Some("Users".to_owned()),
            column: "firstName".to_owned(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "\"Users\".\"firstName\"");
    }

    #[test]
    fn test_select_column_alias_deparse() {
        let mut buf = String::new();

        // Simple column
        SelectColumn {
            expr: ColumnExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            }),
            alias: Some("alias".to_owned()),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " id AS alias");
        buf.clear();

        // Qualified column
        SelectColumn {
            expr: ColumnExpr::Column(ColumnNode {
                table: Some("users".to_owned()),
                column: "name".to_owned(),
            }),
            alias: Some("alias".to_owned()),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " users.name AS alias");
        buf.clear();

        // alias requires quoting
        SelectColumn {
            expr: ColumnExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            }),
            alias: Some("Alias".to_owned()),
        }
        .deparse(&mut buf);
        assert_eq!(buf, " id AS \"Alias\"");
    }

    #[test]
    fn test_unary_op_deparse() {
        let mut buf = String::new();

        UnaryOp::Not.deparse(&mut buf);
        assert_eq!(buf, "NOT");
        buf.clear();

        UnaryOp::IsNull.deparse(&mut buf);
        assert_eq!(buf, "IS NULL");
        buf.clear();

        UnaryOp::IsNotNull.deparse(&mut buf);
        assert_eq!(buf, "IS NOT NULL");
    }

    #[test]
    fn test_binary_op_deparse() {
        let mut buf = String::new();

        // Comparison operators
        BinaryOp::Equal.deparse(&mut buf);
        assert_eq!(buf, "=");
        buf.clear();

        BinaryOp::NotEqual.deparse(&mut buf);
        assert_eq!(buf, "!=");
        buf.clear();

        BinaryOp::LessThan.deparse(&mut buf);
        assert_eq!(buf, "<");
        buf.clear();

        BinaryOp::GreaterThanOrEqual.deparse(&mut buf);
        assert_eq!(buf, ">=");
        buf.clear();

        // Pattern matching
        BinaryOp::Like.deparse(&mut buf);
        assert_eq!(buf, "LIKE");
        buf.clear();

        BinaryOp::NotLike.deparse(&mut buf);
        assert_eq!(buf, "NOT LIKE");
        buf.clear();

        // Logical operators
        BinaryOp::And.deparse(&mut buf);
        assert_eq!(buf, "AND");
        buf.clear();

        BinaryOp::Or.deparse(&mut buf);
        assert_eq!(buf, "OR");
    }

    #[test]
    fn test_multi_op_deparse() {
        let mut buf = String::new();

        MultiOp::In.deparse(&mut buf);
        assert_eq!(buf, "IN");
        buf.clear();

        MultiOp::NotIn.deparse(&mut buf);
        assert_eq!(buf, "NOT IN");
    }

    #[test]
    fn test_binary_expr_deparse() {
        let mut buf = String::new();

        // Simple equality: id = 1
        let expr = BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(1))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "id = 1");
        buf.clear();

        // Complex expression: users.name = 'john'
        let expr = BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: Some("users".to_owned()),
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "users.name = 'john'");
    }

    #[test]
    fn test_unary_expr_deparse() {
        let mut buf = String::new();

        // NOT active (prefix operator)
        let expr = UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "active".to_owned(),
            })),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "NOT active");
        buf.clear();

        // deleted_at IS NULL (postfix operator)
        let expr = UnaryExpr {
            op: UnaryOp::IsNull,
            expr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "deleted_at".to_owned(),
            })),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "deleted_at IS NULL");
        buf.clear();

        // name IS NOT NULL (postfix operator)
        let expr = UnaryExpr {
            op: UnaryOp::IsNotNull,
            expr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
        };

        expr.deparse(&mut buf);
        assert_eq!(buf, "name IS NOT NULL");
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
    fn test_parameterized_query_single_param() {
        let sql = "SELECT * FROM users WHERE id = $1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Verify the WHERE clause contains a parameter
        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();
        assert_eq!(literals.len(), 1);
        assert_eq!(literals[0], &LiteralValue::Parameter("$1".to_owned()));

        // Test deparsing
        let mut deparsed = String::with_capacity(1024);
        ast.deparse(&mut deparsed);
        assert_eq!(deparsed, sql);
    }

    #[test]
    fn test_parameterized_query_multiple_params() {
        let sql = "SELECT * FROM users WHERE name = $1 AND age > $2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Verify the WHERE clause contains both parameters
        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();
        assert_eq!(literals.len(), 2);
        assert_eq!(literals[0], &LiteralValue::Parameter("$1".to_owned()));
        assert_eq!(literals[1], &LiteralValue::Parameter("$2".to_owned()));

        // Test deparsing
        let mut deparsed = String::with_capacity(1024);
        ast.deparse(&mut deparsed);
        assert_eq!(deparsed, sql);
    }

    #[test]
    fn test_parameterized_query_mixed_params_and_literals() {
        let sql = "SELECT * FROM users WHERE name = $1 AND active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Verify the WHERE clause contains parameter and boolean literal
        let where_clause = ast.where_clause().unwrap();
        let literals: Vec<&LiteralValue> = where_clause.nodes().collect();
        assert_eq!(literals.len(), 2);
        assert_eq!(literals[0], &LiteralValue::Parameter("$1".to_owned()));
        assert_eq!(literals[1], &LiteralValue::Boolean(true));

        // Test deparsing
        let mut deparsed = String::with_capacity(1024);
        ast.deparse(&mut deparsed);
        assert_eq!(deparsed, sql);
    }

    #[test]
    fn test_literal_empty_string() {
        let mut buf = String::new();
        LiteralValue::String("".to_owned()).deparse(&mut buf);
        assert_eq!(buf, "''");
    }

    #[test]
    fn test_literal_string_with_quotes() {
        let mut buf = String::new();
        LiteralValue::String("test'quote".to_owned()).deparse(&mut buf);
        // postgres-protocol should properly escape the quote
        assert_eq!(buf, "'test''quote'");
    }

    #[test]
    fn test_literal_string_with_backslashes() {
        let mut buf = String::new();
        LiteralValue::String("test\\path".to_owned()).deparse(&mut buf);
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
        assert_eq!(literals[0], &LiteralValue::String("john".to_owned()));
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
        assert_eq!(binary_exprs[0].op, BinaryOp::And);
        assert_eq!(binary_exprs[1].op, BinaryOp::Equal);
        assert_eq!(binary_exprs[2].op, BinaryOp::GreaterThan);
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
        assert_eq!(columns[0].table, Some("u".to_owned()));
        assert_eq!(columns[0].column, "id");
        assert_eq!(columns[1].table, Some("o".to_owned()));
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
        let func = FunctionCall {
            name: "COUNT".to_owned(),
            args: vec![ColumnExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![],
            over: None,
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
        assert_eq!(tables[0].schema, Some("public".to_owned()));
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
        assert_eq!(unary_exprs[0].op, UnaryOp::Not);
    }

    #[test]
    fn test_binary_expr_nodes() {
        let sql = "SELECT * FROM users WHERE name = 'john'";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Test that BinaryExpr::nodes() returns itself
        let binary_exprs: Vec<&BinaryExpr> = ast.nodes().collect();
        assert_eq!(binary_exprs.len(), 1);
        assert_eq!(binary_exprs[0].op, BinaryOp::Equal);
    }

    #[test]
    fn test_multi_expr_nodes() {
        // Test MultiExpr::nodes() with a manually constructed MultiExpr
        let multi = MultiExpr {
            op: MultiOp::In,
            exprs: vec![
                WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                }),
                WhereExpr::Value(LiteralValue::Integer(1)),
                WhereExpr::Value(LiteralValue::Integer(2)),
            ],
        };

        // Test that MultiExpr::nodes() returns itself
        let multi_exprs: Vec<&MultiExpr> = multi.nodes().collect();
        assert_eq!(multi_exprs.len(), 1);
        assert_eq!(multi_exprs[0].op, MultiOp::In);

        // Test that it can extract child nodes
        let columns: Vec<&ColumnNode> = multi.nodes().collect();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column, "id");
    }

    #[test]
    fn test_multi_expr_in_deparse() {
        let multi = MultiExpr {
            op: MultiOp::In,
            exprs: vec![
                WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "status".to_owned(),
                }),
                WhereExpr::Value(LiteralValue::String("active".to_owned())),
                WhereExpr::Value(LiteralValue::String("pending".to_owned())),
            ],
        };

        let mut buf = String::new();
        multi.deparse(&mut buf);
        assert_eq!(buf, "status IN ('active', 'pending')");
    }

    #[test]
    fn test_multi_expr_not_in_deparse() {
        let multi = MultiExpr {
            op: MultiOp::NotIn,
            exprs: vec![
                WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                }),
                WhereExpr::Value(LiteralValue::Integer(1)),
                WhereExpr::Value(LiteralValue::Integer(2)),
                WhereExpr::Value(LiteralValue::Integer(3)),
            ],
        };

        let mut buf = String::new();
        multi.deparse(&mut buf);
        assert_eq!(buf, "id NOT IN (1, 2, 3)");
    }

    #[test]
    fn test_in_clause_parse_and_deparse() {
        // Test that IN clause round-trips through parse and deparse
        let sql = "SELECT * FROM t WHERE status IN ('active', 'pending')";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let where_clause = select.where_clause.as_ref().unwrap();

        let mut buf = String::new();
        where_clause.deparse(&mut buf);
        assert_eq!(buf, "status IN ('active', 'pending')");
    }

    #[test]
    fn test_order_by_simple_asc() {
        let sql = "SELECT * FROM users ORDER BY name ASC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].direction, OrderDirection::Asc);

        if let ColumnExpr::Column(col) = &select.order_by[0].expr {
            assert_eq!(col.column, "name");
            assert_eq!(col.table, None);
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_simple_desc() {
        let sql = "SELECT * FROM users ORDER BY age DESC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].direction, OrderDirection::Desc);

        if let ColumnExpr::Column(col) = &select.order_by[0].expr {
            assert_eq!(col.column, "age");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_default_direction() {
        let sql = "SELECT * FROM users ORDER BY name";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.order_by.len(), 1);
        // Default direction should be ASC
        assert_eq!(select.order_by[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let sql = "SELECT * FROM users ORDER BY last_name ASC, first_name DESC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.order_by.len(), 2);

        // First ORDER BY clause
        assert_eq!(select.order_by[0].direction, OrderDirection::Asc);
        if let ColumnExpr::Column(col) = &select.order_by[0].expr {
            assert_eq!(col.column, "last_name");
        } else {
            panic!("Expected column expression");
        }

        // Second ORDER BY clause
        assert_eq!(select.order_by[1].direction, OrderDirection::Desc);
        if let ColumnExpr::Column(col) = &select.order_by[1].expr {
            assert_eq!(col.column, "first_name");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_qualified_column() {
        let sql = "SELECT * FROM users u ORDER BY u.name ASC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.order_by.len(), 1);

        if let ColumnExpr::Column(col) = &select.order_by[0].expr {
            assert_eq!(col.table, Some("u".to_owned()));
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column expression");
        }
    }

    #[test]
    fn test_order_by_with_where() {
        let sql = "SELECT * FROM users WHERE active = true ORDER BY name ASC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(select.where_clause.is_some());
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_order_by_deparse_asc() {
        let sql = "SELECT * FROM users ORDER BY name ASC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_order_by_deparse_desc() {
        let sql = "SELECT * FROM users ORDER BY age DESC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_order_by_deparse_multiple() {
        let sql = "SELECT * FROM users ORDER BY last_name ASC, first_name DESC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::with_capacity(1024);
        ast.deparse(&mut buf);
        assert_eq!(buf, sql);
    }

    #[test]
    fn test_order_by_round_trip() {
        let queries = vec![
            "SELECT * FROM users ORDER BY name ASC",
            "SELECT * FROM users ORDER BY age DESC",
            "SELECT id, name FROM users WHERE active = true ORDER BY created_at DESC",
            "SELECT * FROM users ORDER BY last_name ASC, first_name ASC, id DESC",
        ];

        for sql in queries {
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
            assert_eq!(ast1, ast2, "Round trip failed for: {sql}");
        }
    }

    #[test]
    fn test_order_by_nodes_extraction() {
        let sql = "SELECT * FROM users ORDER BY u.name ASC, age DESC";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        // Extract OrderByClause nodes
        let order_clauses: Vec<&OrderByClause> = ast.nodes().collect();
        assert_eq!(order_clauses.len(), 2);

        // Extract ColumnNode from ORDER BY
        let columns: Vec<&ColumnNode> = ast.nodes().collect();
        // Should find columns in ORDER BY clause
        assert!(columns.iter().any(|c| c.column == "name"));
        assert!(columns.iter().any(|c| c.column == "age"));
    }

    #[test]
    fn test_group_by_single_column() {
        let sql = "SELECT status FROM orders GROUP BY status";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.group_by.len(), 1);
        assert_eq!(select.group_by[0].column, "status");
        assert_eq!(select.group_by[0].table, None);
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let sql = "SELECT status, category FROM orders GROUP BY status, category";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.group_by.len(), 2);
        assert_eq!(select.group_by[0].column, "status");
        assert_eq!(select.group_by[1].column, "category");
    }

    #[test]
    fn test_group_by_qualified_column() {
        let sql = "SELECT o.status FROM orders o GROUP BY o.status";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.group_by.len(), 1);
        assert_eq!(select.group_by[0].column, "status");
        assert_eq!(select.group_by[0].table, Some("o".to_owned()));
    }

    #[test]
    fn test_having_simple() {
        let sql = "SELECT status FROM orders GROUP BY status HAVING status = 'active'";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(select.having.is_some());
    }

    #[test]
    fn test_having_with_and() {
        let sql = "SELECT category FROM sales GROUP BY category HAVING category = 'electronics' AND category != 'toys'";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(!select.group_by.is_empty());
        assert!(select.having.is_some());
    }

    #[test]
    fn test_limit_only() {
        let sql = "SELECT * FROM users LIMIT 10";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let limit = select.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, None);
    }

    #[test]
    fn test_offset_only() {
        let sql = "SELECT * FROM users OFFSET 20";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let limit = select.limit.as_ref().unwrap();
        assert_eq!(limit.count, None);
        assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
    }

    #[test]
    fn test_limit_and_offset() {
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 20";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let limit = select.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
    }

    #[test]
    fn test_no_limit() {
        let sql = "SELECT * FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(select.limit.is_none());
    }

    #[test]
    fn test_no_group_by() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(select.group_by.is_empty());
        assert!(select.having.is_none());
    }

    #[test]
    fn test_combined_group_by_having_limit() {
        let sql = "SELECT status FROM orders GROUP BY status HAVING status != 'cancelled' ORDER BY status DESC LIMIT 10";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert_eq!(select.group_by.len(), 1);
        assert!(select.having.is_some());
        assert!(select.limit.is_some());
        assert!(!select.order_by.is_empty());
    }

    #[test]
    fn test_limit_parameterized() {
        let sql = "SELECT * FROM users LIMIT $1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let limit = select.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".to_owned())));
        assert_eq!(limit.offset, None);
    }

    #[test]
    fn test_limit_and_offset_parameterized() {
        let sql = "SELECT * FROM users LIMIT $1 OFFSET $2";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let limit = select.limit.as_ref().unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".to_owned())));
        assert_eq!(limit.offset, Some(LiteralValue::Parameter("$2".to_owned())));
    }

    #[test]
    fn test_has_sublink_in_select_list() {
        let sql = "SELECT id, (SELECT x FROM other WHERE id = 1) as val FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(
            select.has_sublink(),
            "has_sublink() should detect subquery in SELECT list"
        );
    }

    #[test]
    fn test_has_sublink_in_from_clause() {
        let sql = "SELECT * FROM (SELECT id FROM users) sub";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(
            select.has_sublink(),
            "has_sublink() should detect subquery in FROM clause"
        );
    }

    #[test]
    fn test_has_sublink_in_join() {
        let sql = "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(
            select.has_sublink(),
            "has_sublink() should detect subquery in JOIN"
        );
    }

    #[test]
    fn test_has_sublink_in_where_clause() {
        let sql = "SELECT * FROM t WHERE id IN (SELECT id FROM other)";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(
            select.has_sublink(),
            "has_sublink() should detect subquery in WHERE clause"
        );
    }

    #[test]
    fn test_has_sublink_no_subquery() {
        let sql = "SELECT id, name FROM users WHERE active = true";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(
            !select.has_sublink(),
            "has_sublink() should return false when no subquery exists"
        );
    }

    #[test]
    fn test_function_count_star() {
        let sql = "SELECT COUNT(*) FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns.len(), 1);
        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "count");
        assert!(func.agg_star);
        assert!(func.args.is_empty());
    }

    #[test]
    fn test_function_count_column() {
        let sql = "SELECT COUNT(id) FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "count");
        assert!(!func.agg_star);
        assert_eq!(func.args.len(), 1);
    }

    #[test]
    fn test_function_count_distinct() {
        let sql = "SELECT COUNT(DISTINCT status) FROM orders";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "count");
        assert!(func.agg_distinct);
    }

    #[test]
    fn test_function_sum() {
        let sql = "SELECT SUM(amount) FROM orders WHERE tenant_id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "sum");
    }

    #[test]
    fn test_function_nested() {
        // Use ROUND(AVG(...)) since COALESCE is parsed as a special CoalesceExpr
        let sql = "SELECT ROUND(AVG(value)) FROM data";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "round");
        assert_eq!(func.args.len(), 1);

        // First arg should be AVG(value)
        let ColumnExpr::Function(inner) = &func.args[0] else {
            panic!("expected nested function");
        };
        assert_eq!(inner.name, "avg");
    }

    #[test]
    fn test_function_with_alias() {
        let sql = "SELECT COUNT(*) as total FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns[0].alias, Some("total".to_owned()));
    }

    #[test]
    fn test_function_mixed_with_columns() {
        let sql = "SELECT id, name, COUNT(*) as cnt FROM users GROUP BY id, name";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns.len(), 3);
        assert!(matches!(columns[0].expr, ColumnExpr::Column(_)));
        assert!(matches!(columns[1].expr, ColumnExpr::Column(_)));
        assert!(matches!(columns[2].expr, ColumnExpr::Function(_)));
    }

    #[test]
    fn test_literal_in_select() {
        let sql = "SELECT 42 as answer, 'hello' as greeting FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        assert_eq!(columns.len(), 2);
        assert!(matches!(
            columns[0].expr,
            ColumnExpr::Literal(LiteralValue::Integer(42))
        ));
        assert!(matches!(
            &columns[1].expr,
            ColumnExpr::Literal(LiteralValue::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn test_function_deparse_count_star() {
        let func = FunctionCall {
            name: "count".to_owned(),
            args: vec![],
            agg_star: true,
            agg_distinct: false,
            agg_order: vec![],
            over: None,
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "COUNT(*)");
    }

    #[test]
    fn test_function_deparse_count_distinct() {
        let func = FunctionCall {
            name: "count".to_owned(),
            args: vec![ColumnExpr::Column(ColumnNode {
                table: None,
                column: "status".to_owned(),
            })],
            agg_star: false,
            agg_distinct: true,
            agg_order: vec![],
            over: None,
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "COUNT(DISTINCT status)");
    }

    #[test]
    fn test_coalesce() {
        let sql = "SELECT COALESCE(name, 'unknown') FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "coalesce");
        assert_eq!(func.args.len(), 2);
    }

    #[test]
    fn test_coalesce_nested_with_function() {
        let sql = "SELECT COALESCE(MAX(value), 0) FROM data";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "coalesce");
        assert_eq!(func.args.len(), 2);

        // First arg should be MAX(value)
        let ColumnExpr::Function(inner) = &func.args[0] else {
            panic!("expected nested function");
        };
        assert_eq!(inner.name, "max");
    }

    #[test]
    fn test_greatest() {
        let sql = "SELECT GREATEST(a, b, c) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "greatest");
        assert_eq!(func.args.len(), 3);
    }

    #[test]
    fn test_least() {
        let sql = "SELECT LEAST(a, b) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "least");
        assert_eq!(func.args.len(), 2);
    }

    #[test]
    fn test_nullif() {
        let sql = "SELECT NULLIF(status, 'deleted') FROM items";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "nullif");
        assert_eq!(func.args.len(), 2);
    }

    #[test]
    fn test_case_searched() {
        let sql = "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Case(case) = &columns[0].expr else {
            panic!("expected case expression");
        };
        assert!(case.arg.is_none(), "searched CASE should have no arg");
        assert_eq!(case.whens.len(), 1);
        assert!(case.default.is_some(), "should have ELSE clause");
    }

    #[test]
    fn test_case_simple() {
        let sql = "SELECT CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END FROM items";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Case(case) = &columns[0].expr else {
            panic!("expected case expression");
        };
        assert!(case.arg.is_some(), "simple CASE should have arg");
        assert_eq!(case.whens.len(), 2);
        assert!(case.default.is_some(), "should have ELSE clause");
    }

    #[test]
    fn test_case_no_else() {
        let sql = "SELECT CASE WHEN x > 0 THEN 'positive' END FROM items";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Case(case) = &columns[0].expr else {
            panic!("expected case expression");
        };
        assert!(case.arg.is_none());
        assert_eq!(case.whens.len(), 1);
        assert!(case.default.is_none(), "should have no ELSE clause");
    }

    #[test]
    fn test_case_deparse() {
        let sql = "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::new();
        ast.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1"
        );
    }

    #[test]
    fn test_case_has_sublink() {
        // CASE with subquery in WHEN condition
        let sql = "SELECT CASE WHEN id IN (SELECT id FROM other) THEN 1 ELSE 0 END FROM items";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        assert!(select.has_sublink(), "CASE with subquery should have sublink");
    }

    #[test]
    fn test_window_function_simple() {
        let sql = "SELECT sum(amount) OVER (ORDER BY date) FROM orders";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "sum");
        assert!(func.over.is_some(), "should have OVER clause");

        let over = func.over.as_ref().unwrap();
        assert!(over.partition_by.is_empty(), "no PARTITION BY");
        assert_eq!(over.order_by.len(), 1, "one ORDER BY clause");
    }

    #[test]
    fn test_window_function_with_partition() {
        let sql = "SELECT sum(amount) OVER (PARTITION BY category ORDER BY date) FROM orders";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert!(func.over.is_some(), "should have OVER clause");

        let over = func.over.as_ref().unwrap();
        assert_eq!(over.partition_by.len(), 1, "one PARTITION BY column");
        assert_eq!(over.order_by.len(), 1, "one ORDER BY clause");
    }

    #[test]
    fn test_window_function_row_number() {
        let sql = "SELECT row_number() OVER (ORDER BY id) FROM users";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };
        assert_eq!(func.name, "row_number");
        assert!(func.args.is_empty(), "row_number has no args");
        assert!(func.over.is_some(), "should have OVER clause");
    }

    #[test]
    fn test_window_function_deparse() {
        let func = FunctionCall {
            name: "sum".to_owned(),
            args: vec![ColumnExpr::Column(ColumnNode {
                table: None,
                column: "amount".to_owned(),
            })],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![],
            over: Some(WindowSpec {
                partition_by: vec![ColumnExpr::Column(ColumnNode {
                    table: None,
                    column: "category".to_owned(),
                })],
                order_by: vec![OrderByClause {
                    expr: ColumnExpr::Column(ColumnNode {
                        table: None,
                        column: "date".to_owned(),
                    }),
                    direction: OrderDirection::Asc,
                }],
            }),
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "SUM(amount) OVER (PARTITION BY category ORDER BY date ASC)");
    }

    #[test]
    fn test_window_function_multiple_order_by() {
        let sql = "SELECT sum(x) OVER (ORDER BY a ASC, b DESC) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };

        let over = func.over.as_ref().unwrap();
        assert_eq!(over.order_by.len(), 2, "two ORDER BY clauses");
        assert_eq!(over.order_by[0].direction, OrderDirection::Asc);
        assert_eq!(over.order_by[1].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_aggregate_order_by_parse() {
        let sql = "SELECT string_agg(name, ', ' ORDER BY name) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };

        assert_eq!(func.name, "string_agg");
        assert_eq!(func.agg_order.len(), 1, "should have one ORDER BY clause");
        assert_eq!(func.agg_order[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_aggregate_order_by_deparse() {
        let sql = "SELECT string_agg(name, ', ' ORDER BY name ASC) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let mut buf = String::new();
        ast.deparse(&mut buf);
        assert!(buf.contains("ORDER BY"), "deparsed should contain ORDER BY");
        assert!(buf.contains("STRING_AGG"), "deparsed should contain STRING_AGG");
    }

    #[test]
    fn test_aggregate_distinct_and_order_by() {
        let sql = "SELECT string_agg(DISTINCT name, ', ' ORDER BY name) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };

        assert!(func.agg_distinct, "should have DISTINCT");
        assert_eq!(func.agg_order.len(), 1, "should have ORDER BY");
    }

    #[test]
    fn test_aggregate_multiple_order_by() {
        let sql = "SELECT array_agg(x ORDER BY y ASC, z DESC) FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Function(func) = &columns[0].expr else {
            panic!("expected function");
        };

        assert_eq!(func.name, "array_agg");
        assert_eq!(func.agg_order.len(), 2, "should have two ORDER BY clauses");
        assert_eq!(func.agg_order[0].direction, OrderDirection::Asc);
        assert_eq!(func.agg_order[1].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_aggregate_order_by_deparse_roundtrip() {
        let func = FunctionCall {
            name: "string_agg".to_owned(),
            args: vec![
                ColumnExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                }),
                ColumnExpr::Literal(LiteralValue::String(", ".to_owned())),
            ],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![OrderByClause {
                expr: ColumnExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                }),
                direction: OrderDirection::Asc,
            }],
            over: None,
        };
        let mut buf = String::new();
        func.deparse(&mut buf);
        assert_eq!(buf, "STRING_AGG(name, ', ' ORDER BY name ASC)");
    }

    #[test]
    fn test_arithmetic_multiply_parse() {
        let sql = "SELECT amount * 2 FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Arithmetic(arith) = &columns[0].expr else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Multiply);
    }

    #[test]
    fn test_arithmetic_multiply_negative() {
        let sql = "SELECT amount * -1 FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Arithmetic(arith) = &columns[0].expr else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Multiply);
        // Right side should be -1 (negative literal)
        assert!(matches!(
            arith.right.as_ref(),
            ColumnExpr::Literal(LiteralValue::Integer(-1))
        ));
    }

    #[test]
    fn test_arithmetic_add() {
        let sql = "SELECT price + tax FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Arithmetic(arith) = &columns[0].expr else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Add);
    }

    #[test]
    fn test_arithmetic_subtract() {
        let sql = "SELECT total - discount FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Arithmetic(arith) = &columns[0].expr else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Subtract);
    }

    #[test]
    fn test_arithmetic_divide() {
        let sql = "SELECT total / count FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Arithmetic(arith) = &columns[0].expr else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(arith.op, ArithmeticOp::Divide);
    }

    #[test]
    fn test_arithmetic_deparse() {
        let arith = ArithmeticExpr {
            left: Box::new(ColumnExpr::Column(ColumnNode {
                table: None,
                column: "amount".to_owned(),
            })),
            op: ArithmeticOp::Multiply,
            right: Box::new(ColumnExpr::Literal(LiteralValue::Integer(-1))),
        };
        let mut buf = String::new();
        arith.deparse(&mut buf);
        assert_eq!(buf, "(amount * -1)");
    }

    #[test]
    fn test_arithmetic_nested() {
        // (a + b) * c
        let sql = "SELECT (a + b) * c FROM t";
        let pg_ast = pg_query::parse(sql).unwrap();
        let ast = sql_query_convert(&pg_ast).unwrap();

        let Statement::Select(select) = &ast.statement;
        let SelectColumns::Columns(columns) = &select.columns else {
            panic!("expected columns");
        };

        let ColumnExpr::Arithmetic(outer) = &columns[0].expr else {
            panic!("expected arithmetic expression");
        };

        assert_eq!(outer.op, ArithmeticOp::Multiply);

        // Left side should be another arithmetic expression (a + b)
        let ColumnExpr::Arithmetic(inner) = outer.left.as_ref() else {
            panic!("expected nested arithmetic expression");
        };
        assert_eq!(inner.op, ArithmeticOp::Add);
    }
}

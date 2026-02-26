use std::any::{Any, TypeId};
use std::marker::PhantomData;

use ordered_float::NotNan;
use pg_query::protobuf::SortByDir;
use postgres_protocol::escape;
use strum_macros::AsRefStr;

use crate::cache::{SubqueryKind, UpdateQuerySource};

use super::{AstError, Deparse};

// Core literal value types that can appear in SQL expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LiteralValue {
    String(String),
    StringWithCast(String, String),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    Null,
    NullWithCast(String),
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
            (None, LiteralValue::NullWithCast(_)) => true,
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
                // PostgreSQL sends booleans as "t"/"f" in text protocol (used by CDC)
                match row_str.as_str() {
                    "t" | "true" => *constraint_bool,
                    "f" | "false" => !*constraint_bool,
                    _ => false,
                }
            }
            (Some(_), LiteralValue::Null) => false,
            (Some(_), LiteralValue::NullWithCast(_)) => false,
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
            LiteralValue::NullWithCast(cast) => {
                buf.push_str("NULL::");
                buf.push_str(cast);
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
    #[strum(to_string = "IS TRUE")]
    IsTrue,
    #[strum(to_string = "IS NOT TRUE")]
    IsNotTrue,
    #[strum(to_string = "IS FALSE")]
    IsFalse,
    #[strum(to_string = "IS NOT FALSE")]
    IsNotFalse,
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

impl BinaryOp {
    /// Returns true if this is a logical operator (AND/OR).
    pub fn is_logical(&self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or)
    }

    /// Returns true if this is a comparison operator (=, !=, <, <=, >, >=).
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual
        )
    }

    /// Flip a comparison operator for `value op column` -> `column op' value` normalization.
    /// Returns `None` for non-comparison ops.
    pub fn op_flip(&self) -> Option<BinaryOp> {
        match self {
            BinaryOp::Equal => Some(BinaryOp::Equal),
            BinaryOp::NotEqual => Some(BinaryOp::NotEqual),
            BinaryOp::LessThan => Some(BinaryOp::GreaterThan),
            BinaryOp::LessThanOrEqual => Some(BinaryOp::GreaterThanOrEqual),
            BinaryOp::GreaterThan => Some(BinaryOp::LessThan),
            BinaryOp::GreaterThanOrEqual => Some(BinaryOp::LessThanOrEqual),
            BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => None,
        }
    }
}

// Multi-operand operators (one subject with multiple values)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MultiOp {
    In,
    NotIn,
    Between,
    NotBetween,
    BetweenSymmetric,
    NotBetweenSymmetric,
    /// `expr op ANY (values)` -- comparison operator determines the test
    Any {
        comparison: BinaryOp,
    },
    /// `expr op ALL (values)` -- comparison operator determines the test
    All {
        comparison: BinaryOp,
    },
}

impl Deparse for MultiOp {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            MultiOp::In => buf.push_str("IN"),
            MultiOp::NotIn => buf.push_str("NOT IN"),
            MultiOp::Between => buf.push_str("BETWEEN"),
            MultiOp::NotBetween => buf.push_str("NOT BETWEEN"),
            MultiOp::BetweenSymmetric => buf.push_str("BETWEEN SYMMETRIC"),
            MultiOp::NotBetweenSymmetric => buf.push_str("NOT BETWEEN SYMMETRIC"),
            MultiOp::Any { comparison } => {
                comparison.deparse(buf);
                buf.push_str(" ANY");
            }
            MultiOp::All { comparison } => {
                comparison.deparse(buf);
                buf.push_str(" ALL");
            }
        }
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
            UnaryOp::IsNull
            | UnaryOp::IsNotNull
            | UnaryOp::IsTrue
            | UnaryOp::IsNotTrue
            | UnaryOp::IsFalse
            | UnaryOp::IsNotFalse => {
                // Postfix operators: expr IS NULL, expr IS TRUE, etc.
                self.expr.deparse(buf);
                buf.push(' ');
                self.op.deparse(buf);
            }
            UnaryOp::Not => {
                // Prefix operator: NOT expr
                // NOT has higher precedence than AND/OR, so NOT applied to a
                // logical binary expression needs parentheses to preserve
                // semantics: NOT (a AND b) != NOT a AND b
                let needs_parens = matches!(
                    self.expr.as_ref(),
                    WhereExpr::Binary(child) if child.op.is_logical()
                );
                self.op.deparse(buf);
                buf.push(' ');
                if needs_parens {
                    buf.push('(');
                }
                self.expr.deparse(buf);
                if needs_parens {
                    buf.push(')');
                }
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

    /// Whether a child expression needs parentheses to preserve semantics.
    /// This occurs when the child is a logical op with lower precedence than
    /// the parent (i.e., OR nested inside AND).
    fn child_needs_parens(&self, child: &WhereExpr) -> bool {
        if let WhereExpr::Binary(child_expr) = child {
            matches!((&self.op, &child_expr.op), (BinaryOp::And, BinaryOp::Or))
        } else {
            false
        }
    }
}

impl Deparse for BinaryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if self.child_needs_parens(&self.lexpr) {
            buf.push('(');
            self.lexpr.deparse(buf);
            buf.push(')');
        } else {
            self.lexpr.deparse(buf);
        }

        buf.push(' ');
        self.op.deparse(buf);
        buf.push(' ');

        if self.child_needs_parens(&self.rexpr) {
            buf.push('(');
            self.rexpr.deparse(buf);
            buf.push(')');
        } else {
            self.rexpr.deparse(buf);
        }

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
            MultiOp::Between
            | MultiOp::NotBetween
            | MultiOp::BetweenSymmetric
            | MultiOp::NotBetweenSymmetric => {
                buf.push(' ');
                self.op.deparse(buf);
                buf.push(' ');
                // BETWEEN low AND high -- exactly 2 bounds
                let mut sep = "";
                for expr in rest {
                    buf.push_str(sep);
                    expr.deparse(buf);
                    sep = " AND ";
                }
                return buf;
            }
            MultiOp::Any { .. } | MultiOp::All { .. } => {
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

    // Array literal: ARRAY[val1, val2, ...]
    Array(Vec<WhereExpr>),

    // Function calls (for extensibility)
    Function {
        name: String,
        args: Vec<WhereExpr>,
        agg_star: bool,
    },

    // Subqueries in WHERE clauses (EXISTS, IN, ANY, ALL, scalar)
    Subquery {
        query: Box<QueryExpr>,
        sublink_type: SubLinkType,
        /// Left-hand expression for IN/ANY/ALL (e.g., `id` in `id IN (SELECT ...)`)
        test_expr: Option<Box<WhereExpr>>,
    },
}

pub type WhereExprNodeIter<'a, N> =
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
                WhereExpr::Array(_) => None,
                WhereExpr::Function { .. } => None,
                WhereExpr::Subquery { .. } => None,
            }))
        .into_iter();

        // Chain with child nodes
        let children = match self {
            WhereExpr::Unary(unary) => Box::new(unary.expr.nodes()) as Box<dyn Iterator<Item = &N>>,
            WhereExpr::Binary(binary) => Box::new(binary.lexpr.nodes().chain(binary.rexpr.nodes())),
            WhereExpr::Multi(multi) => Box::new(multi.exprs.iter().flat_map(|expr| expr.nodes())),
            WhereExpr::Array(elems) => Box::new(elems.iter().flat_map(|expr| expr.nodes())),
            WhereExpr::Function { args, .. } => Box::new(args.iter().flat_map(|expr| expr.nodes())),
            WhereExpr::Subquery {
                query, test_expr, ..
            } => {
                let query_nodes = query.nodes();
                let test_nodes = test_expr.iter().flat_map(|e| e.nodes());
                Box::new(query_nodes.chain(test_nodes))
            }
            WhereExpr::Value(_) | WhereExpr::Column(_) => Box::new(std::iter::empty()),
        };

        current.chain(children)
    }

    /// Check if this WHERE expression contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        match self {
            WhereExpr::Binary(binary) => {
                binary.lexpr.has_subqueries() || binary.rexpr.has_subqueries()
            }
            WhereExpr::Unary(unary) => unary.expr.has_subqueries(),
            WhereExpr::Multi(multi) => multi.exprs.iter().any(|e| e.has_subqueries()),
            WhereExpr::Array(elems) => elems.iter().any(|e| e.has_subqueries()),
            WhereExpr::Function { args, .. } => args.iter().any(|e| e.has_subqueries()),
            WhereExpr::Subquery { .. } => true,
            WhereExpr::Value(_) | WhereExpr::Column(_) => false,
        }
    }

    /// Recursively collect SELECT branches from subqueries in this WHERE expression.
    /// Recursively collect subquery branches with source tracking.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion for
    /// EXISTS/ANY subqueries. ALL is already Exclusion (NOT IN).
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            WhereExpr::Binary(binary) => {
                binary.lexpr.subquery_nodes_collect(branches, negated);
                binary.rexpr.subquery_nodes_collect(branches, negated);
            }
            WhereExpr::Unary(unary) => {
                let child_negated = if unary.op == UnaryOp::Not {
                    !negated
                } else {
                    negated
                };
                unary.expr.subquery_nodes_collect(branches, child_negated);
            }
            WhereExpr::Multi(multi) => {
                for expr in &multi.exprs {
                    expr.subquery_nodes_collect(branches, negated);
                }
            }
            WhereExpr::Array(elems) => {
                for elem in elems {
                    elem.subquery_nodes_collect(branches, negated);
                }
            }
            WhereExpr::Function { args, .. } => {
                for arg in args {
                    arg.subquery_nodes_collect(branches, negated);
                }
            }
            WhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
            } => {
                let kind = match sublink_type {
                    SubLinkType::Expr => SubqueryKind::Scalar,
                    SubLinkType::Any | SubLinkType::Exists => {
                        if negated {
                            SubqueryKind::Exclusion
                        } else {
                            SubqueryKind::Inclusion
                        }
                    }
                    SubLinkType::All => {
                        if negated {
                            SubqueryKind::Inclusion
                        } else {
                            SubqueryKind::Exclusion
                        }
                    }
                };
                let source = UpdateQuerySource::Subquery(kind);
                query.select_nodes_collect(branches, source, negated);
                if let Some(test) = test_expr {
                    test.subquery_nodes_collect(branches, negated);
                }
            }
            WhereExpr::Value(_) | WhereExpr::Column(_) => {}
        }
    }
}

impl Deparse for WhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            WhereExpr::Value(literal) => {
                literal.deparse(buf);
            }
            WhereExpr::Column(col) => {
                col.deparse(buf);
            }
            WhereExpr::Unary(expr) => {
                expr.deparse(buf);
            }
            WhereExpr::Binary(expr) => {
                expr.deparse(buf);
            }
            WhereExpr::Multi(expr) => {
                expr.deparse(buf);
            }
            WhereExpr::Array(elems) => {
                buf.push_str("ARRAY[");
                let mut sep = "";
                for elem in elems {
                    buf.push_str(sep);
                    elem.deparse(buf);
                    sep = ", ";
                }
                buf.push(']');
            }
            WhereExpr::Function {
                name,
                args,
                agg_star,
            } => {
                buf.push_str(name);
                buf.push('(');
                if *agg_star {
                    buf.push('*');
                } else {
                    let mut sep = "";
                    for arg in args {
                        buf.push_str(sep);
                        arg.deparse(buf);
                        sep = ", ";
                    }
                }
                buf.push(')');
            }
            WhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
            } => {
                match sublink_type {
                    SubLinkType::Exists => {
                        buf.push_str("EXISTS (");
                        query.deparse(buf);
                        buf.push(')');
                    }
                    SubLinkType::Any => {
                        // IN is a special case of ANY
                        if let Some(test) = test_expr {
                            test.deparse(buf);
                            buf.push_str(" IN (");
                            query.deparse(buf);
                            buf.push(')');
                        } else {
                            buf.push('(');
                            query.deparse(buf);
                            buf.push(')');
                        }
                    }
                    SubLinkType::All => {
                        if let Some(test) = test_expr {
                            test.deparse(buf);
                            buf.push_str(" <> ALL (");
                            query.deparse(buf);
                            buf.push(')');
                        } else {
                            buf.push_str("ALL (");
                            query.deparse(buf);
                            buf.push(')');
                        }
                    }
                    SubLinkType::Expr => {
                        // Scalar subquery - just parenthesized query
                        buf.push('(');
                        query.deparse(buf);
                        buf.push(')');
                    }
                }
            }
        }

        buf
    }
}

// ============================================================================
// New Query Type Hierarchy (for UNION/INTERSECT/EXCEPT support)
// ============================================================================

/// Set operation type for UNION/INTERSECT/EXCEPT
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

impl Deparse for SetOpType {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SetOpType::Union => buf.push_str("UNION"),
            SetOpType::Intersect => buf.push_str("INTERSECT"),
            SetOpType::Except => buf.push_str("EXCEPT"),
        }
        buf
    }
}

/// VALUES clause with typed rows - represents `VALUES (1, 'a'), (2, 'b')`
#[derive(Debug, Clone, PartialEq, Hash, Default)]
pub struct ValuesClause {
    pub rows: Vec<Vec<LiteralValue>>,
}

impl ValuesClause {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        self.rows
            .iter()
            .flat_map(|row| row.iter().flat_map(|v| v.nodes()))
    }

    pub fn has_subqueries(&self) -> bool {
        false // VALUES clauses contain only literals
    }
}

impl Deparse for ValuesClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("VALUES ");
        let mut row_sep = "";
        buf.push('(');
        for row in &self.rows {
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
        buf
    }
}

/// Core SELECT without ORDER BY/LIMIT (those go on the parent query)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectNode {
    pub distinct: bool,
    pub columns: SelectColumns,
    pub from: Vec<TableSource>,
    pub where_clause: Option<WhereExpr>,
    pub group_by: Vec<ColumnNode>,
    pub having: Option<WhereExpr>,
}

impl Default for SelectNode {
    fn default() -> Self {
        Self {
            distinct: false,
            columns: SelectColumns::None,
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
        }
    }
}

impl SelectNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> + '_ {
        let columns_nodes = self.columns.nodes();
        let from_nodes = self.from.iter().flat_map(|t| t.nodes());
        let where_nodes = self.where_clause.iter().flat_map(|w| w.nodes());
        let group_by_nodes = self.group_by.iter().flat_map(|c| c.nodes());
        let having_nodes = self.having.iter().flat_map(|h| h.nodes());

        columns_nodes
            .chain(from_nodes)
            .chain(where_nodes)
            .chain(group_by_nodes)
            .chain(having_nodes)
    }

    /// Return table nodes directly in this SELECT's FROM clause.
    /// Traverses JOINs but does NOT descend into subqueries.
    pub fn direct_table_nodes(&self) -> Vec<&TableNode> {
        let mut tables = Vec::new();
        for source in &self.from {
            source.direct_table_nodes_collect(&mut tables);
        }
        tables
    }

    /// Check if this SELECT references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [TableSource::Table(_)])
    }

    /// Check if this SELECT contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        // Check columns for subqueries
        if let SelectColumns::Columns(columns) = &self.columns
            && columns.iter().any(|col| col.expr.has_subqueries())
        {
            return true;
        }

        // Check FROM clause for subqueries
        if self.from.iter().any(TableSource::has_subqueries) {
            return true;
        }

        // Check WHERE clause for subqueries
        if let Some(where_clause) = &self.where_clause
            && where_clause.has_subqueries()
        {
            return true;
        }

        // Check HAVING clause for subqueries
        if let Some(having) = &self.having
            && having.has_subqueries()
        {
            return true;
        }

        false
    }
}

impl Deparse for SelectNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
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

        buf
    }
}

/// Set operation node: `left UNION/INTERSECT/EXCEPT [ALL] right`
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SetOpNode {
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<QueryExpr>,
    pub right: Box<QueryExpr>,
}

impl SetOpNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_nodes = self.left.nodes();
        let right_nodes = self.right.nodes();
        Box::new(current.chain(left_nodes).chain(right_nodes))
    }

    pub fn has_subqueries(&self) -> bool {
        self.left.has_subqueries() || self.right.has_subqueries()
    }
}

impl Deparse for SetOpNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        buf.push(' ');
        self.op.deparse(buf);
        if self.all {
            buf.push_str(" ALL");
        }
        buf.push(' ');
        self.right.deparse(buf);
        buf
    }
}

/// The body of a query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum QueryBody {
    Select(SelectNode),
    Values(ValuesClause),
    SetOp(SetOpNode),
}

impl QueryBody {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &N> + '_> = match self {
            QueryBody::Select(select) => Box::new(select.nodes()),
            QueryBody::Values(values) => Box::new(values.nodes()),
            QueryBody::SetOp(set_op) => set_op.nodes(),
        };
        Box::new(current.chain(children))
    }

    pub fn has_subqueries(&self) -> bool {
        match self {
            QueryBody::Select(select) => select.has_subqueries(),
            QueryBody::Values(values) => values.has_subqueries(),
            QueryBody::SetOp(set_op) => set_op.has_subqueries(),
        }
    }
}

impl Deparse for QueryBody {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            QueryBody::Select(select) => select.deparse(buf),
            QueryBody::Values(values) => values.deparse(buf),
            QueryBody::SetOp(set_op) => set_op.deparse(buf),
        }
    }
}

/// A complete query expression with optional ordering/limiting
///
/// This represents a complete query that can be:
/// - A simple SELECT
/// - A VALUES clause
/// - A set operation (UNION/INTERSECT/EXCEPT)
///
/// ORDER BY and LIMIT apply to the entire query expression.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct QueryExpr {
    pub ctes: Vec<CteDefinition>,
    pub body: QueryBody,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<LimitClause>,
}

impl Default for QueryExpr {
    fn default() -> Self {
        Self {
            ctes: Vec::new(),
            body: QueryBody::Values(ValuesClause::default()),
            order_by: Vec::new(),
            limit: None,
        }
    }
}

/// Materialization hint for a CTE definition.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum CteMaterialization {
    /// No keyword -- optimizer decides
    Default,
    /// AS MATERIALIZED -- evaluate once, results reused
    Materialized,
    /// AS NOT MATERIALIZED -- optimizer may inline
    NotMaterialized,
}

/// A CTE definition from a WITH clause.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CteDefinition {
    pub name: String,
    pub query: QueryExpr,
    pub column_aliases: Vec<String>,
    pub materialization: CteMaterialization,
}

impl QueryExpr {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let cte_nodes = self.ctes.iter().flat_map(|c| c.query.nodes::<N>());
        let body_nodes = self.body.nodes();
        let order_by_nodes = self.order_by.iter().flat_map(|o| o.nodes());
        Box::new(
            current
                .chain(cte_nodes)
                .chain(body_nodes)
                .chain(order_by_nodes),
        )
    }

    /// Check if query only references a single table
    pub fn is_single_table(&self) -> bool {
        self.nodes::<TableNode>().nth(1).is_none()
    }

    /// Check if query has a WHERE clause (only applies to SELECT bodies)
    pub fn has_where_clause(&self) -> bool {
        match &self.body {
            QueryBody::Select(select) => select.where_clause.is_some(),
            QueryBody::Values(_) | QueryBody::SetOp(_) => false,
        }
    }

    /// Get the WHERE clause if it exists (only for SELECT bodies)
    pub fn where_clause(&self) -> Option<&WhereExpr> {
        match &self.body {
            QueryBody::Select(select) => select.where_clause.as_ref(),
            QueryBody::Values(_) | QueryBody::SetOp(_) => None,
        }
    }

    /// Check if this query contains subqueries
    pub fn has_subqueries(&self) -> bool {
        !self.ctes.is_empty()
            || self.body.has_subqueries()
            || self.order_by.iter().any(|o| o.expr.has_subqueries())
    }

    /// Get the SELECT body if this is a simple SELECT query
    pub fn as_select(&self) -> Option<&SelectNode> {
        match &self.body {
            QueryBody::Select(select) => Some(select),
            QueryBody::Values(_) | QueryBody::SetOp(_) => None,
        }
    }

    /// Extract all SELECT branches from this query expression.
    ///
    /// For a simple SELECT query, returns a single-element vector.
    /// For set operations (UNION/INTERSECT/EXCEPT), recursively extracts
    /// all SELECT branches from both sides.
    /// VALUES clauses are skipped (they don't reference tables).
    /// CTE definitions are collected via CteRef references, not eagerly.
    pub fn select_nodes(&self) -> Vec<&SelectNode> {
        self.select_nodes_with_source()
            .into_iter()
            .map(|(node, _)| node)
            .collect()
    }

    /// Extract all SELECT branches with their source context (FromClause vs Subquery).
    ///
    /// Tracks whether each branch came from the top-level body (FromClause) or
    /// from a subquery context (Subquery with kind). CTE definitions are not
    /// collected directly -- their branches are collected when referenced via
    /// CteRef, inheriting the reference site's source context.
    pub fn select_nodes_with_source(&self) -> Vec<(&SelectNode, UpdateQuerySource)> {
        let mut branches = Vec::new();
        self.select_nodes_collect(&mut branches, UpdateQuerySource::FromClause, false);
        branches
    }

    /// Collects branches with source tracking.
    /// `outer_source` is the source assigned to this query's body branches.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion.
    pub(crate) fn select_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        outer_source: UpdateQuerySource,
        negated: bool,
    ) {
        match &self.body {
            QueryBody::Select(select) => {
                branches.push((select, outer_source));
                for source in &select.from {
                    source.subquery_nodes_collect(branches, negated);
                }
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect(branches, negated);
                }
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect(branches, negated);
                }
                select.columns.subquery_nodes_collect(branches);
            }
            QueryBody::SetOp(set_op) => {
                set_op
                    .left
                    .select_nodes_collect(branches, outer_source, negated);
                set_op
                    .right
                    .select_nodes_collect(branches, outer_source, negated);
            }
            QueryBody::Values(_) => {}
        }
    }
}

impl Deparse for QueryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if !self.ctes.is_empty() {
            buf.push_str("WITH ");
            for (i, cte) in self.ctes.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                cte.name.deparse(buf);
                if !cte.column_aliases.is_empty() {
                    buf.push('(');
                    for (j, col) in cte.column_aliases.iter().enumerate() {
                        if j > 0 {
                            buf.push_str(", ");
                        }
                        col.deparse(buf);
                    }
                    buf.push(')');
                }
                buf.push_str(" AS ");
                match cte.materialization {
                    CteMaterialization::Default => {}
                    CteMaterialization::Materialized => buf.push_str("MATERIALIZED "),
                    CteMaterialization::NotMaterialized => buf.push_str("NOT MATERIALIZED "),
                }
                buf.push('(');
                cte.query.deparse(buf);
                buf.push(')');
            }
            buf.push(' ');
        }

        self.body.deparse(buf);

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
            if let Some(count) = &limit.count {
                buf.push_str(" LIMIT ");
                count.deparse(buf);
            }
            if let Some(offset) = &limit.offset {
                buf.push_str(" OFFSET ");
                offset.deparse(buf);
            }
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumns {
    None,
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ... (includes Star entries)
}

impl SelectColumns {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();

        let children: Box<dyn Iterator<Item = &N> + '_> = match self {
            SelectColumns::None => Box::new(std::iter::empty()),
            SelectColumns::Columns(columns) => Box::new(columns.iter().flat_map(|col| col.nodes())),
        };

        Box::new(current.chain(children))
    }

    /// Collect subquery branches from SELECT list with source tracking.
    /// All subqueries in a SELECT list are Scalar (must return single value).
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
    ) {
        if let SelectColumns::Columns(columns) = self {
            for col in columns {
                col.expr.subquery_nodes_collect(branches);
            }
        }
    }
}

impl Deparse for SelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SelectColumns::None => buf.push(' '),
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

    pub fn has_subqueries(&self) -> bool {
        self.left.has_subqueries() || self.right.has_subqueries()
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
    Star(Option<String>),       // * or table.*
    Function(FunctionCall),     // COUNT(*), SUM(col), etc.
    Literal(LiteralValue),      // Constant values
    Case(CaseExpr),             // CASE WHEN ... THEN ... END
    Arithmetic(ArithmeticExpr), // amount * -1, price + tax
    Subquery(Box<QueryExpr>),
}

impl ColumnExpr {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            return Box::new(std::iter::once(r));
        }
        match self {
            ColumnExpr::Column(col) => Box::new((col as &dyn Any).downcast_ref::<N>().into_iter()),
            ColumnExpr::Star(_) => Box::new(std::iter::empty()),
            ColumnExpr::Function(func) => {
                Box::new((func as &dyn Any).downcast_ref::<N>().into_iter())
            }
            ColumnExpr::Literal(lit) => Box::new((lit as &dyn Any).downcast_ref::<N>().into_iter()),
            ColumnExpr::Case(case) => Box::new((case as &dyn Any).downcast_ref::<N>().into_iter()),
            ColumnExpr::Arithmetic(arith) => {
                Box::new((arith as &dyn Any).downcast_ref::<N>().into_iter())
            }
            ColumnExpr::Subquery(query) => query.nodes(),
        }
    }

    /// Check if this column expression contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        match self {
            ColumnExpr::Function(func) => func.has_subqueries(),
            ColumnExpr::Case(case) => case.has_subqueries(),
            ColumnExpr::Arithmetic(arith) => arith.has_subqueries(),
            ColumnExpr::Subquery(_) => true,
            ColumnExpr::Column(_) | ColumnExpr::Star(_) | ColumnExpr::Literal(_) => false,
        }
    }

    /// Collect subquery branches from column expressions with source tracking.
    /// All subqueries within column expressions are Scalar.
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
    ) {
        match self {
            ColumnExpr::Column(_) | ColumnExpr::Star(_) | ColumnExpr::Literal(_) => {}
            ColumnExpr::Function(func) => {
                for arg in &func.args {
                    arg.subquery_nodes_collect(branches);
                }
            }
            ColumnExpr::Case(case) => {
                if let Some(arg) = &case.arg {
                    arg.subquery_nodes_collect(branches);
                }
                for when in &case.whens {
                    // condition is WhereExpr -- use negated=false (Scalar context)
                    when.condition.subquery_nodes_collect(branches, false);
                    when.result.subquery_nodes_collect(branches);
                }
                if let Some(default) = &case.default {
                    default.subquery_nodes_collect(branches);
                }
            }
            ColumnExpr::Arithmetic(arith) => {
                arith.left.subquery_nodes_collect(branches);
                arith.right.subquery_nodes_collect(branches);
            }
            ColumnExpr::Subquery(query) => {
                let source = UpdateQuerySource::Subquery(SubqueryKind::Scalar);
                query.select_nodes_collect(branches, source, false);
            }
        }
    }
}

impl Deparse for ColumnExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ColumnExpr::Column(col) => col.deparse(buf),
            ColumnExpr::Star(qualifier) => {
                if let Some(table) = qualifier {
                    buf.push_str(table);
                    buf.push('.');
                }
                buf.push('*');
                buf
            }
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
    pub agg_star: bool,                // COUNT(*)
    pub agg_distinct: bool,            // COUNT(DISTINCT col)
    pub agg_order: Vec<OrderByClause>, // ORDER BY inside aggregate: string_agg(x, ',' ORDER BY x)
    pub over: Option<WindowSpec>,      // Window function OVER clause
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
    pub fn has_subqueries(&self) -> bool {
        self.args.iter().any(|arg| arg.has_subqueries())
            || self.agg_order.iter().any(|o| o.expr.has_subqueries())
            || self.over.as_ref().is_some_and(|w| w.has_subqueries())
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
    pub fn has_subqueries(&self) -> bool {
        self.partition_by.iter().any(|p| p.has_subqueries())
            || self.order_by.iter().any(|o| o.expr.has_subqueries())
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
    pub fn has_subqueries(&self) -> bool {
        self.arg.as_ref().is_some_and(|a| a.has_subqueries())
            || self.whens.iter().any(|w| w.has_subqueries())
            || self.default.as_ref().is_some_and(|d| d.has_subqueries())
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

    pub fn has_subqueries(&self) -> bool {
        self.condition.has_subqueries() || self.result.has_subqueries()
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
    CteRef(CteRefNode),
}

/// A reference to a CTE in a FROM clause or subquery position.
///
/// Contains a clone of the CTE's query body so that traversal methods
/// (nodes, select_nodes, etc.) are self-contained without needing
/// external context.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CteRefNode {
    pub cte_name: String,
    pub query: Box<QueryExpr>,
    pub column_aliases: Vec<String>,
    pub materialization: CteMaterialization,
    pub alias: Option<TableAlias>,
}

impl TableSource {
    fn nodes<N: Any>(&self) -> TableSourceNodeIter<'_, N> {
        TableSourceNodeIter {
            source: Some(self),
            join_iter: None,
            subquery_iter: None,
            _phantom: PhantomData,
        }
    }

    /// Collect direct table nodes, traversing JOINs but not subqueries/CTEs.
    fn direct_table_nodes_collect<'a>(&'a self, tables: &mut Vec<&'a TableNode>) {
        match self {
            TableSource::Table(table) => tables.push(table),
            TableSource::Subquery(_) | TableSource::CteRef(_) => {} // handled as separate branch
            TableSource::Join(join) => {
                join.left.direct_table_nodes_collect(tables);
                join.right.direct_table_nodes_collect(tables);
            }
        }
    }

    /// Check if this table source contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        match self {
            TableSource::Subquery(_) | TableSource::CteRef(_) => true,
            TableSource::Table(_) => false,
            TableSource::Join(join) => {
                join.left.has_subqueries()
                    || join.right.has_subqueries()
                    || join.condition.as_ref().is_some_and(|c| c.has_subqueries())
            }
        }
    }

    /// Collect subquery branches from table sources with source tracking.
    /// FROM subqueries and CteRef inherit the negation context as Inclusion/Exclusion.
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            TableSource::Table(_) => {}
            TableSource::Subquery(sub) => {
                let kind = if negated {
                    SubqueryKind::Exclusion
                } else {
                    SubqueryKind::Inclusion
                };
                sub.query.select_nodes_collect(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            TableSource::CteRef(cte_ref) => {
                let kind = if negated {
                    SubqueryKind::Exclusion
                } else {
                    SubqueryKind::Inclusion
                };
                cte_ref.query.select_nodes_collect(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            TableSource::Join(join) => {
                join.left.subquery_nodes_collect(branches, negated);
                join.right.subquery_nodes_collect(branches, negated);
                if let Some(condition) = &join.condition {
                    condition.subquery_nodes_collect(branches, negated);
                }
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
            TableSource::CteRef(cte_ref) => cte_ref.deparse(buf),
        }
    }
}

struct TableSourceNodeIter<'a, N> {
    source: Option<&'a TableSource>,
    join_iter: Option<Box<JoinNodeIter<'a, N>>>,
    subquery_iter: Option<Box<dyn Iterator<Item = &'a N> + 'a>>,
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
            Some(TableSource::Subquery(sub)) => {
                if let Some(iter) = &mut self.subquery_iter {
                    if let Some(node) = iter.next() {
                        return Some(node);
                    } else {
                        self.source = None;
                        return None;
                    }
                }

                let mut iter = sub.nodes();
                if let Some(node) = iter.next() {
                    self.subquery_iter = Some(iter);
                    Some(node)
                } else {
                    self.source = None;
                    None
                }
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
            Some(TableSource::CteRef(cte_ref)) => {
                if let Some(iter) = &mut self.subquery_iter {
                    if let Some(node) = iter.next() {
                        return Some(node);
                    } else {
                        self.source = None;
                        return None;
                    }
                }

                let mut iter = cte_ref.nodes();
                if let Some(node) = iter.next() {
                    self.subquery_iter = Some(iter);
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
    pub query: Box<QueryExpr>,
    pub alias: Option<TableAlias>,
}

impl TableSubqueryNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.query.nodes();
        Box::new(current.chain(children))
    }
}

impl Deparse for TableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if self.lateral {
            buf.push_str("LATERAL ");
        }

        buf.push('(');
        self.query.deparse(buf);
        buf.push(')');

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

impl CteRefNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.query.nodes();
        Box::new(current.chain(children))
    }
}

impl Deparse for CteRefNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.cte_name.deparse(buf);

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

/// SubLink type for subqueries in WHERE clauses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubLinkType {
    /// EXISTS (SELECT ...)
    Exists,
    /// expr IN (SELECT ...) / expr op ANY (SELECT ...)
    Any,
    /// expr op ALL (SELECT ...)
    All,
    /// Scalar subquery returning single value
    Expr,
}

impl TryFrom<pg_query::protobuf::SubLinkType> for SubLinkType {
    type Error = AstError;

    fn try_from(v: pg_query::protobuf::SubLinkType) -> Result<Self, Self::Error> {
        use pg_query::protobuf::SubLinkType as PgSubLinkType;
        match v {
            PgSubLinkType::ExistsSublink => Ok(Self::Exists),
            PgSubLinkType::AnySublink => Ok(Self::Any),
            PgSubLinkType::AllSublink => Ok(Self::All),
            PgSubLinkType::ExprSublink => Ok(Self::Expr),
            PgSubLinkType::Undefined
            | PgSubLinkType::RowcompareSublink
            | PgSubLinkType::MultiexprSublink
            | PgSubLinkType::ArraySublink
            | PgSubLinkType::CteSublink => Err(AstError::UnsupportedSubLinkType {
                sublink_type: format!("{:?}", v),
            }),
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

impl Deparse for LimitClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if let Some(count) = &self.count {
            buf.push_str(" LIMIT ");
            count.deparse(buf);
        }
        if let Some(offset) = &self.offset {
            buf.push_str(" OFFSET ");
            offset.deparse(buf);
        }
        buf
    }
}

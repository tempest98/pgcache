use std::any::Any;

use ecow::EcoString;
use error_set::error_set;
use iddqd::BiHashMap;
use rootcause::Report;
use tokio_postgres::types::Type;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::catalog::{ColumnMetadata, TableMetadata};
use crate::query::ast::{
    ArithmeticOp, BinaryOp, ColumnExpr, ColumnNode, Deparse, JoinType, LimitClause, LiteralValue,
    MultiOp, OrderDirection, QueryBody, QueryExpr, SelectColumns, SelectNode, SubLinkType,
    TableAlias, TableNode, TableSource, UnaryOp, WhereExpr, WindowSpec,
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

/// Result type with location-tracking error reports for resolution operations.
pub type ResolveResult<T> = Result<T, Report<ResolveError>>;

/// Resolved table reference with complete metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTableNode {
    /// Full schema name (resolved from 'public' default if needed)
    pub schema: EcoString,
    /// Table name
    pub name: EcoString,
    /// Optional alias used in query
    pub alias: Option<EcoString>,
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
        self.schema.deparse(buf);
        buf.push('.');
        self.name.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }
        buf
    }
}

/// Resolved column reference with type information
///
/// Note: PartialEq and Hash are implemented manually to exclude `table_alias`
/// since aliases are for deparsing only and don't affect column identity.
///
/// String fields use `EcoString`: short identifiers (schema, table, column names)
/// are stored inline; the clone cost is a fixed 16-byte memcpy regardless of
/// string length for inline values.
#[derive(Debug, Clone, Eq)]
pub struct ResolvedColumnNode {
    /// Schema name where the table is located
    pub schema: EcoString,
    /// Table name (not alias) where column is defined
    pub table: EcoString,
    /// Table alias if one was used in the query (for deparsing only, not included in equality)
    pub table_alias: Option<EcoString>,
    /// Column name
    pub column: EcoString,
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
            alias.deparse(buf);
        } else {
            self.schema.deparse(buf);
            buf.push('.');
            self.table.deparse(buf);
        }
        buf.push('.');
        self.column.deparse(buf);
        buf
    }
}

/// Resolved unary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedUnaryExpr {
    pub op: UnaryOp,
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
    pub op: BinaryOp,
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
    pub op: MultiOp,
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
    /// Array literal: ARRAY[val1, val2, ...]
    Array(Vec<ResolvedWhereExpr>),
    /// Function call (for future support)
    Function {
        name: EcoString,
        args: Vec<ResolvedWhereExpr>,
        agg_star: bool,
    },
    /// Subquery in WHERE clause (EXISTS, IN, ANY, ALL, scalar)
    Subquery {
        query: Box<ResolvedQueryExpr>,
        sublink_type: SubLinkType,
        /// Left-hand expression for IN/ANY/ALL (e.g., `id` in `id IN (SELECT ...)`)
        test_expr: Option<Box<ResolvedWhereExpr>>,
        /// Columns from the outer query scope referenced inside this subquery.
        /// Empty for non-correlated subqueries.
        outer_refs: Vec<ResolvedColumnNode>,
    },
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
            ResolvedWhereExpr::Array(elems) => Box::new(elems.iter().flat_map(|elem| elem.nodes())),
            ResolvedWhereExpr::Function { args, .. } => {
                Box::new(args.iter().flat_map(|arg| arg.nodes()))
            }
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                let query_nodes = query.nodes();
                let test_nodes = test_expr.iter().flat_map(|e| e.nodes());
                Box::new(query_nodes.chain(test_nodes))
            }
        };
        current.chain(children)
    }

    /// Compute the maximum subquery nesting depth in this WHERE expression.
    /// Returns 0 if there are no subqueries.
    pub fn subquery_depth(&self) -> usize {
        match self {
            ResolvedWhereExpr::Binary(b) => b.lexpr.subquery_depth().max(b.rexpr.subquery_depth()),
            ResolvedWhereExpr::Unary(u) => u.expr.subquery_depth(),
            ResolvedWhereExpr::Multi(m) => m
                .exprs
                .iter()
                .map(|e| e.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedWhereExpr::Array(elems) => {
                elems.iter().map(|e| e.subquery_depth()).max().unwrap_or(0)
            }
            ResolvedWhereExpr::Function { args, .. } => {
                args.iter().map(|a| a.subquery_depth()).max().unwrap_or(0)
            }
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                let inner = 1 + query.subquery_depth();
                let test = test_expr.as_ref().map_or(0, |t| t.subquery_depth());
                inner.max(test)
            }
            ResolvedWhereExpr::Value(_) | ResolvedWhereExpr::Column(_) => 0,
        }
    }

    /// Count the number of leaf predicates (comparisons) in this expression.
    /// AND/OR nodes are not counted themselves, only their leaf children.
    pub fn predicate_count(&self) -> usize {
        match self {
            ResolvedWhereExpr::Binary(b) => match b.op {
                BinaryOp::And | BinaryOp::Or => {
                    b.lexpr.predicate_count() + b.rexpr.predicate_count()
                }
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual
                | BinaryOp::Like
                | BinaryOp::ILike
                | BinaryOp::NotLike
                | BinaryOp::NotILike => 1,
            },
            ResolvedWhereExpr::Multi(_) => 1, // Multi ops (IN, BETWEEN, etc.) are single predicates
            ResolvedWhereExpr::Unary(u) => u.expr.predicate_count(),
            ResolvedWhereExpr::Array(_) => 0, // Array literals are not predicates
            ResolvedWhereExpr::Function { .. } => 1, // Treat function calls as single predicate
            ResolvedWhereExpr::Subquery { .. } => 1, // Treat subqueries as single predicate
            ResolvedWhereExpr::Value(_) | ResolvedWhereExpr::Column(_) => 0,
        }
    }

    /// Recursively collect subquery branches with source tracking.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion for
    /// EXISTS/ANY subqueries. ALL is already Exclusion (NOT IN).
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            ResolvedWhereExpr::Binary(binary) => {
                binary
                    .lexpr
                    .subquery_nodes_collect_with_source(branches, negated);
                binary
                    .rexpr
                    .subquery_nodes_collect_with_source(branches, negated);
            }
            ResolvedWhereExpr::Unary(unary) => {
                let child_negated = if unary.op == UnaryOp::Not {
                    !negated
                } else {
                    negated
                };
                unary
                    .expr
                    .subquery_nodes_collect_with_source(branches, child_negated);
            }
            ResolvedWhereExpr::Multi(multi) => {
                for expr in &multi.exprs {
                    expr.subquery_nodes_collect_with_source(branches, negated);
                }
            }
            ResolvedWhereExpr::Array(elems) => {
                for elem in elems {
                    elem.subquery_nodes_collect_with_source(branches, negated);
                }
            }
            ResolvedWhereExpr::Function { args, .. } => {
                for arg in args {
                    arg.subquery_nodes_collect_with_source(branches, negated);
                }
            }
            ResolvedWhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
                ..
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
                query.select_nodes_collect_with_source(branches, source, negated);
                if let Some(test) = test_expr {
                    test.subquery_nodes_collect_with_source(branches, negated);
                }
            }
            ResolvedWhereExpr::Value(_) | ResolvedWhereExpr::Column(_) => {}
        }
    }

    /// Recursively collect SELECT branches from subqueries in this WHERE expression.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match self {
            ResolvedWhereExpr::Binary(binary) => {
                binary.lexpr.subquery_nodes_collect(branches);
                binary.rexpr.subquery_nodes_collect(branches);
            }
            ResolvedWhereExpr::Unary(unary) => {
                unary.expr.subquery_nodes_collect(branches);
            }
            ResolvedWhereExpr::Multi(multi) => {
                for expr in &multi.exprs {
                    expr.subquery_nodes_collect(branches);
                }
            }
            ResolvedWhereExpr::Array(elems) => {
                for elem in elems {
                    elem.subquery_nodes_collect(branches);
                }
            }
            ResolvedWhereExpr::Function { args, .. } => {
                for arg in args {
                    arg.subquery_nodes_collect(branches);
                }
            }
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                query.select_nodes_collect(branches);
                if let Some(test) = test_expr {
                    test.subquery_nodes_collect(branches);
                }
            }
            ResolvedWhereExpr::Value(_) | ResolvedWhereExpr::Column(_) => {}
        }
    }
}

impl Deparse for ResolvedWhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedWhereExpr::Value(lit) => lit.deparse(buf),
            ResolvedWhereExpr::Column(col) => col.deparse(buf),
            ResolvedWhereExpr::Unary(unary) => {
                match unary.op {
                    UnaryOp::IsNull
                    | UnaryOp::IsNotNull
                    | UnaryOp::IsTrue
                    | UnaryOp::IsNotTrue
                    | UnaryOp::IsFalse
                    | UnaryOp::IsNotFalse => {
                        // Postfix operators: expr IS NULL, expr IS TRUE, etc.
                        unary.expr.deparse(buf);
                        buf.push(' ');
                        unary.op.deparse(buf);
                    }
                    UnaryOp::Not => {
                        // Prefix operator: NOT expr
                        // NOT has higher precedence than AND/OR, so NOT applied
                        // to a logical binary expression needs parentheses.
                        let needs_parens = matches!(
                            unary.expr.as_ref(),
                            ResolvedWhereExpr::Binary(child) if child.op.is_logical()
                        );
                        unary.op.deparse(buf);
                        buf.push(' ');
                        if needs_parens {
                            buf.push('(');
                        }
                        unary.expr.deparse(buf);
                        if needs_parens {
                            buf.push(')');
                        }
                    }
                }
                buf
            }
            ResolvedWhereExpr::Binary(binary) => {
                let left_needs_parens = matches!(
                    (&binary.op, binary.lexpr.as_ref()),
                    (BinaryOp::And, ResolvedWhereExpr::Binary(child)) if child.op == BinaryOp::Or
                );
                let right_needs_parens = matches!(
                    (&binary.op, binary.rexpr.as_ref()),
                    (BinaryOp::And, ResolvedWhereExpr::Binary(child)) if child.op == BinaryOp::Or
                );

                if left_needs_parens {
                    buf.push('(');
                }
                binary.lexpr.deparse(buf);
                if left_needs_parens {
                    buf.push(')');
                }

                buf.push(' ');
                binary.op.deparse(buf);
                buf.push(' ');

                if right_needs_parens {
                    buf.push('(');
                }
                binary.rexpr.deparse(buf);
                if right_needs_parens {
                    buf.push(')');
                }

                buf
            }
            ResolvedWhereExpr::Multi(multi) => {
                // Format: column IN (value1, value2, ...) or column NOT IN (...)
                let [first, rest @ ..] = multi.exprs.as_slice() else {
                    return buf;
                };

                // First expression is the column/left side
                first.deparse(buf);

                match multi.op {
                    MultiOp::In => buf.push_str(" IN ("),
                    MultiOp::NotIn => buf.push_str(" NOT IN ("),
                    MultiOp::Between
                    | MultiOp::NotBetween
                    | MultiOp::BetweenSymmetric
                    | MultiOp::NotBetweenSymmetric => {
                        buf.push(' ');
                        multi.op.deparse(buf);
                        buf.push(' ');
                        // BETWEEN low AND high — exactly 2 bounds
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
                        multi.op.deparse(buf);
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
            ResolvedWhereExpr::Array(elems) => {
                buf.push_str("ARRAY[");
                let mut sep = "";
                for elem in elems {
                    buf.push_str(sep);
                    elem.deparse(buf);
                    sep = ", ";
                }
                buf.push(']');
                buf
            }
            ResolvedWhereExpr::Function {
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
                buf
            }
            ResolvedWhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
                ..
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
                buf
            }
        }
    }
}

/// Resolved arithmetic expression: `left op right`
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedArithmeticExpr {
    pub left: Box<ResolvedColumnExpr>,
    pub op: ArithmeticOp,
    pub right: Box<ResolvedColumnExpr>,
}

impl ResolvedArithmeticExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_children = self.left.nodes();
        let right_children = self.right.nodes();
        current.chain(left_children).chain(right_children)
    }
}

impl Deparse for ResolvedArithmeticExpr {
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

/// Resolved column expression in SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedColumnExpr {
    /// Fully qualified column reference
    Column(ResolvedColumnNode),
    /// Unqualified column name (used in set operation ORDER BY)
    Identifier(EcoString),
    /// Function call (including window functions)
    Function {
        name: EcoString,
        args: Vec<ResolvedColumnExpr>,
        agg_star: bool,
        agg_distinct: bool,
        agg_order: Vec<ResolvedOrderByClause>,
        over: Option<ResolvedWindowSpec>,
    },
    /// Literal value
    Literal(LiteralValue),
    /// CASE expression
    Case(ResolvedCaseExpr),
    /// Arithmetic expression: `left op right`
    Arithmetic(ResolvedArithmeticExpr),
    /// Scalar subquery in SELECT list
    Subquery(Box<ResolvedQueryExpr>, Vec<ResolvedColumnNode>),
}

/// Resolved window specification for OVER clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedWindowSpec {
    /// PARTITION BY columns
    pub partition_by: Vec<ResolvedColumnExpr>,
    /// ORDER BY clauses
    pub order_by: Vec<ResolvedOrderByClause>,
}

impl ResolvedWindowSpec {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let partition_children = self.partition_by.iter().flat_map(|p| p.nodes());
        let order_children = self.order_by.iter().flat_map(|o| o.nodes());
        current.chain(partition_children).chain(order_children)
    }
}

impl Deparse for ResolvedWindowSpec {
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

/// Resolved CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCaseExpr {
    /// For simple CASE, the expression being tested
    pub arg: Option<Box<ResolvedColumnExpr>>,
    /// List of WHEN clauses
    pub whens: Vec<ResolvedCaseWhen>,
    /// ELSE result
    pub default: Option<Box<ResolvedColumnExpr>>,
}

/// Resolved CASE WHEN clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCaseWhen {
    /// The condition (for searched CASE) or value (for simple CASE)
    pub condition: ResolvedWhereExpr,
    /// The result if condition is true/matches
    pub result: ResolvedColumnExpr,
}

impl ResolvedColumnExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedColumnExpr::Column(col) => Box::new(col.nodes()),
            ResolvedColumnExpr::Identifier(_) => Box::new(std::iter::empty()),
            ResolvedColumnExpr::Literal(lit) => Box::new(lit.nodes()),
            ResolvedColumnExpr::Function {
                args,
                agg_order,
                over,
                ..
            } => {
                let arg_nodes = args.iter().flat_map(|arg| arg.nodes());
                let agg_order_nodes = agg_order.iter().flat_map(|o| o.nodes());
                let over_nodes = over.iter().flat_map(|w| w.nodes());
                Box::new(arg_nodes.chain(agg_order_nodes).chain(over_nodes))
            }
            ResolvedColumnExpr::Case(case) => Box::new(case.nodes()),
            ResolvedColumnExpr::Arithmetic(arith) => Box::new(arith.nodes()),
            ResolvedColumnExpr::Subquery(query, _) => Box::new(query.nodes()),
        };
        current.chain(children)
    }

    /// Compute the maximum subquery nesting depth in this column expression.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedColumnExpr::Column(_)
            | ResolvedColumnExpr::Identifier(_)
            | ResolvedColumnExpr::Literal(_) => 0,
            ResolvedColumnExpr::Function { args, .. } => {
                args.iter().map(|a| a.subquery_depth()).max().unwrap_or(0)
            }
            ResolvedColumnExpr::Case(case) => {
                let arg_depth = case.arg.as_ref().map_or(0, |a| a.subquery_depth());
                let when_depth = case
                    .whens
                    .iter()
                    .map(|w| w.condition.subquery_depth().max(w.result.subquery_depth()))
                    .max()
                    .unwrap_or(0);
                let default_depth = case.default.as_ref().map_or(0, |d| d.subquery_depth());
                arg_depth.max(when_depth).max(default_depth)
            }
            ResolvedColumnExpr::Arithmetic(arith) => arith
                .left
                .subquery_depth()
                .max(arith.right.subquery_depth()),
            ResolvedColumnExpr::Subquery(query, _) => 1 + query.subquery_depth(),
        }
    }

    /// Collect subquery branches from column expressions with source tracking.
    /// All subqueries within column expressions are Scalar.
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
    ) {
        match self {
            ResolvedColumnExpr::Column(_)
            | ResolvedColumnExpr::Identifier(_)
            | ResolvedColumnExpr::Literal(_) => {}
            ResolvedColumnExpr::Function { args, .. } => {
                for arg in args {
                    arg.subquery_nodes_collect_with_source(branches);
                }
            }
            ResolvedColumnExpr::Case(case) => {
                if let Some(arg) = &case.arg {
                    arg.subquery_nodes_collect_with_source(branches);
                }
                for when in &case.whens {
                    // condition is WhereExpr — use negated=false (Scalar context)
                    when.condition
                        .subquery_nodes_collect_with_source(branches, false);
                    when.result.subquery_nodes_collect_with_source(branches);
                }
                if let Some(default) = &case.default {
                    default.subquery_nodes_collect_with_source(branches);
                }
            }
            ResolvedColumnExpr::Arithmetic(arith) => {
                arith.left.subquery_nodes_collect_with_source(branches);
                arith.right.subquery_nodes_collect_with_source(branches);
            }
            ResolvedColumnExpr::Subquery(query, _) => {
                let source = UpdateQuerySource::Subquery(SubqueryKind::Scalar);
                query.select_nodes_collect_with_source(branches, source, false);
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in this column expression.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match self {
            ResolvedColumnExpr::Column(_)
            | ResolvedColumnExpr::Identifier(_)
            | ResolvedColumnExpr::Literal(_) => {}
            ResolvedColumnExpr::Function { args, .. } => {
                for arg in args {
                    arg.subquery_nodes_collect(branches);
                }
            }
            ResolvedColumnExpr::Case(case) => {
                if let Some(arg) = &case.arg {
                    arg.subquery_nodes_collect(branches);
                }
                for when in &case.whens {
                    when.condition.subquery_nodes_collect(branches);
                    when.result.subquery_nodes_collect(branches);
                }
                if let Some(default) = &case.default {
                    default.subquery_nodes_collect(branches);
                }
            }
            ResolvedColumnExpr::Arithmetic(arith) => {
                arith.left.subquery_nodes_collect(branches);
                arith.right.subquery_nodes_collect(branches);
            }
            ResolvedColumnExpr::Subquery(query, _) => {
                query.select_nodes_collect(branches);
            }
        }
    }
}

impl ResolvedCaseExpr {
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
}

impl ResolvedCaseWhen {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let condition_nodes = self.condition.nodes();
        let result_nodes = self.result.nodes();
        condition_nodes.chain(result_nodes)
    }
}

impl Deparse for ResolvedColumnExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedColumnExpr::Column(col) => col.deparse(buf),
            ResolvedColumnExpr::Identifier(name) => name.deparse(buf),
            ResolvedColumnExpr::Literal(lit) => lit.deparse(buf),
            ResolvedColumnExpr::Function {
                name,
                args,
                agg_star,
                agg_distinct,
                agg_order,
                over,
            } => {
                buf.push_str(name);
                buf.push('(');
                if *agg_distinct {
                    buf.push_str("DISTINCT ");
                }
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
                if !agg_order.is_empty() {
                    buf.push_str(" ORDER BY ");
                    let mut sep = "";
                    for clause in agg_order {
                        buf.push_str(sep);
                        clause.deparse(buf);
                        sep = ", ";
                    }
                }
                buf.push(')');
                if let Some(window_spec) = over {
                    buf.push_str(" OVER ");
                    window_spec.deparse(buf);
                }
                buf
            }
            ResolvedColumnExpr::Case(case) => case.deparse(buf),
            ResolvedColumnExpr::Arithmetic(arith) => arith.deparse(buf),
            ResolvedColumnExpr::Subquery(query, _) => {
                buf.push('(');
                query.deparse(buf);
                buf.push(')');
                buf
            }
        }
    }
}

impl Deparse for ResolvedCaseExpr {
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

/// Resolved SELECT column with optional alias
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectColumn {
    pub expr: ResolvedColumnExpr,
    pub alias: Option<EcoString>,
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
    /// Specific columns (stars are expanded to explicit columns during resolution)
    Columns(Vec<ResolvedSelectColumn>),
}

impl ResolvedSelectColumns {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedSelectColumns::None => Box::new(std::iter::empty()),
            ResolvedSelectColumns::Columns(cols) => {
                Box::new(cols.iter().flat_map(|col| col.nodes()))
            }
        };
        current.chain(children)
    }

    /// Compute the maximum subquery nesting depth in the SELECT list.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedSelectColumns::Columns(columns) => columns
                .iter()
                .map(|c| c.expr.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedSelectColumns::None => 0,
        }
    }

    /// Collect subquery branches from SELECT list with source tracking.
    /// All subqueries in a SELECT list are Scalar (must return single value).
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
    ) {
        if let ResolvedSelectColumns::Columns(columns) = self {
            for col in columns {
                col.expr.subquery_nodes_collect_with_source(branches);
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in the SELECT list.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        if let ResolvedSelectColumns::Columns(columns) = self {
            for col in columns {
                col.expr.subquery_nodes_collect(branches);
            }
        }
    }
}

impl Deparse for ResolvedSelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedSelectColumns::None => buf.push(' '),
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

    /// Collect direct table nodes from this source, traversing JOINs but not subqueries.
    fn direct_table_nodes_collect<'a>(&'a self, tables: &mut Vec<&'a ResolvedTableNode>) {
        match self {
            ResolvedTableSource::Table(table) => tables.push(table),
            ResolvedTableSource::Subquery(_) => {} // handled as separate branch
            ResolvedTableSource::Join(join) => {
                join.left.direct_table_nodes_collect(tables);
                join.right.direct_table_nodes_collect(tables);
            }
        }
    }

    /// Compute the maximum subquery nesting depth from this table source.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedTableSource::Table(_) => 0,
            ResolvedTableSource::Subquery(sub) => 1 + sub.query.subquery_depth(),
            ResolvedTableSource::Join(join) => {
                let condition_depth = join.condition.as_ref().map_or(0, |c| c.subquery_depth());
                join.left
                    .subquery_depth()
                    .max(join.right.subquery_depth())
                    .max(condition_depth)
            }
        }
    }

    /// Collect subquery branches from table sources with source tracking.
    /// FROM subqueries inherit the negation context as Inclusion/Exclusion.
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            ResolvedTableSource::Table(_) => {}
            ResolvedTableSource::Subquery(sub) => {
                let kind = match (sub.subquery_kind, negated) {
                    (SubqueryKind::Scalar, _) => SubqueryKind::Scalar,
                    (SubqueryKind::Inclusion, true) => SubqueryKind::Exclusion,
                    (SubqueryKind::Exclusion, true) => SubqueryKind::Inclusion,
                    (kind, false) => kind,
                };
                sub.query.select_nodes_collect_with_source(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            ResolvedTableSource::Join(join) => {
                join.left
                    .subquery_nodes_collect_with_source(branches, negated);
                join.right
                    .subquery_nodes_collect_with_source(branches, negated);
                if let Some(condition) = &join.condition {
                    condition.subquery_nodes_collect_with_source(branches, negated);
                }
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in this table source.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match self {
            ResolvedTableSource::Table(_) => {}
            ResolvedTableSource::Subquery(sub) => {
                sub.query.select_nodes_collect(branches);
            }
            ResolvedTableSource::Join(join) => {
                join.left.subquery_nodes_collect(branches);
                join.right.subquery_nodes_collect(branches);
                if let Some(condition) = &join.condition {
                    condition.subquery_nodes_collect(branches);
                }
            }
        }
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
    pub query: Box<ResolvedQueryExpr>,
    pub alias: TableAlias,
    /// What role this subquery plays for CDC invalidation purposes.
    /// Scalar subqueries always invalidate; Inclusion/Exclusion are flipped
    /// by negation context during traversal.
    pub subquery_kind: SubqueryKind,
}

impl ResolvedTableSubqueryNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.query.nodes();
        Box::new(current.chain(children))
    }
}

impl Deparse for ResolvedTableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(" (");
        self.query.deparse(buf);
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
    pub count: Option<LiteralValue>,
    pub offset: Option<LiteralValue>,
}

// ============================================================================
// New Resolved Query Type Hierarchy (parallel to QueryExpr/QueryBody/etc.)
// ============================================================================

use crate::query::ast::{SetOpType, ValuesClause};

/// Resolved core SELECT (without ORDER BY/LIMIT - those go on ResolvedQueryExpr)
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectNode {
    pub distinct: bool,
    pub columns: ResolvedSelectColumns,
    pub from: Vec<ResolvedTableSource>,
    pub where_clause: Option<ResolvedWhereExpr>,
    pub group_by: Vec<ResolvedColumnNode>,
    pub having: Option<ResolvedWhereExpr>,
}

impl Default for ResolvedSelectNode {
    fn default() -> Self {
        Self {
            distinct: false,
            columns: ResolvedSelectColumns::None,
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
        }
    }
}

impl ResolvedSelectNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> + '_ {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let columns_nodes = self.columns.nodes();
        let from_nodes = self.from.iter().flat_map(|t| t.nodes());
        let where_nodes = self.where_clause.iter().flat_map(|w| w.nodes());
        let group_by_nodes = self.group_by.iter().flat_map(|c| c.nodes());
        let having_nodes = self.having.iter().flat_map(|h| h.nodes());

        current
            .chain(columns_nodes)
            .chain(from_nodes)
            .chain(where_nodes)
            .chain(group_by_nodes)
            .chain(having_nodes)
    }

    /// Returns table nodes directly in the FROM clause, traversing JOINs but not
    /// entering subqueries (FROM-clause derived tables or WHERE-clause subqueries).
    ///
    /// Use this instead of `nodes::<ResolvedTableNode>()` when you only want the
    /// tables that this branch can directly SELECT from. Subquery tables are handled
    /// as separate branches via `select_nodes_collect`.
    pub fn direct_table_nodes(&self) -> Vec<&ResolvedTableNode> {
        let mut tables = Vec::new();
        for source in &self.from {
            source.direct_table_nodes_collect(&mut tables);
        }
        tables
    }

    /// Check if this SELECT references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [ResolvedTableSource::Table(_)])
    }

    /// Compute the maximum subquery nesting depth in this SELECT.
    /// A flat query returns 0, one level of subquery returns 1, etc.
    pub fn subquery_depth(&self) -> usize {
        let from_depth = self
            .from
            .iter()
            .map(|s| s.subquery_depth())
            .max()
            .unwrap_or(0);
        let where_depth = self.where_clause.as_ref().map_or(0, |w| w.subquery_depth());
        let having_depth = self.having.as_ref().map_or(0, |h| h.subquery_depth());
        let columns_depth = self.columns.subquery_depth();
        from_depth
            .max(where_depth)
            .max(having_depth)
            .max(columns_depth)
    }

    /// Compute a complexity score for this query.
    ///
    /// Higher scores indicate more complex queries. Update queries are sorted
    /// by complexity (ascending) so simpler/inner queries are tried first during
    /// CDC processing — this ensures inner subquery tables are populated in
    /// the cache before outer queries that depend on them.
    ///
    /// Components:
    /// - Joins: each join adds 3 (joins require matching across tables)
    /// - Predicates: each WHERE clause comparison adds 1
    /// - Subquery depth: each nesting level adds 5 (outer queries depend on inner)
    pub fn complexity(&self) -> usize {
        let direct_table_count = self.direct_table_nodes().len();
        let join_count = direct_table_count.saturating_sub(1);
        let predicate_count = self
            .where_clause
            .as_ref()
            .map(|w| w.predicate_count())
            .unwrap_or(0);
        let subquery_depth = self.subquery_depth();
        (join_count * 3) + predicate_count + (subquery_depth * 5)
    }
}

impl Deparse for ResolvedSelectNode {
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

/// Resolved set operation node
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSetOpNode {
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<ResolvedQueryExpr>,
    pub right: Box<ResolvedQueryExpr>,
}

impl ResolvedSetOpNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_nodes = self.left.nodes();
        let right_nodes = self.right.nodes();
        Box::new(current.chain(left_nodes).chain(right_nodes))
    }
}

impl Deparse for ResolvedSetOpNode {
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

/// The body of a resolved query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedQueryBody {
    Select(Box<ResolvedSelectNode>),
    Values(ValuesClause), // No resolution needed for literals
    SetOp(ResolvedSetOpNode),
}

impl ResolvedQueryBody {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &N> + '_> = match self {
            ResolvedQueryBody::Select(select) => Box::new(select.nodes()),
            ResolvedQueryBody::Values(values) => Box::new(values.nodes()),
            ResolvedQueryBody::SetOp(set_op) => set_op.nodes(),
        };
        Box::new(current.chain(children))
    }
}

impl Deparse for ResolvedQueryBody {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedQueryBody::Select(select) => select.deparse(buf),
            ResolvedQueryBody::Values(values) => values.deparse(buf),
            ResolvedQueryBody::SetOp(set_op) => set_op.deparse(buf),
        }
    }
}

/// A complete resolved query expression with optional ordering/limiting
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedQueryExpr {
    pub body: ResolvedQueryBody,
    pub order_by: Vec<ResolvedOrderByClause>,
    pub limit: Option<ResolvedLimitClause>,
}

impl Default for ResolvedQueryExpr {
    fn default() -> Self {
        Self {
            body: ResolvedQueryBody::Values(ValuesClause::default()),
            order_by: Vec::new(),
            limit: None,
        }
    }
}

impl ResolvedQueryExpr {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let body_nodes = self.body.nodes();
        let order_by_nodes = self.order_by.iter().flat_map(|o| o.nodes());
        Box::new(current.chain(body_nodes).chain(order_by_nodes))
    }

    /// Check if query only references a single table
    pub fn is_single_table(&self) -> bool {
        self.nodes::<ResolvedTableNode>().nth(1).is_none()
    }

    /// Check if query has a WHERE clause (only applies to SELECT bodies)
    pub fn has_where_clause(&self) -> bool {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.where_clause.is_some(),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => false,
        }
    }

    /// Get the WHERE clause if it exists (only for SELECT bodies)
    pub fn where_clause(&self) -> Option<&ResolvedWhereExpr> {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.where_clause.as_ref(),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
        }
    }

    /// Get the SELECT body if this is a simple SELECT query
    pub fn as_select(&self) -> Option<&ResolvedSelectNode> {
        match &self.body {
            ResolvedQueryBody::Select(select) => Some(select),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
        }
    }

    /// Compute the maximum subquery nesting depth in this query.
    pub fn subquery_depth(&self) -> usize {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.subquery_depth(),
            ResolvedQueryBody::Values(_) => 0,
            ResolvedQueryBody::SetOp(set_op) => set_op
                .left
                .subquery_depth()
                .max(set_op.right.subquery_depth()),
        }
    }

    /// Compute a complexity score for this query.
    pub fn complexity(&self) -> usize {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.complexity(),
            ResolvedQueryBody::Values(_) => 0,
            ResolvedQueryBody::SetOp(set_op) => {
                set_op.left.complexity() + set_op.right.complexity() + 1
            }
        }
    }

    /// Extract all SELECT branches with source tracking (Direct, Subquery, etc.).
    ///
    /// Mirrors `QueryExpr::select_nodes_with_source()` but for the resolved AST.
    /// Top-level branches are Direct; subquery/CTE branches carry their
    /// Inclusion/Exclusion/Scalar classification.
    pub fn select_nodes_with_source(&self) -> Vec<(&ResolvedSelectNode, UpdateQuerySource)> {
        let mut branches = Vec::new();
        self.select_nodes_collect_with_source(&mut branches, UpdateQuerySource::Direct, false);
        branches
    }

    /// Collects branches with source tracking.
    /// `outer_source` is the source assigned to this query's body branches.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion.
    fn select_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
        outer_source: UpdateQuerySource,
        negated: bool,
    ) {
        match &self.body {
            ResolvedQueryBody::Select(select) => {
                branches.push((select, outer_source));
                for source in &select.from {
                    source.subquery_nodes_collect_with_source(branches, negated);
                }
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect_with_source(branches, negated);
                }
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect_with_source(branches, negated);
                }
                select.columns.subquery_nodes_collect_with_source(branches);
            }
            ResolvedQueryBody::Values(_) => {}
            ResolvedQueryBody::SetOp(set_op) => {
                set_op
                    .left
                    .select_nodes_collect_with_source(branches, outer_source, negated);
                set_op
                    .right
                    .select_nodes_collect_with_source(branches, outer_source, negated);
            }
        }
    }

    /// Extract all SELECT branches from this query expression.
    ///
    /// For a simple SELECT query, returns a single-element vector.
    /// For set operations (UNION/INTERSECT/EXCEPT), recursively extracts
    /// all SELECT branches from both sides.
    /// VALUES clauses are skipped (they don't reference tables).
    pub fn select_nodes(&self) -> Vec<&ResolvedSelectNode> {
        let mut branches = Vec::new();
        self.select_nodes_collect(&mut branches);
        branches
    }

    /// Helper to recursively collect SELECT branches.
    fn select_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match &self.body {
            ResolvedQueryBody::Select(select) => {
                branches.push(select);
                // Descend into subqueries in FROM clause
                for source in &select.from {
                    source.subquery_nodes_collect(branches);
                }
                // Descend into subqueries in WHERE clause
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect(branches);
                }
                // Descend into subqueries in HAVING clause
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect(branches);
                }
                // Descend into subqueries in SELECT list
                select.columns.subquery_nodes_collect(branches);
            }
            ResolvedQueryBody::Values(_) => {
                // VALUES clauses don't reference tables, skip
            }
            ResolvedQueryBody::SetOp(set_op) => {
                set_op.left.select_nodes_collect(branches);
                set_op.right.select_nodes_collect(branches);
            }
        }
    }
}

impl Deparse for ResolvedQueryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
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

/// Resolution scope tracking available tables and their aliases
#[derive(Debug)]
struct ResolutionScope<'a> {
    /// Tables available in this scope, indexed by alias (or table name if no alias)
    tables: Vec<(&'a TableMetadata, Option<&'a str>)>, // (metadata, alias)
    /// Derived tables (FROM subqueries) with owned synthetic metadata.
    /// These are virtual tables whose columns are determined by the subquery's output.
    derived_tables: Vec<(TableMetadata, String)>, // (synthetic metadata, alias)
    /// Catalog of all known tables (for subquery resolution)
    catalog_tables: &'a BiHashMap<TableMetadata>,
    /// Search path for schema resolution
    search_path: Vec<&'a str>,
    /// Owned snapshot of ancestor scope tables for correlated reference fallback.
    /// Populated when this scope was created for a WHERE/SELECT subquery body.
    /// Empty at top level.
    outer_tables: Vec<(TableMetadata, Option<String>)>,
    /// Correlated column references found during resolution of this scope's expressions.
    /// Populated by `column_resolve` when it falls back to `outer_tables`.
    outer_refs: Vec<ResolvedColumnNode>,
}

impl<'a> ResolutionScope<'a> {
    fn new(catalog_tables: &'a BiHashMap<TableMetadata>, search_path: &[&'a str]) -> Self {
        Self {
            tables: Vec::new(),
            derived_tables: Vec::new(),
            catalog_tables,
            search_path: search_path.to_vec(),
            outer_tables: Vec::new(),
            outer_refs: Vec::new(),
        }
    }

    /// Create a scope for resolving an inner subquery body.
    ///
    /// `outer_tables` is an owned snapshot of the ancestor scopes' tables, used as
    /// fallback when column resolution fails in this scope (correlated references).
    fn new_with_outer(
        catalog_tables: &'a BiHashMap<TableMetadata>,
        search_path: &[&'a str],
        outer_tables: Vec<(TableMetadata, Option<String>)>,
    ) -> Self {
        Self {
            tables: Vec::new(),
            derived_tables: Vec::new(),
            catalog_tables,
            search_path: search_path.to_vec(),
            outer_tables,
            outer_refs: Vec::new(),
        }
    }

    /// Snapshot the current scope's tables (including derived and outer) for passing
    /// to a child subquery scope. The child needs access to all ancestor tables.
    fn scope_tables_snapshot(&self) -> Vec<(TableMetadata, Option<String>)> {
        let mut snapshot: Vec<(TableMetadata, Option<String>)> = self
            .tables
            .iter()
            .map(|(meta, alias)| ((*meta).clone(), alias.map(str::to_owned)))
            .collect();
        for (meta, alias) in &self.derived_tables {
            snapshot.push((meta.clone(), Some(alias.clone())));
        }
        // Include ancestors so nested correlation can reach any level
        snapshot.extend(self.outer_tables.iter().cloned());
        snapshot
    }

    /// Find a table in the outer scope by name or alias.
    fn outer_table_scope_find(&self, name: &str) -> Option<(&TableMetadata, Option<&str>)> {
        self.outer_tables
            .iter()
            .find(|(meta, alias)| {
                if let Some(alias_name) = alias {
                    alias_name == name
                } else {
                    meta.name == name
                }
            })
            .map(|(meta, alias)| (meta, alias.as_deref()))
    }

    /// Add a table to the scope
    fn table_scope_add(&mut self, metadata: &'a TableMetadata, alias: Option<&'a str>) {
        self.tables.push((metadata, alias));
    }

    /// Find table metadata by name or alias.
    /// Checks both catalog tables and derived tables (FROM subqueries).
    fn table_scope_find(&self, name: &str) -> Option<(&TableMetadata, Option<&str>)> {
        // Check catalog tables first
        if let Some((meta, alias)) = self.tables.iter().find(|(meta, alias)| {
            if let Some(alias_name) = alias {
                *alias_name == name
            } else {
                meta.name == name
            }
        }) {
            return Some((*meta, *alias));
        }

        // Check derived tables
        self.derived_tables
            .iter()
            .find(|(_, alias)| alias == name)
            .map(|(meta, alias)| (meta, Some(alias.as_str())))
    }

    /// Add a derived table (FROM subquery) to the scope.
    ///
    /// Extracts output columns from the resolved inner query and creates synthetic
    /// `TableMetadata` so the outer query can resolve column references against
    /// the subquery alias.
    fn derived_table_scope_add(&mut self, resolved_query: &ResolvedQueryExpr, alias: &str) {
        let columns = derived_table_columns_extract(resolved_query);
        let mut column_map = BiHashMap::new();
        for col in columns {
            column_map.insert_overwrite(col);
        }

        let synthetic_metadata = TableMetadata {
            relation_oid: 0,
            name: alias.into(),
            schema: "".into(),
            primary_key_columns: Vec::new(),
            columns: column_map,
            indexes: Vec::new(),
        };

        self.derived_tables
            .push((synthetic_metadata, alias.to_owned()));
    }

    /// Find all tables in scope that contain a given column (for unqualified column resolution).
    fn column_matches_find<'b>(
        &'b self,
        column: &str,
    ) -> Vec<(&'b TableMetadata, Option<&'b str>, &'b ColumnMetadata)> {
        let mut matches = Vec::new();

        for (table_metadata, alias) in &self.tables {
            if let Some(col_meta) = table_metadata.columns.get1(column) {
                matches.push((*table_metadata, *alias, col_meta));
            }
        }

        for (table_metadata, alias) in &self.derived_tables {
            if let Some(col_meta) = table_metadata.columns.get1(column) {
                matches.push((table_metadata, Some(alias.as_str()), col_meta));
            }
        }

        matches
    }

    /// Resolve an inner subquery, collecting any outer column references.
    ///
    /// Used for WHERE-clause and SELECT-list subqueries where correlated references
    /// are allowed. Outer column references are resolved against `outer_tables` and
    /// collected; they appear as normal `Column` nodes in the resolved inner query.
    fn subquery_resolve(
        &self,
        query: &QueryExpr,
    ) -> ResolveResult<(ResolvedQueryExpr, Vec<ResolvedColumnNode>)> {
        query_expr_resolve_scoped(
            query,
            self.catalog_tables,
            &self.search_path,
            self.scope_tables_snapshot(),
        )
    }
}

/// Extract output column metadata from a resolved query for derived table scope.
///
/// Handles the three cases:
/// - `SELECT *`: returns all columns from all tables in the inner query
/// - `SELECT col1, col2`: returns column metadata for each, using aliases as names
/// - `SELECT <none>`: returns empty (e.g., EXISTS subqueries)
fn derived_table_columns_extract(resolved_query: &ResolvedQueryExpr) -> Vec<ColumnMetadata> {
    let select = match &resolved_query.body {
        ResolvedQueryBody::Select(select) => select,
        // Set operation output columns are defined by the leftmost SELECT
        ResolvedQueryBody::SetOp(set_op) => {
            return derived_table_columns_extract(&set_op.left);
        }
        ResolvedQueryBody::Values(_) => return Vec::new(),
    };

    match &select.columns {
        ResolvedSelectColumns::None => Vec::new(),
        ResolvedSelectColumns::Columns(cols) => cols
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                // Determine the column name: alias if present, otherwise
                // infer from the expression
                let name = if let Some(alias) = &col.alias {
                    alias.clone()
                } else {
                    match &col.expr {
                        ResolvedColumnExpr::Column(c) => c.column.clone(),
                        ResolvedColumnExpr::Identifier(ident) => ident.clone(),
                        // Functions, literals, etc. — without an alias we
                        // can't determine a stable column name
                        ResolvedColumnExpr::Function { .. }
                        | ResolvedColumnExpr::Literal(_)
                        | ResolvedColumnExpr::Case(_)
                        | ResolvedColumnExpr::Arithmetic(_)
                        | ResolvedColumnExpr::Subquery(..) => return None,
                    }
                };

                // Use column metadata from the source column if available,
                // otherwise create a synthetic entry with TEXT type
                let base_meta = match &col.expr {
                    ResolvedColumnExpr::Column(c) => c.column_metadata.clone(),
                    ResolvedColumnExpr::Identifier(_)
                    | ResolvedColumnExpr::Function { .. }
                    | ResolvedColumnExpr::Literal(_)
                    | ResolvedColumnExpr::Case(_)
                    | ResolvedColumnExpr::Arithmetic(_)
                    | ResolvedColumnExpr::Subquery(..) => ColumnMetadata {
                        name: name.clone(),
                        position: (i + 1) as i16,
                        type_oid: 25, // TEXT OID
                        data_type: Type::TEXT,
                        type_name: EcoString::from("text"),
                        cache_type_name: EcoString::from("text"),
                        is_primary_key: false,
                    },
                };

                // Override name with alias if provided (the column metadata
                // from the source has the original name)
                Some(ColumnMetadata {
                    name,
                    position: (i + 1) as i16,
                    ..base_meta
                })
            })
            .collect(),
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

/// Resolve a column reference to a resolved column node.
///
/// When the column cannot be found in the inner scope and the scope has
/// `outer_tables` set (i.e. we are inside a subquery body), the outer tables
/// are tried as a fallback. On a successful outer-table match the resolved node
/// is recorded in `scope.outer_refs` — marking this as a correlated reference —
/// and returned as a normal column node so the inner query remains fully resolved.
fn column_resolve(
    column_node: &ColumnNode,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedColumnNode> {
    let column_name = &column_node.column;

    // Table-qualified reference (e.g. `o.id`)
    if let Some(table_qualifier) = &column_node.table {
        // Try inner scope first
        if let Some((table_metadata, alias)) = scope.table_scope_find(table_qualifier) {
            let column_metadata = table_metadata
                .columns
                .get1(column_name.as_str())
                .ok_or_else(|| {
                    Report::from(ResolveError::ColumnNotFound {
                        table: table_metadata.name.to_string(),
                        column: column_name.clone(),
                    })
                })?;
            return Ok(ResolvedColumnNode {
                schema: table_metadata.schema.clone(),
                table: table_metadata.name.clone(),
                table_alias: alias.map(EcoString::from),
                column: column_metadata.name.clone(),
                column_metadata: column_metadata.clone(),
            });
        }

        // Fall back to outer scope (correlated reference)
        if let Some((outer_meta, outer_alias)) = scope.outer_table_scope_find(table_qualifier) {
            let column_metadata =
                outer_meta
                    .columns
                    .get1(column_name.as_str())
                    .ok_or_else(|| {
                        Report::from(ResolveError::ColumnNotFound {
                            table: outer_meta.name.to_string(),
                            column: column_name.clone(),
                        })
                    })?;
            let resolved = ResolvedColumnNode {
                schema: outer_meta.schema.clone(),
                table: outer_meta.name.clone(),
                table_alias: outer_alias.map(EcoString::from),
                column: column_metadata.name.clone(),
                column_metadata: column_metadata.clone(),
            };
            scope.outer_refs.push(resolved.clone());
            return Ok(resolved);
        }

        return Err(Report::from(ResolveError::TableNotFound {
            name: table_qualifier.clone(),
        }));
    }

    // Unqualified column — search inner scope first
    let matches = scope.column_matches_find(column_name.as_str());
    match matches.as_slice() {
        [] => {
            // Fall back to outer scope (correlated reference)
            let outer_match = scope.outer_tables.iter().find_map(|(meta, alias)| {
                meta.columns
                    .get1(column_name.as_str())
                    .map(|col_meta| (meta, alias.as_deref(), col_meta))
            });
            if let Some((outer_meta, outer_alias, col_meta)) = outer_match {
                let resolved = ResolvedColumnNode {
                    schema: outer_meta.schema.clone(),
                    table: outer_meta.name.clone(),
                    table_alias: outer_alias.map(EcoString::from),
                    column: col_meta.name.clone(),
                    column_metadata: col_meta.clone(),
                };
                scope.outer_refs.push(resolved.clone());
                return Ok(resolved);
            }
            Err(Report::from(ResolveError::ColumnNotFound {
                table: "<unknown>".to_owned(),
                column: column_name.clone(),
            }))
        }
        [(table_metadata, alias, column_metadata)] => Ok(ResolvedColumnNode {
            schema: table_metadata.schema.clone(),
            table: table_metadata.name.clone(),
            table_alias: alias.map(EcoString::from),
            column: column_metadata.name.clone(),
            column_metadata: (*column_metadata).clone(),
        }),
        _ => Err(Report::from(ResolveError::AmbiguousColumn {
            column: column_name.clone(),
        })),
    }
}

/// Resolve a WHERE expression
fn where_expr_resolve(
    expr: &WhereExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWhereExpr> {
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
            let mut resolved_exprs = Vec::with_capacity(multi.exprs.len());
            for e in &multi.exprs {
                resolved_exprs.push(where_expr_resolve(e, scope)?);
            }
            Ok(ResolvedWhereExpr::Multi(ResolvedMultiExpr {
                op: multi.op,
                exprs: resolved_exprs,
            }))
        }
        WhereExpr::Array(elems) => {
            let mut resolved_elems = Vec::with_capacity(elems.len());
            for e in elems {
                resolved_elems.push(where_expr_resolve(e, scope)?);
            }
            Ok(ResolvedWhereExpr::Array(resolved_elems))
        }
        WhereExpr::Function {
            name,
            args,
            agg_star,
        } => {
            let mut resolved_args = Vec::with_capacity(args.len());
            for arg in args {
                resolved_args.push(where_expr_resolve(arg, scope)?);
            }
            Ok(ResolvedWhereExpr::Function {
                name: name.as_str().into(),
                args: resolved_args,
                agg_star: *agg_star,
            })
        }
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            // Resolve the test expression (left-hand side for IN/ANY/ALL) in the outer scope
            let resolved_test = match test_expr {
                Some(e) => Some(Box::new(where_expr_resolve(e, scope)?)),
                None => None,
            };

            // Resolve the inner query, collecting any correlated outer references
            let (resolved_query, outer_refs) = scope.subquery_resolve(query)?;

            Ok(ResolvedWhereExpr::Subquery {
                query: Box::new(resolved_query),
                sublink_type: *sublink_type,
                test_expr: resolved_test,
                outer_refs,
            })
        }
    }
}

/// Resolve a table source (table, join, or subquery)
fn table_source_resolve<'a>(
    source: &'a TableSource,
    tables: &'a BiHashMap<TableMetadata>,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<ResolvedTableSource> {
    match source {
        TableSource::Table(table_node) => {
            // First find the table metadata (which gives us the schema)
            let table_metadata =
                table_metadata_find(table_node, tables, search_path).ok_or_else(|| {
                    Report::from(ResolveError::TableNotFound {
                        name: table_node.name.clone(),
                    })
                })?;

            scope.table_scope_add(
                table_metadata,
                table_node.alias.as_ref().map(|a| a.name.as_str()),
            );

            let resolved = ResolvedTableNode {
                schema: table_metadata.schema.clone(),
                name: table_metadata.name.clone(),
                alias: table_node.alias.as_ref().map(|a| a.name.as_str().into()),
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
            let resolved_condition = match &join_node.condition {
                Some(cond) => Some(where_expr_resolve(cond, scope)?),
                None => None,
            };

            Ok(ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
                join_type: join_node.join_type,
                left: resolved_left,
                right: resolved_right,
                condition: resolved_condition,
            })))
        }
        TableSource::Subquery(subquery) => {
            // Require alias for table subqueries
            let alias = subquery.alias.as_ref().ok_or_else(|| {
                Report::from(ResolveError::SubqueryAliasNotFound {
                    alias: "<missing>".to_owned(),
                })
            })?;

            // FROM-clause subqueries use a fresh scope — LATERAL (which would require access
            // to the outer scope) is not supported and produces TableNotFound if attempted.
            let resolved_query = query_expr_resolve(&subquery.query, tables, search_path)?;

            // Add derived table to outer scope so outer columns can reference it
            scope.derived_table_scope_add(&resolved_query, &alias.name);

            Ok(ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
                query: Box::new(resolved_query),
                alias: alias.clone(),
                subquery_kind: SubqueryKind::Inclusion,
            }))
        }
        TableSource::CteRef(cte_ref) => {
            let alias_name = cte_ref
                .alias
                .as_ref()
                .map(|a| a.name.as_str())
                .unwrap_or(&cte_ref.cte_name);

            // CTE bodies use a fresh scope (non-correlated)
            let resolved_query = query_expr_resolve(&cte_ref.query, tables, search_path)?;

            let alias = TableAlias {
                name: alias_name.to_owned(),
                columns: cte_ref.column_aliases.clone(),
            };

            scope.derived_table_scope_add(&resolved_query, alias_name);

            Ok(ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
                query: Box::new(resolved_query),
                alias,
                subquery_kind: SubqueryKind::Inclusion,
            }))
        }
    }
}

/// Resolve a column expression in SELECT list
fn column_expr_resolve(
    expr: &ColumnExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedColumnExpr> {
    match expr {
        ColumnExpr::Star(_) => unreachable!("Star expanded in select_columns_resolve"),
        ColumnExpr::Column(col) => {
            let resolved = column_resolve(col, scope)?;
            Ok(ResolvedColumnExpr::Column(resolved))
        }
        ColumnExpr::Literal(lit) => Ok(ResolvedColumnExpr::Literal(lit.clone())),
        ColumnExpr::Function(func) => {
            let mut resolved_args = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                resolved_args.push(column_expr_resolve(arg, scope)?);
            }
            let resolved_agg_order = order_by_resolve(&func.agg_order, scope)?;
            let resolved_over = match &func.over {
                Some(w) => Some(window_spec_resolve(w, scope)?),
                None => None,
            };
            Ok(ResolvedColumnExpr::Function {
                name: func.name.as_str().into(),
                args: resolved_args,
                agg_star: func.agg_star,
                agg_distinct: func.agg_distinct,
                agg_order: resolved_agg_order,
                over: resolved_over,
            })
        }
        ColumnExpr::Case(case) => {
            let arg = match &case.arg {
                Some(a) => Some(Box::new(column_expr_resolve(a, scope)?)),
                None => None,
            };
            let mut whens = Vec::with_capacity(case.whens.len());
            for w in &case.whens {
                let condition = where_expr_resolve(&w.condition, scope)?;
                let result = column_expr_resolve(&w.result, scope)?;
                whens.push(ResolvedCaseWhen { condition, result });
            }
            let default = match &case.default {
                Some(d) => Some(Box::new(column_expr_resolve(d, scope)?)),
                None => None,
            };
            Ok(ResolvedColumnExpr::Case(ResolvedCaseExpr {
                arg,
                whens,
                default,
            }))
        }
        ColumnExpr::Arithmetic(arith) => {
            let left = column_expr_resolve(&arith.left, scope)?;
            let right = column_expr_resolve(&arith.right, scope)?;
            Ok(ResolvedColumnExpr::Arithmetic(ResolvedArithmeticExpr {
                left: Box::new(left),
                op: arith.op,
                right: Box::new(right),
            }))
        }
        ColumnExpr::Subquery(query) => {
            // Resolve the scalar subquery, collecting any correlated outer references
            let (resolved_query, outer_refs) = scope.subquery_resolve(query)?;
            Ok(ResolvedColumnExpr::Subquery(
                Box::new(resolved_query),
                outer_refs,
            ))
        }
    }
}

/// Resolve a window specification
fn window_spec_resolve(
    window_spec: &WindowSpec,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWindowSpec> {
    let mut partition_by = Vec::with_capacity(window_spec.partition_by.len());
    for col in &window_spec.partition_by {
        partition_by.push(column_expr_resolve(col, scope)?);
    }
    let mut order_by = Vec::with_capacity(window_spec.order_by.len());
    for clause in &window_spec.order_by {
        let resolved_expr = column_expr_resolve(&clause.expr, scope)?;
        order_by.push(ResolvedOrderByClause {
            expr: resolved_expr,
            direction: clause.direction.clone(),
        });
    }
    Ok(ResolvedWindowSpec {
        partition_by,
        order_by,
    })
}

/// Resolve SELECT columns
///
/// Star expressions (`*` or `t1.*`) are expanded inline to all columns from
/// matching tables in scope.
fn select_columns_resolve(
    columns: &SelectColumns,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedSelectColumns> {
    match columns {
        SelectColumns::None => Ok(ResolvedSelectColumns::None),
        SelectColumns::Columns(cols) => {
            let mut resolved_cols = Vec::new();
            for col in cols {
                if let ColumnExpr::Star(qualifier) = &col.expr {
                    // Expand star to all columns from matching table(s)
                    for (table_metadata, alias) in &scope.tables {
                        let matches = match qualifier {
                            None => true,
                            Some(q) => alias.is_some_and(|a| a == q) || table_metadata.name == *q,
                        };
                        if matches {
                            for column_metadata in &table_metadata.columns {
                                resolved_cols.push(ResolvedSelectColumn {
                                    expr: ResolvedColumnExpr::Column(ResolvedColumnNode {
                                        schema: table_metadata.schema.clone(),
                                        table: table_metadata.name.clone(),
                                        table_alias: alias.map(EcoString::from),
                                        column: column_metadata.name.clone(),
                                        column_metadata: column_metadata.clone(),
                                    }),
                                    alias: None,
                                });
                            }
                        }
                    }
                } else {
                    let resolved_expr = column_expr_resolve(&col.expr, scope)?;
                    resolved_cols.push(ResolvedSelectColumn {
                        expr: resolved_expr,
                        alias: col.alias.as_deref().map(EcoString::from),
                    });
                }
            }
            Ok(ResolvedSelectColumns::Columns(resolved_cols))
        }
    }
}

/// Resolve ORDER BY clauses
fn order_by_resolve(
    order_by: &[crate::query::ast::OrderByClause],
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Vec<ResolvedOrderByClause>> {
    let mut resolved = Vec::with_capacity(order_by.len());
    for clause in order_by {
        let resolved_expr = column_expr_resolve(&clause.expr, scope)?;
        resolved.push(ResolvedOrderByClause {
            expr: resolved_expr,
            direction: clause.direction.clone(),
        });
    }
    Ok(resolved)
}

/// Convert ORDER BY clauses to use unqualified Identifier expressions.
/// Used for set operations where ORDER BY references output columns by name.
fn order_by_as_identifiers(
    order_by: &[crate::query::ast::OrderByClause],
) -> Vec<ResolvedOrderByClause> {
    order_by
        .iter()
        .map(|clause| {
            let expr = column_expr_to_identifier(&clause.expr);
            ResolvedOrderByClause {
                expr,
                direction: clause.direction.clone(),
            }
        })
        .collect()
}

/// Convert a ColumnExpr to a ResolvedColumnExpr using unqualified Identifier for columns.
/// Used for ORDER BY in set operations where columns reference output names, not table columns.
fn column_expr_to_identifier(expr: &ColumnExpr) -> ResolvedColumnExpr {
    match expr {
        ColumnExpr::Star(_) => unreachable!("Star expanded in select_columns_resolve"),
        ColumnExpr::Column(col) => ResolvedColumnExpr::Identifier(col.column.as_str().into()),
        ColumnExpr::Literal(lit) => ResolvedColumnExpr::Literal(lit.clone()),
        ColumnExpr::Function(func) => ResolvedColumnExpr::Function {
            name: func.name.as_str().into(),
            args: func.args.iter().map(column_expr_to_identifier).collect(),
            agg_star: func.agg_star,
            agg_distinct: func.agg_distinct,
            agg_order: vec![], // ORDER BY within aggregate not needed for set operation ORDER BY
            over: None,        // Window spec not needed for set operation ORDER BY
        },
        ColumnExpr::Case(_) | ColumnExpr::Subquery(_) => {
            // CASE and subquery expressions in ORDER BY are uncommon; use null as fallback
            ResolvedColumnExpr::Literal(LiteralValue::Null)
        }
        ColumnExpr::Arithmetic(arith) => ResolvedColumnExpr::Arithmetic(ResolvedArithmeticExpr {
            left: Box::new(column_expr_to_identifier(&arith.left)),
            op: arith.op,
            right: Box::new(column_expr_to_identifier(&arith.right)),
        }),
    }
}

/// Resolve GROUP BY clauses
fn group_by_resolve(
    group_by: &[ColumnNode],
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Vec<ResolvedColumnNode>> {
    let mut resolved = Vec::with_capacity(group_by.len());
    for col in group_by {
        resolved.push(column_resolve(col, scope)?);
    }
    Ok(resolved)
}

/// Resolve HAVING clause
fn having_resolve(
    having: Option<&WhereExpr>,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Option<ResolvedWhereExpr>> {
    match having {
        Some(h) => Ok(Some(where_expr_resolve(h, scope)?)),
        None => Ok(None),
    }
}

/// Resolve LIMIT clause
fn limit_resolve(limit: Option<&LimitClause>) -> Option<ResolvedLimitClause> {
    let limit = limit?;

    Some(ResolvedLimitClause {
        count: limit.count.clone(),
        offset: limit.offset.clone(),
    })
}

// ============================================================================
// Resolution functions for new QueryExpr type hierarchy
// ============================================================================

/// Resolve a QueryExpr to a ResolvedQueryExpr
pub fn query_expr_resolve(
    query: &QueryExpr,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedQueryExpr> {
    let body = query_body_resolve(&query.body, tables, search_path)?;

    // ORDER BY resolution depends on query type:
    // - Simple SELECT: resolve against table columns
    // - Set operations/VALUES: use unqualified identifiers (output column names)
    let order_by = match &query.body {
        QueryBody::Select(select) => {
            let mut scope = ResolutionScope::new(tables, search_path);
            for table_source in &select.from {
                let _ = table_source_resolve(table_source, tables, &mut scope, search_path);
            }
            order_by_resolve(&query.order_by, &mut scope)?
        }
        QueryBody::SetOp(_) | QueryBody::Values(_) => order_by_as_identifiers(&query.order_by),
    };

    let limit = limit_resolve(query.limit.as_ref());

    Ok(ResolvedQueryExpr {
        body,
        order_by,
        limit,
    })
}

/// Resolve a QueryBody to a ResolvedQueryBody
fn query_body_resolve(
    body: &QueryBody,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedQueryBody> {
    match body {
        QueryBody::Select(select) => {
            let resolved = select_node_resolve(select, tables, search_path)?;
            Ok(ResolvedQueryBody::Select(Box::new(resolved)))
        }
        QueryBody::Values(values) => {
            // VALUES clauses contain only literals, no resolution needed
            Ok(ResolvedQueryBody::Values(values.clone()))
        }
        QueryBody::SetOp(set_op) => {
            let left = query_expr_resolve(&set_op.left, tables, search_path)?;
            let right = query_expr_resolve(&set_op.right, tables, search_path)?;
            Ok(ResolvedQueryBody::SetOp(ResolvedSetOpNode {
                op: set_op.op,
                all: set_op.all,
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
    }
}

/// Resolve a SelectNode to a ResolvedSelectNode
pub fn select_node_resolve(
    select: &SelectNode,
    tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
) -> ResolveResult<ResolvedSelectNode> {
    let mut scope = ResolutionScope::new(tables, search_path);
    select_node_resolve_scoped(select, tables, &mut scope, search_path)
}

/// Resolve a SelectNode using a pre-built scope.
///
/// Called by the public `select_node_resolve` (with a fresh scope) and by
/// `query_expr_resolve_scoped` (with a scope that has `outer_tables` set for
/// correlated subquery resolution).
fn select_node_resolve_scoped<'a>(
    select: &'a SelectNode,
    tables: &'a BiHashMap<TableMetadata>,
    scope: &mut ResolutionScope<'a>,
    search_path: &[&'a str],
) -> ResolveResult<ResolvedSelectNode> {
    // First pass: resolve all table references and build scope
    let mut resolved_from = Vec::new();
    for table_source in &select.from {
        let resolved = table_source_resolve(table_source, tables, scope, search_path)?;
        resolved_from.push(resolved);
    }

    // Resolve SELECT columns
    let resolved_columns = select_columns_resolve(&select.columns, scope)?;

    // Resolve WHERE clause
    let resolved_where = match &select.where_clause {
        Some(w) => Some(where_expr_resolve(w, scope)?),
        None => None,
    };

    // Resolve GROUP BY clause
    let resolved_group_by = group_by_resolve(&select.group_by, scope)?;

    // Resolve HAVING clause
    let resolved_having = having_resolve(select.having.as_ref(), scope)?;

    Ok(ResolvedSelectNode {
        distinct: select.distinct,
        columns: resolved_columns,
        from: resolved_from,
        where_clause: resolved_where,
        group_by: resolved_group_by,
        having: resolved_having,
    })
}

/// Resolve a QueryExpr using a pre-built outer_tables context, collecting outer refs.
///
/// Used by `ResolutionScope::subquery_resolve` to resolve correlated subquery bodies.
/// Returns the resolved query and the outer column references found within it.
fn query_expr_resolve_scoped(
    query: &QueryExpr,
    catalog_tables: &BiHashMap<TableMetadata>,
    search_path: &[&str],
    outer_tables: Vec<(TableMetadata, Option<String>)>,
) -> ResolveResult<(ResolvedQueryExpr, Vec<ResolvedColumnNode>)> {
    let mut scope = ResolutionScope::new_with_outer(catalog_tables, search_path, outer_tables);

    let body = match &query.body {
        QueryBody::Select(select) => {
            let resolved =
                select_node_resolve_scoped(select, catalog_tables, &mut scope, search_path)?;
            ResolvedQueryBody::Select(Box::new(resolved))
        }
        QueryBody::Values(values) => ResolvedQueryBody::Values(values.clone()),
        QueryBody::SetOp(set_op) => {
            // Each branch is independent — resolve with the same outer_tables but
            // separate scopes so their FROM tables don't bleed across branches.
            let outer = scope.outer_tables.clone();
            let (left_resolved, left_outer_refs) = query_expr_resolve_scoped(
                &set_op.left,
                catalog_tables,
                search_path,
                outer.clone(),
            )?;
            let (right_resolved, right_outer_refs) =
                query_expr_resolve_scoped(&set_op.right, catalog_tables, search_path, outer)?;
            scope.outer_refs.extend(left_outer_refs);
            scope.outer_refs.extend(right_outer_refs);
            ResolvedQueryBody::SetOp(ResolvedSetOpNode {
                op: set_op.op,
                all: set_op.all,
                left: Box::new(left_resolved),
                right: Box::new(right_resolved),
            })
        }
    };

    // ORDER BY: build a fresh scope with the same outer_tables so correlated refs
    // in ORDER BY (rare but possible) are handled correctly.
    let order_by = match &query.body {
        QueryBody::Select(select) => {
            let mut order_scope = ResolutionScope::new_with_outer(
                catalog_tables,
                search_path,
                scope.outer_tables.clone(),
            );
            for table_source in &select.from {
                let _ = table_source_resolve(
                    table_source,
                    catalog_tables,
                    &mut order_scope,
                    search_path,
                );
            }
            order_by_resolve(&query.order_by, &mut order_scope)?
        }
        QueryBody::SetOp(_) | QueryBody::Values(_) => order_by_as_identifiers(&query.order_by),
    };

    let limit = limit_resolve(query.limit.as_ref());
    let outer_refs = scope.outer_refs;

    Ok((
        ResolvedQueryExpr {
            body,
            order_by,
            limit,
        },
        outer_refs,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::wildcard_enum_match_arm)]

    use tokio_postgres::types::Type;

    use super::*;

    /// Parse SQL and return a SelectNode (for tests using new types)
    fn parse_select_node(sql: &str) -> SelectNode {
        use crate::query::ast::{QueryBody, query_expr_convert};
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => node,
            _ => panic!("expected SELECT"),
        }
    }

    /// Parse SQL and resolve to ResolvedSelectNode
    fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
        let node = parse_select_node(sql);
        select_node_resolve(&node, tables, &["public"]).expect("resolve")
    }

    /// Parse SQL and resolve to ResolvedQueryExpr (for ORDER BY/LIMIT tests)
    fn resolve_query(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
        use crate::query::ast::query_expr_convert;
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        query_expr_resolve(&query_expr, tables, &["public"]).expect("resolve")
    }

    #[test]
    fn test_resolved_table_node_construction() {
        let table_node = ResolvedTableNode {
            schema: "public".into(),
            name: "users".into(),
            alias: Some("u".into()),
            relation_oid: 12345,
        };

        assert_eq!(table_node.schema, "public");
        assert_eq!(table_node.name, "users");
        assert_eq!(table_node.alias.as_deref(), Some("u"));
        assert_eq!(table_node.relation_oid, 12345);
    }

    #[test]
    fn test_resolved_column_node_construction() {
        let col_node = ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u".into()),
            column: "id".into(),
            column_metadata: ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
        };

        assert_eq!(col_node.schema, "public");
        assert_eq!(col_node.table, "users");
        assert_eq!(col_node.table_alias.as_deref(), Some("u"));
        assert_eq!(col_node.column, "id");
        assert_eq!(col_node.column_metadata.type_name, "int4");
        assert_eq!(col_node.column_metadata.position, 1);
        assert!(col_node.column_metadata.is_primary_key);
    }

    #[test]
    fn test_resolved_select_node_default() {
        let node = ResolvedSelectNode::default();
        assert!(matches!(node.columns, ResolvedSelectColumns::None));
        assert!(node.from.is_empty());
        assert!(node.where_clause.is_none());
        assert!(node.group_by.is_empty());
        assert!(node.having.is_none());
        assert!(!node.distinct);
    }

    // Helper function to create test table metadata
    fn test_table_metadata(name: &str, relation_oid: u32) -> TableMetadata {
        let mut columns = BiHashMap::new();

        // Add id column
        columns.insert_overwrite(ColumnMetadata {
            name: "id".into(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        });

        // Add name column
        columns.insert_overwrite(ColumnMetadata {
            name: "name".into(),
            position: 2,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: false,
        });

        TableMetadata {
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Create test table metadata with custom column names (all text type, first is PK).
    fn test_table_metadata_with_columns(
        name: &str,
        relation_oid: u32,
        column_names: &[&str],
    ) -> TableMetadata {
        let mut columns = BiHashMap::new();
        for (i, col_name) in column_names.iter().enumerate() {
            columns.insert_overwrite(ColumnMetadata {
                name: (*col_name).into(),
                position: (i + 1) as i16,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: i == 0,
            });
        }
        TableMetadata {
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![column_names[0].to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    #[test]
    fn test_table_resolve_simple() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users u", &tables);

        assert_eq!(resolved.from.len(), 1);
        if let ResolvedTableSource::Table(table) = &resolved.from[0] {
            assert_eq!(table.schema, "public");
            assert_eq!(table.name, "users");
            assert_eq!(table.alias.as_deref(), Some("u"));
            assert_eq!(table.relation_oid, 1001);
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_table_resolve_not_found() {
        let tables = BiHashMap::new();
        let node = parse_select_node("SELECT * FROM users");
        let result = select_node_resolve(&node, &tables, &["public"]);

        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(ResolveError::TableNotFound { .. })
        ));
    }

    #[test]
    fn test_column_resolve_qualified() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE users.id = 1", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users u WHERE u.name = 'john'", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // Both tables have 'id' column, unqualified reference is ambiguous
        let node = parse_select_node("SELECT * FROM users, orders WHERE id = 1");
        let result = select_node_resolve(&node, &tables, &["public"]);

        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(ResolveError::AmbiguousColumn { .. })
        ));
    }

    #[test]
    fn test_select_star_expansion() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users", &tables);

        // Check that SELECT * was expanded to all columns
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 2);
        let ResolvedColumnExpr::Column(col) = &cols[0].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "id");
        assert_eq!(col.table, "users");
        let ResolvedColumnExpr::Column(col) = &cols[1].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "users");
    }

    #[test]
    fn test_select_specific_columns() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT id, name FROM users", &tables);

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
    fn test_select_star_with_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT *, name FROM users", &tables);

        // Star expands to all columns, then the explicit column follows
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("Expected Columns");
        };
        assert_eq!(cols.len(), 3); // id, name (from *), name (explicit)

        let ResolvedColumnExpr::Column(col) = &cols[0].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "id");

        let ResolvedColumnExpr::Column(col) = &cols[1].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");

        let ResolvedColumnExpr::Column(col) = &cols[2].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
    }

    #[test]
    fn test_select_qualified_star_with_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT u.*, o.name FROM users u JOIN orders o ON o.id = u.id",
            &tables,
        );

        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("Expected Columns");
        };
        // u.* expands to users.id, users.name, then o.name
        assert_eq!(cols.len(), 3);

        let ResolvedColumnExpr::Column(col) = &cols[0].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "id");
        assert_eq!(col.table, "users");

        let ResolvedColumnExpr::Column(col) = &cols[1].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "users");

        let ResolvedColumnExpr::Column(col) = &cols[2].expr else {
            panic!("Expected column expression");
        };
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "orders");
    }

    #[test]
    fn test_join_resolution() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            &tables,
        );

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users u JOIN orders o ON u.id = o.id",
            &tables,
        );

        // Check that JOIN with aliases was resolved
        if let ResolvedTableSource::Join(join) = &resolved.from[0] {
            // Check left side has alias
            if let ResolvedTableSource::Table(left) = &join.left {
                assert_eq!(left.name, "users");
                assert_eq!(left.alias.as_deref(), Some("u"));
            } else {
                panic!("Expected table on left side");
            }

            // Check right side has alias
            if let ResolvedTableSource::Table(right) = &join.right {
                assert_eq!(right.name, "orders");
                assert_eq!(right.alias.as_deref(), Some("o"));
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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id = 1 AND name = 'john'",
            &tables,
        );

        // Check that complex WHERE was resolved
        if let Some(ResolvedWhereExpr::Binary(and_expr)) = &resolved.where_clause {
            assert_eq!(and_expr.op, BinaryOp::And);

            // Left side: id = 1
            if let ResolvedWhereExpr::Binary(left_binary) = &*and_expr.lexpr {
                assert_eq!(left_binary.op, BinaryOp::Equal);
                if let ResolvedWhereExpr::Column(col) = &*left_binary.lexpr {
                    assert_eq!(col.column, "id");
                }
            } else {
                panic!("Expected binary expression on left");
            }

            // Right side: name = 'john'
            if let ResolvedWhereExpr::Binary(right_binary) = &*and_expr.rexpr {
                assert_eq!(right_binary.op, BinaryOp::Equal);
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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users ORDER BY name ASC", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users ORDER BY name ASC, id DESC", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users u ORDER BY u.name DESC", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql =
            "SELECT * FROM users u JOIN orders o ON u.id = o.id ORDER BY u.name ASC, o.id DESC";
        let resolved = resolve_query(sql, &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users ORDER BY name", &tables);

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
        use crate::query::ast::query_expr_convert;

        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users ORDER BY nonexistent_column ASC";
        let ast = pg_query::parse(sql).unwrap();
        let query_expr = query_expr_convert(&ast).unwrap();

        let result = query_expr_resolve(&query_expr, &tables, &["public"]);

        // Should fail with column not found error
        assert!(matches!(
            result.map_err(|e| e.into_current_context()),
            Err(ResolveError::ColumnNotFound { .. })
        ));
    }

    // ==================== Deparse Tests ====================

    fn id_column_metadata() -> ColumnMetadata {
        ColumnMetadata {
            name: "id".into(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        }
    }

    #[test]
    fn test_resolved_column_node_deparse_with_alias() {
        let mut buf = String::new();

        // Column with alias - should use alias
        ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u".into()),
            column: "id".into(),
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
            schema: "public".into(),
            table: "users".into(),
            table_alias: None,
            column: "id".into(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "public.users.id");
    }

    #[test]
    fn test_resolved_column_node_deparse_quoting() {
        let mut buf = String::new();

        // Column without alias - should use schema.table
        ResolvedColumnNode {
            schema: "Public".into(),
            table: "Users".into(),
            table_alias: None,
            column: "firstName".into(),
            column_metadata: id_column_metadata(),
        }
        .deparse(&mut buf);
        assert_eq!(buf, "\"Public\".\"Users\".\"firstName\"");
    }

    #[test]
    fn test_resolved_table_node_deparse_with_alias() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "public".into(),
            name: "users".into(),
            alias: Some("u".into()),
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users u");
    }

    #[test]
    fn test_resolved_table_node_deparse_without_alias() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "public".into(),
            name: "users".into(),
            alias: None,
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " public.users");
    }

    #[test]
    fn test_resolved_table_node_deparse_quoting() {
        let mut buf = String::new();

        ResolvedTableNode {
            schema: "Public".into(),
            name: "Users".into(),
            alias: None,
            relation_oid: 1001,
        }
        .deparse(&mut buf);
        assert_eq!(buf, " \"Public\".\"Users\"");
    }

    #[test]
    fn test_resolved_select_deparse_with_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // SELECT * is expanded to explicit columns, table and column references are fully qualified
        assert_eq!(
            buf,
            "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_with_alias() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT u.id, u.name FROM users u WHERE u.id = 1", &tables);

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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT u.id, o.name FROM users u JOIN orders o ON u.id = o.id WHERE u.id = 1",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT u.id, o.name FROM public.users u JOIN public.orders o ON u.id = o.id WHERE u.id = 1"
        );
    }

    #[test]
    fn test_resolved_query_deparse_order_by() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT id FROM users u ORDER BY name DESC", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(buf, "SELECT u.id FROM public.users u ORDER BY u.name DESC");
    }

    #[test]
    fn test_resolved_select_deparse_count_star() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT COUNT(*) FROM users WHERE id = 1", &tables);

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT count(*) FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_count_distinct() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT COUNT(DISTINCT name) FROM users WHERE id = 1",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT count(DISTINCT public.users.name) FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_select_deparse_case() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT CASE WHEN name = 'admin' THEN 1 ELSE 0 END FROM users WHERE id = 1",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT CASE WHEN public.users.name = 'admin' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_resolved_column_equality_ignores_alias() {
        // Two columns with same schema/table/column but different aliases should be equal
        let col1 = ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u".into()),
            column: "id".into(),
            column_metadata: id_column_metadata(),
        };

        let col2 = ResolvedColumnNode {
            schema: "public".into(),
            table: "users".into(),
            table_alias: Some("u2".into()), // Different alias
            column: "id".into(),
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

    #[test]
    fn test_complexity_single_table_no_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users", &tables);

        // Single table, no predicates = complexity 0
        assert_eq!(resolved.complexity(), 0);
    }

    #[test]
    fn test_complexity_single_table_with_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        // Single table, 1 predicate = complexity 1
        assert_eq!(resolved.complexity(), 1);
    }

    #[test]
    fn test_complexity_single_table_multiple_predicates() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id = 1 AND name = 'john'",
            &tables,
        );

        // Single table, 2 predicates = complexity 2
        assert_eq!(resolved.complexity(), 2);
    }

    #[test]
    fn test_complexity_join_no_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            &tables,
        );

        // 2 tables (1 join) * 3 = 3, no WHERE predicates
        assert_eq!(resolved.complexity(), 3);
    }

    #[test]
    fn test_complexity_join_with_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id WHERE users.id = 1",
            &tables,
        );

        // 2 tables (1 join) * 3 = 3, plus 1 WHERE predicate = 4
        assert_eq!(resolved.complexity(), 4);
    }

    #[test]
    fn test_complexity_ordering() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // Simple query: SELECT * FROM users
        let resolved1 = resolve_sql("SELECT * FROM users", &tables);

        // Query with WHERE: SELECT * FROM users WHERE id = 1
        let resolved2 = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        // Query with JOIN: SELECT * FROM users JOIN orders ON ...
        let resolved3 = resolve_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.id",
            &tables,
        );

        // Verify ordering: simple < with_where < with_join
        assert!(resolved1.complexity() < resolved2.complexity());
        assert!(resolved2.complexity() < resolved3.complexity());
    }

    #[test]
    fn test_complexity_subquery_depth() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        // No subquery: complexity = 1 predicate
        let flat = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);
        assert_eq!(flat.subquery_depth(), 0);

        // One level of subquery: depth 1
        let one_deep = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM orders)",
            &tables,
        );
        assert_eq!(one_deep.subquery_depth(), 1);

        // Subquery adds 5 per depth level, so one_deep > flat
        assert!(
            one_deep.complexity() > flat.complexity(),
            "subquery should increase complexity: {} > {}",
            one_deep.complexity(),
            flat.complexity()
        );
    }

    #[test]
    fn test_complexity_nested_subquery_depth() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("products", 1001));
        tables.insert_overwrite(test_table_metadata("stores", 1002));
        tables.insert_overwrite(test_table_metadata("regions", 1003));

        // Double-nested: depth 2
        let double_nested = resolve_sql(
            "SELECT * FROM products WHERE id IN (SELECT id FROM stores WHERE id IN (SELECT id FROM regions))",
            &tables,
        );
        assert_eq!(double_nested.subquery_depth(), 2);

        // Single-nested: depth 1
        let single_nested = resolve_sql(
            "SELECT * FROM stores WHERE id IN (SELECT id FROM regions)",
            &tables,
        );
        assert_eq!(single_nested.subquery_depth(), 1);

        // Inner query (no subqueries): depth 0
        let inner = resolve_sql("SELECT * FROM regions", &tables);
        assert_eq!(inner.subquery_depth(), 0);

        // Verify ordering: inner < single_nested < double_nested
        assert!(
            inner.complexity() < single_nested.complexity(),
            "inner ({}) < single_nested ({})",
            inner.complexity(),
            single_nested.complexity()
        );
        assert!(
            single_nested.complexity() < double_nested.complexity(),
            "single_nested ({}) < double_nested ({})",
            single_nested.complexity(),
            double_nested.complexity()
        );
    }

    #[test]
    fn test_complexity_from_subquery_depth() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // FROM subquery: depth 1
        let from_sub = resolve_sql(
            "SELECT * FROM (SELECT * FROM users WHERE id = 1) sub",
            &tables,
        );
        assert_eq!(from_sub.subquery_depth(), 1);
    }

    #[test]
    fn test_group_by_resolve_single_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT name FROM users GROUP BY name", &tables);

        assert_eq!(resolved.group_by.len(), 1);
        assert_eq!(resolved.group_by[0].schema, "public");
        assert_eq!(resolved.group_by[0].table, "users");
        assert_eq!(resolved.group_by[0].column, "name");
    }

    #[test]
    fn test_group_by_resolve_multiple_columns() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT id, name FROM users GROUP BY id, name", &tables);

        assert_eq!(resolved.group_by.len(), 2);
        assert_eq!(resolved.group_by[0].column, "id");
        assert_eq!(resolved.group_by[1].column, "name");
    }

    #[test]
    fn test_group_by_resolve_qualified_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql("SELECT u.name FROM users u GROUP BY u.name", &tables);

        assert_eq!(resolved.group_by.len(), 1);
        assert_eq!(resolved.group_by[0].table, "users");
        assert_eq!(resolved.group_by[0].table_alias.as_deref(), Some("u"));
        assert_eq!(resolved.group_by[0].column, "name");
    }

    #[test]
    fn test_having_resolve() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT name FROM users GROUP BY name HAVING name = 'alice'",
            &tables,
        );

        assert!(resolved.having.is_some());
        if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.having {
            if let ResolvedWhereExpr::Column(col) = &*binary.lexpr {
                assert_eq!(col.column, "name");
            } else {
                panic!("Expected column in HAVING clause");
            }
        } else {
            panic!("Expected binary expression in HAVING clause");
        }
    }

    #[test]
    fn test_limit_resolve_count_only() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users LIMIT 10", &tables);

        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, None);
    }

    #[test]
    fn test_limit_resolve_offset_only() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users OFFSET 5", &tables);

        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, None);
        assert_eq!(limit.offset, Some(LiteralValue::Integer(5)));
    }

    #[test]
    fn test_limit_resolve_count_and_offset() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users LIMIT 10 OFFSET 20", &tables);

        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
    }

    #[test]
    fn test_limit_resolve_parameterized() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users LIMIT $1 OFFSET $2", &tables);

        // Parameterized values are preserved through resolution
        let limit = resolved.limit.unwrap();
        assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".to_owned())));
        assert_eq!(limit.offset, Some(LiteralValue::Parameter("$2".to_owned())));
    }

    #[test]
    fn test_no_limit_clause() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_query("SELECT * FROM users", &tables);

        assert!(resolved.limit.is_none());
    }

    #[test]
    fn test_combined_group_by_having_limit_resolve() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql =
            "SELECT name FROM users GROUP BY name HAVING name != 'test' ORDER BY name LIMIT 10";
        let resolved = resolve_query(sql, &tables);

        // GROUP BY and HAVING are on the select body
        let ResolvedQueryBody::Select(select) = &resolved.body else {
            panic!("Expected SELECT body");
        };
        assert_eq!(select.group_by.len(), 1);
        assert!(select.having.is_some());

        // ORDER BY and LIMIT are on the QueryExpr
        assert!(!resolved.order_by.is_empty());
        assert!(resolved.limit.is_some());
        assert_eq!(
            resolved.limit.unwrap().count,
            Some(LiteralValue::Integer(10))
        );
    }

    #[test]
    fn test_resolved_window_function() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Use columns that exist in test_table_metadata: id, name
        let resolved = resolve_sql(
            "SELECT sum(id) OVER (PARTITION BY name ORDER BY id) FROM users",
            &tables,
        );

        let ResolvedSelectColumns::Columns(columns) = &resolved.columns else {
            panic!("expected columns");
        };

        let ResolvedSelectColumn {
            expr: ResolvedColumnExpr::Function { name, over, .. },
            ..
        } = &columns[0]
        else {
            panic!("expected function");
        };

        assert_eq!(name, "sum");
        assert!(over.is_some(), "should have OVER clause");

        let window_spec = over.as_ref().unwrap();
        assert_eq!(window_spec.partition_by.len(), 1);
        assert_eq!(window_spec.order_by.len(), 1);
    }

    #[test]
    fn test_resolved_window_function_deparse() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Use columns that exist in test_table_metadata: id, name
        let resolved = resolve_sql(
            "SELECT sum(id) OVER (ORDER BY name DESC) FROM users",
            &tables,
        );

        let mut buf = String::new();
        resolved.deparse(&mut buf);

        // Should contain the window function with OVER clause
        assert!(
            buf.contains("OVER"),
            "deparsed SQL should contain OVER: {}",
            buf
        );
        assert!(
            buf.contains("ORDER BY"),
            "deparsed SQL should contain ORDER BY: {}",
            buf
        );
    }

    #[test]
    fn test_select_nodes_simple_select() {
        let query_expr = ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::default()),
            order_by: vec![],
            limit: None,
        };

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 1, "simple SELECT should have one branch");
    }

    #[test]
    fn test_select_nodes_union() {
        use crate::query::ast::SetOpType;

        let left_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "a".into(),
                alias: None,
                relation_oid: 1001,
            })],
            ..Default::default()
        };

        let right_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "b".into(),
                alias: None,
                relation_oid: 1002,
            })],
            ..Default::default()
        };

        let set_op = ResolvedSetOpNode {
            op: SetOpType::Union,
            all: false,
            left: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(left_select)),
                order_by: vec![],
                limit: None,
            }),
            right: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(right_select)),
                order_by: vec![],
                limit: None,
            }),
        };

        let query_expr = ResolvedQueryExpr {
            body: ResolvedQueryBody::SetOp(set_op),
            order_by: vec![],
            limit: None,
        };

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 2, "UNION should have two branches");

        // Verify each branch has the correct table
        assert_eq!(branches[0].from.len(), 1);
        assert_eq!(branches[1].from.len(), 1);

        if let ResolvedTableSource::Table(t) = &branches[0].from[0] {
            assert_eq!(t.name, "a");
        } else {
            panic!("Expected table source");
        }

        if let ResolvedTableSource::Table(t) = &branches[1].from[0] {
            assert_eq!(t.name, "b");
        } else {
            panic!("Expected table source");
        }
    }

    #[test]
    fn test_select_nodes_nested_union() {
        use crate::query::ast::SetOpType;

        // Build: (SELECT FROM a UNION SELECT FROM b) UNION SELECT FROM c
        let a_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "a".into(),
                alias: None,
                relation_oid: 1001,
            })],
            ..Default::default()
        };

        let b_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "b".into(),
                alias: None,
                relation_oid: 1002,
            })],
            ..Default::default()
        };

        let c_select = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".into(),
                name: "c".into(),
                alias: None,
                relation_oid: 1003,
            })],
            ..Default::default()
        };

        let inner_union = ResolvedSetOpNode {
            op: SetOpType::Union,
            all: false,
            left: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(a_select)),
                order_by: vec![],
                limit: None,
            }),
            right: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(b_select)),
                order_by: vec![],
                limit: None,
            }),
        };

        let outer_union = ResolvedSetOpNode {
            op: SetOpType::Union,
            all: false,
            left: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::SetOp(inner_union),
                order_by: vec![],
                limit: None,
            }),
            right: Box::new(ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(c_select)),
                order_by: vec![],
                limit: None,
            }),
        };

        let query_expr = ResolvedQueryExpr {
            body: ResolvedQueryBody::SetOp(outer_union),
            order_by: vec![],
            limit: None,
        };

        let branches = query_expr.select_nodes();
        assert_eq!(branches.len(), 3, "nested UNION should have three branches");
    }

    // ==========================================================================
    // Subquery Resolution Tests
    // ==========================================================================

    #[test]
    fn test_where_subquery_in_resolution() {
        // Test resolving WHERE ... IN (SELECT ...) subquery
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
            &tables,
        );

        // Should have resolved WHERE clause with subquery
        let where_clause = resolved
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        match where_clause {
            ResolvedWhereExpr::Subquery {
                sublink_type,
                test_expr,
                query,
                ..
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Any,
                    "IN should resolve as SubLinkType::Any"
                );
                assert!(test_expr.is_some(), "IN should have test expression");

                // Verify inner query was resolved
                match &query.body {
                    ResolvedQueryBody::Select(inner_select) => {
                        assert_eq!(inner_select.from.len(), 1);
                        if let ResolvedTableSource::Table(t) = &inner_select.from[0] {
                            assert_eq!(t.name, "active_users");
                            assert_eq!(t.relation_oid, 1002);
                        } else {
                            panic!("Expected table source");
                        }
                    }
                    _ => panic!("Expected SELECT body in subquery"),
                }
            }
            _ => panic!(
                "Expected ResolvedWhereExpr::Subquery, got {:?}",
                where_clause
            ),
        }
    }

    #[test]
    fn test_where_subquery_exists_resolution() {
        // Test resolving WHERE EXISTS (SELECT ...) subquery
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));
        tables.insert_overwrite(test_table_metadata("items", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM orders WHERE EXISTS (SELECT id FROM items)",
            &tables,
        );

        let where_clause = resolved
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        match where_clause {
            ResolvedWhereExpr::Subquery {
                sublink_type,
                test_expr,
                ..
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Exists,
                    "EXISTS should resolve correctly"
                );
                assert!(
                    test_expr.is_none(),
                    "EXISTS should not have test expression"
                );
            }
            _ => panic!("Expected ResolvedWhereExpr::Subquery"),
        }
    }

    #[test]
    fn test_where_subquery_scalar_resolution() {
        // Test resolving scalar subquery in WHERE clause
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id > (SELECT id FROM users)",
            &tables,
        );

        let where_clause = resolved
            .where_clause
            .as_ref()
            .expect("should have WHERE clause");

        // The scalar subquery should be on the right side of the > comparison
        match where_clause {
            ResolvedWhereExpr::Binary(binary) => match binary.rexpr.as_ref() {
                ResolvedWhereExpr::Subquery { sublink_type, .. } => {
                    assert_eq!(
                        *sublink_type,
                        SubLinkType::Expr,
                        "Scalar subquery should be SubLinkType::Expr"
                    );
                }
                _ => panic!("Expected ResolvedWhereExpr::Subquery on right side"),
            },
            _ => panic!("Expected ResolvedWhereExpr::Binary"),
        }
    }

    #[test]
    fn test_table_subquery_resolution() {
        // Test resolving subquery in FROM clause (derived table)
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Note: Column resolution from subqueries is limited, but the subquery itself should resolve
        let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");
        let result = select_node_resolve(&node, &tables, &["public"]);

        // Should resolve successfully
        let resolved = result.expect("should resolve table subquery");
        assert_eq!(resolved.from.len(), 1);

        match &resolved.from[0] {
            ResolvedTableSource::Subquery(sub) => {
                assert_eq!(sub.alias.name, "sub", "Should preserve alias");

                // Verify inner query was resolved
                match &sub.query.body {
                    ResolvedQueryBody::Select(inner_select) => {
                        assert_eq!(inner_select.from.len(), 1);
                        if let ResolvedTableSource::Table(t) = &inner_select.from[0] {
                            assert_eq!(t.name, "users");
                        } else {
                            panic!("Expected table source in inner query");
                        }
                    }
                    _ => panic!("Expected SELECT body"),
                }
            }
            _ => panic!("Expected ResolvedTableSource::Subquery"),
        }
    }

    #[test]
    fn test_table_subquery_requires_alias() {
        // Test that table subquery without alias fails resolution
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Parse a query with subquery without alias
        // Note: PostgreSQL parser typically requires alias, but we should still handle the error
        // gracefully if it somehow gets through
        let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");

        // This should succeed since it has an alias
        let result = select_node_resolve(&node, &tables, &["public"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_subquery_nodes_traversal() {
        // Test that nodes() traverses into subqueries to find all tables
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
            &tables,
        );

        // Should find both outer table and inner table via nodes() traversal
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(
            table_nodes.len(),
            2,
            "Should find tables in both outer and inner query"
        );

        let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"), "Should find outer table");
        assert!(
            table_names.contains(&"active_users"),
            "Should find inner table"
        );
    }

    #[test]
    fn test_subquery_nodes_traversal_derived_table() {
        // Test that nodes() traverses into FROM subqueries
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
            &tables,
        );

        // Should find the table inside the derived table
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(table_nodes.len(), 1, "Should find table in FROM subquery");
        assert_eq!(table_nodes[0].name, "users");
    }

    #[test]
    fn test_subquery_nodes_traversal_scalar() {
        // Test that nodes() traverses into scalar subqueries in SELECT list
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));
        tables.insert_overwrite(test_table_metadata("users", 1002));

        let resolved = resolve_sql(
            "SELECT id, (SELECT COUNT(*) FROM users) AS user_count FROM orders WHERE id = 1",
            &tables,
        );

        // Should find both tables
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(
            table_nodes.len(),
            2,
            "Should find tables in outer and scalar subquery"
        );

        let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"orders"), "Should find outer table");
        assert!(
            table_names.contains(&"users"),
            "Should find scalar subquery table"
        );
    }

    #[test]
    fn test_subquery_nodes_traversal_nested() {
        // Test that nodes() traverses into nested subqueries
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));
        tables.insert_overwrite(test_table_metadata("c", 1003));

        let resolved = resolve_sql(
            "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
            &tables,
        );

        // Should find all three tables
        let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(
            table_nodes.len(),
            3,
            "Should find all tables in nested subqueries"
        );

        let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"a"), "Should find outermost table");
        assert!(table_names.contains(&"b"), "Should find middle table");
        assert!(table_names.contains(&"c"), "Should find innermost table");
    }

    // ==========================================================================
    // Direct Table Nodes Tests (population uses these, not nodes())
    // ==========================================================================

    #[test]
    fn test_direct_table_nodes_excludes_where_subquery() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
            &tables,
        );

        // nodes() finds both tables (full traversal)
        let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(all_tables.len(), 2);

        // direct_table_nodes() only finds the FROM-clause table
        let direct_tables = resolved.direct_table_nodes();
        assert_eq!(direct_tables.len(), 1, "Should only find direct FROM table");
        assert_eq!(direct_tables[0].name, "users");
    }

    #[test]
    fn test_direct_table_nodes_with_join_and_subquery() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "items",
            1001,
            &["id", "name", "category_id"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "inventory",
            1002,
            &["id", "item_id", "quantity"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "categories",
            1003,
            &["id", "name", "active"],
        ));

        let resolved = resolve_sql(
            "SELECT i.name FROM items i \
             JOIN inventory inv ON i.id = inv.item_id \
             WHERE i.category_id IN (SELECT c.id FROM categories c WHERE c.active = true) \
             ORDER BY i.name",
            &tables,
        );

        // nodes() finds all 3 tables
        let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(all_tables.len(), 3);

        // direct_table_nodes() only finds the 2 JOIN tables, not the WHERE subquery table
        let direct_tables = resolved.direct_table_nodes();
        assert_eq!(
            direct_tables.len(),
            2,
            "Should find items and inventory but not categories"
        );
        let names: Vec<&str> = direct_tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"items"));
        assert!(names.contains(&"inventory"));
        assert!(!names.contains(&"categories"));
    }

    #[test]
    fn test_direct_table_nodes_derived_table() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let resolved = resolve_sql(
            "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
            &tables,
        );

        // nodes() finds the table inside the derived table
        let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
        assert_eq!(all_tables.len(), 1);

        // direct_table_nodes() finds nothing — the derived table is a subquery, not a direct table
        let direct_tables = resolved.direct_table_nodes();
        assert_eq!(
            direct_tables.len(),
            0,
            "Derived table should not appear in direct_table_nodes"
        );
    }

    // ==========================================================================
    // Correlated Subquery Tests
    // ==========================================================================

    #[test]
    fn test_correlated_exists_subquery_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("orders", 1001));
        tables.insert_overwrite(test_table_metadata("items", 1002));

        let node = parse_select_node(
            "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.id = orders.id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated EXISTS should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "orders");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_correlated_in_subquery_resolves() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let node = parse_select_node(
            "SELECT * FROM users WHERE id IN (SELECT id FROM orders WHERE orders.name = users.name)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated IN should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "name");
    }

    #[test]
    fn test_correlated_scalar_subquery_resolves() {
        // Scalar correlated subquery in SELECT list
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let node = parse_select_node(
            "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.id = users.id) FROM users",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated scalar subquery should resolve successfully");
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("expected Columns");
        };
        let outer_refs = cols
            .iter()
            .find_map(|col| match &col.expr {
                ResolvedColumnExpr::Subquery(_, outer_refs) => Some(outer_refs),
                _ => None,
            })
            .expect("subquery column");
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_correlated_subquery_with_alias_resolves() {
        // Table alias in outer scope should be resolved to the aliased table
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let node = parse_select_node(
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = u.id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("correlated subquery with alias should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_correlated_unqualified_column_in_where() {
        // `email` only exists on `users`, not `orders` — bare `email` in the subquery
        // is an implicit correlated reference resolved via outer scope fallback
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "email"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "user_id", "total"],
        ));

        let node = parse_select_node(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE email = 'test@example.com')",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("unqualified outer column should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "email");
    }

    #[test]
    fn test_correlated_unqualified_column_in_select_list() {
        // `id` exists in both `users` and `orders` — resolves to inner scope (non-correlated)
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "email"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "user_id"],
        ));

        let node = parse_select_node(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("column present in both scopes should resolve to inner scope");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        // `id` resolves to `orders.id` in the inner scope — not a correlated reference
        assert!(
            outer_refs.is_empty(),
            "inner-scope column should not appear in outer_refs"
        );
    }

    #[test]
    fn test_correlated_unqualified_column_scalar_subquery() {
        // `status` only exists on `users`, bare reference in SELECT-list scalar subquery
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "status"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "amount"],
        ));

        let node = parse_select_node(
            "SELECT id, (SELECT COUNT(*) FROM orders WHERE status = 'active') FROM users",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("unqualified outer column in scalar subquery should resolve");
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("expected Columns");
        };
        let outer_refs = cols
            .iter()
            .find_map(|col| match &col.expr {
                ResolvedColumnExpr::Subquery(_, outer_refs) => Some(outer_refs),
                _ => None,
            })
            .expect("subquery column");
        assert_eq!(outer_refs.len(), 1, "should have one outer ref");
        assert_eq!(outer_refs[0].table, "users");
        assert_eq!(outer_refs[0].column, "status");
    }

    #[test]
    fn test_non_correlated_subquery_has_empty_outer_refs() {
        // Non-correlated subquery should have outer_refs: []
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let node =
            parse_select_node("SELECT * FROM users WHERE id IN (SELECT id FROM active_users)");
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("non-correlated subquery should resolve successfully");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert!(
            outer_refs.is_empty(),
            "non-correlated subquery must have empty outer_refs"
        );
    }

    #[test]
    fn test_correlated_mixed_inner_and_outer_columns() {
        // Same predicate references both an inner-scope column and an outer-scope column.
        // The inner column resolves normally; the outer column goes into outer_refs.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "departments",
            1001,
            &["id", "region"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "employees",
            1002,
            &["id", "dept_id", "region"],
        ));

        // `employees.dept_id = departments.id` — dept_id is inner, departments.id is outer
        let node = parse_select_node(
            "SELECT d.id FROM departments d \
             WHERE EXISTS (SELECT 1 FROM employees WHERE dept_id = d.id)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        let resolved = result.expect("mixed inner/outer predicate should resolve");
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert_eq!(
            outer_refs.len(),
            1,
            "only the outer-scope column should be in outer_refs"
        );
        assert_eq!(outer_refs[0].table, "departments");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_doubly_nested_correlated_subquery() {
        // Grandchild subquery references the grandparent scope
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "departments",
            1001,
            &["id", "name"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "employees",
            1002,
            &["id", "dept_id"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "projects",
            1003,
            &["id", "employee_id"],
        ));

        // departments d → employees e (correlated on d.id) → projects (correlated on e.id)
        let node = parse_select_node(
            "SELECT d.id FROM departments d \
             WHERE EXISTS (\
               SELECT 1 FROM employees e WHERE e.dept_id = d.id AND EXISTS (\
                 SELECT 1 FROM projects WHERE employee_id = e.id\
               )\
             )",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        // Resolution must succeed; the outer EXISTS subquery is correlated on d.id
        assert!(
            result.is_ok(),
            "doubly-nested correlated subquery should resolve, got: {:?}",
            result
        );
        let resolved = result.unwrap();
        let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
            panic!("expected Subquery WHERE");
        };
        assert!(
            !outer_refs.is_empty(),
            "outer EXISTS should be correlated on departments.id"
        );
        assert_eq!(outer_refs[0].table, "departments");
        assert_eq!(outer_refs[0].column, "id");
    }

    #[test]
    fn test_unqualified_column_not_in_any_scope() {
        // `nonexistent` doesn't exist in any table — should remain ColumnNotFound
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns(
            "users",
            1001,
            &["id", "name"],
        ));
        tables.insert_overwrite(test_table_metadata_with_columns(
            "orders",
            1002,
            &["id", "total"],
        ));

        let node = parse_select_node(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE nonexistent = 1)",
        );
        let result = select_node_resolve(&node, &tables, &["public"]);

        assert!(
            matches!(
                result.as_ref().map_err(|e| e.current_context()),
                Err(ResolveError::ColumnNotFound { .. })
            ),
            "Column not in any scope should remain ColumnNotFound, got: {:?}",
            result
        );
    }
}

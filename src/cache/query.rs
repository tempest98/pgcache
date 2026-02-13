use std::collections::HashSet;

use crate::{
    cache::QueryParameters,
    query::{
        ast::{
            BinaryOp, CteRefNode, JoinNode, JoinType, LimitClause, LiteralValue, MultiOp,
            QueryBody, QueryExpr, SelectNode, SetOpNode, SubLinkType, TableSource,
            TableSubqueryNode, WhereExpr,
        },
        resolved::{
            ResolvedColumnNode, ResolvedJoinNode, ResolvedSelectNode, ResolvedTableSource,
            ResolvedWhereExpr,
        },
        transform::{AstTransformResult, query_expr_parameters_replace},
    },
};
use error_set::error_set;

error_set! {
    CacheabilityError := {
        UnsupportedQueryType,
        UnsupportedFrom,
        #[display("Unsupported subquery type")]
        UnsupportedSubquery,
        UnsupportedWhereClause,
        HasLimit,
    }
}

#[derive(Debug, Clone)]
pub struct CacheableQuery {
    pub query: QueryExpr,
}

impl CacheableQuery {
    /// Replace parameter placeholders ($1, $2, etc.) with actual values.
    /// This mutates the query in place, replacing all parameter nodes with literal values.
    ///
    /// # Arguments
    /// * `parameters` - The parameter values, indexed from 0 (for $1, $2, etc.)
    ///
    /// # Errors
    /// Returns `AstTransformError` if parameter replacement fails (e.g., invalid index, invalid UTF-8)
    pub fn parameters_replace(&mut self, parameters: &QueryParameters) -> AstTransformResult<()> {
        self.query = query_expr_parameters_replace(&self.query, parameters)?;
        Ok(())
    }

    /// Get the SELECT body of this query, if it is a simple SELECT.
    ///
    /// Returns `Some` if the query body is a SELECT statement, `None` if it's
    /// a set operation (UNION/INTERSECT/EXCEPT) or VALUES clause.
    pub fn as_select(&self) -> Option<&SelectNode> {
        self.query.as_select()
    }
}

impl TryFrom<&QueryExpr> for CacheableQuery {
    type Error = CacheabilityError;

    fn try_from(query: &QueryExpr) -> Result<Self, Self::Error> {
        // Validate the query body (SELECT, VALUES, or SetOp)
        is_cacheable_body(&query.body)?;

        Ok(CacheableQuery {
            query: query.clone(),
        })
    }
}

/// Check if a query body is cacheable.
/// Recursively validates SELECT nodes and set operation branches.
fn is_cacheable_body(body: &QueryBody) -> Result<(), CacheabilityError> {
    match body {
        QueryBody::Select(node) => is_cacheable_select(node),
        QueryBody::Values(_) => {
            // VALUES clauses are not cacheable as standalone queries
            Err(CacheabilityError::UnsupportedQueryType)
        }
        QueryBody::SetOp(set_op) => is_cacheable_set_op(set_op),
    }
}

/// Check if a SELECT node is cacheable.
fn is_cacheable_select(node: &SelectNode) -> Result<(), CacheabilityError> {
    // Check FROM clause (tables, joins, subqueries)
    is_supported_from(node, ExprContext::FromClause)?;

    // Check WHERE clause (including any subqueries)
    is_cacheable_where(node, ExprContext::WhereClause)?;

    // Check SELECT list for subqueries
    is_cacheable_select_list(node, ExprContext::SelectList)?;

    Ok(())
}

/// Check if a set operation (UNION/INTERSECT/EXCEPT) is cacheable.
/// Both branches must be cacheable.
fn is_cacheable_set_op(set_op: &SetOpNode) -> Result<(), CacheabilityError> {
    // LIMIT/OFFSET on branches makes them non-cacheable
    if set_op.left.limit.is_some() || set_op.right.limit.is_some() {
        return Err(CacheabilityError::HasLimit);
    }

    // Recursively validate both branches (subquery cacheability checked recursively)
    is_cacheable_body(&set_op.left.body)?;
    is_cacheable_body(&set_op.right.body)?;

    Ok(())
}

fn is_supported_from(select: &SelectNode, ctx: ExprContext) -> Result<(), CacheabilityError> {
    match select.from.as_slice() {
        [TableSource::Join(join)] => is_supported_join(join, ctx),
        [TableSource::Table(_)] => Ok(()),
        [TableSource::Subquery(sub)] => is_cacheable_table_subquery(sub, ctx),
        [TableSource::CteRef(cte_ref)] => is_cacheable_cte_ref(cte_ref, ctx),
        _ => Err(CacheabilityError::UnsupportedFrom),
    }
}

/// Check if a table subquery (derived table) is cacheable.
fn is_cacheable_table_subquery(
    subquery: &TableSubqueryNode,
    _ctx: ExprContext,
) -> Result<(), CacheabilityError> {
    // LATERAL subqueries are not supported (they reference outer scope)
    if subquery.lateral {
        return Err(CacheabilityError::UnsupportedSubquery);
    }

    // Subquery must have an alias
    if subquery.alias.is_none() {
        return Err(CacheabilityError::UnsupportedSubquery);
    }

    // Inner query must be cacheable
    // Note: We check the inner query for cacheability but don't check for LIMIT
    // since LIMIT in a derived table subquery is valid SQL
    is_cacheable_body(&subquery.query.body)
}

fn is_supported_join(join: &JoinNode, ctx: ExprContext) -> Result<(), CacheabilityError> {
    // FULL OUTER JOINs are not cacheable — both sides are optional
    if join.join_type == JoinType::Full {
        return Err(CacheabilityError::UnsupportedFrom);
    }

    // Validate join condition: must be equality, AND of equalities, or absent
    let condition_valid = match &join.condition {
        Some(expr) => join_condition_is_valid(expr),
        None => true,
    };

    if !condition_valid {
        return Err(CacheabilityError::UnsupportedFrom);
    }

    // Recursively validate nested joins/tables/subqueries
    is_supported_table_source(&join.left, ctx)?;
    is_supported_table_source(&join.right, ctx)?;

    Ok(())
}

/// Check if a join condition contains only equalities or AND of equalities.
fn join_condition_is_valid(expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Binary(b) => match b.op {
            BinaryOp::Equal => true,
            BinaryOp::And => join_condition_is_valid(&b.lexpr) && join_condition_is_valid(&b.rexpr),
            BinaryOp::Or
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => false,
        },
        WhereExpr::Value(_)
        | WhereExpr::Column(_)
        | WhereExpr::Unary(_)
        | WhereExpr::Multi(_)
        | WhereExpr::Function { .. }
        | WhereExpr::Subquery { .. } => false,
    }
}

/// Check if a table source (in a join) is supported.
fn is_supported_table_source(
    source: &TableSource,
    ctx: ExprContext,
) -> Result<(), CacheabilityError> {
    match source {
        TableSource::Join(nested) => is_supported_join(nested, ctx),
        TableSource::Table(_) => Ok(()),
        TableSource::Subquery(sub) => is_cacheable_table_subquery(sub, ctx),
        TableSource::CteRef(cte_ref) => is_cacheable_cte_ref(cte_ref, ctx),
    }
}

/// Check if a CTE reference is cacheable.
fn is_cacheable_cte_ref(cte_ref: &CteRefNode, _ctx: ExprContext) -> Result<(), CacheabilityError> {
    is_cacheable_body(&cte_ref.query.body)
}

/// Check if a SELECT's WHERE clause can be efficiently cached.
///
/// Supports:
/// - Simple equality, AND of equalities, OR of equalities in WHERE
/// - GROUP BY and HAVING (aggregation performed on cached rows at retrieval time)
/// - Non-correlated subqueries (EXISTS, IN, scalar)
///
fn is_cacheable_where(select: &SelectNode, ctx: ExprContext) -> Result<(), CacheabilityError> {
    match &select.where_clause {
        Some(where_expr) => is_cacheable_expr(where_expr, ctx),
        None => Ok(()), // No WHERE clause is always cacheable
    }
}

/// Where in the query tree a cacheability check is being evaluated.
///
/// Functions in the SELECT list (e.g. CASE WHEN conditions) are safe — they're
/// re-evaluated against cached rows. Functions in the WHERE clause affect row-set
/// membership and need separate design work, so they're rejected there.
#[derive(Clone, Copy)]
enum ExprContext {
    /// Expression in the FROM clause — functions not yet supported
    FromClause,
    /// Expression in the WHERE clause — functions not yet supported
    WhereClause,
    /// Expression in the SELECT list — functions allowed
    SelectList,
}

/// Determine if a WHERE expression can be efficiently cached.
/// Supports simple comparisons, AND/OR of comparisons, and non-correlated subqueries.
fn is_cacheable_expr(expr: &WhereExpr, ctx: ExprContext) -> Result<(), CacheabilityError> {
    match expr {
        WhereExpr::Binary(binary_expr) => match binary_expr.op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => {
                // Recursively check both sides are cacheable
                is_cacheable_expr(&binary_expr.lexpr, ctx)?;
                is_cacheable_expr(&binary_expr.rexpr, ctx)?;
                // Allow comparisons where both sides are cacheable:
                // - (Column, Value) - simple comparisons for cache filtering
                // - (Column, Column) - join conditions in subqueries
                // - (Column, Subquery) - scalar subquery comparisons
                Ok(())
            }
            BinaryOp::And | BinaryOp::Or => {
                is_cacheable_expr(&binary_expr.lexpr, ctx)?;
                is_cacheable_expr(&binary_expr.rexpr, ctx)
            }
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                Err(CacheabilityError::UnsupportedWhereClause)
            }
        },
        WhereExpr::Value(_) => Ok(()),
        WhereExpr::Column(_) => Ok(()),
        WhereExpr::Multi(multi_expr) => match multi_expr.op {
            MultiOp::In | MultiOp::NotIn => {
                for e in &multi_expr.exprs {
                    is_cacheable_expr(e, ctx)?;
                }
                Ok(())
            }
            MultiOp::Between | MultiOp::NotBetween | MultiOp::Any | MultiOp::All => {
                Err(CacheabilityError::UnsupportedWhereClause)
            }
        },
        WhereExpr::Unary(unary_expr) => is_cacheable_expr(&unary_expr.expr, ctx),
        WhereExpr::Function { args, .. } => match ctx {
            ExprContext::FromClause | ExprContext::WhereClause => {
                Err(CacheabilityError::UnsupportedWhereClause)
            }
            ExprContext::SelectList => args.iter().try_for_each(|arg| is_cacheable_expr(arg, ctx)),
        },
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            // Check the inner query is cacheable
            is_cacheable_subquery_inner(query)?;

            // Check test_expr (left-hand side for IN/ANY/ALL) is cacheable
            if let Some(test) = test_expr {
                is_cacheable_expr(test, ctx)?;
            }

            // All supported sublink types are cacheable if inner query is cacheable
            match sublink_type {
                SubLinkType::Exists | SubLinkType::Any | SubLinkType::Expr => Ok(()),
                SubLinkType::All => {
                    // ALL subqueries can be complex - allow for now
                    Ok(())
                }
            }
        }
    }
}

/// Check if a subquery's inner query is cacheable.
/// For subqueries, we allow LIMIT since it's valid in derived tables.
fn is_cacheable_subquery_inner(query: &QueryExpr) -> Result<(), CacheabilityError> {
    // Check the query body (SELECT, VALUES, or SetOp)
    is_cacheable_body(&query.body)
}

/// Check if a SELECT list contains cacheable expressions.
/// Currently rejects functions that contain subqueries.
fn is_cacheable_select_list(
    select: &SelectNode,
    ctx: ExprContext,
) -> Result<(), CacheabilityError> {
    use crate::query::ast::SelectColumns;

    match &select.columns {
        SelectColumns::All | SelectColumns::None => Ok(()),
        SelectColumns::Columns(cols) => {
            for col in cols {
                is_cacheable_column_expr(&col.expr, ctx)?;
            }
            Ok(())
        }
    }
}

/// Check if a column expression is cacheable.
fn is_cacheable_column_expr(
    expr: &crate::query::ast::ColumnExpr,
    ctx: ExprContext,
) -> Result<(), CacheabilityError> {
    use crate::query::ast::ColumnExpr;

    match expr {
        ColumnExpr::Column(_) | ColumnExpr::Literal(_) => Ok(()),
        ColumnExpr::Function(func) => {
            // Recursively check function arguments
            for arg in &func.args {
                is_cacheable_column_expr(arg, ctx)?;
            }
            Ok(())
        }
        ColumnExpr::Case(case) => {
            // Check case argument if present
            if let Some(arg) = &case.arg {
                is_cacheable_column_expr(arg, ctx)?;
            }
            // Check when conditions and results
            for when in &case.whens {
                is_cacheable_expr(&when.condition, ctx)?;
                is_cacheable_column_expr(&when.result, ctx)?;
            }
            // Check default
            if let Some(default) = &case.default {
                is_cacheable_column_expr(default, ctx)?;
            }
            Ok(())
        }
        ColumnExpr::Arithmetic(arith) => {
            is_cacheable_column_expr(&arith.left, ctx)?;
            is_cacheable_column_expr(&arith.right, ctx)
        }
        ColumnExpr::Subquery(query) => {
            // Scalar subquery in SELECT list - check inner query
            is_cacheable_subquery_inner(query)
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Outer join terminality analysis
// ──────────────────────────────────────────────────────────────────────

/// Categorizes optional-side tables in outer joins as terminal or non-terminal.
///
/// - **Terminal**: columns don't appear in WHERE or other join conditions.
///   CDC INSERT/DELETE handled in place — the preserved side already has the row,
///   changes here only affect NULL-padded columns.
/// - **Non-terminal**: columns appear in WHERE or other join conditions.
///   CDC events trigger full query invalidation (conservative but correct).
///
/// Uses the resolved AST where column references carry the real table name
/// (not aliases), eliminating alias ambiguity.
pub fn outer_join_optional_tables(
    select: &ResolvedSelectNode,
) -> (HashSet<String>, HashSet<String>) {
    let join = match select.from.as_slice() {
        [ResolvedTableSource::Join(join)] => join,
        _ => return (HashSet::new(), HashSet::new()),
    };

    // Pass 1: collect real table names from WHERE clause column references.
    // GROUP BY, HAVING, and SELECT list are excluded — population queries strip
    // GROUP BY/HAVING, and all three are re-evaluated at retrieval time against
    // cached rows.
    let mut non_terminal_refs = HashSet::new();
    if let Some(where_clause) = &select.where_clause {
        resolved_column_table_refs_collect(where_clause, &mut non_terminal_refs);
    }

    // Pass 2: walk the join tree, collecting all optional-side tables and
    // identifying which are non-terminal
    let mut all_optional = HashSet::new();
    let mut non_terminal = HashSet::new();
    resolved_join_terminality_walk(
        join,
        &non_terminal_refs,
        &mut all_optional,
        &mut non_terminal,
    );

    let terminal = all_optional.difference(&non_terminal).cloned().collect();
    (terminal, non_terminal)
}

/// Collect real table names from all column references in a resolved WHERE expression.
fn resolved_column_table_refs_collect(expr: &ResolvedWhereExpr, tables: &mut HashSet<String>) {
    for col in expr.nodes::<ResolvedColumnNode>() {
        tables.insert(col.table.clone());
    }
}

/// Collect the real table names from all table nodes in a resolved table source subtree.
/// Traverses JOINs but not subqueries.
fn resolved_source_table_names_collect(source: &ResolvedTableSource, names: &mut HashSet<String>) {
    match source {
        ResolvedTableSource::Table(table) => {
            names.insert(table.name.clone());
        }
        ResolvedTableSource::Join(join) => {
            resolved_source_table_names_collect(&join.left, names);
            resolved_source_table_names_collect(&join.right, names);
        }
        ResolvedTableSource::Subquery(_) => {}
    }
}

/// Recursive walk of the resolved join tree to collect optional-side tables
/// and identify which are non-terminal.
///
/// `non_terminal_refs` accumulates: WHERE column table refs + ancestor join
/// condition column table refs. At each outer join, the optional side's tables
/// are checked against this set.
///
/// The current join's own ON condition is NOT in `non_terminal_refs` during the
/// check — it's only merged before recursing into children. This correctly
/// excludes a join's own condition from the terminal definition.
fn resolved_join_terminality_walk(
    join: &ResolvedJoinNode,
    non_terminal_refs: &HashSet<String>,
    all_optional: &mut HashSet<String>,
    non_terminal: &mut HashSet<String>,
) {
    // Collect optional-side tables at this level
    if matches!(join.join_type, JoinType::Left | JoinType::Right) {
        let optional_side = match join.join_type {
            JoinType::Left => &join.right,
            JoinType::Right => &join.left,
            JoinType::Inner | JoinType::Full => unreachable!(),
        };

        let mut optional_tables = HashSet::new();
        resolved_source_table_names_collect(optional_side, &mut optional_tables);
        for table in &optional_tables {
            all_optional.insert(table.clone());
            if non_terminal_refs.contains(table) {
                non_terminal.insert(table.clone());
            }
        }
    }

    // Before recursing, merge this join's condition refs so children see them
    // as "ancestor join conditions"
    let mut child_refs = non_terminal_refs.clone();
    if let Some(condition) = &join.condition {
        resolved_column_table_refs_collect(condition, &mut child_refs);
    }

    // Recurse into nested joins
    if let ResolvedTableSource::Join(left) = &join.left {
        resolved_join_terminality_walk(left, &child_refs, all_optional, non_terminal);
    }
    if let ResolvedTableSource::Join(right) = &join.right {
        resolved_join_terminality_walk(right, &child_refs, all_optional, non_terminal);
    }
}

/// Extract the total rows needed from a LIMIT clause.
///
/// Returns `None` if there is no LIMIT count (= unlimited rows needed).
/// Returns `Some(limit + offset)` when a LIMIT count is present.
pub fn limit_rows_needed(limit: &Option<LimitClause>) -> Option<u64> {
    let limit_clause = limit.as_ref()?;
    let count = match &limit_clause.count {
        Some(LiteralValue::Integer(n)) => *n as u64,
        _ => return None,
    };
    let offset = match &limit_clause.offset {
        Some(LiteralValue::Integer(n)) => *n as u64,
        _ => 0,
    };
    Some(count + offset)
}

/// Check whether the cached `max_limit` is sufficient for the incoming `needed` rows.
///
/// - `cached_max`: `None` means all rows are cached.
/// - `needed`: `None` means all rows needed (no LIMIT).
pub fn limit_is_sufficient(cached_max: Option<u64>, needed: Option<u64>) -> bool {
    match (cached_max, needed) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(cached), Some(needed)) => cached >= needed,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm
)]
mod tests {
    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, TableMetadata};
    use crate::query::ast::{query_expr_convert, query_expr_fingerprint};
    use crate::query::resolved::select_node_resolve;

    /// Parse SQL and check cacheability
    fn check_cacheable(sql: &str) -> Result<CacheableQuery, CacheabilityError> {
        let ast = pg_query::parse(sql).expect("parse");
        let query_expr = query_expr_convert(&ast).expect("convert");
        CacheableQuery::try_from(&query_expr)
    }

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: u32, column_names: &[&str]) -> TableMetadata {
        let mut columns = BiHashMap::new();
        for (i, col_name) in column_names.iter().enumerate() {
            let is_pk = i == 0;
            columns.insert_overwrite(ColumnMetadata {
                name: (*col_name).to_owned(),
                position: (i + 1) as i16,
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.to_owned(),
                cache_type_name: if is_pk { "int4" } else { "text" }.to_owned(),
                is_primary_key: is_pk,
            });
        }
        TableMetadata {
            relation_oid,
            name: name.to_owned(),
            schema: "public".to_owned(),
            primary_key_columns: vec![column_names[0].to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Parse SQL and resolve the SELECT node using the given tables.
    fn resolve_select(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
        let ast = pg_query::parse(sql).expect("parse");
        let query_expr = query_expr_convert(&ast).expect("convert");
        let select = match query_expr.body {
            QueryBody::Select(s) => s,
            _ => panic!("expected SELECT"),
        };
        select_node_resolve(&select, tables, &["public"]).expect("resolve")
    }

    #[test]
    fn test_two_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "2-table inner join should be cacheable");
    }

    #[test]
    fn test_three_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "3-table inner join should be cacheable");
    }

    #[test]
    fn test_four_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "4-table inner join should be cacheable");
    }

    #[test]
    fn test_left_join_cacheable() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "LEFT JOIN should be cacheable");
    }

    #[test]
    fn test_right_join_cacheable() {
        let sql = "SELECT * FROM a RIGHT JOIN b ON a.id = b.id WHERE b.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "RIGHT JOIN should be cacheable");
    }

    #[test]
    fn test_mixed_join_types_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Mixed join types (INNER + LEFT) should be cacheable"
        );
    }

    #[test]
    fn test_chained_left_joins_cacheable() {
        let sql =
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Chained LEFT JOINs should be cacheable");
    }

    #[test]
    fn test_non_terminal_left_join_cacheable() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE b.status = 'active'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Non-terminal LEFT JOIN should be cacheable (CDC handles correctness)"
        );
    }

    #[test]
    fn test_full_join_not_cacheable() {
        let sql = "SELECT * FROM a FULL JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "FULL JOIN should not be cacheable"
        );
    }

    #[test]
    fn test_join_and_condition_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id AND a.tenant = b.tenant WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "AND of equalities in join condition should be cacheable"
        );
    }

    #[test]
    fn test_left_join_and_condition_cacheable() {
        let sql =
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND a.tenant = b.tenant WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "LEFT JOIN with AND condition should be cacheable"
        );
    }

    #[test]
    fn test_join_with_non_equality_condition_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id > b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "Non-equality join condition should not be cacheable"
        );
    }

    #[test]
    fn test_nested_join_with_non_equality_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id > b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "Nested join with non-equality condition should not be cacheable"
        );
    }

    #[test]
    fn test_group_by_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "GROUP BY should be cacheable");
    }

    #[test]
    fn test_group_by_multiple_columns_cacheable() {
        let sql =
            "SELECT status, category FROM orders WHERE tenant_id = 1 GROUP BY status, category";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "GROUP BY with multiple columns should be cacheable"
        );
    }

    #[test]
    fn test_having_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status HAVING status = 'active'";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "HAVING should be cacheable");
    }

    #[test]
    fn test_limit_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "LIMIT should be cacheable");
    }

    #[test]
    fn test_offset_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "OFFSET should be cacheable");
    }

    #[test]
    fn test_group_by_with_limit_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status LIMIT 5";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "GROUP BY with LIMIT should be cacheable");
    }

    // ==================== Subquery Tests ====================

    #[test]
    fn test_subquery_in_select_cacheable() {
        // Scalar subqueries in SELECT list are now cacheable
        let sql = "SELECT id, (SELECT x FROM other WHERE id = 1) FROM t WHERE id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Scalar subquery in SELECT list should be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_from_cacheable() {
        // Derived tables (non-LATERAL subqueries) in FROM are now cacheable
        let sql = "SELECT * FROM (SELECT id FROM users) sub WHERE id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Subquery in FROM clause should be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_join_cacheable() {
        // Subqueries in JOIN are now cacheable
        let sql = "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Subquery in JOIN should be cacheable");
    }

    #[test]
    fn test_subquery_in_where_cacheable() {
        // IN subqueries in WHERE are now cacheable
        let sql = "SELECT * FROM t WHERE id IN (SELECT id FROM other)";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Subquery in WHERE clause should be cacheable"
        );
    }

    #[test]
    fn test_subquery_exists_cacheable() {
        let sql = "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "EXISTS subquery should be cacheable, got: {:?}",
            result
        );
    }

    #[test]
    fn test_subquery_scalar_in_where_cacheable() {
        let sql = "SELECT * FROM users WHERE id > (SELECT AVG(id) FROM users)";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Scalar subquery in WHERE should be cacheable, got: {:?}",
            result
        );
    }

    #[test]
    fn test_subquery_nested_cacheable() {
        let sql = "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Nested subqueries should be cacheable");
    }

    #[test]
    fn test_subquery_with_limit_in_outer_cacheable() {
        let sql = "SELECT * FROM (SELECT id FROM users) sub LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Outer LIMIT on simple SELECT should be cacheable"
        );
    }

    #[test]
    fn test_function_in_select_cacheable() {
        let sql = "SELECT COUNT(*), SUM(amount) FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Functions in SELECT should be cacheable");
    }

    // ==================== Set Operation Tests ====================

    #[test]
    fn test_union_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 UNION SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION should be cacheable");
    }

    #[test]
    fn test_union_all_cacheable() {
        let sql =
            "SELECT id FROM a WHERE tenant_id = 1 UNION ALL SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION ALL should be cacheable");
    }

    #[test]
    fn test_intersect_cacheable() {
        let sql =
            "SELECT id FROM a WHERE tenant_id = 1 INTERSECT SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "INTERSECT should be cacheable");
    }

    #[test]
    fn test_except_cacheable() {
        let sql =
            "SELECT id FROM a WHERE tenant_id = 1 EXCEPT SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "EXCEPT should be cacheable");
    }

    #[test]
    fn test_nested_union_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 \
                   UNION SELECT id FROM b WHERE tenant_id = 1 \
                   UNION SELECT id FROM c WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Nested UNION should be cacheable");
    }

    #[test]
    fn test_union_with_join_cacheable() {
        let sql = "SELECT a.id FROM a JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
                   UNION \
                   SELECT c.id FROM c WHERE c.tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION with JOIN should be cacheable");
    }

    #[test]
    fn test_union_with_outer_limit_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 \
                   UNION SELECT id FROM b WHERE tenant_id = 1 \
                   LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "UNION with outer LIMIT should be cacheable — \
             all rows are populated per-branch, LIMIT applied at serve time"
        );
    }

    #[test]
    fn test_union_with_branch_limit_not_cacheable() {
        let sql = "(SELECT id FROM a WHERE tenant_id = 1 LIMIT 5) \
                   UNION SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "UNION with LIMIT in branch should not be cacheable"
        );
    }

    #[test]
    fn test_union_with_subquery_cacheable() {
        // Subqueries in UNION branches are now cacheable
        let sql = "SELECT id FROM a WHERE id IN (SELECT id FROM other) \
                   UNION SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION with subquery should be cacheable");
    }

    #[test]
    fn test_union_with_left_join_cacheable() {
        let sql = "SELECT a.id FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
                   UNION SELECT id FROM c WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION with LEFT JOIN should be cacheable");
    }

    // CTE cacheability tests

    #[test]
    fn test_cte_simple_cacheable() {
        let sql = "WITH x AS (SELECT id FROM users WHERE id = 1) SELECT * FROM x";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "simple CTE should be cacheable: {result:?}");
    }

    #[test]
    fn test_cte_with_join_cacheable() {
        let sql = "WITH active AS (SELECT id, name FROM users WHERE active = true) \
                    SELECT u.id, a.name FROM users u JOIN active a ON u.id = a.id WHERE u.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "CTE in join should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_cte_multiple_cacheable() {
        let sql = "WITH a AS (SELECT id FROM users WHERE id = 1), \
                    b AS (SELECT id FROM products WHERE id = 2) \
                    SELECT * FROM a JOIN b ON a.id = b.id";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "multiple CTEs should be cacheable: {result:?}"
        );
    }

    // ==================== Function in CASE WHEN Tests ====================

    #[test]
    fn test_case_with_function_in_condition_cacheable() {
        let sql = "SELECT CASE WHEN date_trunc('day', created_at) = '2024-01-01' THEN 'yes' ELSE 'no' END FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "CASE with function in condition should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_case_with_nested_function_cacheable() {
        let sql = "SELECT CASE WHEN date_trunc('day', now()) = '2024-01-01' THEN 'yes' ELSE 'no' END FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "CASE with nested function calls should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_function_in_where_clause_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE date_trunc('day', created_at) = '2024-01-01'";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "Function in WHERE clause should not be cacheable: {result:?}"
        );
    }

    // ==================== Outer Join Terminality Tests ====================

    /// Create standard test tables for terminality tests:
    /// a(id, name, status), b(id, a_id, name, status, val, x), c(id, b_id, val, x)
    fn terminality_test_tables() -> BiHashMap<TableMetadata> {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table("a", 1, &["id", "name", "status"]));
        tables.insert_overwrite(test_table(
            "b",
            2,
            &["id", "a_id", "name", "status", "val", "x"],
        ));
        tables.insert_overwrite(test_table("c", 3, &["id", "b_id", "val", "x"]));
        tables
    }

    #[test]
    fn test_terminal_left_join() {
        let tables = terminality_test_tables();
        // b is terminal: only appears in its own ON clause and SELECT list
        let select = resolve_select(
            "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(terminal.contains("b"), "b should be terminal: {terminal:?}");
    }

    #[test]
    fn test_non_terminal_where_reference() {
        let tables = terminality_test_tables();
        // b is non-terminal: b.status appears in WHERE
        let select = resolve_select(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status = 'active'",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(
            non_terminal.contains("b"),
            "b should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_non_terminal_chained_join() {
        let tables = terminality_test_tables();
        // b is non-terminal: b.val appears in the downstream INNER JOIN condition
        // c is not on an outer join's optional side
        let select = resolve_select(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.val = c.val",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(
            non_terminal.contains("b"),
            "b should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_chained_outer_joins() {
        let tables = terminality_test_tables();
        // a LEFT JOIN b ... LEFT JOIN c ON b.x = c.x
        // b is non-terminal: appears in the outer LEFT JOIN's condition (ancestor)
        // c is terminal: only appears in its own ON clause
        let select = resolve_select(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.x = c.x",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(
            non_terminal.contains("b"),
            "b should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.contains("c"), "c should be terminal: {terminal:?}");
    }

    #[test]
    fn test_terminal_right_join() {
        let tables = terminality_test_tables();
        // a is terminal optional side (RIGHT JOIN makes left side optional)
        let select = resolve_select(
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(terminal.contains("a"), "a should be terminal: {terminal:?}");
    }

    #[test]
    fn test_non_terminal_right_join() {
        let tables = terminality_test_tables();
        // a is non-terminal optional side: a.status in WHERE
        let select = resolve_select(
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.status = 'active'",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(
            non_terminal.contains("a"),
            "a should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_inner_join_no_optional() {
        let tables = terminality_test_tables();
        // INNER JOIN has no optional side
        let select = resolve_select(
            "SELECT * FROM a JOIN b ON a.id = b.id WHERE b.x = 1",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
    }

    #[test]
    fn test_terminal_with_alias() {
        let tables = terminality_test_tables();
        // Aliased table on optional side, terminal.
        // Resolved AST resolves alias "t" back to real table name "b".
        let select = resolve_select(
            "SELECT a.id, t.name FROM a LEFT JOIN b t ON a.id = t.a_id WHERE a.id = 1",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(
            terminal.contains("b"),
            "aliased b should be terminal by real name: {terminal:?}"
        );
    }

    #[test]
    fn test_non_terminal_with_alias() {
        let tables = terminality_test_tables();
        // Aliased table on optional side, non-terminal (alias used in WHERE).
        // Resolved AST uses real table name "b" (not alias "t").
        let select = resolve_select(
            "SELECT * FROM a LEFT JOIN b t ON a.id = t.a_id WHERE t.status = 'active'",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(
            non_terminal.contains("b"),
            "aliased b should be non-terminal by real name: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_mixed_inner_and_terminal_left() {
        let tables = terminality_test_tables();
        // a JOIN b is inner, LEFT JOIN c is terminal
        let select = resolve_select(
            "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1",
            &tables,
        );
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(terminal.contains("c"), "c should be terminal: {terminal:?}");
    }

    #[test]
    fn test_no_join_no_optional() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table("users", 1, &["id", "name"]));
        let select = resolve_select("SELECT * FROM users WHERE id = 1", &tables);
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(terminal.is_empty());
        assert!(non_terminal.is_empty());
    }

    // ==================== LIMIT Helper Tests ====================

    #[test]
    fn test_limit_rows_needed() {
        use crate::query::ast::{LimitClause, LiteralValue};

        // No limit clause
        assert_eq!(limit_rows_needed(&None), None);

        // LIMIT 10
        assert_eq!(
            limit_rows_needed(&Some(LimitClause {
                count: Some(LiteralValue::Integer(10)),
                offset: None,
            })),
            Some(10)
        );

        // LIMIT 10 OFFSET 5
        assert_eq!(
            limit_rows_needed(&Some(LimitClause {
                count: Some(LiteralValue::Integer(10)),
                offset: Some(LiteralValue::Integer(5)),
            })),
            Some(15)
        );

        // OFFSET only (no count) = unlimited
        assert_eq!(
            limit_rows_needed(&Some(LimitClause {
                count: None,
                offset: Some(LiteralValue::Integer(5)),
            })),
            None
        );
    }

    #[test]
    fn test_limit_is_sufficient() {
        // All rows cached → always sufficient
        assert!(limit_is_sufficient(None, None));
        assert!(limit_is_sufficient(None, Some(100)));

        // Some rows cached, need unlimited → insufficient
        assert!(!limit_is_sufficient(Some(50), None));

        // Some rows cached, need fewer → sufficient
        assert!(limit_is_sufficient(Some(50), Some(30)));
        assert!(limit_is_sufficient(Some(50), Some(50)));

        // Some rows cached, need more → insufficient
        assert!(!limit_is_sufficient(Some(50), Some(51)));
    }

    #[test]
    fn test_limit_offset_fingerprint_match() {

        let base = "SELECT * FROM orders WHERE tenant_id = 1";
        let with_limit = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10";
        let with_offset = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let with_both = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10 OFFSET 5";

        let fp_base = {
            let ast = pg_query::parse(base).unwrap();
            query_expr_fingerprint(&query_expr_convert(&ast).unwrap())
        };
        let fp_limit = {
            let ast = pg_query::parse(with_limit).unwrap();
            query_expr_fingerprint(&query_expr_convert(&ast).unwrap())
        };
        let fp_offset = {
            let ast = pg_query::parse(with_offset).unwrap();
            query_expr_fingerprint(&query_expr_convert(&ast).unwrap())
        };
        let fp_both = {
            let ast = pg_query::parse(with_both).unwrap();
            query_expr_fingerprint(&query_expr_convert(&ast).unwrap())
        };

        assert_eq!(fp_base, fp_limit, "LIMIT should not affect fingerprint");
        assert_eq!(fp_base, fp_offset, "OFFSET should not affect fingerprint");
        assert_eq!(
            fp_base, fp_both,
            "LIMIT+OFFSET should not affect fingerprint"
        );
    }
}

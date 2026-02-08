use crate::{
    cache::QueryParameters,
    query::{
        ast::{
            BinaryOp, CteRefNode, JoinNode, JoinType, MultiOp, QueryBody, QueryExpr, SelectNode,
            SetOpNode, SubLinkType, TableSource, TableSubqueryNode, WhereExpr,
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
        // LIMIT/OFFSET at top level makes queries non-cacheable
        if query.limit.is_some() {
            return Err(CacheabilityError::HasLimit);
        }

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
    // Only INNER joins are cacheable
    if join.join_type != JoinType::Inner {
        return Err(CacheabilityError::UnsupportedFrom);
    }

    // Validate join condition: must be simple equality or absent
    let condition_valid = match &join.condition {
        Some(WhereExpr::Binary(binary_expr)) => binary_expr.op == BinaryOp::Equal,
        Some(
            WhereExpr::Value(_)
            | WhereExpr::Column(_)
            | WhereExpr::Unary(_)
            | WhereExpr::Multi(_)
            | WhereExpr::Function { .. }
            | WhereExpr::Subquery { .. },
        ) => false,
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
/// Note: LIMIT/OFFSET check is done separately in TryFrom
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::query_expr_convert;

    /// Parse SQL and check cacheability
    fn check_cacheable(sql: &str) -> Result<CacheableQuery, CacheabilityError> {
        let ast = pg_query::parse(sql).expect("parse");
        let query_expr = query_expr_convert(&ast).expect("convert");
        CacheableQuery::try_from(&query_expr)
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
    fn test_left_join_not_cacheable() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "LEFT JOIN should not be cacheable"
        );
    }

    #[test]
    fn test_mixed_join_types_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "Mixed join types (INNER + LEFT) should not be cacheable"
        );
    }

    #[test]
    fn test_nested_left_join_not_cacheable() {
        let sql =
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "All LEFT JOINs should not be cacheable"
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
    fn test_limit_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "LIMIT should not be cacheable yet"
        );
    }

    #[test]
    fn test_offset_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "OFFSET should not be cacheable yet"
        );
    }

    #[test]
    fn test_group_by_with_limit_not_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status LIMIT 5";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "GROUP BY with LIMIT should not be cacheable"
        );
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
    fn test_subquery_with_limit_in_outer_not_cacheable() {
        // LIMIT at outer level still makes query not cacheable
        let sql = "SELECT * FROM (SELECT id FROM users) sub LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "Outer LIMIT should make query not cacheable"
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
    fn test_union_with_outer_limit_not_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 \
                   UNION SELECT id FROM b WHERE tenant_id = 1 \
                   LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "UNION with outer LIMIT should not be cacheable"
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
    fn test_union_with_left_join_not_cacheable() {
        let sql = "SELECT a.id FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
                   UNION SELECT id FROM c WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "UNION with LEFT JOIN should not be cacheable"
        );
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
}

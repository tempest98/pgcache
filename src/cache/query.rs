use crate::{
    cache::QueryParameters,
    query::{
        ast::{
            BinaryOp, JoinNode, JoinType, LimitClause, MultiOp, OrderByClause, QueryBody,
            QueryExpr, SelectNode, TableSource, UnaryOp, WhereExpr,
        },
        evaluate::is_simple_comparison,
        transform::{AstTransformResult, select_node_parameters_replace},
    },
};
use error_set::error_set;

error_set! {
    CacheabilityError := {
        NotSelect,
        UnsupportedFrom,
        HasSublink,
        UnsupportedWhereClause,
    }
}

#[derive(Debug, Clone)]
pub struct CacheableQuery {
    pub node: SelectNode,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<LimitClause>,
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
        select_node_parameters_replace(&mut self.node, parameters)?;
        // Note: order_by and limit are not processed here because:
        // - Queries with LIMIT are not cacheable (checked in TryFrom)
        // - ORDER BY expressions typically don't have parameter placeholders
        Ok(())
    }
}

impl TryFrom<&QueryExpr> for CacheableQuery {
    type Error = CacheabilityError;

    fn try_from(query: &QueryExpr) -> Result<Self, Self::Error> {
        let QueryBody::Select(node) = &query.body else {
            return Err(CacheabilityError::NotSelect);
        };

        // Check cacheability
        if !is_supported_from(node) {
            return Err(CacheabilityError::UnsupportedFrom);
        }
        if node.has_sublink() {
            return Err(CacheabilityError::HasSublink);
        }
        // LIMIT makes queries non-cacheable
        if query.limit.is_some() {
            return Err(CacheabilityError::UnsupportedWhereClause);
        }
        if !is_cacheable_where(node) {
            return Err(CacheabilityError::UnsupportedWhereClause);
        }
        Ok(CacheableQuery {
            node: node.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit.clone(),
        })
    }
}

fn is_supported_from(select: &SelectNode) -> bool {
    match select.from.as_slice() {
        [TableSource::Join(join)] => is_supported_join(join),
        [_] => true,
        _ => false,
    }
}

fn is_supported_join(join: &JoinNode) -> bool {
    // Only INNER joins are cacheable
    if join.join_type != JoinType::Inner {
        return false;
    }

    // Validate join condition: must be simple equality or absent
    let condition_valid = match &join.condition {
        Some(WhereExpr::Binary(binary_expr)) => {
            binary_expr.op == BinaryOp::Equal
                && !join.condition.as_ref().is_some_and(|e| e.has_sublink())
        }
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
        return false;
    }

    // Recursively validate nested joins
    let left_valid = match join.left.as_ref() {
        TableSource::Join(nested) => is_supported_join(nested),
        TableSource::Table(_) => true,
        TableSource::Subquery(_) => false,
    };

    let right_valid = match join.right.as_ref() {
        TableSource::Join(nested) => is_supported_join(nested),
        TableSource::Table(_) => true,
        TableSource::Subquery(_) => false,
    };

    left_valid && right_valid
}

/// Check if a SELECT's WHERE clause can be efficiently cached.
///
/// Supports:
/// - Simple equality, AND of equalities, OR of equalities in WHERE
/// - GROUP BY and HAVING (aggregation performed on cached rows at retrieval time)
///
/// Note: LIMIT/OFFSET check is done separately in TryFrom
fn is_cacheable_where(select: &SelectNode) -> bool {
    match &select.where_clause {
        Some(where_expr) => is_cacheable_expr(where_expr),
        None => true, // No WHERE clause is always cacheable
    }
}

/// Determine if a WHERE expression can be efficiently cached.
/// Step 2: Support simple equality, AND of equalities, OR of equalities.
fn is_cacheable_expr(expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Binary(binary_expr) => match binary_expr.op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => {
                // Simple comparison: column op value
                is_simple_comparison(binary_expr)
            }
            BinaryOp::And | BinaryOp::Or => {
                is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
            }
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => false,
        },
        WhereExpr::Value(_) => true,
        WhereExpr::Column(_) => true,
        WhereExpr::Multi(multi_expr) => match multi_expr.op {
            MultiOp::In | MultiOp::NotIn => multi_expr.exprs.iter().all(is_cacheable_expr),
            MultiOp::Between | MultiOp::NotBetween | MultiOp::Any | MultiOp::All => false,
        },
        WhereExpr::Unary(unary_expr) => match unary_expr.op {
            UnaryOp::IsNull | UnaryOp::IsNotNull => is_cacheable_expr(&unary_expr.expr),
            UnaryOp::Not => is_cacheable_expr(&unary_expr.expr),
            UnaryOp::Exists | UnaryOp::NotExists => false,
        },
        WhereExpr::Function { .. } | WhereExpr::Subquery { .. } => false,
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
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "LIMIT should not be cacheable yet"
        );
    }

    #[test]
    fn test_offset_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "OFFSET should not be cacheable yet"
        );
    }

    #[test]
    fn test_group_by_with_limit_not_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status LIMIT 5";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "GROUP BY with LIMIT should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_select_not_cacheable() {
        let sql = "SELECT id, (SELECT x FROM other WHERE id = 1) FROM t WHERE id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasSublink)),
            "Subquery in SELECT list should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_from_not_cacheable() {
        let sql = "SELECT * FROM (SELECT id FROM users) sub WHERE id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasSublink)),
            "Subquery in FROM clause should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_join_not_cacheable() {
        let sql = "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        // Note: Currently returns UnsupportedFrom because is_supported_from() runs first
        // and rejects subqueries in joins. Both errors correctly reject the query.
        assert!(
            matches!(
                result,
                Err(CacheabilityError::HasSublink) | Err(CacheabilityError::UnsupportedFrom)
            ),
            "Subquery in JOIN should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_where_not_cacheable() {
        let sql = "SELECT * FROM t WHERE id IN (SELECT id FROM other)";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasSublink)),
            "Subquery in WHERE clause should not be cacheable"
        );
    }

    #[test]
    fn test_function_in_select_cacheable() {
        let sql = "SELECT COUNT(*), SUM(amount) FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Functions in SELECT should be cacheable");
    }
}

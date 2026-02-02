use crate::{
    cache::QueryParameters,
    query::{
        ast::{
            ExprOp, JoinNode, JoinType, SelectStatement, SqlQuery, Statement, TableSource,
            WhereExpr,
        },
        evaluate::is_simple_comparison,
        transform::{AstTransformResult, ast_parameters_replace},
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
    statement: SelectStatement,
}

impl CacheableQuery {
    pub fn statement(&self) -> &SelectStatement {
        &self.statement
    }

    pub fn into_statement(self) -> SelectStatement {
        self.statement
    }

    /// Replace parameter placeholders ($1, $2, etc.) with actual values.
    /// This mutates the query in place, replacing all parameter nodes with literal values.
    ///
    /// # Arguments
    /// * `parameters` - The parameter values, indexed from 0 (for $1, $2, etc.)
    ///
    /// # Errors
    /// Returns `AstTransformError` if parameter replacement fails (e.g., invalid index, invalid UTF-8)
    pub fn parameters_replace(&mut self, parameters: &QueryParameters) -> AstTransformResult<()> {
        self.statement = ast_parameters_replace(&self.statement, parameters)?;
        Ok(())
    }
}

impl TryFrom<&SqlQuery> for CacheableQuery {
    type Error = CacheabilityError;

    fn try_from(query: &SqlQuery) -> Result<Self, Self::Error> {
        match &query.statement {
            Statement::Select(select) => {
                if !is_supported_from(select) {
                    return Err(CacheabilityError::UnsupportedFrom);
                }
                if select.has_sublink() {
                    return Err(CacheabilityError::HasSublink);
                }
                if !is_cacheable_select(select) {
                    return Err(CacheabilityError::UnsupportedWhereClause);
                }
                Ok(CacheableQuery {
                    statement: select.clone(),
                })
            }
        }
    }
}

fn is_supported_from(select: &SelectStatement) -> bool {
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
            binary_expr.op == ExprOp::Equal
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

/// Check if a SELECT statement can be efficiently cached.
///
/// Supports:
/// - Simple equality, AND of equalities, OR of equalities in WHERE
/// - GROUP BY and HAVING (aggregation performed on cached rows at retrieval time)
///
/// Not yet supported:
/// - LIMIT/OFFSET (subset caching has invalidation challenges)
fn is_cacheable_select(select: &SelectStatement) -> bool {
    if select.limit.is_some() {
        return false;
    }

    match &select.where_clause {
        Some(where_expr) => is_cacheable_expr(where_expr),
        None => true, // No WHERE clause is always cacheable
    }
}

/// Determine if a WHERE expression can be efficiently cached.
/// Step 2: Support simple equality, AND of equalities, OR of equalities.
fn is_cacheable_expr(expr: &WhereExpr) -> bool {
    dbg!(&expr);

    let rv = match expr {
        WhereExpr::Binary(binary_expr) => {
            match binary_expr.op {
                ExprOp::Equal
                | ExprOp::NotEqual
                | ExprOp::LessThan
                | ExprOp::LessThanOrEqual
                | ExprOp::GreaterThan
                | ExprOp::GreaterThanOrEqual => {
                    // Simple comparison: column op value
                    is_simple_comparison(binary_expr)
                }
                ExprOp::And | ExprOp::Or | ExprOp::In | ExprOp::NotIn => {
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                ExprOp::Not
                | ExprOp::Like
                | ExprOp::ILike
                | ExprOp::NotLike
                | ExprOp::NotILike
                | ExprOp::Between
                | ExprOp::NotBetween
                | ExprOp::IsNull
                | ExprOp::IsNotNull
                | ExprOp::Any
                | ExprOp::All
                | ExprOp::Exists
                | ExprOp::NotExists => false,
            }
        }
        WhereExpr::Value(_) => true,
        WhereExpr::Column(_) => true,
        WhereExpr::Multi(multi_expr) => multi_expr.exprs.iter().all(|e| is_cacheable_expr(e)),
        WhereExpr::Unary(_) | WhereExpr::Function { .. } | WhereExpr::Subquery { .. } => false,
    };

    dbg!(rv);
    rv
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::sql_query_convert;

    #[test]
    fn test_two_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(result.is_ok(), "2-table inner join should be cacheable");
    }

    #[test]
    fn test_three_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(result.is_ok(), "3-table inner join should be cacheable");
    }

    #[test]
    fn test_four_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(result.is_ok(), "4-table inner join should be cacheable");
    }

    #[test]
    fn test_left_join_not_cacheable() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "LEFT JOIN should not be cacheable"
        );
    }

    #[test]
    fn test_mixed_join_types_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "Mixed join types (INNER + LEFT) should not be cacheable"
        );
    }

    #[test]
    fn test_nested_left_join_not_cacheable() {
        let sql =
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "All LEFT JOINs should not be cacheable"
        );
    }

    #[test]
    fn test_join_with_non_equality_condition_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id > b.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "Non-equality join condition should not be cacheable"
        );
    }

    #[test]
    fn test_nested_join_with_non_equality_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id > b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom)),
            "Nested join with non-equality condition should not be cacheable"
        );
    }

    #[test]
    fn test_group_by_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(result.is_ok(), "GROUP BY should be cacheable");
    }

    #[test]
    fn test_group_by_multiple_columns_cacheable() {
        let sql =
            "SELECT status, category FROM orders WHERE tenant_id = 1 GROUP BY status, category";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            result.is_ok(),
            "GROUP BY with multiple columns should be cacheable"
        );
    }

    #[test]
    fn test_having_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status HAVING status = 'active'";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(result.is_ok(), "HAVING should be cacheable");
    }

    #[test]
    fn test_limit_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "LIMIT should not be cacheable yet"
        );
    }

    #[test]
    fn test_offset_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "OFFSET should not be cacheable yet"
        );
    }

    #[test]
    fn test_group_by_with_limit_not_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status LIMIT 5";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedWhereClause)),
            "GROUP BY with LIMIT should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_select_not_cacheable() {
        let sql = "SELECT id, (SELECT x FROM other WHERE id = 1) FROM t WHERE id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::HasSublink)),
            "Subquery in SELECT list should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_from_not_cacheable() {
        let sql = "SELECT * FROM (SELECT id FROM users) sub WHERE id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::HasSublink)),
            "Subquery in FROM clause should not be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_join_not_cacheable() {
        let sql = "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
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
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(
            matches!(result, Err(CacheabilityError::HasSublink)),
            "Subquery in WHERE clause should not be cacheable"
        );
    }

    #[test]
    fn test_function_in_select_cacheable() {
        let sql = "SELECT COUNT(*), SUM(amount) FROM orders WHERE tenant_id = 1";
        let ast = pg_query::parse(sql).expect("parse");
        let sql_query = sql_query_convert(&ast).expect("convert");

        let result = CacheableQuery::try_from(&sql_query);
        assert!(result.is_ok(), "Functions in SELECT should be cacheable");
    }
}

use crate::query::{
    ast::{
        ExprOp, JoinNode, JoinType, SelectStatement, SqlQuery, Statement, TableSource, WhereExpr,
    },
    evaluate::is_simple_comparison,
    transform::{AstTransformError, ast_parameters_replace},
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
    /// Returns `CacheabilityError` if parameter replacement fails (e.g., invalid index, invalid UTF-8)
    pub fn parameters_replace(
        &mut self,
        parameters: &[Option<Vec<u8>>],
    ) -> Result<(), AstTransformError> {
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
    if select.from.len() == 1 {
        if let TableSource::Join(join) = &select.from[0] {
            is_supported_join(join)
        } else {
            true
        }
    } else {
        false
    }
}

fn is_supported_join(join: &JoinNode) -> bool {
    if join.join_type != JoinType::Inner {
        return false;
    }
    match &join.condition {
        Some(where_expr) => match where_expr {
            WhereExpr::Binary(binary_expr) => {
                binary_expr.op == ExprOp::Equal && !where_expr.has_sublink()
            }
            _ => false,
        },
        None => true,
    }
}

/// Check if a SELECT statement can be efficiently cached.
/// Currently supports: simple equality, AND of equalities, OR of equalities.
fn is_cacheable_select(select: &SelectStatement) -> bool {
    match &select.where_clause {
        Some(where_expr) => is_cacheable_expr(where_expr),
        None => true, // No WHERE clause is always cacheable
    }
}

/// Determine if a WHERE expression can be efficiently cached.
/// Step 2: Support simple equality, AND of equalities, OR of equalities.
fn is_cacheable_expr(expr: &WhereExpr) -> bool {
    match expr {
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
                ExprOp::And => {
                    // AND: both sides must be cacheable
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                ExprOp::Or => {
                    // OR: both sides must be cacheable
                    is_cacheable_expr(&binary_expr.lexpr) && is_cacheable_expr(&binary_expr.rexpr)
                }
                _ => false, // Other operators not supported yet
            }
        }
        _ => false, // Other expression types not supported yet
    }
}

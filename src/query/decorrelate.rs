use std::collections::HashSet;

use error_set::error_set;
use rootcause::Report;

use crate::query::ast::{BinaryOp, JoinType, SubLinkType, UnaryOp};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedSelectNode, ResolvedSetOpNode, ResolvedTableSource,
    ResolvedUnaryExpr, ResolvedWhereExpr,
};
use crate::query::transform::{where_expr_conjuncts_join, where_expr_conjuncts_split};

error_set! {
    DecorrelateError := {
        #[display("Non-decorrelatable correlated subquery: {reason}")]
        NonDecorrelatable { reason: String },
    }
}

pub type DecorrelateResult<T> = Result<T, Report<DecorrelateError>>;

/// Outcome of decorrelation: the (possibly transformed) resolved query.
pub struct DecorrelateOutcome {
    pub resolved: ResolvedQueryExpr,
    pub transformed: bool,
}

/// A correlation predicate extracted from an inner subquery WHERE.
struct CorrelationPredicate {
    /// Column from the outer query scope
    outer_column: ResolvedColumnNode,
    /// Column from the inner subquery scope
    inner_column: ResolvedColumnNode,
}

/// Build a lookup set of `(schema, table, column)` tuples from outer_refs.
fn outer_ref_keys(outer_refs: &[ResolvedColumnNode]) -> HashSet<(&str, &str, &str)> {
    outer_refs
        .iter()
        .map(|col| (col.schema.as_str(), col.table.as_str(), col.column.as_str()))
        .collect()
}

/// Check if a column matches any entry in the outer_refs key set.
fn column_matches_outer_ref(
    col: &ResolvedColumnNode,
    outer_keys: &HashSet<(&str, &str, &str)>,
) -> bool {
    outer_keys.contains(&(col.schema.as_str(), col.table.as_str(), col.column.as_str()))
}

/// Partition an inner subquery's WHERE clause into correlation predicates and residual predicates.
///
/// Correlation predicates are equalities where one side matches an outer_ref and the other
/// references an inner table. Returns `None` if no correlation predicates are found, or if
/// an unsupported correlation pattern is detected (e.g., non-equality with an outer ref).
fn where_clause_correlation_partition(
    where_clause: &ResolvedWhereExpr,
    outer_refs: &[ResolvedColumnNode],
) -> Option<(Vec<CorrelationPredicate>, Option<ResolvedWhereExpr>)> {
    let outer_keys = outer_ref_keys(outer_refs);
    let conjuncts = where_expr_conjuncts_split(where_clause.clone());

    let mut correlation_predicates = Vec::new();
    let mut residual = Vec::new();

    for conjunct in conjuncts {
        match &conjunct {
            ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::Equal => {
                // Check if this is a Column = Column with one side being an outer ref
                match (binary.lexpr.as_ref(), binary.rexpr.as_ref()) {
                    (ResolvedWhereExpr::Column(left), ResolvedWhereExpr::Column(right)) => {
                        let left_is_outer = column_matches_outer_ref(left, &outer_keys);
                        let right_is_outer = column_matches_outer_ref(right, &outer_keys);

                        if left_is_outer && !right_is_outer {
                            correlation_predicates.push(CorrelationPredicate {
                                outer_column: left.clone(),
                                inner_column: right.clone(),
                            });
                        } else if right_is_outer && !left_is_outer {
                            correlation_predicates.push(CorrelationPredicate {
                                outer_column: right.clone(),
                                inner_column: left.clone(),
                            });
                        } else {
                            // Both outer or both inner — treat as residual
                            residual.push(conjunct);
                        }
                    }
                    _ => {
                        // Non-column equality — check if it references outer refs
                        if conjunct_references_outer_ref(&conjunct, &outer_keys) {
                            // Non-column-to-column correlation — unsupported
                            return None;
                        }
                        residual.push(conjunct);
                    }
                }
            }
            ResolvedWhereExpr::Value(_)
            | ResolvedWhereExpr::Column(_)
            | ResolvedWhereExpr::Unary(_)
            | ResolvedWhereExpr::Binary(_)
            | ResolvedWhereExpr::Multi(_)
            | ResolvedWhereExpr::Array(_)
            | ResolvedWhereExpr::Function { .. }
            | ResolvedWhereExpr::Subquery { .. } => {
                // Non-equality predicate — check if it references outer refs
                if conjunct_references_outer_ref(&conjunct, &outer_keys) {
                    // Non-equality correlation — unsupported
                    return None;
                }
                residual.push(conjunct);
            }
        }
    }

    if correlation_predicates.is_empty() {
        return None;
    }

    let residual_where = where_expr_conjuncts_join(residual);
    Some((correlation_predicates, residual_where))
}

/// Check whether a WHERE expression references any column matching the outer_ref keys.
fn conjunct_references_outer_ref(
    expr: &ResolvedWhereExpr,
    outer_keys: &HashSet<(&str, &str, &str)>,
) -> bool {
    match expr {
        ResolvedWhereExpr::Column(col) => column_matches_outer_ref(col, outer_keys),
        ResolvedWhereExpr::Value(_) => false,
        ResolvedWhereExpr::Unary(u) => conjunct_references_outer_ref(&u.expr, outer_keys),
        ResolvedWhereExpr::Binary(b) => {
            conjunct_references_outer_ref(&b.lexpr, outer_keys)
                || conjunct_references_outer_ref(&b.rexpr, outer_keys)
        }
        ResolvedWhereExpr::Multi(m) => m
            .exprs
            .iter()
            .any(|e| conjunct_references_outer_ref(e, outer_keys)),
        ResolvedWhereExpr::Array(elems) => elems
            .iter()
            .any(|e| conjunct_references_outer_ref(e, outer_keys)),
        ResolvedWhereExpr::Function { args, .. } => args
            .iter()
            .any(|a| conjunct_references_outer_ref(a, outer_keys)),
        ResolvedWhereExpr::Subquery { .. } => false,
    }
}

/// Build a JOIN ON condition from correlation predicates.
fn correlation_predicates_to_condition(predicates: &[CorrelationPredicate]) -> ResolvedWhereExpr {
    let conditions: Vec<ResolvedWhereExpr> = predicates
        .iter()
        .map(|p| {
            ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(ResolvedWhereExpr::Column(p.inner_column.clone())),
                rexpr: Box::new(ResolvedWhereExpr::Column(p.outer_column.clone())),
            })
        })
        .collect();

    // Always non-empty since we checked for empty above
    where_expr_conjuncts_join(conditions).expect("correlation predicates non-empty")
}

/// Extract the inner SELECT node and its FROM sources from a subquery's ResolvedQueryExpr.
/// Returns None if the inner query isn't a simple SELECT.
fn inner_query_select(query: &ResolvedQueryExpr) -> Option<&ResolvedSelectNode> {
    match &query.body {
        ResolvedQueryBody::Select(select) => Some(select),
        ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
    }
}

/// EXISTS → INNER JOIN + DISTINCT (semi-join).
///
/// Merges inner FROM sources into a JOIN with the outer FROM, using correlation
/// predicates as the ON condition. Sets DISTINCT on the result to preserve
/// semi-join semantics. Residual inner predicates are merged into the outer WHERE.
fn subquery_exists_decorrelate(
    select: &ResolvedSelectNode,
    inner_query: &ResolvedQueryExpr,
    predicates: &[CorrelationPredicate],
    residual: Option<ResolvedWhereExpr>,
) -> Option<ResolvedSelectNode> {
    let inner_select = inner_query_select(inner_query)?;

    let join_condition = correlation_predicates_to_condition(predicates);

    // Build the right side of the JOIN from the inner query's FROM sources
    let right = from_sources_to_table_source(&inner_select.from);

    // Build the left side from the outer query's FROM sources
    let left = from_sources_to_table_source(&select.from);

    let join = ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
        join_type: JoinType::Inner,
        left,
        right,
        condition: Some(join_condition),
    }));

    // Merge residual inner predicates into outer WHERE
    let new_where = merge_where_clauses(&select.where_clause, &residual);

    Some(ResolvedSelectNode {
        distinct: true,
        columns: select.columns.clone(),
        from: vec![join],
        where_clause: new_where,
        group_by: select.group_by.clone(),
        having: select.having.clone(),
    })
}

/// NOT EXISTS → LEFT JOIN + IS NULL (anti-join).
///
/// Merges inner FROM sources into a LEFT JOIN with the outer FROM. Correlation
/// predicates AND residual inner predicates both go into the ON clause (not the
/// outer WHERE) to preserve LEFT JOIN semantics. An IS NULL check on one of the
/// inner correlation columns is added to the outer WHERE.
fn subquery_not_exists_decorrelate(
    select: &ResolvedSelectNode,
    inner_query: &ResolvedQueryExpr,
    predicates: &[CorrelationPredicate],
    residual: Option<ResolvedWhereExpr>,
) -> Option<ResolvedSelectNode> {
    let inner_select = inner_query_select(inner_query)?;

    // For NOT EXISTS, residual predicates go into the ON clause
    let correlation_condition = correlation_predicates_to_condition(predicates);
    let on_condition = match residual {
        Some(res) => ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(correlation_condition),
            rexpr: Box::new(res),
        }),
        None => correlation_condition,
    };

    let right = from_sources_to_table_source(&inner_select.from);
    let left = from_sources_to_table_source(&select.from);

    let join = ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
        join_type: JoinType::Left,
        left,
        right,
        condition: Some(on_condition),
    }));

    // Add IS NULL check on the first inner correlation column.
    // predicates is guaranteed non-empty by where_clause_correlation_partition.
    let first_predicate = predicates
        .first()
        .expect("correlation predicates non-empty");
    let is_null_check = ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
        op: UnaryOp::IsNull,
        expr: Box::new(ResolvedWhereExpr::Column(
            first_predicate.inner_column.clone(),
        )),
    });

    // Merge IS NULL with existing outer WHERE
    let new_where = match &select.where_clause {
        Some(existing) => Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(existing.clone()),
            rexpr: Box::new(is_null_check),
        })),
        None => Some(is_null_check),
    };

    Some(ResolvedSelectNode {
        distinct: select.distinct,
        columns: select.columns.clone(),
        from: vec![join],
        where_clause: new_where,
        group_by: select.group_by.clone(),
        having: select.having.clone(),
    })
}

/// Combine a list of FROM sources into a single ResolvedTableSource.
///
/// If there's exactly one source, returns it directly. Multiple sources are
/// combined into a chain of cross joins (no condition).
///
/// # Panics
/// Panics if `sources` is empty — callers must ensure the inner query has FROM.
fn from_sources_to_table_source(sources: &[ResolvedTableSource]) -> ResolvedTableSource {
    let mut iter = sources.iter().cloned();
    let first = iter.next().expect("inner subquery has FROM sources");
    iter.fold(first, |acc, next| {
        ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
            join_type: JoinType::Inner,
            left: acc,
            right: next,
            condition: None,
        }))
    })
}

/// Merge two optional WHERE clauses with AND.
fn merge_where_clauses(
    outer: &Option<ResolvedWhereExpr>,
    inner: &Option<ResolvedWhereExpr>,
) -> Option<ResolvedWhereExpr> {
    match (outer, inner) {
        (Some(o), Some(i)) => Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(o.clone()),
            rexpr: Box::new(i.clone()),
        })),
        (Some(o), None) => Some(o.clone()),
        (None, Some(i)) => Some(i.clone()),
        (None, None) => None,
    }
}

/// Check whether a ResolvedColumnExpr contains any correlated subquery (non-empty outer_refs).
fn column_expr_has_correlation(expr: &ResolvedColumnExpr) -> bool {
    match expr {
        ResolvedColumnExpr::Subquery(_, outer_refs) => !outer_refs.is_empty(),
        ResolvedColumnExpr::Function { args, .. } => args.iter().any(column_expr_has_correlation),
        ResolvedColumnExpr::Case(case) => {
            case.arg
                .as_ref()
                .is_some_and(|a| column_expr_has_correlation(a))
                || case.whens.iter().any(|w| {
                    where_expr_has_correlation(&w.condition)
                        || column_expr_has_correlation(&w.result)
                })
                || case
                    .default
                    .as_ref()
                    .is_some_and(|d| column_expr_has_correlation(d))
        }
        ResolvedColumnExpr::Arithmetic(arith) => {
            column_expr_has_correlation(&arith.left) || column_expr_has_correlation(&arith.right)
        }
        ResolvedColumnExpr::Column(_)
        | ResolvedColumnExpr::Identifier(_)
        | ResolvedColumnExpr::Literal(_) => false,
    }
}

/// Check whether a ResolvedWhereExpr contains any correlated subquery.
fn where_expr_has_correlation(expr: &ResolvedWhereExpr) -> bool {
    match expr {
        ResolvedWhereExpr::Subquery { outer_refs, .. } => !outer_refs.is_empty(),
        ResolvedWhereExpr::Unary(u) => where_expr_has_correlation(&u.expr),
        ResolvedWhereExpr::Binary(b) => {
            where_expr_has_correlation(&b.lexpr) || where_expr_has_correlation(&b.rexpr)
        }
        ResolvedWhereExpr::Multi(m) => m.exprs.iter().any(where_expr_has_correlation),
        ResolvedWhereExpr::Array(elems) => elems.iter().any(where_expr_has_correlation),
        ResolvedWhereExpr::Function { args, .. } => args.iter().any(where_expr_has_correlation),
        ResolvedWhereExpr::Value(_) | ResolvedWhereExpr::Column(_) => false,
    }
}

/// Prepare a correlated EXISTS inner query for decorrelation by stripping
/// clauses that are safe to remove for update query (CDC invalidation) purposes.
///
/// EXISTS is a boolean "does any row match?" check. For invalidation we only need
/// to know which outer rows *could* be affected — so stripping GROUP BY, HAVING,
/// and LIMIT produces a conservative (over-invalidation) result that is always safe.
///
/// Returns the cleaned inner query and select node.
fn correlated_exists_inner_prepare(
    inner_query: &ResolvedQueryExpr,
    inner_select: &ResolvedSelectNode,
) -> (ResolvedQueryExpr, ResolvedSelectNode) {
    let cleaned_select = ResolvedSelectNode {
        group_by: Vec::new(),
        having: None,
        ..inner_select.clone()
    };
    let cleaned_query = ResolvedQueryExpr {
        limit: None,
        body: ResolvedQueryBody::Select(Box::new(cleaned_select.clone())),
        order_by: inner_query.order_by.clone(),
    };
    (cleaned_query, cleaned_select)
}

/// Prepare a correlated NOT EXISTS inner query for decorrelation.
///
/// LIMIT is safe to strip (NOT EXISTS is a boolean check, LIMIT doesn't change the answer).
/// GROUP BY and HAVING are NOT safe to strip for NOT EXISTS — the anti-join pattern
/// (LEFT JOIN + IS NULL) would under-invalidate because it tests for zero matching rows,
/// while the original NOT EXISTS tests for no rows surviving the GROUP BY/HAVING filter.
fn correlated_not_exists_inner_prepare(
    inner_query: &ResolvedQueryExpr,
    inner_select: &ResolvedSelectNode,
) -> DecorrelateResult<(ResolvedQueryExpr, ResolvedSelectNode)> {
    if !inner_select.group_by.is_empty() {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated NOT EXISTS with GROUP BY".to_owned(),
        }
        .into());
    }
    if inner_select.having.is_some() {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated NOT EXISTS with HAVING".to_owned(),
        }
        .into());
    }
    let cleaned_query = ResolvedQueryExpr {
        limit: None,
        body: inner_query.body.clone(),
        order_by: inner_query.order_by.clone(),
    };
    Ok((cleaned_query, inner_select.clone()))
}

/// Main entry point: decorrelate correlated subqueries in a single SELECT node.
///
/// Walks WHERE conjuncts looking for correlated EXISTS/NOT EXISTS subqueries,
/// and flattens them into JOINs. Non-correlated subqueries and non-subquery
/// predicates are left unchanged.
///
/// For EXISTS, inner GROUP BY/HAVING/LIMIT are stripped before decorrelation —
/// this is safe because it produces conservative (over-) invalidation.
/// For NOT EXISTS, LIMIT is stripped (boolean check, irrelevant), but GROUP BY/HAVING
/// are rejected because the anti-join would under-invalidate.
///
/// Returns `Err(NonDecorrelatable)` if a correlated subquery is found in an
/// unsupported position (SELECT list, HAVING, OR-connected, non-EXISTS type),
/// or if a NOT EXISTS inner subquery has GROUP BY/HAVING.
fn select_node_decorrelate(
    select: &ResolvedSelectNode,
) -> DecorrelateResult<(ResolvedSelectNode, bool)> {
    // Reject correlated subqueries in SELECT columns
    if let ResolvedSelectColumns::Columns(cols) = &select.columns {
        for col in cols {
            if column_expr_has_correlation(&col.expr) {
                return Err(DecorrelateError::NonDecorrelatable {
                    reason: "correlated subquery in SELECT list".to_owned(),
                }
                .into());
            }
        }
    }

    // Reject correlated subqueries in HAVING
    if let Some(having) = &select.having
        && where_expr_has_correlation(having)
    {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated subquery in HAVING clause".to_owned(),
        }
        .into());
    }

    let Some(where_clause) = &select.where_clause else {
        return Ok((select.clone(), false));
    };

    let conjuncts = where_expr_conjuncts_split(where_clause.clone());

    let mut current_select = select.clone();
    let mut transformed = false;

    // Rebuild the WHERE clause without the correlated subquery conjuncts,
    // processing one at a time since each decorrelation modifies the FROM.
    let mut remaining_conjuncts = Vec::new();

    for conjunct in conjuncts {
        match &conjunct {
            // EXISTS (SELECT ... WHERE correlated)
            ResolvedWhereExpr::Subquery {
                sublink_type: SubLinkType::Exists,
                outer_refs,
                query,
                ..
            } if !outer_refs.is_empty() => {
                let inner_select = match inner_query_select(query) {
                    Some(s) => s,
                    None => {
                        remaining_conjuncts.push(conjunct);
                        continue;
                    }
                };

                // Strip GROUP BY/HAVING/LIMIT — safe for EXISTS (conservative invalidation)
                let (cleaned_query, cleaned_select) =
                    correlated_exists_inner_prepare(query, inner_select);

                let Some(inner_where) = &cleaned_select.where_clause else {
                    remaining_conjuncts.push(conjunct);
                    continue;
                };

                match where_clause_correlation_partition(inner_where, outer_refs) {
                    Some((predicates, residual)) => {
                        // Set up a temporary select with the current state
                        current_select.where_clause =
                            where_expr_conjuncts_join(remaining_conjuncts.clone());

                        match subquery_exists_decorrelate(
                            &current_select,
                            &cleaned_query,
                            &predicates,
                            residual,
                        ) {
                            Some(new_select) => {
                                current_select = new_select;
                                remaining_conjuncts.clear();
                                // Collect the new WHERE conjuncts so further iterations can
                                // build on top
                                if let Some(w) = &current_select.where_clause {
                                    remaining_conjuncts = where_expr_conjuncts_split(w.clone());
                                }
                                current_select.where_clause = None;
                                transformed = true;
                            }
                            None => {
                                remaining_conjuncts.push(conjunct);
                            }
                        }
                    }
                    None => {
                        return Err(DecorrelateError::NonDecorrelatable {
                            reason: "unsupported correlation pattern in EXISTS".to_owned(),
                        }
                        .into());
                    }
                }
            }

            // NOT EXISTS (SELECT ... WHERE correlated)
            ResolvedWhereExpr::Unary(unary)
                if unary.op == UnaryOp::Not
                    && matches!(
                        unary.expr.as_ref(),
                        ResolvedWhereExpr::Subquery {
                            sublink_type: SubLinkType::Exists,
                            outer_refs,
                            ..
                        } if !outer_refs.is_empty()
                    ) =>
            {
                let ResolvedWhereExpr::Subquery {
                    outer_refs, query, ..
                } = unary.expr.as_ref()
                else {
                    unreachable!()
                };

                let inner_select = match inner_query_select(query) {
                    Some(s) => s,
                    None => {
                        remaining_conjuncts.push(conjunct);
                        continue;
                    }
                };

                // Strip LIMIT (safe); reject GROUP BY/HAVING (unsafe for anti-join)
                let (cleaned_query, cleaned_select) =
                    correlated_not_exists_inner_prepare(query, inner_select)?;

                let Some(inner_where) = &cleaned_select.where_clause else {
                    remaining_conjuncts.push(conjunct);
                    continue;
                };

                match where_clause_correlation_partition(inner_where, outer_refs) {
                    Some((predicates, residual)) => {
                        current_select.where_clause =
                            where_expr_conjuncts_join(remaining_conjuncts.clone());

                        match subquery_not_exists_decorrelate(
                            &current_select,
                            &cleaned_query,
                            &predicates,
                            residual,
                        ) {
                            Some(new_select) => {
                                current_select = new_select;
                                remaining_conjuncts.clear();
                                if let Some(w) = &current_select.where_clause {
                                    remaining_conjuncts = where_expr_conjuncts_split(w.clone());
                                }
                                current_select.where_clause = None;
                                transformed = true;
                            }
                            None => {
                                remaining_conjuncts.push(conjunct);
                            }
                        }
                    }
                    None => {
                        return Err(DecorrelateError::NonDecorrelatable {
                            reason: "unsupported correlation pattern in NOT EXISTS".to_owned(),
                        }
                        .into());
                    }
                }
            }

            // Correlated subquery inside OR — reject
            ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::Or => {
                if where_expr_has_correlation(&conjunct) {
                    return Err(DecorrelateError::NonDecorrelatable {
                        reason: "correlated subquery inside OR".to_owned(),
                    }
                    .into());
                }
                remaining_conjuncts.push(conjunct);
            }

            // Any other correlated subquery type (IN, scalar, ALL) — reject
            ResolvedWhereExpr::Subquery { outer_refs, .. } if !outer_refs.is_empty() => {
                return Err(DecorrelateError::NonDecorrelatable {
                    reason: "correlated non-EXISTS subquery (IN/ALL/scalar)".to_owned(),
                }
                .into());
            }

            // NOT wrapping a correlated non-EXISTS subquery
            ResolvedWhereExpr::Unary(unary) if unary.op == UnaryOp::Not => {
                if matches!(
                    unary.expr.as_ref(),
                    ResolvedWhereExpr::Subquery { outer_refs, .. } if !outer_refs.is_empty()
                ) {
                    return Err(DecorrelateError::NonDecorrelatable {
                        reason: "correlated NOT-wrapped non-EXISTS subquery".to_owned(),
                    }
                    .into());
                }
                remaining_conjuncts.push(conjunct);
            }

            // Non-correlated or non-subquery — keep as-is
            ResolvedWhereExpr::Value(_)
            | ResolvedWhereExpr::Column(_)
            | ResolvedWhereExpr::Unary(_)
            | ResolvedWhereExpr::Binary(_)
            | ResolvedWhereExpr::Multi(_)
            | ResolvedWhereExpr::Array(_)
            | ResolvedWhereExpr::Function { .. }
            | ResolvedWhereExpr::Subquery { .. } => {
                remaining_conjuncts.push(conjunct);
            }
        }
    }

    // Rebuild WHERE from remaining conjuncts
    current_select.where_clause = where_expr_conjuncts_join(remaining_conjuncts);

    Ok((current_select, transformed))
}

use crate::query::resolved::ResolvedSelectColumns;

/// Top-level entry: decorrelate correlated subqueries in a resolved query expression.
///
/// Handles SELECT bodies directly, and recursively processes SetOp branches.
/// Returns `DecorrelateOutcome` with the (possibly transformed) query and a
/// flag indicating whether any transformation occurred.
pub fn query_expr_decorrelate(
    resolved: &ResolvedQueryExpr,
) -> DecorrelateResult<DecorrelateOutcome> {
    match &resolved.body {
        ResolvedQueryBody::Select(select) => {
            let (new_select, transformed) = select_node_decorrelate(select)?;
            Ok(DecorrelateOutcome {
                resolved: ResolvedQueryExpr {
                    body: ResolvedQueryBody::Select(Box::new(new_select)),
                    order_by: resolved.order_by.clone(),
                    limit: resolved.limit.clone(),
                },
                transformed,
            })
        }
        ResolvedQueryBody::SetOp(set_op) => {
            let left_outcome = query_expr_decorrelate(&set_op.left)?;
            let right_outcome = query_expr_decorrelate(&set_op.right)?;
            let transformed = left_outcome.transformed || right_outcome.transformed;
            Ok(DecorrelateOutcome {
                resolved: ResolvedQueryExpr {
                    body: ResolvedQueryBody::SetOp(ResolvedSetOpNode {
                        op: set_op.op,
                        all: set_op.all,
                        left: Box::new(left_outcome.resolved),
                        right: Box::new(right_outcome.resolved),
                    }),
                    order_by: resolved.order_by.clone(),
                    limit: resolved.limit.clone(),
                },
                transformed,
            })
        }
        ResolvedQueryBody::Values(_) => Ok(DecorrelateOutcome {
            resolved: resolved.clone(),
            transformed: false,
        }),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::wildcard_enum_match_arm)]
    #![allow(clippy::unwrap_used)]

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::catalog::{ColumnMetadata, TableMetadata};
    use crate::query::ast::{Deparse, query_expr_convert};
    use crate::query::resolved::query_expr_resolve;

    use super::*;

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: u32, column_names: &[&str]) -> TableMetadata {
        let mut columns = BiHashMap::new();
        for (i, col_name) in column_names.iter().enumerate() {
            let is_pk = i == 0;
            columns.insert_overwrite(ColumnMetadata {
                name: (*col_name).into(),
                position: (i + 1) as i16,
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
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

    fn test_tables() -> BiHashMap<TableMetadata> {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table(
            "employees",
            1,
            &["id", "name", "dept_id", "status", "manager_id"],
        ));
        tables.insert_overwrite(test_table(
            "orders",
            2,
            &["id", "emp_id", "status", "total", "customer_id"],
        ));
        tables.insert_overwrite(test_table(
            "departments",
            3,
            &["id", "name", "location", "budget"],
        ));
        tables.insert_overwrite(test_table(
            "customers",
            4,
            &["id", "name", "region", "emp_id"],
        ));
        tables.insert_overwrite(test_table("users", 5, &["id", "name", "email", "status"]));
        tables.insert_overwrite(test_table("active_users", 6, &["id", "user_id"]));
        tables.insert_overwrite(test_table(
            "projects",
            7,
            &["id", "name", "dept_id", "status"],
        ));
        tables
    }

    /// Parse SQL → resolve → decorrelate → return outcome.
    fn resolve_and_decorrelate(
        sql: &str,
        tables: &BiHashMap<TableMetadata>,
    ) -> DecorrelateResult<DecorrelateOutcome> {
        let parsed = pg_query::parse(sql).expect("parse SQL");
        let ast = query_expr_convert(&parsed).expect("convert to AST");
        let resolved = query_expr_resolve(&ast, tables, &["public"]).expect("resolve query");
        query_expr_decorrelate(&resolved)
    }

    fn deparse(query: &ResolvedQueryExpr) -> String {
        let mut buf = String::new();
        query.deparse(&mut buf);
        buf
    }

    fn as_select(query: &ResolvedQueryExpr) -> &ResolvedSelectNode {
        match &query.body {
            ResolvedQueryBody::Select(s) => s,
            _ => panic!("expected Select"),
        }
    }

    // ==================== No-op Cases ====================

    #[test]
    fn test_no_subqueries_unchanged() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT id, name FROM employees WHERE status = 'active'",
            &tables,
        )
        .unwrap();

        assert!(!outcome.transformed);
    }

    #[test]
    fn test_non_correlated_exists_unchanged() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT id FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE status = 'active')",
            &tables,
        )
        .unwrap();

        assert!(!outcome.transformed);
        // Subquery should still be present
        let sql = deparse(&outcome.resolved);
        assert!(sql.contains("EXISTS"), "should still have EXISTS: {sql}");
    }

    // ==================== EXISTS Decorrelation ====================

    #[test]
    fn test_exists_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, e.name FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        // Should have JOIN instead of EXISTS
        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        assert!(sql.contains("JOIN"), "should have JOIN: {sql}");
        assert!(sql.contains("DISTINCT"), "should have DISTINCT: {sql}");

        // JOIN condition should reference the correlation columns (using aliases)
        assert!(
            sql.contains("o.emp_id = e.id"),
            "should have correlation in ON: {sql}"
        );
    }

    #[test]
    fn test_exists_multiple_correlation_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = e.status)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        assert!(sql.contains("JOIN"), "should have JOIN: {sql}");
        // Both correlation predicates in ON clause (using aliases)
        assert!(
            sql.contains("o.emp_id = e.id"),
            "first correlation: {sql}"
        );
        assert!(
            sql.contains("o.status = e.status"),
            "second correlation: {sql}"
        );
    }

    #[test]
    fn test_exists_with_residual_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = 'active')",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        assert!(sql.contains("JOIN"), "should have JOIN: {sql}");
        // Residual predicate should be in outer WHERE (uses alias)
        assert!(
            sql.contains("o.status = 'active'"),
            "residual should be in WHERE: {sql}"
        );
    }

    #[test]
    fn test_exists_outer_query_already_has_joins() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             JOIN departments d ON d.id = e.dept_id \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        // Should still have the original JOIN plus the new one
        assert!(
            sql.contains("public.departments"),
            "original join table: {sql}"
        );
        assert!(
            sql.contains("public.orders"),
            "decorrelated join table: {sql}"
        );
    }

    #[test]
    fn test_exists_inner_query_has_joins() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 JOIN customers c ON c.id = o.customer_id \
                 WHERE o.emp_id = e.id \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        // Inner join should be preserved
        assert!(
            sql.contains("public.customers"),
            "inner join table preserved: {sql}"
        );
        assert!(
            sql.contains("public.orders"),
            "inner table preserved: {sql}"
        );
    }

    #[test]
    fn test_multiple_exists_in_where() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
             AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        // Both subquery tables should be joined in
        assert!(
            sql.contains("public.orders"),
            "first subquery table: {sql}"
        );
        assert!(
            sql.contains("public.projects"),
            "second subquery table: {sql}"
        );
    }

    #[test]
    fn test_exists_sets_distinct() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        let select = as_select(&outcome.resolved);
        assert!(select.distinct, "EXISTS decorrelation should set DISTINCT");
    }

    // ==================== NOT EXISTS Decorrelation ====================

    #[test]
    fn test_not_exists_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id, d.name FROM departments d \
             WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(
            !sql.contains("NOT EXISTS"),
            "should not have NOT EXISTS: {sql}"
        );
        assert!(sql.contains("LEFT JOIN"), "should have LEFT JOIN: {sql}");
        assert!(sql.contains("IS NULL"), "should have IS NULL check: {sql}");
    }

    #[test]
    fn test_not_exists_residual_in_on_clause() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.status = 'active' \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        assert!(sql.contains("LEFT JOIN"), "should have LEFT JOIN: {sql}");
        // Residual should be in ON clause, not outer WHERE
        // The ON clause should contain both correlation and residual
        assert!(
            sql.contains("e.dept_id = d.id"),
            "correlation in ON: {sql}"
        );
        assert!(
            sql.contains("e.status = 'active'"),
            "residual in ON: {sql}"
        );
        assert!(sql.contains("IS NULL"), "IS NULL check present: {sql}");
    }

    #[test]
    fn test_not_exists_adds_is_null_check() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
            &tables,
        )
        .unwrap();

        let sql = deparse(&outcome.resolved);

        // WHERE should contain IS NULL on the inner correlation column
        assert!(
            sql.contains("IS NULL"),
            "should have IS NULL in WHERE: {sql}"
        );
    }

    // ==================== Rejection Cases ====================

    #[test]
    fn test_correlated_non_equality_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id > e.id)",
            &tables,
        );

        assert!(result.is_err(), "non-equality correlation should be rejected");
    }

    #[test]
    fn test_correlated_in_subquery_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status)",
            &tables,
        );

        assert!(result.is_err(), "correlated IN should be rejected");
    }

    #[test]
    fn test_correlated_scalar_subquery_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
             FROM employees e",
            &tables,
        );

        assert!(
            result.is_err(),
            "correlated subquery in SELECT list should be rejected"
        );
    }

    #[test]
    fn test_exists_inside_or_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.status = 'active' \
                OR EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        );

        assert!(result.is_err(), "EXISTS inside OR should be rejected");
    }

    // ==================== EXISTS: Inner Clause Stripping ====================

    #[test]
    fn test_exists_with_inner_group_by_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 WHERE o.emp_id = e.id \
                 GROUP BY o.status \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "EXISTS with GROUP BY should be decorrelated"
        );
        let sql = deparse(&outcome.resolved);
        assert!(sql.contains("JOIN"), "should have JOIN: {sql}");
        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        assert!(
            !sql.contains("GROUP BY"),
            "GROUP BY should be stripped: {sql}"
        );
    }

    #[test]
    fn test_exists_with_inner_having_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 WHERE o.emp_id = e.id \
                 GROUP BY o.status \
                 HAVING count(*) > 5 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "EXISTS with HAVING should be decorrelated"
        );
        let sql = deparse(&outcome.resolved);
        assert!(sql.contains("JOIN"), "should have JOIN: {sql}");
        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
        assert!(
            !sql.contains("HAVING"),
            "HAVING should be stripped: {sql}"
        );
    }

    #[test]
    fn test_exists_with_inner_limit_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 WHERE o.emp_id = e.id \
                 LIMIT 1 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "EXISTS with LIMIT should be decorrelated"
        );
        let sql = deparse(&outcome.resolved);
        assert!(sql.contains("JOIN"), "should have JOIN: {sql}");
        assert!(!sql.contains("EXISTS"), "should not have EXISTS: {sql}");
    }

    // ==================== NOT EXISTS: Inner Clause Handling ====================

    #[test]
    fn test_not_exists_with_inner_group_by_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e \
                 WHERE e.dept_id = d.id \
                 GROUP BY e.status \
             )",
            &tables,
        );

        assert!(
            result.is_err(),
            "NOT EXISTS with GROUP BY should be rejected"
        );
    }

    #[test]
    fn test_not_exists_with_inner_having_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e \
                 WHERE e.dept_id = d.id \
                 GROUP BY e.status \
                 HAVING count(*) > 5 \
             )",
            &tables,
        );

        assert!(
            result.is_err(),
            "NOT EXISTS with HAVING should be rejected"
        );
    }

    #[test]
    fn test_not_exists_with_inner_limit_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id, d.name FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e \
                 WHERE e.dept_id = d.id \
                 LIMIT 1 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "NOT EXISTS with LIMIT should be decorrelated"
        );
        let sql = deparse(&outcome.resolved);
        assert!(sql.contains("LEFT JOIN"), "should have LEFT JOIN: {sql}");
        assert!(
            !sql.contains("NOT EXISTS"),
            "should not have NOT EXISTS: {sql}"
        );
    }

    // ==================== Mixed Cases ====================

    #[test]
    fn test_mixed_correlated_and_non_correlated() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
             AND EXISTS (SELECT 1 FROM departments d WHERE d.name = 'Engineering')",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        // Correlated EXISTS should be decorrelated
        assert!(
            sql.contains("public.orders"),
            "correlated subquery decorrelated: {sql}"
        );
        // Non-correlated EXISTS should remain
        assert!(
            sql.contains("EXISTS"),
            "non-correlated EXISTS should remain: {sql}"
        );
    }

    #[test]
    fn test_exists_with_outer_where_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.status = 'active' \
             AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let sql = deparse(&outcome.resolved);

        // Outer predicate should be preserved (uses alias)
        assert!(
            sql.contains("e.status = 'active'"),
            "outer predicate preserved: {sql}"
        );
        // Correlation should be in JOIN
        assert!(
            sql.contains("JOIN"),
            "correlation should be JOIN: {sql}"
        );
    }
}

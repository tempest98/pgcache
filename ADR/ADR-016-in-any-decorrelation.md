# ADR-016: IN/ANY Correlated Subquery Decorrelation

## Status
Accepted

## Context
ADR-014 introduced decorrelation for EXISTS/NOT EXISTS and ADR-015 for scalar correlated subqueries. Correlated IN subqueries — `WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)` — remained unsupported, rejected with "correlated IN/ALL subquery". IN is a common pattern for membership checks with correlated filtering, so rejecting it left a useful class of queries uncacheable.

IN is semantically a semi-join, the same as EXISTS. The difference is that IN carries an implicit equality predicate between the test expression (left side of IN) and the inner query's output column, whereas EXISTS has no such predicate. This means the EXISTS decorrelation approach (INNER JOIN + DISTINCT) applies directly, with the addition of the IN predicate to the JOIN ON condition.

## Decision
Decorrelate correlated IN (`SubLinkType::Any`) into INNER JOIN + DISTINCT, extending the EXISTS semi-join pattern. The IN predicate (test_expr = inner_output_column) is added to the JOIN ON condition alongside correlation predicates extracted from the inner WHERE.

- **INNER JOIN + DISTINCT** preserves semi-join semantics, same as EXISTS (ADR-014)
- **IN predicate** is an equality between the test expression and the inner query's single output column — this becomes part of the JOIN ON condition
- **Inner output column must be a simple column reference** — expressions (e.g., `SELECT d.id + 1`) are rejected since they can't be directly referenced in the flattened JOIN scope
- **GROUP BY/HAVING/LIMIT stripping** reuses `correlated_exists_inner_prepare()` — same rationale: IN is a membership check, stripping produces conservative (over-) invalidation for CDC
- **Residual inner predicates** merge into the outer WHERE, same as EXISTS
- **ALL/NOT IN remain rejected** — NOT IN maps to `SubLinkType::All` and requires anti-join semantics (future work)

Three resolved forms are maintained, consistent with EXISTS and scalar decorrelation: the original is preserved for serving, the decorrelated form drives population and update query generation, and update queries are stored for CDC.

## Rationale
**Natural extension of EXISTS**: IN and EXISTS are both semi-joins. The only structural difference is the IN predicate, which maps directly to an additional equality in the JOIN ON condition. The implementation reuses most of the EXISTS infrastructure.

**Common query pattern**: `WHERE column IN (SELECT ... WHERE correlated)` is widely used for filtered membership checks. Supporting it alongside EXISTS covers the most common correlated subquery patterns.

**CDC source classification falls out naturally**: Inner tables land in an INNER JOIN, receiving `FromClause` source classification. Any CDC event on inner tables triggers re-evaluation, which is correct — removing a matching inner row can shrink the outer result set.

## Consequences

### Positive
- Correlated IN subqueries are now cacheable
- CDC correctly tracks inner tables and invalidates on changes
- Reuses EXISTS infrastructure — minimal new code
- Combined with ADR-014 and ADR-015, all common correlated subquery patterns are now supported

### Negative
- DISTINCT adds deduplication overhead at serve time even when unnecessary (same as EXISTS)
- Inner output column must be a simple column reference (expressions rejected)
- Only equality correlation predicates are supported
- Correlated IN inside OR is rejected (same limitation as EXISTS)
- NOT IN / ALL remain unsupported

## Implementation Notes
`in_any_inner_output_column()` validates the inner query has exactly one output column that is a simple column reference. `subquery_in_any_decorrelate()` builds the INNER JOIN + DISTINCT, combining the IN predicate with correlation predicates into the JOIN ON condition. `conjunct_in_any_try_decorrelate()` is the entry point, reusing `correlated_exists_inner_prepare()` for inner query cleaning and `where_clause_correlation_partition()` for predicate extraction.

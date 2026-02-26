# ADR-017: NOT IN / ALL Correlated Subquery Decorrelation

## Status
Accepted

## Context
ADR-014 introduced decorrelation for EXISTS/NOT EXISTS and ADR-016 for correlated IN subqueries. Correlated NOT IN subqueries — `WHERE e.id NOT IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status)` — remained unsupported, rejected with "correlated NOT-wrapped non-EXISTS subquery" or "correlated IN/ALL subquery". NOT IN is a common pattern for exclusion checks with correlated filtering.

NOT IN is semantically an anti-join, the same as NOT EXISTS. The difference is that NOT IN carries an implicit inequality predicate between the test expression and the inner query's output column, whereas NOT EXISTS has no such predicate. This means the NOT EXISTS decorrelation approach (LEFT JOIN + IS NULL) applies directly, with the addition of the NOT IN predicate to the JOIN ON condition.

## Decision
Decorrelate correlated NOT IN (`SubLinkType::All` and `NOT(SubLinkType::Any)`) into LEFT JOIN + IS NULL, extending the NOT EXISTS anti-join pattern. The NOT IN predicate (test_expr = inner_output_column) is added to the JOIN ON condition alongside correlation predicates extracted from the inner WHERE.

- **LEFT JOIN + IS NULL** preserves anti-join semantics, same as NOT EXISTS (ADR-014)
- **NOT IN predicate** is an equality between the test expression and the inner query's single output column — this becomes part of the JOIN ON condition
- **Inner output column must be a simple column reference** — reuses `in_any_inner_output_column()` from ADR-016
- **GROUP BY/HAVING rejection** reuses `correlated_not_exists_inner_prepare()` — same rationale as NOT EXISTS: stripping GROUP BY/HAVING could cause under-invalidation for CDC (anti-join semantics mean fewer matching inner rows could incorrectly expand the outer result)
- **LIMIT stripping** is safe — NOT IN is a boolean membership check
- **Residual inner predicates** are placed in the JOIN ON condition (not outer WHERE) to preserve LEFT JOIN semantics
- **IS NULL check** on the inner output column is added to the outer WHERE to complete the anti-join
- **No DISTINCT** — LEFT JOIN + IS NULL naturally produces at most one match per outer row
- **Two AST representations**: PostgreSQL parses `NOT IN (SELECT ...)` as `NOT(Subquery { sublink_type: Any })` and `<> ALL (SELECT ...)` as `Subquery { sublink_type: All }`. Both forms are handled in the decorrelation match logic.

Three resolved forms are maintained, consistent with EXISTS and IN decorrelation: the original is preserved for serving, the decorrelated form drives population and update query generation, and update queries are stored for CDC.

## Rationale
**Natural extension of NOT EXISTS**: NOT IN and NOT EXISTS are both anti-joins. The only structural difference is the NOT IN predicate, which maps directly to an additional equality in the JOIN ON condition. The implementation reuses most of the NOT EXISTS infrastructure.

**NULL semantics difference is safe**: NOT IN has different NULL behavior than LEFT JOIN + IS NULL (NOT IN returns NULL/false when NULLs are present, LEFT JOIN + IS NULL treats NULLs as non-matching). This doesn't matter because the decorrelated form is transient — it's used only for population and CDC update query generation, never executed for serving. The original form with correct NULL semantics is preserved for serving. For CDC, the difference is conservative (over-invalidation), which is safe.

**Common query pattern**: `WHERE column NOT IN (SELECT ... WHERE correlated)` is widely used for exclusion checks. Supporting it alongside IN covers both membership and exclusion correlated subquery patterns.

**CDC source classification falls out naturally**: Inner tables land in a LEFT JOIN, receiving `FromClause` source classification. Any CDC event on inner tables triggers re-evaluation, which is correct — adding a matching inner row can shrink the outer result set.

## Consequences

### Positive
- Correlated NOT IN subqueries are now cacheable
- Both `NOT IN (SELECT ...)` and `<> ALL (SELECT ...)` syntax are supported
- CDC correctly tracks inner tables and invalidates on changes
- Reuses NOT EXISTS and IN infrastructure — minimal new code
- Combined with ADR-014, ADR-015, and ADR-016, all common correlated subquery patterns are now supported

### Negative
- GROUP BY/HAVING in inner query causes rejection (same as NOT EXISTS)
- Inner output column must be a simple column reference (expressions rejected)
- Only equality correlation predicates are supported
- Correlated NOT IN inside OR is rejected (same limitation as all decorrelation)
- Two AST representations (`NOT(ANY)` and `ALL`) require two match arms in the decorrelation dispatcher

## Implementation Notes
`subquery_not_in_all_decorrelate()` builds the LEFT JOIN + IS NULL, combining the NOT IN predicate with correlation predicates and residual predicates into the JOIN ON condition, and adding the IS NULL check to the outer WHERE. `conjunct_not_in_all_try_decorrelate()` is the entry point, reusing `correlated_not_exists_inner_prepare()` for inner query validation and `in_any_inner_output_column()` for output column extraction. The `select_node_decorrelate()` dispatcher handles both `SubLinkType::All` and `Unary(Not, Subquery { sublink_type: Any })` forms.

# Complex Query Support

This document tracks query feature support in pgcache — what's implemented, what's cacheable, and what remains.

## Target Query

The following production query represents the features we need to support:

```sql
SELECT
  acq_inst.id,
  acq_inst.num_j as num_aa,
  acq_inst.int_a,
  acq_inst.date_a,
  acq_inst.num_a,
  acq_inst.text_a,
  acq_inst.text_b,
  acq_inst.text_c,
  acq_inst.text_d,
  acq_inst.uuid_a,
  acq_inst.text_f,
  acq_inst.bool_f,
  acq_inst.uuid_c,
  acq_inst.uuid_d,
  acq_inst.num_h,
  acq_inst.uuid_b,
  acq_inst.bool_b,
  acq_inst.int_d,
  acq_inst.uuid_e,
  acq_inst.text_g,
  acq_inst.uuid_f,
  acq_inst.uuid_g,
  acq_inst.int_c,
  acq_inst.text_e,
  acq_inst.num_b,
  acq_inst.num_c,
  acq_inst.num_d,
  acq_inst.num_e,
  acq_inst.num_f,
  acq_inst.num_g,
  acq_inst.bool_a,
  acq_inst.uuid_h,
  acq_inst.date_b,
  acq_inst.date_c,
  acq_inst.date_d,
  acq_inst.num_i,
  acq_inst.uuid_i,
  acq_inst.timestamp_a,
  acq_inst.uuid_j,
  acq_inst.bool_c,
  acq_inst.text_h,
  acq_inst.bool_d,
  acq_inst.bool_e,
  acq_inst.text_i,
  acq_inst.text_j,
  acq_inst.text_n,
  acq_inst.text_o,
  acq_inst.text_p,
  acq_inst.text_q,
  acq_inst.text_r,
  acq_inst.text_s,
  acq_inst.text_t,
  acq_inst.timestamp_b,
  acq_inst.timestamp_c,
  acq_inst.timestamp_d,
  acq_inst.text_u,
  acq_inst.text_v,
  acq_inst.text_w,
  acq_inst.int_e,
  acq_inst.int_b,
  acq_inst.text_k,
  acq_inst.text_l,
  acq_inst.uuid_k,
  acq_inst.text_m,
  acq_inst.int_f,
  string_agg(
    distinct acq_inst.name,
    ', '
    order by
      acq_inst.name
  ) as name,
  count(distinct acq_inst.uuid_l) as int_g,
  false as bool_g,
  false as bool_h,
  sum(
    case
      when (
        acq_inst.text_f in ('ACQUITTED', 'CONCILIATED')
        or acq_inst.date_a >= date_trunc('day', now())
      )
      and acq_inst.text_c = 'REVENUE' then acq_inst.num_a
      when (
        acq_inst.text_f in ('ACQUITTED', 'CONCILIATED')
        or acq_inst.date_a >= date_trunc('day', now())
      )
      and acq_inst.text_c = 'EXPENSE' then acq_inst.num_a * -1
      else 0
    end
  ) over (
    order by
      acq_inst.date_a asc,
      acq_inst.text_f asc,
      acq_inst.timestamp_a asc,
      acq_inst.text_c asc,
      acq_inst.id asc
  ) as num_bal
from
  (
    select
      pay.id,
      null as int_a,
      pay.date_a,
      pay.num_a,
      'NONE' as text_a,
      'ACQUITTANCE' as text_b,
      ev.text_b as text_c,
      inst.text_a as text_d,
      ev.uuid_a,
      1 as int_b,
      inst.num_f as num_b,
      inst.num_c as num_c,
      inst.num_e as num_d,
      inst.num_d as num_e,
      inst.num_a as num_f,
      inst.num_b as num_g,
      true as bool_a,
      null as uuid_b,
      null as int_c,
      ev.text_a as text_e,
      pay.text_a as text_f,
      null as uuid_c,
      null as uuid_d,
      null as num_h,
      false as bool_b,
      null as int_d,
      ev.uuid_b as uuid_e,
      ent.name as text_g,
      null as uuid_f,
      ev.id as uuid_g,
      1 as int_e,
      pay.uuid_b as uuid_h,
      inst.date_a as date_b,
      ev.date_a as date_c,
      inst.date_b as date_d,
      null as num_i,
      pay.uuid_a as uuid_i,
      pay.timestamp_a,
      null as uuid_j,
      false as bool_c,
      ev.num_a as num_j,
      facc.text_a as text_h,
      false as bool_d,
      false as bool_e,
      pay.text_b as text_i,
      ev.text_c as text_j,
      null as text_k,
      null as text_l,
      null as text_m,
      null as uuid_k,
      null as text_n,
      null as text_o,
      null as text_p,
      null as text_q,
      null as text_r,
      null as text_s,
      null as text_t,
      null as timestamp_b,
      null as timestamp_c,
      null as timestamp_d,
      null as text_u,
      null as text_v,
      null as text_w,
      null as int_f,
      null as bool_f,
      cat.id as uuid_l,
      cat.name as name
    from
      tb_payments pay
      join tb_installments inst on inst.id = pay.uuid_a
      and inst.tenant_id = 1528
      join tb_events ev on ev.id = inst.uuid_a
      and ev.tenant_id = 1528
      join tb_event_categories ec on ec.uuid_a = ev.id
      and ec.tenant_id = 1528
      join tb_categories cat on cat.id = ec.uuid_b
      left join tb_accounts facc on pay.uuid_b = facc.id
      and facc.tenant_id = 1528
      left join tb_entities ent on ev.uuid_a = ent.id
      and ent.tenant_id = 1528
    where
      pay.tenant_id = 1528
      and pay.timestamp_b is null
      and inst.timestamp_a is null
      and ev.timestamp_b is null

    union

    select
      inst.id,
      null as int_a,
      inst.date_b as date_a,
      inst.num_g as num_a,
      'NONE' as text_a,
      'INSTALLMENT' as text_b,
      ev.text_b as text_c,
      inst.text_a as text_d,
      ev.uuid_a,
      1 as int_b,
      inst.num_f,
      inst.num_c,
      inst.num_e,
      inst.num_d,
      inst.num_a,
      inst.num_b,
      false as bool_a,
      null as uuid_b,
      null as int_c,
      ev.text_a as text_e,
      inst.text_b as text_f,
      null as uuid_c,
      null as uuid_d,
      null as num_h,
      false as bool_b,
      null as int_d,
      ev.uuid_b as uuid_e,
      ent.name as text_g,
      null as uuid_f,
      ev.id as uuid_g,
      1 as int_e,
      null as uuid_h,
      inst.date_a as date_b,
      ev.date_a as date_c,
      inst.date_b as date_d,
      null as num_i,
      null as uuid_i,
      inst.timestamp_b as timestamp_a,
      null as uuid_j,
      false as bool_c,
      ev.num_a as num_j,
      null as text_h,
      false as bool_d,
      false as bool_e,
      null as text_i,
      ev.text_c as text_j,
      null as text_k,
      null as text_l,
      null as text_m,
      null as uuid_k,
      null as text_n,
      null as text_o,
      null as text_p,
      null as text_q,
      null as text_r,
      null as text_s,
      null as text_t,
      null as timestamp_b,
      null as timestamp_c,
      null as timestamp_d,
      null as text_u,
      null as text_v,
      null as text_w,
      null as int_f,
      null as bool_f,
      cat.id as uuid_l,
      cat.name as name
    from
      tb_installments inst
      join tb_events ev on ev.id = inst.uuid_a
      and ev.tenant_id = 1528
      join tb_event_categories ec on ec.uuid_a = ev.id
      and ec.tenant_id = 1528
      join tb_categories cat on cat.id = ec.uuid_b
      left join tb_entities ent on ev.uuid_a = ent.id
      and ent.tenant_id = 1528
    where
      inst.tenant_id = 1528
      and inst.timestamp_a is null
      and ev.timestamp_b is null
      and (
        inst.num_g > 0
      )
  ) acq_inst
where
  1 = 1
  and acq_inst.date_a >= '2026-01-01'
  and acq_inst.date_a <= '2026-12-31'
group by
  acq_inst.id,
  acq_inst.num_j,
  acq_inst.int_a,
  acq_inst.date_a,
  acq_inst.num_a,
  acq_inst.text_a,
  acq_inst.text_b,
  acq_inst.text_c,
  acq_inst.text_d,
  acq_inst.uuid_a,
  acq_inst.text_f,
  acq_inst.bool_f,
  acq_inst.uuid_c,
  acq_inst.uuid_d,
  acq_inst.num_h,
  acq_inst.uuid_b,
  acq_inst.bool_b,
  acq_inst.int_d,
  acq_inst.uuid_e,
  acq_inst.text_g,
  acq_inst.uuid_g,
  acq_inst.uuid_f,
  acq_inst.int_c,
  acq_inst.text_e,
  acq_inst.num_b,
  acq_inst.num_c,
  acq_inst.num_d,
  acq_inst.num_e,
  acq_inst.num_f,
  acq_inst.num_g,
  acq_inst.bool_a,
  acq_inst.uuid_h,
  acq_inst.date_b,
  acq_inst.date_c,
  acq_inst.date_d,
  acq_inst.num_i,
  acq_inst.uuid_i,
  acq_inst.timestamp_a,
  acq_inst.uuid_j,
  acq_inst.bool_c,
  acq_inst.text_h,
  acq_inst.bool_d,
  acq_inst.bool_e,
  acq_inst.text_i,
  acq_inst.text_j,
  acq_inst.text_n,
  acq_inst.text_o,
  acq_inst.text_p,
  acq_inst.text_q,
  acq_inst.text_r,
  acq_inst.text_s,
  acq_inst.text_t,
  acq_inst.timestamp_b,
  acq_inst.timestamp_c,
  acq_inst.timestamp_d,
  acq_inst.text_u,
  acq_inst.text_v,
  acq_inst.text_w,
  acq_inst.int_e,
  acq_inst.int_b,
  acq_inst.text_k,
  acq_inst.text_l,
  acq_inst.uuid_k,
  acq_inst.text_m,
  acq_inst.int_f
order by
  acq_inst.date_a asc,
  acq_inst.text_f asc,
  acq_inst.timestamp_a asc
limit
  100 offset 0;
```

### Target Query Cacheability

This query **cannot be cached** due to several design-level blockers:

- **LEFT JOIN** — `left join tb_accounts`, `left join tb_entities` — not cacheable (NULL semantics)
- **LIMIT/OFFSET** — `limit 100 offset 0` — not cacheable at top level
- **Non-deterministic functions** — `date_trunc('day', now())` — result changes over time

The target query is useful as a parsing benchmark (can we parse all features?) but will need structural changes to become cacheable. A cacheable version would need to replace LEFT JOINs with INNER JOINs, remove LIMIT, and eliminate non-deterministic functions.

## Feature Status

### Fully Implemented and Cacheable

| Feature | Parsing | Resolution | Cacheability | Integration Tests |
|---------|---------|------------|--------------|-------------------|
| INNER JOINs (equality conditions) | `ast.rs` | `resolved.rs` | `query.rs` | multiple |
| Basic WHERE (`=`, `>=`, `<=`, `AND`, `OR`, `NOT`) | `parse.rs` | `resolved.rs` | `query.rs` | multiple |
| IS NULL / IS NOT NULL in WHERE | `parse.rs` `null_test_convert` | `resolved.rs` | `query.rs` | multiple |
| IN clause (value list) | `parse.rs` `a_expr_convert` | `resolved.rs` | `query.rs` | multiple |
| Column aliases | `ast.rs` | `resolved.rs` | `query.rs` | multiple |
| ORDER BY | `ast.rs` | `resolved.rs` | `query.rs` | multiple |
| Literal values in SELECT (NULL, bool, int, string) | `ast.rs` `select_columns_convert` | `resolved.rs` | `query.rs` | unit |
| Aggregate functions (`COUNT(*)`, `SUM`, `string_agg`) | `ast.rs` `func_call_convert` | `resolved.rs` | `query.rs` | unit |
| COUNT(DISTINCT col) | `ast.rs` via `agg_distinct` | `resolved.rs` | `query.rs` | unit |
| ORDER BY in aggregates | `ast.rs` via `agg_order` | `resolved.rs` | `query.rs` | unit |
| Window functions (`OVER (PARTITION BY ... ORDER BY ...)`) | `ast.rs` `WindowSpec` | `resolved.rs` `ResolvedWindowSpec` | `query.rs` | unit |
| CASE expressions (searched and simple) | `ast.rs` `CaseExpr` | `resolved.rs` `ResolvedCaseExpr` | `query.rs` | unit |
| Arithmetic expressions (`+`, `-`, `*`, `/`) | `ast.rs` `ArithmeticExpr` | `resolved.rs` `ResolvedArithmeticExpr` | `query.rs` | unit |
| Function calls in SELECT | `ast.rs` `FunctionCall` | `resolved.rs` | `query.rs` | unit |
| GROUP BY | `ast.rs` | `resolved.rs` | `query.rs` — cacheable, aggregation at retrieval | unit |
| Subqueries in FROM (derived tables) | `ast.rs` `TableSubqueryNode` | `resolved.rs` `ResolvedTableSubqueryNode` | `query.rs` `is_cacheable_table_subquery` | `subquery_test.rs` |
| UNION / UNION ALL / INTERSECT / EXCEPT | `ast.rs` `SetOpNode` | `resolved.rs` `ResolvedSetOpNode` | `query.rs` `is_cacheable_set_op` | `set_operations_test.rs` |
| CTEs (WITH ... AS) | `ast.rs` `CteDefinition`, `CteRefNode` | `resolved.rs` via `ResolvedTableSubqueryNode` | `query.rs` `is_cacheable_cte_ref` | `cte_test.rs` |
| MATERIALIZED / NOT MATERIALIZED CTEs | `ast.rs` `CteMaterialize` | `resolved.rs` | `query.rs` | `cte_test.rs` |
| Subqueries in WHERE (IN, NOT IN, scalar) | `parse.rs` `sublink_convert` | `resolved.rs` | `query.rs` `is_cacheable_expr` | `subquery_test.rs` |
| Nested subqueries (multi-level) | `parse.rs` | `resolved.rs` | `query.rs` | `subquery_test.rs` |

### Parsed but Not Cacheable (by design)

| Feature | Reason | Notes |
|---------|--------|-------|
| LIMIT / OFFSET (top level) | Cache keys would vary per limit value | Allowed inside derived tables |
| LEFT JOIN | NULL semantics on right side make cache invalidation unreliable | Parsed and resolved, rejected at cacheability |
| GROUP BY + LIMIT | Combined constraint | GROUP BY alone is cacheable |

### Not Supported

| Feature | Parse | Resolve | Notes |
|---------|-------|---------|-------|
| FuncCall in WHERE/HAVING | No | No | `node_convert_to_expr` doesn't handle `NodeEnum::FuncCall`. `WhereExpr::Function` variant exists but is never produced. Needed for `HAVING SUM(x) > ...` |
| Correlated subqueries | Yes | No — rejected with `CorrelatedSubqueryNotSupported` | Subqueries that reference outer table columns (e.g., `WHERE t2.id = t1.id`). Includes correlated EXISTS, NOT EXISTS, and scalar-in-SELECT |
| RECURSIVE CTEs | No — rejected at parse time | No | `WITH RECURSIVE` explicitly rejected |
| LATERAL subqueries | Yes | No | Rejected at cacheability (`is_cacheable_table_subquery` checks for lateral) |
| Window frame specification | No | No | `ROWS BETWEEN ...` not parsed. `PARTITION BY` and `ORDER BY` are supported |
| Aggregate FILTER clause | No | No | `FILTER (WHERE ...)` on aggregates not parsed |
| Non-deterministic functions | Parsed as regular functions | Resolved | Not detected — `now()`, `random()` etc. are cached as if deterministic |

## CDC Invalidation for Subqueries

Subqueries and CTEs track `SubqueryKind` for directional CDC invalidation:

| SubqueryKind | CDC INSERT | CDC DELETE | CDC UPDATE |
|--------------|-----------|-----------|-----------|
| **Inclusion** (IN, derived tables, CTEs) | Invalidates | Skips | Invalidates |
| **Exclusion** (NOT IN) | Skips | Invalidates | Invalidates |
| **Scalar** | Invalidates | Invalidates | Invalidates |

Tables are tracked as either `Direct` (FROM clause) or `Subquery` (within a subquery/CTE) via `UpdateQuerySource`.

## Integration Test Coverage

| Test File | Tests | Features Covered |
|-----------|-------|-----------------|
| `tests/subquery_test.rs` | `test_subquery_from_derived_table` | Derived table cache + CDC INSERT/DELETE |
| | `test_subquery_where_in` | IN subquery cache + CDC INSERT |
| | `test_subquery_where_not_in` | NOT IN (exclusion) cache |
| | `test_subquery_scalar_in_where` | Scalar subquery in WHERE |
| | `test_subquery_nested` | 3-level nested IN subqueries |
| | `test_subquery_multi_table_dependency` | Multi-table CDC dependency tracking |
| `tests/cte_test.rs` | `cte_simple` | Basic CTE cache miss/hit |
| | `cte_simple_cdc` | CTE CDC UPDATE invalidation + DELETE skip |
| | `cte_with_join` | CTE joined with regular table |
| | `cte_multiple_tables` | Multiple CTE definitions |
| | `cte_materialized` | MATERIALIZED hint |
| `tests/set_operations_test.rs` | `set_op_union` | UNION cache miss/hit |
| | `set_op_union_cdc` | UNION with CDC updates |
| | `set_op_union_all` | UNION ALL vs UNION deduplication |

### Not Tested (correlated subqueries — not supported)

These patterns fail resolution with `CorrelatedSubqueryNotSupported` and cannot be cached:

- `WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)`
- `WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)`
- `SELECT col, (SELECT COUNT(*) FROM t2 WHERE t2.ref = t1.ref) FROM t1`

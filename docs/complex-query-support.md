# Complex Query Support Plan

This document tracks the work needed to support complex queries with features like UNION, aggregate functions, window functions, and CASE expressions.

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

## Simplified Test Query

Use this query for incremental development and testing. It contains all the key features in a more manageable form:

```sql
SELECT
  t.id,
  t.name,
  t.amount,
  null as nullable_col,
  false as bool_col,
  123 as int_col,
  'literal' as str_col,
  count(distinct t.category_id) as category_count,
  string_agg(distinct t.tag, ', ' order by t.tag) as tags,
  sum(
    case
      when t.status in ('ACTIVE', 'PENDING') and t.type = 'CREDIT'
        then t.amount
      when t.status in ('ACTIVE', 'PENDING') and t.type = 'DEBIT'
        then t.amount * -1
      else 0
    end
  ) over (order by t.date_col asc, t.id asc) as running_balance
from
  (
    select
      a.id,
      a.name,
      a.amount,
      a.status,
      a.type,
      a.date_col,
      c.id as category_id,
      tg.name as tag
    from table_a a
    join table_b b on b.id = a.b_id and b.tenant_id = 1
    join table_c c on c.id = b.c_id
    left join table_tags tg on tg.a_id = a.id
    where a.tenant_id = 1
      and a.deleted_at is null
      and b.active = true

    union

    select
      x.id,
      x.name,
      x.amount,
      x.status,
      x.type,
      x.date_col,
      c.id as category_id,
      tg.name as tag
    from table_x x
    join table_c c on c.id = x.c_id
    left join table_tags tg on tg.x_id = x.id
    where x.tenant_id = 1
      and x.deleted_at is null
  ) t
where t.date_col >= '2026-01-01'
  and t.date_col <= '2026-12-31'
group by t.id, t.name, t.amount, t.status, t.type, t.date_col
order by t.date_col asc, t.id asc
limit 100 offset 0;
```

## Feature Analysis

### Currently Supported

| Feature | Status | Notes |
|---------|--------|-------|
| Subqueries in FROM | **Partial** | Parsed via `table_subquery_node_convert`, but **resolution not implemented** |
| INNER JOINs | Yes | Multiple joins with equality conditions |
| LEFT JOINs | Yes | Parsed but marked non-cacheable |
| Basic WHERE (=, >=, <=, AND) | Yes | Simple comparisons work |
| Column aliases | Yes | `col as alias` |
| GROUP BY columns | Yes | Recently added, marks query non-cacheable |
| LIMIT/OFFSET | Yes | Recently added, marks query non-cacheable |
| ORDER BY | Yes | `OrderByClause` exists |
| CASE expressions | Yes | Both searched (`CASE WHEN...`) and simple (`CASE expr WHEN...`) forms |
| Aggregate functions | Yes | `FunctionCall` with `agg_star` and `agg_distinct` |
| COUNT(*) | Yes | Properly parsed and deparsed via `agg_star` field |
| COUNT(DISTINCT col) | Yes | Properly parsed and deparsed via `agg_distinct` field |
| Literal values in SELECT | Yes | NULL, booleans, integers, strings supported |
| Function calls in SELECT | Yes | `FunctionCall` struct handles named functions |
| Window functions | Yes | `sum(...) OVER (PARTITION BY ... ORDER BY ...)` |

### Not Supported (Blocking)

| Priority | Feature | Example | Impact |
|----------|---------|---------|--------|
| **P0** | **Subquery resolution** | `SELECT * FROM (SELECT ...) sub` | Resolution fails with `InvalidTableRef` |
| P0 | UNION | `select ... union select ...` | Query won't parse |
| ~~P1~~ | ~~Aggregate functions~~ | ~~`count(...)`, `sum(...)`, `string_agg(...)`~~ | ✅ Implemented |
| ~~P1~~ | ~~Window functions~~ | ~~`sum(...) over (order by ...)`~~ | ✅ Implemented |
| ~~P1~~ | ~~CASE expressions~~ | ~~`case when ... then ... else ... end`~~ | ✅ Implemented |
| ~~P1~~ | ~~Function calls in SELECT~~ | ~~`date_trunc('day', now())`~~ | ✅ Implemented |
| ~~P2~~ | ~~Boolean literals in SELECT~~ | ~~`false as bool_col`~~ | ✅ Implemented |
| ~~P2~~ | ~~NULL literals in SELECT~~ | ~~`null as nullable_col`~~ | ✅ Implemented |
| ~~P2~~ | ~~Integer literals in SELECT~~ | ~~`123 as int_col`~~ | ✅ Implemented |
| ~~P2~~ | ~~String literals in SELECT~~ | ~~`'literal' as str_col`~~ | ✅ Implemented |
| P2 | Arithmetic expressions | `amount * -1` | Won't parse |
| ~~P2~~ | ~~IS NULL in WHERE~~ | ~~`deleted_at is null`~~ | ✅ Implemented via `UnaryOp::IsNull/IsNotNull` |
| ~~P2~~ | ~~IN clause~~ | ~~`status in ('A', 'B')`~~ | ✅ Implemented via `MultiOp::In` |
| ~~P3~~ | ~~DISTINCT in aggregates~~ | ~~`count(distinct ...)`~~ | ✅ Implemented via `agg_distinct` |
| P3 | ORDER BY in aggregates | `string_agg(... order by ...)` | Part of aggregate support |

## Implementation Plan

### Phase 1: Parse the Query

#### Step 1.1: Add literal support to SELECT columns ✅ DONE

**File:** `src/query/ast.rs`

Literals (NULL, booleans, integers, strings) are now handled via `NodeEnum::AConst` in `select_columns_convert`.

**Test query:**
```sql
SELECT null as a, false as b, 123 as c, 'text' as d FROM t
```

#### Step 1.2: Add UNION support

**File:** `src/query/ast.rs`

1. Add new types:
```rust
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SetOperation {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SetOperationStatement {
    pub op: SetOperation,
    pub left: Box<SelectStatement>,
    pub right: Box<SelectStatement>,
}
```

2. Update `SelectStatement` or create wrapper enum:
```rust
pub enum SelectBody {
    Simple(SelectStatement),
    SetOp(SetOperationStatement),
}
```

3. In `select_statement_convert`, check `select_stmt.op` field

**Test query:**
```sql
SELECT id, name FROM table_a UNION SELECT id, name FROM table_b
```

#### Step 1.3: Add IS NULL support in WHERE ✅ DONE

**File:** `src/query/parse.rs`

Implemented via `null_test_convert()` function that handles `NodeEnum::NullTest`. Creates `UnaryExpr` with `UnaryOp::IsNull` or `UnaryOp::IsNotNull`.

Note: IS NULL uses postfix syntax (`expr IS NULL`), so `UnaryExpr::deparse` was updated to handle both prefix operators (NOT, EXISTS) and postfix operators (IS NULL, IS NOT NULL).

**Test queries:**
```sql
SELECT * FROM t WHERE deleted_at IS NULL
SELECT * FROM t WHERE name IS NOT NULL
SELECT * FROM t WHERE id = 1 AND deleted_at IS NULL
```

#### Step 1.4: Add IN clause support ✅ DONE

**File:** `src/query/parse.rs`

Implemented via `AExprKind::AexprIn` handling in `a_expr_convert()`. Uses `MultiExpr` with `MultiOp::In` or `MultiOp::NotIn`.

**Test query:**
```sql
SELECT * FROM t WHERE status IN ('A', 'B', 'C')
SELECT * FROM t WHERE id NOT IN (1, 2, 3)
```

#### Step 1.5: Add function calls to SELECT columns ✅ DONE

**File:** `src/query/ast.rs`

`FunctionCall` struct is implemented with:
```rust
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<ColumnExpr>,
    pub agg_star: bool,     // For COUNT(*)
    pub agg_distinct: bool, // For COUNT(DISTINCT ...)
}
```

Still TODO for future enhancement:
- `order_by: Vec<OrderByClause>` - For `string_agg(... ORDER BY ...)`
- `filter: Option<WhereExpr>` - For aggregate FILTER clause

**Test query:**
```sql
SELECT count(*), sum(amount), count(distinct category) FROM t
```

#### Step 1.6: Add window function support ✅ DONE

**File:** `src/query/ast.rs`, `src/query/resolved.rs`

Implemented structures:

```rust
// In ast.rs
pub struct WindowSpec {
    pub partition_by: Vec<ColumnExpr>,
    pub order_by: Vec<OrderByClause>,
}

pub struct FunctionCall {
    pub name: String,
    pub args: Vec<ColumnExpr>,
    pub agg_star: bool,
    pub agg_distinct: bool,
    pub over: Option<WindowSpec>,  // Window function OVER clause
}

// In resolved.rs
pub struct ResolvedWindowSpec {
    pub partition_by: Vec<ResolvedColumnExpr>,
    pub order_by: Vec<ResolvedOrderByClause>,
}
```

Supports PARTITION BY and ORDER BY clauses. Frame specification (`ROWS BETWEEN...`) not yet implemented.

**Test query:**
```sql
SELECT id, sum(amount) OVER (ORDER BY date ASC) as running_total FROM t
SELECT sum(amount) OVER (PARTITION BY category ORDER BY date) FROM orders
```

#### Step 1.7: Add CASE expression support ✅ DONE

**File:** `src/query/ast.rs`, `src/query/resolved.rs`

Implemented structures:

```rust
// In ast.rs
pub struct CaseExpr {
    pub arg: Option<Box<ColumnExpr>>,  // For simple CASE (CASE expr WHEN val...)
    pub whens: Vec<CaseWhen>,
    pub default: Option<Box<ColumnExpr>>,  // ELSE clause
}

pub struct CaseWhen {
    pub condition: WhereExpr,  // Uses WhereExpr for full boolean expression support
    pub result: ColumnExpr,
}

// In resolved.rs - corresponding resolved types
pub struct ResolvedCaseExpr { ... }
pub struct ResolvedCaseWhen { ... }
```

Both searched CASE (`CASE WHEN condition...`) and simple CASE (`CASE expr WHEN value...`) are supported.
The implementation uses `WhereExpr` for conditions, enabling complex boolean expressions.
Includes `has_sublink()` detection for cacheability analysis.

**Test queries:**
```sql
SELECT CASE WHEN status = 'A' THEN 1 WHEN status = 'B' THEN 2 ELSE 0 END FROM t
SELECT CASE status WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 0 END FROM t
```

#### Step 1.8: Add arithmetic expressions

**File:** `src/query/ast.rs`

Add binary operation support:

```rust
pub enum ColumnExpr {
    // ... existing variants
    BinaryOp(BinaryOpExpr),
    UnaryOp(UnaryOpExpr),
}

pub struct BinaryOpExpr {
    pub left: Box<ColumnExpr>,
    pub op: ArithmeticOp,
    pub right: Box<ColumnExpr>,
}

pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}
```

**Test query:**
```sql
SELECT amount * -1, price + tax, total / count FROM t
```

#### Step 1.9: Add subquery resolution (P0 - BLOCKING)

**File:** `src/query/resolved.rs`

Currently, subqueries are parsed but resolution fails with `InvalidTableRef`. Need to implement:

1. **Subquery in FROM clause** - `table_source_resolve` for `TableSource::Subquery`:
   - Recursively resolve the inner SELECT statement
   - Build a virtual schema from the subquery's SELECT columns
   - Register the subquery alias in the resolution scope
   - Allow outer query to reference `alias.column`

2. **Schema inference from subquery**:
   - Each SELECT column in the subquery defines a "virtual column"
   - Column names come from aliases or derived from expressions
   - These become available to the outer query via the subquery alias

3. **Subquery in SELECT/WHERE** - Lower priority, already detected via `has_sublink()`

```rust
TableSource::Subquery(subquery) => {
    // 1. Resolve the inner select statement
    let resolved_select = select_statement_resolve(&subquery.select, tables, search_path)?;

    // 2. Build virtual schema from SELECT columns
    let virtual_columns = resolved_select.columns.iter().map(|col| {
        // Extract column name from alias or expression
    }).collect();

    // 3. Register alias in scope for outer query resolution
    scope.register_subquery_alias(&subquery.alias, virtual_columns);

    Ok(ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
        select: Box::new(resolved_select),
        alias: subquery.alias.clone(),
    }))
}
```

**Test query:**
```sql
SELECT sub.id, sub.name FROM (SELECT id, name FROM users WHERE active = true) sub WHERE sub.id > 10
```

### Phase 2: Resolution Support

**File:** `src/query/resolved.rs`

Add resolution functions for all new AST types:
- `set_operation_resolve` - TODO
- `case_expr_resolve` - ✅ DONE (integrated into `column_expr_resolve`)
- `window_spec_resolve` - ✅ DONE
- `subquery_table_source_resolve` - TODO (P0)
- Enhanced `function_call_resolve` - ✅ DONE (handles `agg_star`, `agg_distinct`, `over`)

### Phase 3: Cacheability

**File:** `src/cache/query.rs`

Add checks to mark queries as non-cacheable:
- UNION/INTERSECT/EXCEPT
- Window functions
- Aggregate functions (already non-cacheable via GROUP BY)
- Non-deterministic functions (now(), random(), etc.)

## Progress Tracking

- [x] Step 1.1: Literal support in SELECT
- [ ] Step 1.2: UNION support
- [x] Step 1.3: IS NULL in WHERE (via `UnaryOp::IsNull/IsNotNull`)
- [x] Step 1.4: IN clause (via `MultiOp::In`, `MultiOp::NotIn`)
- [x] Step 1.5: Function calls in SELECT (including COUNT(*), COUNT(DISTINCT))
- [x] Step 1.6: Window functions (PARTITION BY, ORDER BY)
- [x] Step 1.7: CASE expressions (searched and simple forms)
- [ ] Step 1.8: Arithmetic expressions
- [ ] **Step 1.9: Subquery resolution** (P0 - blocks target query)
- [x] Phase 2: Resolution support (for implemented features)
- [ ] Phase 3: Cacheability checks
- [ ] Full test query parses successfully

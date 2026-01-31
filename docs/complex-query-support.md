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
| Subqueries in FROM | Yes | `table_subquery_node_convert` handles `(select ...) alias` |
| INNER JOINs | Yes | Multiple joins with equality conditions |
| LEFT JOINs | Yes | Parsed but marked non-cacheable |
| Basic WHERE (=, >=, <=, AND) | Yes | Simple comparisons work |
| Column aliases | Yes | `col as alias` |
| GROUP BY columns | Yes | Recently added, marks query non-cacheable |
| LIMIT/OFFSET | Yes | Recently added, marks query non-cacheable |
| ORDER BY | Yes | `OrderByClause` exists |

### Not Supported (Blocking)

| Priority | Feature | Example | Impact |
|----------|---------|---------|--------|
| P0 | UNION | `select ... union select ...` | Query won't parse |
| P1 | Aggregate functions | `count(...)`, `sum(...)`, `string_agg(...)` | Columns won't parse |
| P1 | Window functions | `sum(...) over (order by ...)` | Columns won't parse |
| P1 | CASE expressions | `case when ... then ... else ... end` | Won't parse |
| P1 | Function calls in SELECT | `date_trunc('day', now())` | Won't parse |
| P2 | Boolean literals in SELECT | `false as bool_col` | Not handled in `select_columns_convert` |
| P2 | NULL literals in SELECT | `null as nullable_col` | Not handled |
| P2 | Integer literals in SELECT | `123 as int_col` | Not handled |
| P2 | String literals in SELECT | `'literal' as str_col` | Not handled |
| P2 | Arithmetic expressions | `amount * -1` | Won't parse |
| P2 | IS NULL in WHERE | `deleted_at is null` | `ExprOp::IsNull` exists but needs WHERE support |
| P2 | IN clause | `status in ('A', 'B')` | `ExprOp::In` exists but needs conversion |
| P3 | DISTINCT in aggregates | `count(distinct ...)` | Part of aggregate support |
| P3 | ORDER BY in aggregates | `string_agg(... order by ...)` | Part of aggregate support |

## Implementation Plan

### Phase 1: Parse the Query

#### Step 1.1: Add literal support to SELECT columns

**File:** `src/query/ast.rs`

In `select_columns_convert`, handle `NodeEnum::AConst` for literals:

```rust
Some(NodeEnum::AConst(const_val)) => {
    let literal = const_value_extract(const_val)?;
    columns.push(SelectColumn {
        expr: ColumnExpr::Literal(literal),
        alias,
    });
}
```

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

#### Step 1.3: Add IS NULL support in WHERE

**File:** `src/query/parse.rs`

Handle `NodeEnum::NullTest` in `node_convert_to_expr`:

```rust
Some(NodeEnum::NullTest(null_test)) => {
    let arg = node_convert_to_expr(null_test.arg.as_ref().unwrap())?;
    let op = if null_test.nulltesttype() == NullTestType::IsNull {
        ExprOp::IsNull
    } else {
        ExprOp::IsNotNull
    };
    Ok(WhereExpr::Unary(UnaryExpr {
        op,
        expr: Box::new(arg),
    }))
}
```

**Test query:**
```sql
SELECT * FROM t WHERE deleted_at IS NULL
```

#### Step 1.4: Add IN clause support

**File:** `src/query/parse.rs`

Handle IN expressions (typically `NodeEnum::AExpr` with specific kind):

**Test query:**
```sql
SELECT * FROM t WHERE status IN ('A', 'B', 'C')
```

#### Step 1.5: Add function calls to SELECT columns

**File:** `src/query/ast.rs`

Handle `NodeEnum::FuncCall` in `select_columns_convert`:

```rust
Some(NodeEnum::FuncCall(func_call)) => {
    let function = function_call_convert(func_call)?;
    columns.push(SelectColumn {
        expr: ColumnExpr::Function(function),
        alias,
    });
}
```

Enhance `FunctionCall` struct:
```rust
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<ColumnExpr>,
    pub distinct: bool,           // For COUNT(DISTINCT ...)
    pub order_by: Vec<OrderByClause>, // For string_agg(... ORDER BY ...)
    pub filter: Option<WhereExpr>,    // For aggregate FILTER clause
}
```

**Test query:**
```sql
SELECT count(*), sum(amount), count(distinct category) FROM t
```

#### Step 1.6: Add window function support

**File:** `src/query/ast.rs`

Add window specification to `FunctionCall` or create new type:

```rust
pub struct WindowSpec {
    pub partition_by: Vec<ColumnExpr>,
    pub order_by: Vec<OrderByClause>,
    pub frame: Option<WindowFrame>,
}

// Extend FunctionCall or create WindowFunction
pub struct FunctionCall {
    // ... existing fields
    pub over: Option<WindowSpec>,
}
```

**Test query:**
```sql
SELECT id, sum(amount) OVER (ORDER BY date ASC) as running_total FROM t
```

#### Step 1.7: Add CASE expression support

**File:** `src/query/ast.rs`

Add new `ColumnExpr` variant:

```rust
pub enum ColumnExpr {
    // ... existing variants
    Case(CaseExpr),
}

pub struct CaseExpr {
    pub operand: Option<Box<ColumnExpr>>,  // For CASE expr WHEN ...
    pub when_clauses: Vec<WhenClause>,
    pub else_result: Option<Box<ColumnExpr>>,
}

pub struct WhenClause {
    pub condition: WhereExpr,  // or ColumnExpr for simple CASE
    pub result: ColumnExpr,
}
```

**Test query:**
```sql
SELECT CASE WHEN status = 'A' THEN 1 WHEN status = 'B' THEN 2 ELSE 0 END FROM t
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

### Phase 2: Resolution Support

**File:** `src/query/resolved.rs`

Add resolution functions for all new AST types:
- `set_operation_resolve`
- `case_expr_resolve`
- `window_spec_resolve`
- Enhanced `function_call_resolve`

### Phase 3: Cacheability

**File:** `src/cache/query.rs`

Add checks to mark queries as non-cacheable:
- UNION/INTERSECT/EXCEPT
- Window functions
- Aggregate functions (already non-cacheable via GROUP BY)
- Non-deterministic functions (now(), random(), etc.)

## Progress Tracking

- [ ] Step 1.1: Literal support in SELECT
- [ ] Step 1.2: UNION support
- [ ] Step 1.3: IS NULL in WHERE
- [ ] Step 1.4: IN clause
- [ ] Step 1.5: Function calls in SELECT
- [ ] Step 1.6: Window functions
- [ ] Step 1.7: CASE expressions
- [ ] Step 1.8: Arithmetic expressions
- [ ] Phase 2: Resolution support
- [ ] Phase 3: Cacheability checks
- [ ] Full test query parses successfully

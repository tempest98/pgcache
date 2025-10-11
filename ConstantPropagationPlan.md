# Constant Propagation Plan

## Goal

Propagate constant constraints through join conditions to determine which columns have known constant values. This enables better cache invalidation decisions by understanding which rows can possibly match a cached query.

## Motivating Example

```sql
SELECT * FROM test t
JOIN test_map tm ON tm.test_id = t.id
WHERE t.id = 1;
```

**Desired outcome**: Know that both `t.id = 1` AND `tm.test_id = 1` (propagated through the join condition)

This information allows the cache system to:
1. Only check rows where `test_map.test_id = 1` when invalidating
2. Skip entire tables if constraints don't match
3. Generate more precise update queries

## Architecture

### Core Data Structures

```rust
/// A constraint that a column equals a constant value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColumnConstraint {
    column: ResolvedColumnNode,
    value: LiteralValue,
}

/// An equivalence between two columns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColumnEquivalence {
    left: ResolvedColumnNode,
    right: ResolvedColumnNode,
}

/// Analysis results for a query
pub struct QueryConstraints {
    /// All column constraints (from WHERE + propagated through JOINs)
    pub column_constraints: HashMap<ResolvedColumnNode, LiteralValue>,

    /// Column equivalences from JOIN conditions and WHERE clause
    pub equivalences: HashSet<ColumnEquivalence>,

    /// Constraints organized by table for quick lookup
    pub table_constraints: HashMap<String, Vec<(String, LiteralValue)>>, // table -> [(column, value)]
}
```

## Implementation Plan

### Step 1: Unified Expression Analysis

Both WHERE clauses and JOIN conditions are `ResolvedWhereExpr`, so use a single function to extract both constraints and equivalences:

```rust
/// Extract equality information from any resolved WHERE expression
fn analyze_equality_expr(
    expr: &ResolvedWhereExpr,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
) {
    match expr {
        ResolvedWhereExpr::Binary(binary) if binary.op == ExprOp::Equal => {
            match (&*binary.lexpr, &*binary.rexpr) {
                // column = literal
                (ResolvedWhereExpr::Column(col), ResolvedWhereExpr::Value(val)) => {
                    constraints.insert(ColumnConstraint {
                        column: col.clone(),
                        value: val.clone(),
                    });
                }
                // literal = column
                (ResolvedWhereExpr::Value(val), ResolvedWhereExpr::Column(col)) => {
                    constraints.insert(ColumnConstraint {
                        column: col.clone(),
                        value: val.clone(),
                    });
                }
                // column = column (equivalence)
                (ResolvedWhereExpr::Column(left), ResolvedWhereExpr::Column(right)) => {
                    equivalences.insert(ColumnEquivalence {
                        left: left.clone(),
                        right: right.clone(),
                    });
                }
                _ => {}
            }
        }

        // AND: recursively analyze both sides
        ResolvedWhereExpr::Binary(binary) if binary.op == ExprOp::And => {
            analyze_equality_expr(&binary.lexpr, constraints, equivalences);
            analyze_equality_expr(&binary.rexpr, constraints, equivalences);
        }

        // Multi-operand AND
        ResolvedWhereExpr::Multi(multi) if multi.op == ExprOp::And => {
            for expr in &multi.exprs {
                analyze_equality_expr(expr, constraints, equivalences);
            }
        }

        // OR, other operators: cannot propagate
        _ => {}
    }
}
```

### Step 2: Collect From Entire Query

Traverse the resolved query to collect all constraints and equivalences:

```rust
fn collect_query_equalities(
    resolved: &ResolvedSelectStatement,
) -> (HashSet<ColumnConstraint>, HashSet<ColumnEquivalence>) {
    let mut constraints = HashSet::new();
    let mut equivalences = HashSet::new();

    // Analyze WHERE clause
    if let Some(where_expr) = &resolved.where_clause {
        analyze_equality_expr(where_expr, &mut constraints, &mut equivalences);
    }

    // Analyze JOIN conditions
    for table_source in &resolved.from {
        collect_from_table_source(table_source, &mut constraints, &mut equivalences);
    }

    (constraints, equivalences)
}

fn collect_from_table_source(
    source: &ResolvedTableSource,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
) {
    match source {
        ResolvedTableSource::Join(join) => {
            // Analyze this join's condition
            if let Some(condition) = &join.condition {
                analyze_equality_expr(condition, constraints, equivalences);
            }

            // Recurse into nested joins
            collect_from_table_source(&join.left, constraints, equivalences);
            collect_from_table_source(&join.right, constraints, equivalences);
        }
        _ => {}
    }
}
```

### Step 3: Propagate Constraints

Use fixpoint iteration to propagate constraints through equivalences:

```rust
fn propagate_constraints(
    initial_constraints: HashSet<ColumnConstraint>,
    equivalences: &HashSet<ColumnEquivalence>,
) -> HashMap<ResolvedColumnNode, LiteralValue> {
    let mut constraints: HashMap<ResolvedColumnNode, LiteralValue> = HashMap::new();

    // Add initial constraints
    for constraint in initial_constraints {
        constraints.insert(constraint.column, constraint.value);
    }

    // Fixpoint iteration: propagate until no changes
    let mut changed = true;
    while changed {
        changed = false;

        for equiv in equivalences {
            // If left has constraint but right doesn't, propagate to right
            if let Some(value) = constraints.get(&equiv.left) {
                if !constraints.contains_key(&equiv.right) {
                    constraints.insert(equiv.right.clone(), value.clone());
                    changed = true;
                }
            }

            // If right has constraint but left doesn't, propagate to left
            if let Some(value) = constraints.get(&equiv.right) {
                if !constraints.contains_key(&equiv.left) {
                    constraints.insert(equiv.left.clone(), value.clone());
                    changed = true;
                }
            }

            // TODO: Detect conflicts (same column, different values)
            // This would indicate an impossible query (empty result set)
        }
    }

    constraints
}
```

### Step 4: Main Entry Point

```rust
pub fn analyze_query_constraints(
    resolved: &ResolvedSelectStatement,
) -> QueryConstraints {
    // Step 1: Collect all equality information (constraints + equivalences)
    let (constraints, equivalences) = collect_query_equalities(resolved);

    // Step 2: Propagate constraints through equivalences
    let column_constraints = propagate_constraints(constraints, &equivalences);

    // Step 3: Organize by table for quick lookup
    let mut table_constraints: HashMap<String, Vec<(String, LiteralValue)>> = HashMap::new();
    for (col, val) in &column_constraints {
        table_constraints
            .entry(col.table.clone())
            .or_insert_with(Vec::new)
            .push((col.column.clone(), val.clone()));
    }

    QueryConstraints {
        column_constraints,
        equivalences,
        table_constraints,
    }
}
```

## Example Walkthrough

For: `SELECT * FROM test t JOIN test_map tm ON tm.test_id = t.id WHERE t.id = 1`

**Step 1**: Collect equalities
- From WHERE: `t.id = 1` → ColumnConstraint
- From JOIN: `tm.test_id = t.id` → ColumnEquivalence

**Step 2**: Propagate
- Initial: `{ test.id -> Integer(1) }`
- See equivalence `test_map.test_id ≡ test.id`
- Propagate: `{ test.id -> Integer(1), test_map.test_id -> Integer(1) }`

**Step 3**: Organize by table
```rust
QueryConstraints {
    column_constraints: {
        ResolvedColumnNode("public", "test", "id") -> Integer(1),
        ResolvedColumnNode("public", "test_map", "test_id") -> Integer(1),
    },
    equivalences: [
        ColumnEquivalence {
            left: ResolvedColumnNode("public", "test_map", "test_id"),
            right: ResolvedColumnNode("public", "test", "id"),
        }
    ],
    table_constraints: {
        "test" -> [("id", Integer(1))],
        "test_map" -> [("test_id", Integer(1))],
    },
}
```

## Additional Examples

### Equivalence in WHERE clause

```sql
SELECT * FROM a JOIN b ON a.x = b.y WHERE a.x = b.y AND a.x = 1
```

The `a.x = b.y` appears in both JOIN and WHERE - both will be collected but automatically deduplicated by the HashSet.

### Transitive propagation

```sql
SELECT * FROM a JOIN b ON a.x = b.y JOIN c ON b.y = c.z WHERE a.x = 1
```

- Collect: `a.x = 1`, `a.x ≡ b.y`, `b.y ≡ c.z`
- Iteration 1: `a.x = 1` → `b.y = 1`
- Iteration 2: `b.y = 1` → `c.z = 1`
- Result: All three columns constrained to 1

## Edge Cases

1. **Conflicting Constraints**: `WHERE a = 1 AND a = 2`
   - Could detect and mark query as impossible
   - For now, last constraint wins (undefined behavior)

2. **OR Conditions**: `WHERE a = 1 OR a = 2`
   - Cannot propagate (multiple possible values)
   - Skip these expressions

3. **Self-Join**: `FROM test t1 JOIN test t2 ON t1.id = t2.id WHERE t1.id = 1`
   - Both `t1.id` and `t2.id` should be constrained to 1
   - Works correctly with current design

4. **Non-Equality Operators**: `WHERE a < 5` or `JOIN ON a < b`
   - Ignored by current implementation
   - Future: could track range constraints

## Testing Strategy

Test cases:
1. **Simple constraint**: `WHERE a = 1` → `{a -> 1}`
2. **Simple equivalence**: `JOIN ON a = b WHERE a = 1` → `{a -> 1, b -> 1}`
3. **Transitive**: `JOIN ON a = b JOIN ON b = c WHERE a = 1` → `{a -> 1, b -> 1, c -> 1}`
4. **Multiple constraints**: `WHERE a = 1 AND b = 2` → `{a -> 1, b -> 2}`
5. **Equivalence in WHERE**: `WHERE a = b AND a = 1` → `{a -> 1, b -> 1}`
6. **No propagation (OR)**: `WHERE a = 1 OR a = 2` → `{}` (empty)
7. **Complex JOIN tree**: Multiple nested joins with propagation
8. **Self-join**: `FROM t t1 JOIN t t2 ON t1.id = t2.id WHERE t1.id = 1`

## Integration with Cache System

Use `QueryConstraints` to:

1. **Generate Precise Update Queries**:
   ```rust
   // If we know test_map.test_id must be 1:
   // SELECT true FROM test_map WHERE test_id = 1
   // Instead of: SELECT true FROM test_map JOIN test ON ...
   ```

2. **Skip Irrelevant CDC Events**:
   ```rust
   fn should_process_insert(constraints: &QueryConstraints, row: &Row) -> bool {
       if let Some(table_constraints) = constraints.table_constraints.get(table_name) {
           for (column, expected_value) in table_constraints {
               if row.get(column) != expected_value {
                   return false; // Row doesn't match query constraints
               }
           }
       }
       true
   }
   ```

3. **Optimize Cache Lookups**: Add constraints to cache SELECT queries

## Future Enhancements

1. **Conflict Detection**: Detect `a = 1 AND a = 2` as impossible
2. **Range Constraints**: `a < 5` + `JOIN ON b = a` → `b < 5`
3. **IN Clause**: `a IN (1, 2)` + `JOIN ON b = a` → `b IN (1, 2)`
4. **Expression Simplification**: `a + 1 = 2` → `a = 1`
5. **NULL Handling**: Track `IS NULL` / `IS NOT NULL`

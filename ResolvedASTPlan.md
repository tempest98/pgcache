# Resolved AST and Constant Propagation - Implementation Plan

## Overview

This document outlines the implementation plan for adding a resolved AST layer with fully qualified names and constant propagation to pgcache's query analysis system.

## Goals

1. **Name Resolution**: Fully qualify all table and column references with schema/table/column information
2. **Type Information**: Attach PostgreSQL type metadata to all resolved references
3. **Constant Propagation**: Perform simple constant folding and value propagation through WHERE clauses
4. **Error Detection**: Detect ambiguous references, unknown columns/tables at analysis time

## Architecture

### Three-Phase Design

```
Unresolved AST (pgcache/src/query/ast.rs)
    ↓ (resolution phase)
Resolved AST (pgcache/src/query/resolved.rs)
    ↓ (constant propagation phase)
Optimized AST with Value Constraints (pgcache/src/query/constprop.rs)
```

**Why separate phases:**
- Clarity: Each phase has clear responsibilities
- Performance: Can cache unresolved AST, rebuild resolved AST when metadata changes
- Testing: Easier to test each phase independently
- Extensibility: Can add additional optimization passes

## Design Decisions

### Metadata Availability
- First time a query is seen, fetch table metadata if needed
- Default unspecified schemas to 'public' (future: respect search_path)
- Resolution is eager - all references must resolve or error

### Subquery Scoping
- Subqueries create new resolution scopes
- Child scopes fall back to parent scopes for alias lookup
- Supports alias shadowing in nested subqueries

### Constant Propagation Depth
- Start with simple constant folding
- Track equality constraints (e.g., `x = 5`)
- Future: Range analysis for inequalities

### Error Handling
- Resolution failures are hard errors
- Rationale: Resolution failure implies malformed query that will fail at database

---

## Implementation Phases

### Phase 1: Foundation - Resolved AST Types

**File**: `pgcache/src/query/resolved.rs` (new)

**Core Types:**

```rust
/// Fully qualified table reference
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedTableRef {
    pub schema: String,           // e.g., "public"
    pub table: String,            // e.g., "users"
    pub original_alias: Option<String>, // e.g., Some("u")
}

/// Column type information from PostgreSQL metadata
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnTypeInfo {
    pub type_oid: u32,           // PostgreSQL type OID
    pub type_name: String,       // e.g., "integer", "text"
    pub data_type: Type,         // tokio_postgres::types::Type
    pub nullable: bool,          // Can be NULL
}

/// Fully qualified column reference with type info
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedColumnRef {
    pub schema: String,          // Table's schema
    pub table: String,           // Table name
    pub column: String,          // Column name
    pub type_info: ColumnTypeInfo,
}

/// Literal value with type information
#[derive(Debug, Clone, PartialEq)]
pub struct TypedLiteralValue {
    pub value: LiteralValue,     // From unresolved AST
    pub type_oid: Option<u32>,   // Inferred or explicit type
}

/// Resolved WHERE expression tree
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedWhereExpr {
    Value(TypedLiteralValue),
    Column(QualifiedColumnRef),
    Unary(ResolvedUnaryExpr),
    Binary(ResolvedBinaryExpr),
    Multi(ResolvedMultiExpr),
    Function {
        name: String,
        args: Vec<ResolvedWhereExpr>,
    },
}

/// Resolved SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectStatement {
    pub columns: ResolvedSelectColumns,
    pub from: Vec<ResolvedTableSource>,
    pub where_clause: Option<ResolvedWhereExpr>,
    // ... other fields similar to SelectStatement
}

/// Top-level resolved query
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSqlQuery {
    pub statement: ResolvedStatement,
}
```

**Tasks:**
- Create all core resolved types
- Implement `Clone`, `Debug`, `PartialEq` derives
- Add basic unit tests for type construction
- Mirror structure of unresolved AST but with qualified refs

**Deliverables:**
- `pgcache/src/query/resolved.rs` with core types
- Basic construction tests

---

### Phase 2: Resolution Context and Errors

**File**: `pgcache/src/query/resolve.rs` (new)

**Error Types:**

```rust
error_set! {
    ResolveError = {
        #[display("Ambiguous column '{column}' found in tables: {tables:?}")]
        AmbiguousColumn { column: String, tables: Vec<String> },

        #[display("Unknown column '{column}'. Available columns: {available:?}")]
        UnknownColumn { column: String, available: Vec<String> },

        #[display("Unknown table '{schema}.{table}'")]
        UnknownTable { table: String, schema: String },

        #[display("Unknown table alias '{alias}'")]
        UnknownAlias { alias: String },

        #[display("Schema '{schema}' not found")]
        SchemaNotFound { schema: String },

        #[display("Table metadata not available for '{table}'")]
        MetadataUnavailable { table: String },
    };
}
```

**Resolution Scope:**

```rust
/// Resolution scope for name lookup
#[derive(Debug, Clone)]
pub struct ResolutionScope {
    /// Map alias -> qualified table name
    pub table_aliases: HashMap<String, QualifiedTableRef>,

    /// Available tables in current scope (from FROM clause)
    pub tables: Vec<QualifiedTableRef>,

    /// Parent scope for nested subqueries
    pub parent: Option<Box<ResolutionScope>>,

    /// Default schema (from search path, defaults to "public")
    pub default_schema: String,
}

impl ResolutionScope {
    /// Look up a table alias in this scope or parent scopes
    pub fn alias_resolve(&self, alias: &str) -> Option<&QualifiedTableRef>;

    /// Get all tables available in this scope
    pub fn tables_available(&self) -> Vec<&QualifiedTableRef>;

    /// Create a child scope inheriting from this one
    pub fn child_create(&self) -> ResolutionScope;
}
```

**Metadata Provider Trait:**

```rust
/// Trait for providing table metadata during resolution
pub trait TableMetadataProvider {
    /// Get metadata for a specific table
    fn table_metadata_get(
        &self,
        schema: &str,
        table: &str
    ) -> Result<&TableMetadata, ResolveError>;

    /// Resolve a table name using search path
    /// For now, just validates the schema exists
    fn table_schema_resolve(
        &self,
        table: &str,
        search_path: &[String]
    ) -> Result<String, ResolveError>;
}
```

**Resolver:**

```rust
/// Main resolver for converting unresolved AST to resolved AST
pub struct Resolver<'a> {
    metadata_provider: &'a dyn TableMetadataProvider,
    scope: ResolutionScope,
}

impl<'a> Resolver<'a> {
    pub fn new(
        metadata_provider: &'a dyn TableMetadataProvider,
        default_schema: &str
    ) -> Self;

    pub fn query_resolve(
        &mut self,
        query: &SqlQuery
    ) -> Result<ResolvedSqlQuery, ResolveError>;

    // Implementation in later phases
    fn select_statement_resolve(&mut self, stmt: &SelectStatement) -> Result<ResolvedSelectStatement, ResolveError>;
    fn column_ref_resolve(&self, col: &ColumnNode) -> Result<QualifiedColumnRef, ResolveError>;
    fn table_ref_resolve(&self, table: &TableNode) -> Result<QualifiedTableRef, ResolveError>;
}
```

**Tasks:**
- Define `ResolveError` error set with all variants
- Create `ResolutionScope` struct with lookup methods
- Implement scope parent traversal for alias lookup
- Define `TableMetadataProvider` trait
- Create `Resolver` struct skeleton
- Add tests for scope lookup and error construction

**Deliverables:**
- `pgcache/src/query/resolve.rs` with errors, scope, trait, and resolver skeleton
- Scope management tests

---

### Phase 3: Table Reference Resolution

**File**: `pgcache/src/query/resolve.rs`

**Implementation:**

```rust
impl<'a> Resolver<'a> {
    /// Resolve a table reference to a qualified table
    fn table_ref_resolve(
        &self,
        table: &TableNode
    ) -> Result<QualifiedTableRef, ResolveError> {
        // 1. Determine schema
        let schema = table.schema.as_ref()
            .map(|s| s.clone())
            .unwrap_or_else(|| self.scope.default_schema.clone());

        // 2. Validate table exists via metadata provider
        self.metadata_provider.table_metadata_get(&schema, &table.name)?;

        // 3. Build qualified reference
        Ok(QualifiedTableRef {
            schema,
            table: table.name.clone(),
            original_alias: table.alias.as_ref().map(|a| a.name.clone()),
        })
    }

    /// Process FROM clause to build resolution scope
    fn from_clause_resolve(
        &mut self,
        from: &[TableSource]
    ) -> Result<Vec<ResolvedTableSource>, ResolveError> {
        let mut resolved_tables = Vec::new();

        for table_source in from {
            match table_source {
                TableSource::Table(table_node) => {
                    let qualified = self.table_ref_resolve(table_node)?;

                    // Register alias if present
                    if let Some(alias) = &qualified.original_alias {
                        self.scope.table_aliases.insert(
                            alias.clone(),
                            qualified.clone()
                        );
                    }

                    self.scope.tables.push(qualified.clone());
                    resolved_tables.push(ResolvedTableSource::Table(qualified));
                }
                // Handle joins, subqueries in later phases
                _ => todo!("Handle joins and subqueries"),
            }
        }

        Ok(resolved_tables)
    }
}
```

**Tests:**
- Table with explicit schema: `public.users` → `QualifiedTableRef { schema: "public", table: "users", ... }`
- Table without schema: `users` → `QualifiedTableRef { schema: "public", table: "users", ... }`
- Table with alias: `users u` → store alias mapping `"u" -> QualifiedTableRef`
- Non-existent table → `ResolveError::UnknownTable`
- Schema not found → `ResolveError::SchemaNotFound`

**Tasks:**
- Implement `Resolver::table_ref_resolve()`
- Implement `Resolver::from_clause_resolve()`
- Handle explicit schema qualification
- Default to "public" schema when not specified
- Validate table exists via metadata provider
- Handle and store table aliases
- Add comprehensive tests

**Deliverables:**
- Working table resolution
- Table resolution tests

---

### Phase 4: Column Reference Resolution

**File**: `pgcache/src/query/resolve.rs`

**Implementation:**

```rust
impl<'a> Resolver<'a> {
    /// Resolve a column reference to a qualified column with type info
    fn column_ref_resolve(
        &self,
        col: &ColumnNode
    ) -> Result<QualifiedColumnRef, ResolveError> {
        match &col.table {
            // Qualified column: table.column or alias.column
            Some(table_qualifier) => {
                // Try to resolve as alias first
                let qualified_table = self.scope.alias_resolve(table_qualifier)
                    .cloned()
                    .or_else(|| {
                        // Try as direct table name
                        self.scope.tables.iter()
                            .find(|t| t.table == *table_qualifier)
                            .cloned()
                    })
                    .ok_or_else(|| ResolveError::UnknownAlias {
                        alias: table_qualifier.clone()
                    })?;

                // Get table metadata and verify column exists
                let metadata = self.metadata_provider.table_metadata_get(
                    &qualified_table.schema,
                    &qualified_table.table
                )?;

                let column_meta = metadata.columns.get1(&col.column)
                    .ok_or_else(|| ResolveError::UnknownColumn {
                        column: col.column.clone(),
                        available: metadata.columns.iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    })?;

                Ok(QualifiedColumnRef {
                    schema: qualified_table.schema.clone(),
                    table: qualified_table.table.clone(),
                    column: col.column.clone(),
                    type_info: ColumnTypeInfo {
                        type_oid: column_meta.type_oid,
                        type_name: column_meta.type_name.clone(),
                        data_type: column_meta.data_type.clone(),
                        nullable: true, // Conservative default
                    },
                })
            }

            // Unqualified column: search all tables in scope
            None => {
                self.column_search_in_scope(&col.column)
            }
        }
    }

    /// Search for unqualified column in all tables in scope
    fn column_search_in_scope(
        &self,
        column_name: &str
    ) -> Result<QualifiedColumnRef, ResolveError> {
        let mut found_in_tables = Vec::new();

        for table in &self.scope.tables {
            let metadata = self.metadata_provider.table_metadata_get(
                &table.schema,
                &table.table
            )?;

            if let Some(column_meta) = metadata.columns.get1(column_name) {
                found_in_tables.push((table.clone(), column_meta));
            }
        }

        match found_in_tables.len() {
            0 => Err(ResolveError::UnknownColumn {
                column: column_name.to_string(),
                available: self.scope.tables.iter()
                    .flat_map(|t| {
                        self.metadata_provider
                            .table_metadata_get(&t.schema, &t.table)
                            .ok()
                            .map(|m| m.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>())
                            .unwrap_or_default()
                    })
                    .collect(),
            }),
            1 => {
                let (table, column_meta) = &found_in_tables[0];
                Ok(QualifiedColumnRef {
                    schema: table.schema.clone(),
                    table: table.table.clone(),
                    column: column_name.to_string(),
                    type_info: ColumnTypeInfo {
                        type_oid: column_meta.type_oid,
                        type_name: column_meta.type_name.clone(),
                        data_type: column_meta.data_type.clone(),
                        nullable: true,
                    },
                })
            }
            _ => Err(ResolveError::AmbiguousColumn {
                column: column_name.to_string(),
                tables: found_in_tables.iter()
                    .map(|(t, _)| format!("{}.{}", t.schema, t.table))
                    .collect(),
            }),
        }
    }
}
```

**Tests:**
- Qualified column: `users.id` → resolve to `public.users.id`
- Alias-qualified: `u.id` where `u` is alias for `users` → resolve to `public.users.id`
- Unqualified unambiguous: `id` in single-table query → resolve to table's `id`
- Unqualified ambiguous: `id` in `users, orders` → `ResolveError::AmbiguousColumn`
- Non-existent column: `users.nonexistent` → `ResolveError::UnknownColumn`
- Type information attached correctly

**Tasks:**
- Implement `Resolver::column_ref_resolve()`
- Handle qualified columns (table.column or alias.column)
- Resolve aliases to actual table names
- Handle unqualified columns
- Implement `Resolver::column_search_in_scope()`
- Detect ambiguity (column in multiple tables)
- Attach type information from table metadata
- Add comprehensive tests

**Deliverables:**
- Working column resolution with ambiguity detection
- Column resolution tests

---

### Phase 5: Full Query Resolution

**File**: `pgcache/src/query/resolve.rs`

**Implementation:**

```rust
impl<'a> Resolver<'a> {
    /// Resolve a complete query (entry point)
    pub fn query_resolve(
        &mut self,
        query: &SqlQuery
    ) -> Result<ResolvedSqlQuery, ResolveError> {
        match &query.statement {
            Statement::Select(select) => {
                let resolved = self.select_statement_resolve(select)?;
                Ok(ResolvedSqlQuery {
                    statement: ResolvedStatement::Select(resolved),
                })
            }
        }
    }

    /// Resolve a SELECT statement
    fn select_statement_resolve(
        &mut self,
        stmt: &SelectStatement
    ) -> Result<ResolvedSelectStatement, ResolveError> {
        // 1. Resolve FROM clause (builds scope with tables/aliases)
        let from = self.from_clause_resolve(&stmt.from)?;

        // 2. Resolve SELECT columns using populated scope
        let columns = self.select_columns_resolve(&stmt.columns)?;

        // 3. Resolve WHERE clause
        let where_clause = stmt.where_clause.as_ref()
            .map(|expr| self.where_expr_resolve(expr))
            .transpose()?;

        // 4. Resolve GROUP BY, HAVING, ORDER BY
        let group_by = stmt.group_by.iter()
            .map(|col| self.column_ref_resolve(col))
            .collect::<Result<Vec<_>, _>>()?;

        let having = stmt.having.as_ref()
            .map(|expr| self.where_expr_resolve(expr))
            .transpose()?;

        // TODO: order_by, limit

        Ok(ResolvedSelectStatement {
            columns,
            from,
            where_clause,
            group_by,
            having,
            distinct: stmt.distinct,
            // ... other fields
        })
    }

    /// Resolve a WHERE expression recursively
    fn where_expr_resolve(
        &self,
        expr: &WhereExpr
    ) -> Result<ResolvedWhereExpr, ResolveError> {
        match expr {
            WhereExpr::Column(col) => {
                let resolved = self.column_ref_resolve(col)?;
                Ok(ResolvedWhereExpr::Column(resolved))
            }
            WhereExpr::Value(val) => {
                Ok(ResolvedWhereExpr::Value(TypedLiteralValue {
                    value: val.clone(),
                    type_oid: None, // Type inference could go here
                }))
            }
            WhereExpr::Binary(binary) => {
                Ok(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                    op: binary.op,
                    lexpr: Box::new(self.where_expr_resolve(&binary.lexpr)?),
                    rexpr: Box::new(self.where_expr_resolve(&binary.rexpr)?),
                }))
            }
            WhereExpr::Unary(unary) => {
                Ok(ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
                    op: unary.op,
                    expr: Box::new(self.where_expr_resolve(&unary.expr)?),
                }))
            }
            // ... other cases
        }
    }

    /// Resolve a subquery with new child scope
    fn subquery_resolve(
        &self,
        subquery: &SelectStatement
    ) -> Result<ResolvedSelectStatement, ResolveError> {
        // Create child scope inheriting parent
        let child_scope = self.scope.child_create();

        let mut child_resolver = Resolver {
            metadata_provider: self.metadata_provider,
            scope: child_scope,
        };

        child_resolver.select_statement_resolve(subquery)
    }
}
```

**Tests:**
- Simple query: `SELECT id FROM users WHERE id = 1`
- Multi-table with JOIN: `SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id`
- Explicit JOIN: Resolve join condition columns
- Subquery: `SELECT * FROM (SELECT id FROM users) sub WHERE id = 1`
- Nested subquery with alias shadowing: `SELECT * FROM (SELECT * FROM (SELECT id FROM users) u) u2`
- All column references fully qualified
- Type information attached

**Tasks:**
- Implement `Resolver::query_resolve()` (entry point)
- Implement `Resolver::select_statement_resolve()`
- Implement `Resolver::where_expr_resolve()` (recursive)
- Implement `Resolver::subquery_resolve()` with child scopes
- Resolve all parts of SELECT: columns, FROM, WHERE, GROUP BY, HAVING, ORDER BY
- Handle joins (resolve join conditions)
- Add integration tests for complex queries

**Deliverables:**
- Complete resolution implementation
- Integration tests for complex queries

---

### Phase 6: Constant Propagation

**File**: `pgcache/src/query/constprop.rs` (new)

**Types:**

```rust
/// Constant value tracking
#[derive(Debug, Clone, PartialEq)]
pub enum ConstantValue {
    Known(LiteralValue),      // Exactly known: x = 5
    Unknown,                   // No constraint
}

/// Context for tracking constant values during propagation
#[derive(Debug, Clone)]
pub struct ConstraintContext {
    /// Map qualified column -> known value
    column_values: HashMap<QualifiedColumnRef, ConstantValue>,
}

impl ConstraintContext {
    pub fn new() -> Self;

    /// Record that a column has a known value
    pub fn value_set(&mut self, col: QualifiedColumnRef, val: LiteralValue);

    /// Get the known value for a column, if any
    pub fn value_get(&self, col: &QualifiedColumnRef) -> Option<&LiteralValue>;
}
```

**Implementation:**

```rust
/// Propagate constants through a WHERE expression
pub fn where_expr_propagate(
    expr: &ResolvedWhereExpr,
    ctx: &mut ConstraintContext
) -> ResolvedWhereExpr {
    match expr {
        // Track equality constraints: col = value
        ResolvedWhereExpr::Binary(binary)
            if binary.op == ExprOp::Equal => {
            match (&*binary.lexpr, &*binary.rexpr) {
                (ResolvedWhereExpr::Column(col), ResolvedWhereExpr::Value(val)) => {
                    ctx.value_set(col.clone(), val.value.clone());
                    expr.clone()
                }
                (ResolvedWhereExpr::Value(val), ResolvedWhereExpr::Column(col)) => {
                    ctx.value_set(col.clone(), val.value.clone());
                    expr.clone()
                }
                _ => expr.clone(),
            }
        }

        // Propagate through AND (accumulate constraints)
        ResolvedWhereExpr::Binary(binary)
            if binary.op == ExprOp::And => {
            let left = where_expr_propagate(&binary.lexpr, ctx);
            let right = where_expr_propagate(&binary.rexpr, ctx);

            ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: ExprOp::And,
                lexpr: Box::new(left),
                rexpr: Box::new(right),
            })
        }

        // Other cases: preserve expression
        _ => expr.clone(),
    }
}

/// Fold constants in a WHERE expression
pub fn where_expr_fold(
    expr: &ResolvedWhereExpr,
    ctx: &ConstraintContext
) -> ResolvedWhereExpr {
    match expr {
        // Replace column with known value
        ResolvedWhereExpr::Column(col) => {
            if let Some(val) = ctx.value_get(col) {
                ResolvedWhereExpr::Value(TypedLiteralValue {
                    value: val.clone(),
                    type_oid: Some(col.type_info.type_oid),
                })
            } else {
                expr.clone()
            }
        }

        // Fold binary operations on constants
        ResolvedWhereExpr::Binary(binary) => {
            let left = where_expr_fold(&binary.lexpr, ctx);
            let right = where_expr_fold(&binary.rexpr, ctx);

            // Try to evaluate if both sides are constants
            match (&left, &right) {
                (ResolvedWhereExpr::Value(lval), ResolvedWhereExpr::Value(rval)) => {
                    if let Some(result) = binary_const_evaluate(binary.op, &lval.value, &rval.value) {
                        ResolvedWhereExpr::Value(TypedLiteralValue {
                            value: LiteralValue::Boolean(result),
                            type_oid: Some(16), // BOOL type OID
                        })
                    } else {
                        ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                            op: binary.op,
                            lexpr: Box::new(left),
                            rexpr: Box::new(right),
                        })
                    }
                }
                _ => ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                    op: binary.op,
                    lexpr: Box::new(left),
                    rexpr: Box::new(right),
                }),
            }
        }

        _ => expr.clone(),
    }
}

/// Evaluate a binary operation on constant values
fn binary_const_evaluate(
    op: ExprOp,
    left: &LiteralValue,
    right: &LiteralValue
) -> Option<bool> {
    match (op, left, right) {
        (ExprOp::Equal, LiteralValue::Integer(l), LiteralValue::Integer(r)) => Some(l == r),
        (ExprOp::GreaterThan, LiteralValue::Integer(l), LiteralValue::Integer(r)) => Some(l > r),
        // ... other cases
        _ => None,
    }
}

/// Optimize a query by propagating constants and folding
pub fn query_optimize(query: &ResolvedSqlQuery) -> ResolvedSqlQuery {
    match &query.statement {
        ResolvedStatement::Select(select) => {
            let mut ctx = ConstraintContext::new();

            // Propagate through WHERE clause
            let where_clause = select.where_clause.as_ref()
                .map(|expr| {
                    let propagated = where_expr_propagate(expr, &mut ctx);
                    where_expr_fold(&propagated, &ctx)
                });

            ResolvedSqlQuery {
                statement: ResolvedStatement::Select(ResolvedSelectStatement {
                    where_clause,
                    ..select.clone()
                }),
            }
        }
    }
}
```

**Tests:**
- Simple constant: `WHERE x = 5 AND x > 3` → track `x = 5`, fold to `WHERE 5 = 5 AND 5 > 3` → `WHERE true AND true`
- Multiple constraints: `WHERE x = 5 AND y = 10` → track both
- Constant folding: `WHERE 5 > 3` → `WHERE true`
- Boolean simplification: `WHERE true AND x > 5` → `WHERE x > 5`
- Contradiction detection: `WHERE x = 5 AND x = 6` → detect impossible constraint
- No-op: `WHERE x > 5 AND y < 10` → no constants to fold, unchanged

**Tasks:**
- Create `ConstantValue` enum and `ConstraintContext`
- Implement `where_expr_propagate()` to track equality constraints
- Implement `where_expr_fold()` to replace columns with known values
- Implement `binary_const_evaluate()` for constant operations
- Implement `query_optimize()` entry point
- Add comprehensive tests

**Deliverables:**
- Constant propagation implementation
- Constant folding tests

---

### Phase 7: Integration with Cache System

**Files**: Update existing files

**Changes to make:**

1. **Add metadata provider implementation**
   - Location: `pgcache/src/cache/` (wherever cache system lives)
   - Implement `TableMetadataProvider` trait
   - Use existing `TableMetadata` infrastructure

2. **Update query analysis pipeline**
   - Location: Query cache or analysis entry point
   - Add resolution step: `unresolved AST → resolved AST`
   - Add optimization step: `resolved AST → optimized AST`
   - Handle `ResolveError` appropriately

```rust
// Example integration
pub fn query_analyze(&mut self, sql: &str) -> Result<AnalyzedQuery, Error> {
    // 1. Parse to unresolved AST
    let pg_ast = pg_query::parse(sql)?;
    let unresolved = sql_query_convert(&pg_ast)?;

    // 2. Resolve references
    let metadata_provider = self.metadata_provider();
    let mut resolver = Resolver::new(metadata_provider, "public");
    let resolved = resolver.query_resolve(&unresolved)?;

    // 3. Propagate constants
    let optimized = constprop::query_optimize(&resolved);

    Ok(AnalyzedQuery {
        unresolved,
        resolved,
        optimized,
    })
}
```

3. **Update evaluation to use resolved AST**
   - Location: `pgcache/src/query/evaluate.rs`
   - Change signature: `where_expr_evaluate(expr: &ResolvedWhereExpr, ...)`
   - Use qualified column refs (eliminates ambiguity in lookups)
   - Leverage type information for better comparisons

```rust
pub fn where_expr_evaluate(
    expr: &ResolvedWhereExpr,  // Changed from WhereExpr
    row_data: &[Option<String>],
    table_metadata: &TableMetadata,
) -> bool {
    match expr {
        ResolvedWhereExpr::Binary(binary_expr) => {
            // Column lookups now use fully qualified refs
            // No need to search or handle ambiguity
            // ...
        }
        // ...
    }
}
```

4. **Update module exports**
   - Location: `pgcache/src/query/mod.rs`
   - Export new modules: `pub mod resolved;`, `pub mod resolve;`, `pub mod constprop;`

**Tasks:**
- Implement `TableMetadataProvider` for cache system
- Integrate resolution and optimization into query analysis pipeline
- Update `evaluate.rs` to use `ResolvedWhereExpr`
- Update module exports
- Add end-to-end tests (parse → resolve → optimize → evaluate)

**Deliverables:**
- Fully integrated system
- End-to-end tests

---

### Phase 8: Comprehensive Testing

**Test Categories:**

1. **Unit Tests** (already covered in phases above):
   - `resolved.rs`: Type construction and equality
   - `resolve.rs`: Each resolution function independently
   - `constprop.rs`: Propagation and folding algorithms

2. **Integration Tests**:
   - Complex multi-table queries with aliases
   - Nested subqueries with scope shadowing
   - JOINs with complex conditions
   - Error cases (ambiguous columns, unknown tables, unknown aliases)
   - Constant propagation through complex WHERE clauses
   - Verify type information is correctly attached

3. **Edge Cases**:
   - Self-joins: `SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.parent_id`
   - Deeply nested subqueries (3+ levels)
   - Ambiguity across schemas: `SELECT id FROM schema1.users, schema2.users`
   - Column references in all parts: SELECT, WHERE, GROUP BY, HAVING, ORDER BY
   - Empty result sets, NULL handling
   - Type mismatches in constant folding

4. **Property-Based Tests** (optional but valuable):
   - Any successfully resolved query should have all columns fully qualified
   - Column references should never be ambiguous after resolution
   - Constant folding should preserve query semantics

**Tasks:**
- Ensure comprehensive coverage of all phases
- Add integration tests for complex scenarios
- Add edge case tests
- Consider property-based tests for invariants
- Verify error messages are helpful

**Deliverables:**
- Comprehensive test suite with >90% coverage
- Edge cases handled correctly

---

### Phase 9: Documentation and ADR

**ADR Document:**

`pgcache/ADR/ADR-002-resolved-ast-and-constant-propagation.md`

**Structure:**
```markdown
# ADR-002: Resolved AST and Constant Propagation

## Status
Accepted

## Context
- Query analysis needs to work with multiple tables and ambiguous column references
- Cache invalidation requires understanding which tables/columns a query depends on
- Optimization opportunities exist for constant folding in WHERE clauses
- Current unresolved AST doesn't track full qualification or types

## Decision
Implement a three-phase architecture:
1. Unresolved AST: Direct conversion from pg_query protobuf
2. Resolved AST: Fully qualified names with type information
3. Optimized AST: Constant propagation and folding applied

Keep each phase separate for clarity, testability, and extensibility.

## Consequences

### Positive
- Unambiguous column references enable accurate cache invalidation
- Type information improves query evaluation accuracy
- Constant folding reduces redundant WHERE clause evaluation
- Clear separation of concerns makes code easier to test and maintain
- Can cache unresolved AST and re-resolve when metadata changes

### Negative
- Additional memory overhead (3 AST representations)
- Processing overhead for resolution and optimization
- Complexity: More code to maintain

### Trade-offs
- Performance vs. Correctness: Chose correctness - better to be slow and right
- Simplicity vs. Capability: Chose capability - need full qualification for correctness
- Memory vs. Speed: Acceptable trade-off for better semantics

## Alternatives Considered
- Single mutable AST: Rejected due to loss of information and debugging difficulty
- Lazy resolution: Rejected because resolution failure indicates malformed query
- No constant propagation: Would miss optimization opportunities

## Implementation Notes
- Default schema is "public" (future: respect search_path)
- Subqueries create new scopes with parent fallback
- Resolution failures are hard errors
- Start with simple constant folding (can extend to range analysis later)
```

**Code Documentation:**

1. **Module-level docs**:
   - `resolved.rs`: Explain resolved AST structure, show examples
   - `resolve.rs`: Document resolution algorithm, scope rules, lookup order
   - `constprop.rs`: Document propagation rules, folding rules, limitations

2. **Public API docs**:
   - Add doc examples to `Resolver::query_resolve()`
   - Add doc examples to `query_optimize()`
   - Document error conditions

3. **Update CLAUDE.md**:
   - Add section on query analysis architecture
   - Document the three-phase pipeline
   - Add references to new modules

**Tasks:**
- Write ADR-002
- Add comprehensive module-level documentation
- Add doc examples to all public APIs
- Update CLAUDE.md with architecture notes
- Ensure all public items have doc comments

**Deliverables:**
- ADR-002 document
- Comprehensive code documentation
- Updated CLAUDE.md

---

## Open Questions

### 1. Caching Strategy
**Question**: Should we cache resolved ASTs, or just unresolved and re-resolve on demand?

**Options**:
- **Cache resolved ASTs**: Faster for repeated queries, but invalidate when metadata changes
- **Cache only unresolved**: Simpler invalidation, but slower for repeated queries
- **Cache both**: Best performance, most complexity

**Recommendation**: Start with caching only unresolved ASTs. Re-resolve on demand. Can optimize later if profiling shows resolution is a bottleneck.

---

### 2. Search Path Handling
**Question**: Should we expose search_path configuration, or always default to "public"?

**Options**:
- **Always use "public"**: Simple, but incorrect for non-public schemas
- **Per-connection search_path**: Accurate, but complex (need to track per connection)
- **Global search_path config**: Middle ground - configurable but shared

**Recommendation**: Start with always using "public". Add search_path support in a future iteration when we track connection-level state.

---

### 3. Type Coercion
**Question**: Should constant propagation handle type coercions (e.g., `'5'::int = 5`)?

**Options**:
- **No type coercion**: Simple, but misses optimization opportunities
- **PostgreSQL-compatible coercion**: Accurate, but very complex to implement correctly
- **Simple numeric coercion**: Middle ground - handle common cases

**Recommendation**: Start with no type coercion. Can add simple numeric coercion in a future iteration.

---

## Implementation Order Summary

1. **Phase 1**: Resolved AST Types (`resolved.rs`)
2. **Phase 2**: Resolution Context and Errors (`resolve.rs`)
3. **Phase 3**: Table Reference Resolution
4. **Phase 4**: Column Reference Resolution
5. **Phase 5**: Full Query Resolution (including subqueries)
6. **Phase 6**: Constant Propagation (`constprop.rs`)
7. **Phase 7**: Integration with Cache System
8. **Phase 8**: Comprehensive Testing
9. **Phase 9**: Documentation and ADR

---

## Success Criteria

- All table and column references are fully qualified after resolution
- Ambiguous references are detected and reported with helpful errors
- Type information is correctly attached to all resolved references
- Constant folding simplifies WHERE clauses where possible
- Integration tests pass for complex multi-table queries
- Documentation is complete and helpful
- Code coverage >90%

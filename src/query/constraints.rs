use std::collections::{HashMap, HashSet};

use crate::query::ast::{BinaryOp, LiteralValue, MultiOp};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedSelectNode, ResolvedTableSource, ResolvedWhereExpr,
};

/// A constraint comparing a column to a constant value with a comparison operator
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnConstraint {
    pub column: ResolvedColumnNode,
    pub op: BinaryOp,
    pub value: LiteralValue,
}

/// An equivalence between two columns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnEquivalence {
    pub left: ResolvedColumnNode,
    pub right: ResolvedColumnNode,
}

impl ColumnEquivalence {
    /// Returns true if this equivalence represents a join condition:
    /// columns from different tables, or same table with different aliases (self-join)
    pub fn is_join(&self) -> bool {
        self.left.table != self.right.table || self.left.table_alias != self.right.table_alias
    }
}

/// Analysis results for a query showing all constant constraints
#[derive(Debug, Clone, Default)]
pub struct QueryConstraints {
    /// All column constraints (from WHERE + propagated through JOINs)
    pub column_constraints: HashSet<ColumnConstraint>,

    /// Column equivalences from JOIN conditions and WHERE clause
    pub equivalences: HashSet<ColumnEquivalence>,

    /// Constraints organized by table for quick lookup
    pub table_constraints: HashMap<String, Vec<(String, BinaryOp, LiteralValue)>>,
}

impl QueryConstraints {
    /// Returns column names involved in join conditions for the given table
    pub fn table_join_columns<'a>(&'a self, table_name: &'a str) -> impl Iterator<Item = &'a str> {
        self.equivalences
            .iter()
            .filter(|eq| eq.is_join())
            .filter_map(move |eq| {
                if eq.left.table == table_name {
                    Some(eq.left.column.as_str())
                } else if eq.right.table == table_name {
                    Some(eq.right.column.as_str())
                } else {
                    None
                }
            })
    }
}

/// Extract constraint information from any resolved WHERE expression.
/// Handles equality, inequality, and BETWEEN operators on column-vs-literal comparisons.
fn analyze_constraint_expr(
    expr: &ResolvedWhereExpr,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
) {
    match expr {
        // Comparison operators: column op value, value op column, column = column
        ResolvedWhereExpr::Binary(binary) if binary.op.is_comparison() => {
            match (&*binary.lexpr, &*binary.rexpr) {
                // column op literal
                (ResolvedWhereExpr::Column(col), ResolvedWhereExpr::Value(val)) => {
                    constraints.insert(ColumnConstraint {
                        column: col.clone(),
                        op: binary.op,
                        value: val.clone(),
                    });
                }
                // literal op column → column op_flip literal
                (ResolvedWhereExpr::Value(val), ResolvedWhereExpr::Column(col)) => {
                    if let Some(flipped) = binary.op.op_flip() {
                        constraints.insert(ColumnConstraint {
                            column: col.clone(),
                            op: flipped,
                            value: val.clone(),
                        });
                    }
                }
                // column = column (equivalence) — equality only
                (ResolvedWhereExpr::Column(left), ResolvedWhereExpr::Column(right))
                    if binary.op == BinaryOp::Equal =>
                {
                    equivalences.insert(ColumnEquivalence {
                        left: left.clone(),
                        right: right.clone(),
                    });
                }
                _ => {}
            }
        }

        // AND: recursively analyze both sides
        ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::And => {
            analyze_constraint_expr(&binary.lexpr, constraints, equivalences);
            analyze_constraint_expr(&binary.rexpr, constraints, equivalences);
        }

        // BETWEEN / BETWEEN SYMMETRIC: extract as two inequality constraints
        ResolvedWhereExpr::Multi(multi)
            if matches!(multi.op, MultiOp::Between | MultiOp::BetweenSymmetric) =>
        {
            between_constraints_extract(&multi.op, &multi.exprs, constraints);
        }

        // Everything else: OR, NOT BETWEEN, IN, subqueries, etc. — cannot extract constraints
        ResolvedWhereExpr::Value(_)
        | ResolvedWhereExpr::Column(_)
        | ResolvedWhereExpr::Unary(_)
        | ResolvedWhereExpr::Binary(_)
        | ResolvedWhereExpr::Multi(_)
        | ResolvedWhereExpr::Array(_)
        | ResolvedWhereExpr::Function { .. }
        | ResolvedWhereExpr::Subquery { .. } => {}
    }
}

/// Extract two inequality constraints from a BETWEEN or BETWEEN SYMMETRIC expression.
/// BETWEEN: column >= low AND column <= high
/// BETWEEN SYMMETRIC: same, but bounds are normalized to (min, max) first.
fn between_constraints_extract(
    op: &MultiOp,
    exprs: &[ResolvedWhereExpr],
    constraints: &mut HashSet<ColumnConstraint>,
) {
    // exprs layout: [subject, low, high]
    let [ResolvedWhereExpr::Column(col), ResolvedWhereExpr::Value(low), ResolvedWhereExpr::Value(high)] =
        exprs
    else {
        return;
    };

    let (low, high) = if *op == MultiOp::BetweenSymmetric {
        match literal_value_order(low, high) {
            Some(std::cmp::Ordering::Greater) => (high, low),
            Some(_) => (low, high),
            None => return, // can't compare bounds (Parameter, Null, mixed types)
        }
    } else {
        (low, high)
    };

    constraints.insert(ColumnConstraint {
        column: col.clone(),
        op: BinaryOp::GreaterThanOrEqual,
        value: low.clone(),
    });
    constraints.insert(ColumnConstraint {
        column: col.clone(),
        op: BinaryOp::LessThanOrEqual,
        value: high.clone(),
    });
}

/// Compare two literal values for ordering. Returns None if the values
/// are not comparable (different types, Parameters, Nulls).
fn literal_value_order(a: &LiteralValue, b: &LiteralValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a.cmp(b)),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a.cmp(b)),
        (LiteralValue::String(a), LiteralValue::String(b)) => Some(a.cmp(b)),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => {
            Some(a.cmp(b))
        }
        _ => None,
    }
}

/// Collect constraints and equivalences from a table source (handles JOINs recursively)
fn collect_from_table_source(
    source: &ResolvedTableSource,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
) {
    if let ResolvedTableSource::Join(join) = source {
        // Analyze this join's condition
        if let Some(condition) = &join.condition {
            analyze_constraint_expr(condition, constraints, equivalences);
        }

        // Recurse into nested joins
        collect_from_table_source(&join.left, constraints, equivalences);
        collect_from_table_source(&join.right, constraints, equivalences);
    }
}

/// Collect all constraints and equivalences from the entire query
fn collect_query_constraints(
    resolved: &ResolvedSelectNode,
) -> (HashSet<ColumnConstraint>, HashSet<ColumnEquivalence>) {
    let mut constraints = HashSet::new();
    let mut equivalences = HashSet::new();

    // Analyze WHERE clause
    if let Some(where_expr) = &resolved.where_clause {
        analyze_constraint_expr(where_expr, &mut constraints, &mut equivalences);
    }

    // Analyze JOIN conditions
    for table_source in &resolved.from {
        collect_from_table_source(table_source, &mut constraints, &mut equivalences);
    }

    (constraints, equivalences)
}

/// Propagate constraints through column equivalences using fixpoint iteration
fn propagate_constraints(
    mut constraints: HashSet<ColumnConstraint>,
    equivalences: &HashSet<ColumnEquivalence>,
) -> HashSet<ColumnConstraint> {
    // Fixpoint iteration: propagate until no changes
    let mut changed = true;
    while changed {
        changed = false;

        let mut new_constraints = Vec::new();

        for equiv in equivalences {
            // Collect constraints on either side and propagate to the other
            for constraint in &constraints {
                if constraint.column == equiv.left {
                    new_constraints.push(ColumnConstraint {
                        column: equiv.right.clone(),
                        op: constraint.op,
                        value: constraint.value.clone(),
                    });
                } else if constraint.column == equiv.right {
                    new_constraints.push(ColumnConstraint {
                        column: equiv.left.clone(),
                        op: constraint.op,
                        value: constraint.value.clone(),
                    });
                }
            }
        }

        for constraint in new_constraints {
            if constraints.insert(constraint) {
                changed = true;
            }
        }
    }

    constraints
}

/// Analyze a resolved query to determine all constant constraints on columns.
///
/// Subquery terms in WHERE clauses are naturally skipped by `analyze_equality_expr`,
/// so outer constraints (e.g., `AND tenant_id = 1`) are still correctly extracted
/// even when subqueries are present.
pub fn analyze_query_constraints(resolved: &ResolvedSelectNode) -> QueryConstraints {
    // Step 1: Collect all constraint information (constraints + equivalences)
    let (constraints, equivalences) = collect_query_constraints(resolved);

    // Step 2: Propagate constraints through equivalences
    let column_constraints = propagate_constraints(constraints, &equivalences);

    // Step 3: Organize by table for quick lookup
    let mut table_constraints: HashMap<String, Vec<(String, BinaryOp, LiteralValue)>> =
        HashMap::new();
    for constraint in &column_constraints {
        table_constraints
            .entry(constraint.column.table.clone())
            .or_default()
            .push((
                constraint.column.column.clone(),
                constraint.op,
                constraint.value.clone(),
            ));
    }

    QueryConstraints {
        column_constraints,
        equivalences,
        table_constraints,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::unwrap_used)]

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::catalog::{ColumnMetadata, TableMetadata};
    use crate::query::ast::{QueryBody, query_expr_convert};
    use crate::query::resolved::select_node_resolve;

    use super::*;

    // Helper function to parse SQL and resolve to ResolvedSelectNode
    fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        let QueryBody::Select(node) = query_expr.body else {
            panic!("expected SELECT");
        };
        select_node_resolve(&node, tables, &["public"]).expect("resolve")
    }

    // Helper function to create test table metadata
    fn test_table_metadata(name: &str, relation_oid: u32) -> TableMetadata {
        let mut columns = BiHashMap::new();

        columns.insert_overwrite(ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".to_owned(),
            cache_type_name: "int4".to_owned(),
            is_primary_key: true,
        });

        columns.insert_overwrite(ColumnMetadata {
            name: "name".to_owned(),
            position: 2,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".to_owned(),
            cache_type_name: "text".to_owned(),
            is_primary_key: false,
        });

        TableMetadata {
            relation_oid,
            name: name.to_owned(),
            schema: "public".to_owned(),
            primary_key_columns: vec!["id".to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Helper to check if table_constraints contains a specific (column, op, value) triple
    fn has_constraint(
        constraints: &QueryConstraints,
        table: &str,
        column: &str,
        op: BinaryOp,
        value: LiteralValue,
    ) -> bool {
        constraints
            .table_constraints
            .get(table)
            .is_some_and(|cs| cs.iter().any(|(c, o, v)| c == column && *o == op && *v == value))
    }

    // ========== Existing equality tests (updated for new tuple format) ==========

    #[test]
    fn test_simple_constraint() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert_eq!(constraints.table_constraints.get("users").unwrap().len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_join_propagation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("test", 1001));
        tables.insert_overwrite(test_table_metadata("test_map", 1002));

        let sql = "SELECT * FROM test t JOIN test_map tm ON tm.id = t.id WHERE t.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate: t.id = 1 -> tm.id = 1
        assert_eq!(constraints.column_constraints.len(), 2);

        let test_constraints = constraints.table_constraints.get("test").unwrap();
        assert_eq!(test_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "test",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));

        let test_map_constraints = constraints.table_constraints.get("test_map").unwrap();
        assert_eq!(test_map_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "test_map",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));

        assert_eq!(constraints.equivalences.len(), 1);
    }

    #[test]
    fn test_transitive_propagation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let mut c_columns = BiHashMap::new();
        c_columns.insert_overwrite(ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".to_owned(),
            cache_type_name: "int4".to_owned(),
            is_primary_key: true,
        });
        tables.insert_overwrite(TableMetadata {
            relation_oid: 1003,
            name: "c".to_owned(),
            schema: "public".to_owned(),
            primary_key_columns: vec!["id".to_owned()],
            columns: c_columns,
            indexes: Vec::new(),
        });

        let sql = "SELECT * FROM (a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id WHERE a.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate through: a.id = 1 -> b.id = 1 -> c.id = 1
        assert_eq!(constraints.column_constraints.len(), 3);

        assert!(has_constraint(&constraints, "a", "id", BinaryOp::Equal, LiteralValue::Integer(1)));
        assert!(has_constraint(&constraints, "b", "id", BinaryOp::Equal, LiteralValue::Integer(1)));
        assert!(has_constraint(&constraints, "c", "id", BinaryOp::Equal, LiteralValue::Integer(1)));
    }

    #[test]
    fn test_multiple_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 AND name = 'john'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);

        let user_constraints = constraints.table_constraints.get("users").unwrap();
        assert_eq!(user_constraints.len(), 2);

        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("john".to_owned()),
        ));
    }

    #[test]
    fn test_equivalence_in_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a, b WHERE a.id = b.id AND a.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(&constraints, "a", "id", BinaryOp::Equal, LiteralValue::Integer(1)));
        assert!(has_constraint(&constraints, "b", "id", BinaryOp::Equal, LiteralValue::Integer(1)));
    }

    #[test]
    fn test_no_propagation_with_or() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 OR id = 2";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
        assert_eq!(constraints.table_constraints.len(), 0);
    }

    #[test]
    fn test_self_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("test", 1001));

        let sql = "SELECT * FROM test t1 JOIN test t2 ON t1.id = t2.id WHERE t1.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Both t1.id and t2.id reference the same column (test.id)
        // So we get 1 unique column with constraint
        assert_eq!(constraints.column_constraints.len(), 1);

        let test_constraints = constraints.table_constraints.get("test").unwrap();
        assert_eq!(test_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "test",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_subquery_extracts_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_derived_table_no_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert!(
            constraints.column_constraints.is_empty(),
            "Derived table with no outer WHERE should have no constraints"
        );
    }

    #[test]
    fn test_scalar_subquery_extracts_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql = "SELECT id, (SELECT COUNT(*) FROM orders) FROM users WHERE id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_subquery_multiple_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1 AND name = 'alice'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("alice".to_owned()),
        ));
    }

    // ========== Inequality tests ==========

    #[test]
    fn test_simple_inequality() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id > 5";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
    }

    #[test]
    fn test_multiple_inequalities_same_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id > 5 AND id < 100";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThan,
            LiteralValue::Integer(100),
        ));
    }

    #[test]
    fn test_reversed_operand() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // 5 < id is equivalent to id > 5
        let sql = "SELECT * FROM users WHERE 5 < id";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
    }

    #[test]
    fn test_not_equal() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE name != 'deleted'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::NotEqual,
            LiteralValue::String("deleted".to_owned()),
        ));
    }

    #[test]
    fn test_mixed_equality_and_inequality() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 AND name != 'deleted'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::NotEqual,
            LiteralValue::String("deleted".to_owned()),
        ));
    }

    #[test]
    fn test_inequality_propagation_through_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id > 5";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate: a.id > 5 -> b.id > 5
        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "a",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
        assert!(has_constraint(
            &constraints,
            "b",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
    }

    #[test]
    fn test_or_prevents_inequality_extraction() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id > 5 OR id < 2";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    // ========== BETWEEN tests ==========

    #[test]
    fn test_between() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id BETWEEN 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_with_and() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE name = 'alice' AND id BETWEEN 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 3);
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("alice".to_owned()),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_propagation_through_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id BETWEEN 1 AND 10";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Both tables should get the two BETWEEN constraints
        assert_eq!(constraints.column_constraints.len(), 4);
        assert!(has_constraint(&constraints, "a", "id", BinaryOp::GreaterThanOrEqual, LiteralValue::Integer(1)));
        assert!(has_constraint(&constraints, "a", "id", BinaryOp::LessThanOrEqual, LiteralValue::Integer(10)));
        assert!(has_constraint(&constraints, "b", "id", BinaryOp::GreaterThanOrEqual, LiteralValue::Integer(1)));
        assert!(has_constraint(&constraints, "b", "id", BinaryOp::LessThanOrEqual, LiteralValue::Integer(10)));
    }

    #[test]
    fn test_not_between() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // NOT BETWEEN is an OR (id < 100 OR id > 500), so no constraints
        let sql = "SELECT * FROM users WHERE id NOT BETWEEN 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    #[test]
    fn test_between_symmetric_reversed_bounds() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Bounds are reversed (500, 100) — should normalize to (100, 500)
        let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 500 AND 100";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_symmetric_already_ordered() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Bounds already in order — same result as reversed
        let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_symmetric_with_parameter() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Can't compare parameter with literal — skip extraction
        let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC $1 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    #[test]
    fn test_not_between_symmetric() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // NOT BETWEEN SYMMETRIC is still an OR — no constraints
        let sql = "SELECT * FROM users WHERE id NOT BETWEEN SYMMETRIC 500 AND 100";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    #[test]
    fn test_between_with_non_literal_bounds() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Non-literal bound (column reference) — skip extraction
        let sql = "SELECT * FROM users WHERE id BETWEEN name AND 10";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }
}

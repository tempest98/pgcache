use std::collections::{HashMap, HashSet};

use crate::query::ast::{ExprOp, LiteralValue};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedSelectStatement, ResolvedTableSource, ResolvedWhereExpr,
};

/// A constraint that a column equals a constant value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnConstraint {
    pub column: ResolvedColumnNode,
    pub value: LiteralValue,
}

/// An equivalence between two columns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnEquivalence {
    pub left: ResolvedColumnNode,
    pub right: ResolvedColumnNode,
}

/// Analysis results for a query showing all constant constraints
#[derive(Debug, Clone)]
pub struct QueryConstraints {
    /// All column constraints (from WHERE + propagated through JOINs)
    pub column_constraints: HashMap<ResolvedColumnNode, LiteralValue>,

    /// Column equivalences from JOIN conditions and WHERE clause
    pub equivalences: HashSet<ColumnEquivalence>,

    /// Constraints organized by table for quick lookup
    pub table_constraints: HashMap<String, Vec<(String, LiteralValue)>>, // table -> [(column, value)]
}

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

/// Collect constraints and equivalences from a table source (handles JOINs recursively)
fn collect_from_table_source(
    source: &ResolvedTableSource,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
) {
    if let ResolvedTableSource::Join(join) = source {
        // Analyze this join's condition
        if let Some(condition) = &join.condition {
            analyze_equality_expr(condition, constraints, equivalences);
        }

        // Recurse into nested joins
        collect_from_table_source(&join.left, constraints, equivalences);
        collect_from_table_source(&join.right, constraints, equivalences);
    }
}

/// Collect all equality constraints and equivalences from the entire query
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

/// Propagate constraints through column equivalences using fixpoint iteration
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
            if let Some(value) = constraints.get(&equiv.left)
                && !constraints.contains_key(&equiv.right)
            {
                constraints.insert(equiv.right.clone(), value.clone());
                changed = true;
            }

            // If right has constraint but left doesn't, propagate to left
            if let Some(value) = constraints.get(&equiv.right)
                && !constraints.contains_key(&equiv.left)
            {
                constraints.insert(equiv.left.clone(), value.clone());
                changed = true;
            }

            // TODO: Detect conflicts (same column, different values)
            // This would indicate an impossible query (empty result set)
        }
    }

    constraints
}

/// Analyze a resolved query to determine all constant constraints on columns
pub fn analyze_query_constraints(resolved: &ResolvedSelectStatement) -> QueryConstraints {
    // Step 1: Collect all equality information (constraints + equivalences)
    let (constraints, equivalences) = collect_query_equalities(resolved);

    // Step 2: Propagate constraints through equivalences
    let column_constraints = propagate_constraints(constraints, &equivalences);

    // Step 3: Organize by table for quick lookup
    let mut table_constraints: HashMap<String, Vec<(String, LiteralValue)>> = HashMap::new();
    for (col, val) in &column_constraints {
        table_constraints
            .entry(col.table.clone())
            .or_default()
            .push((col.column.clone(), val.clone()));
    }

    QueryConstraints {
        column_constraints,
        equivalences,
        table_constraints,
    }
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::catalog::{ColumnMetadata, TableMetadata};
    use crate::query::ast::{Statement, sql_query_convert};
    use crate::query::resolved::select_statement_resolve;

    use super::*;

    // Helper function to create test table metadata
    fn test_table_metadata(name: &str, relation_oid: u32) -> TableMetadata {
        let mut columns = BiHashMap::new();

        columns.insert_overwrite(ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".to_owned(),
            is_primary_key: true,
        });

        columns.insert_overwrite(ColumnMetadata {
            name: "name".to_owned(),
            position: 2,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".to_owned(),
            is_primary_key: false,
        });

        TableMetadata {
            relation_oid,
            name: name.to_owned(),
            schema: "public".to_owned(),
            primary_key_columns: vec!["id".to_owned()],
            columns,
        }
    }

    #[test]
    fn test_simple_constraint() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // Should have constraint: users.id = 1
        assert_eq!(constraints.column_constraints.len(), 1);
        assert_eq!(constraints.table_constraints.get("users").unwrap().len(), 1);
        assert_eq!(
            constraints.table_constraints.get("users").unwrap()[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
    }

    #[test]
    fn test_join_propagation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("test", 1001));
        tables.insert_overwrite(test_table_metadata("test_map", 1002));

        let sql = "SELECT * FROM test t JOIN test_map tm ON tm.id = t.id WHERE t.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate: t.id = 1 -> tm.id = 1
        assert_eq!(constraints.column_constraints.len(), 2);

        let test_constraints = constraints.table_constraints.get("test").unwrap();
        assert_eq!(test_constraints.len(), 1);
        assert_eq!(
            test_constraints[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );

        let test_map_constraints = constraints.table_constraints.get("test_map").unwrap();
        assert_eq!(test_map_constraints.len(), 1);
        assert_eq!(
            test_map_constraints[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );

        // Should have one equivalence
        assert_eq!(constraints.equivalences.len(), 1);
    }

    #[test]
    fn test_transitive_propagation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        // Add a third table 'c'
        let mut c_columns = BiHashMap::new();
        c_columns.insert_overwrite(ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".to_owned(),
            is_primary_key: true,
        });
        tables.insert_overwrite(TableMetadata {
            relation_oid: 1003,
            name: "c".to_owned(),
            schema: "public".to_owned(),
            primary_key_columns: vec!["id".to_owned()],
            columns: c_columns,
        });

        // Use parentheses to avoid parser issues with nested JOINs
        let sql = "SELECT * FROM (a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id WHERE a.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate through: a.id = 1 -> b.id = 1 -> c.id = 1
        assert_eq!(constraints.column_constraints.len(), 3);

        assert_eq!(
            constraints.table_constraints.get("a").unwrap()[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
        assert_eq!(
            constraints.table_constraints.get("b").unwrap()[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
        assert_eq!(
            constraints.table_constraints.get("c").unwrap()[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
    }

    #[test]
    fn test_multiple_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 AND name = 'john'";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // Should have two constraints
        assert_eq!(constraints.column_constraints.len(), 2);

        let user_constraints = constraints.table_constraints.get("users").unwrap();
        assert_eq!(user_constraints.len(), 2);

        // Check both constraints exist (order may vary)
        let has_id = user_constraints
            .iter()
            .any(|(col, val)| col == "id" && *val == LiteralValue::Integer(1));
        let has_name = user_constraints
            .iter()
            .any(|(col, val)| col == "name" && *val == LiteralValue::String("john".to_owned()));
        assert!(has_id);
        assert!(has_name);
    }

    #[test]
    fn test_equivalence_in_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a, b WHERE a.id = b.id AND a.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate through WHERE equivalence
        assert_eq!(constraints.column_constraints.len(), 2);
        assert_eq!(
            constraints.table_constraints.get("a").unwrap()[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
        assert_eq!(
            constraints.table_constraints.get("b").unwrap()[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
    }

    #[test]
    fn test_no_propagation_with_or() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 OR id = 2";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // OR prevents propagation - no constraints should be extracted
        assert_eq!(constraints.column_constraints.len(), 0);
        assert_eq!(constraints.table_constraints.len(), 0);
    }

    #[test]
    fn test_self_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("test", 1001));

        let sql = "SELECT * FROM test t1 JOIN test t2 ON t1.id = t2.id WHERE t1.id = 1";
        let ast = pg_query::parse(sql).unwrap();
        let sql_query = sql_query_convert(&ast).unwrap();
        let Statement::Select(stmt) = &sql_query.statement;
        let resolved = select_statement_resolve(stmt, &tables).unwrap();

        let constraints = analyze_query_constraints(&resolved);

        // Both t1.id and t2.id reference the same column (test.id)
        // So we get 1 unique column with constraint
        assert_eq!(constraints.column_constraints.len(), 1);

        let test_constraints = constraints.table_constraints.get("test").unwrap();
        // Single constraint: test.id = 1 (deduplicated)
        assert_eq!(test_constraints.len(), 1);
        assert_eq!(
            test_constraints[0],
            ("id".to_owned(), LiteralValue::Integer(1))
        );
    }
}

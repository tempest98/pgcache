#![allow(clippy::wildcard_enum_match_arm)]

use crate::query::ast::{BinaryOp, LiteralValue, UnaryOp};
use crate::query::resolved::{ResolvedBinaryExpr, ResolvedColumnNode, ResolvedWhereExpr};

/// Recursively evaluate a resolved WHERE expression against a single row.
///
/// `row_data` is the set of column values for a row from the table named `table_name`.
/// Column references targeting other tables are treated as NULL/unknown (returns false
/// from comparisons), matching the prior unresolved evaluator's behavior for columns
/// not present in the passed-in table metadata.
pub fn where_expr_evaluate(
    expr: &ResolvedWhereExpr,
    row_data: &[Option<String>],
    table_name: &str,
) -> bool {
    match expr {
        ResolvedWhereExpr::Binary(binary_expr) => match binary_expr.op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => {
                expr_comparison_evaluate(binary_expr, row_data, table_name)
            }
            BinaryOp::And => {
                where_expr_evaluate(&binary_expr.lexpr, row_data, table_name)
                    && where_expr_evaluate(&binary_expr.rexpr, row_data, table_name)
            }
            BinaryOp::Or => {
                where_expr_evaluate(&binary_expr.lexpr, row_data, table_name)
                    || where_expr_evaluate(&binary_expr.rexpr, row_data, table_name)
            }
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                // Pattern matching not yet supported
                false
            }
        },
        ResolvedWhereExpr::Unary(unary_expr) => {
            unary_expr_evaluate(&unary_expr.op, &unary_expr.expr, row_data, table_name)
        }
        _ => {
            // Unsupported expression types: Value, Column, Multi, Array, Function, Subquery
            false
        }
    }
}

/// Evaluate a comparison expression (column op value) against row data.
fn expr_comparison_evaluate(
    binary_expr: &ResolvedBinaryExpr,
    row_data: &[Option<String>],
    table_name: &str,
) -> bool {
    // Extract column and value from the comparison expression
    let (column_ref, value) = match (binary_expr.lexpr.as_ref(), binary_expr.rexpr.as_ref()) {
        (ResolvedWhereExpr::Column(col), ResolvedWhereExpr::Value(val)) => (col, val),
        (ResolvedWhereExpr::Value(val), ResolvedWhereExpr::Column(col)) => (col, val),
        _ => return false, // Should not happen if is_cacheable_expr works correctly
    };

    let row_value = column_row_value_get(column_ref, row_data, table_name);

    match row_value {
        ColumnRowValue::Present(row_value_str) => {
            where_value_compare_string(value, row_value_str, binary_expr.op)
        }
        ColumnRowValue::Null => {
            // Row has NULL value - for equality check if filter is also NULL,
            // for other comparisons NULL always returns false (SQL semantics)
            matches!(binary_expr.op, BinaryOp::Equal) && matches!(value, LiteralValue::Null)
        }
        ColumnRowValue::NotInTable => {
            // Column references a different table - can't evaluate from this row
            false
        }
    }
}

/// Evaluate a unary expression (IS NULL, IS TRUE, NOT, etc.) against row data.
fn unary_expr_evaluate(
    op: &UnaryOp,
    expr: &ResolvedWhereExpr,
    row_data: &[Option<String>],
    table_name: &str,
) -> bool {
    let value = if let ResolvedWhereExpr::Column(col) = expr {
        column_value_get(col, row_data, table_name)
    } else {
        None
    };

    match op {
        UnaryOp::Not => !where_expr_evaluate(expr, row_data, table_name),
        UnaryOp::IsNull => value.is_none(),
        UnaryOp::IsNotNull => value.is_some(),
        UnaryOp::IsTrue => matches!(value, Some("t" | "true")),
        UnaryOp::IsNotTrue => !matches!(value, Some("t" | "true")),
        UnaryOp::IsFalse => matches!(value, Some("f" | "false")),
        UnaryOp::IsNotFalse => !matches!(value, Some("f" | "false")),
    }
}

/// Look up a column's value in row data. Returns None for NULL values, or when
/// the column references a different table than the row belongs to.
fn column_value_get<'a>(
    col: &ResolvedColumnNode,
    row_data: &'a [Option<String>],
    table_name: &str,
) -> Option<&'a str> {
    if col.table.as_str() != table_name {
        return None;
    }
    let pos = col.column_metadata.position as usize - 1;
    row_data.get(pos)?.as_deref()
}

/// Three-way result distinguishing "column is in a different table" from
/// "column is NULL in this row" — the comparison evaluator handles them differently.
enum ColumnRowValue<'a> {
    Present(&'a str),
    Null,
    NotInTable,
}

fn column_row_value_get<'a>(
    col: &ResolvedColumnNode,
    row_data: &'a [Option<String>],
    table_name: &str,
) -> ColumnRowValue<'a> {
    if col.table.as_str() != table_name {
        return ColumnRowValue::NotInTable;
    }
    let pos = col.column_metadata.position as usize - 1;
    match row_data.get(pos) {
        Some(Some(v)) => ColumnRowValue::Present(v.as_str()),
        Some(None) => ColumnRowValue::Null,
        None => ColumnRowValue::NotInTable,
    }
}

/// Compare a string value from row data with a LiteralValue using the specified operator.
pub fn where_value_compare_string(
    filter_value: &LiteralValue,
    row_value_str: &str,
    op: BinaryOp,
) -> bool {
    use std::cmp::Ordering;

    match filter_value {
        LiteralValue::String(filter_str) => {
            let cmp = row_value_str.cmp(filter_str);
            match op {
                BinaryOp::Equal => cmp == Ordering::Equal,
                BinaryOp::NotEqual => cmp != Ordering::Equal,
                BinaryOp::LessThan => cmp == Ordering::Less,
                BinaryOp::LessThanOrEqual => cmp != Ordering::Greater,
                BinaryOp::GreaterThan => cmp == Ordering::Greater,
                BinaryOp::GreaterThanOrEqual => cmp != Ordering::Less,
                _ => false,
            }
        }
        LiteralValue::StringWithCast(filter_str, _cast) => {
            let cmp = row_value_str.cmp(filter_str);
            match op {
                BinaryOp::Equal => cmp == Ordering::Equal,
                BinaryOp::NotEqual => cmp != Ordering::Equal,
                BinaryOp::LessThan => cmp == Ordering::Less,
                BinaryOp::LessThanOrEqual => cmp != Ordering::Greater,
                BinaryOp::GreaterThan => cmp == Ordering::Greater,
                BinaryOp::GreaterThanOrEqual => cmp != Ordering::Less,
                _ => false,
            }
        }
        LiteralValue::Integer(filter_int) => {
            if let Ok(row_int) = row_value_str.parse::<i64>() {
                let cmp = row_int.cmp(filter_int);
                match op {
                    BinaryOp::Equal => cmp == Ordering::Equal,
                    BinaryOp::NotEqual => cmp != Ordering::Equal,
                    BinaryOp::LessThan => cmp == Ordering::Less,
                    BinaryOp::LessThanOrEqual => cmp != Ordering::Greater,
                    BinaryOp::GreaterThan => cmp == Ordering::Greater,
                    BinaryOp::GreaterThanOrEqual => cmp != Ordering::Less,
                    _ => false,
                }
            } else {
                false // Can't parse as integer
            }
        }
        LiteralValue::Float(filter_float) => {
            if let Ok(row_float) = row_value_str.parse::<f64>() {
                let filter_f64 = filter_float.into_inner();
                match op {
                    BinaryOp::Equal => (row_float - filter_f64).abs() < f64::EPSILON,
                    BinaryOp::NotEqual => (row_float - filter_f64).abs() >= f64::EPSILON,
                    BinaryOp::LessThan => row_float < filter_f64,
                    BinaryOp::LessThanOrEqual => row_float <= filter_f64,
                    BinaryOp::GreaterThan => row_float > filter_f64,
                    BinaryOp::GreaterThanOrEqual => row_float >= filter_f64,
                    _ => false,
                }
            } else {
                false // Can't parse as float
            }
        }
        LiteralValue::Boolean(filter_bool) => {
            // PostgreSQL sends booleans as "t"/"f" in text protocol
            let row_bool = match row_value_str {
                "t" | "true" => Some(true),
                "f" | "false" => Some(false),
                _ => None,
            };
            if let Some(row_bool) = row_bool {
                match op {
                    BinaryOp::Equal => row_bool == *filter_bool,
                    BinaryOp::NotEqual => row_bool != *filter_bool,
                    _ => false, // Boolean comparisons other than equality don't make sense
                }
            } else {
                false // Can't parse as boolean
            }
        }
        LiteralValue::Null => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::NullWithCast(_) => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::Parameter(_) => false, // Parameters not supported in cache matching
    }
}

/// Check if a binary expression is a simple comparison (column op value).
pub fn is_simple_comparison(binary_expr: &ResolvedBinaryExpr) -> bool {
    matches!(
        (binary_expr.lexpr.as_ref(), binary_expr.rexpr.as_ref()),
        (ResolvedWhereExpr::Column(_), ResolvedWhereExpr::Value(_))
            | (ResolvedWhereExpr::Value(_), ResolvedWhereExpr::Column(_))
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{BinaryOp, LiteralValue, UnaryOp};
    use crate::query::resolved::ResolvedUnaryExpr;
    use ecow::EcoString;
    use ordered_float::NotNan;
    use tokio_postgres::types::Type;

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn test_table_metadata() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "integer".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "active".into(),
                position: 3,
                type_oid: 16,
                data_type: Type::BOOL,
                type_name: "boolean".into(),
                cache_type_name: "bool".into(),
                is_primary_key: false,
            },
        ]);

        TableMetadata {
            name: "test_table".into(),
            schema: "public".into(),
            relation_oid: 12345,
            primary_key_columns: vec!["id".to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn resolved_column(table: &TableMetadata, column: &str) -> ResolvedColumnNode {
        let meta = table.columns.get(column).expect("column exists").clone();
        ResolvedColumnNode {
            schema: table.schema.clone(),
            table: table.name.clone(),
            table_alias: None,
            column: column.into(),
            column_metadata: meta,
        }
    }

    fn col_expr(table: &TableMetadata, column: &str) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Column(resolved_column(table, column))
    }

    fn val_expr(v: LiteralValue) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Value(v)
    }

    fn binary(
        op: BinaryOp,
        lexpr: ResolvedWhereExpr,
        rexpr: ResolvedWhereExpr,
    ) -> ResolvedBinaryExpr {
        ResolvedBinaryExpr {
            op,
            lexpr: Box::new(lexpr),
            rexpr: Box::new(rexpr),
        }
    }

    fn binary_expr(
        op: BinaryOp,
        lexpr: ResolvedWhereExpr,
        rexpr: ResolvedWhereExpr,
    ) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Binary(binary(op, lexpr, rexpr))
    }

    fn unary_expr(op: UnaryOp, expr: ResolvedWhereExpr) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
            op,
            expr: Box::new(expr),
        })
    }

    const TABLE: &str = "test_table";

    // ------------------------------------------------------------------
    // where_value_compare_string tests (shape-agnostic)
    // ------------------------------------------------------------------

    #[test]
    fn where_value_compare_string_string_match() {
        let filter_value = LiteralValue::String("hello".to_owned());
        assert!(where_value_compare_string(
            &filter_value,
            "hello",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "world",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "world",
            BinaryOp::NotEqual
        ));
    }

    #[test]
    fn where_value_compare_string_integer_match() {
        let filter_value = LiteralValue::Integer(123);
        assert!(where_value_compare_string(
            &filter_value,
            "123",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "124",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "abc",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "100",
            BinaryOp::LessThan
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "150",
            BinaryOp::GreaterThan
        ));
    }

    #[test]
    fn where_value_compare_string_float_match() {
        let filter_value = LiteralValue::Float(NotNan::new(123.45).unwrap());
        assert!(where_value_compare_string(
            &filter_value,
            "123.45",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "123.46",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "invalid",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "100.0",
            BinaryOp::LessThan
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "150.0",
            BinaryOp::GreaterThan
        ));
    }

    #[test]
    fn where_value_compare_string_boolean_match() {
        let filter_value_true = LiteralValue::Boolean(true);
        let filter_value_false = LiteralValue::Boolean(false);

        assert!(where_value_compare_string(
            &filter_value_true,
            "true",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "false",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value_true,
            "t",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "f",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value_false,
            "false",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "true",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value_false,
            "f",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "t",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "1",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "0",
            BinaryOp::Equal
        ));
    }

    #[test]
    fn where_value_compare_string_null_never_matches() {
        let filter_value = LiteralValue::Null;
        assert!(!where_value_compare_string(
            &filter_value,
            "anything",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "null",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "NULL",
            BinaryOp::Equal
        ));
    }

    #[test]
    fn where_value_compare_string_parameter_never_matches() {
        let filter_value = LiteralValue::Parameter("$1".to_owned());
        assert!(!where_value_compare_string(
            &filter_value,
            "$1",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "anything",
            BinaryOp::Equal
        ));
    }

    // ------------------------------------------------------------------
    // expr_comparison_evaluate tests
    // ------------------------------------------------------------------

    #[test]
    fn expr_comparison_evaluate_string_match() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("john".to_owned())),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_string_no_match() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("jane".to_owned())),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_integer_match() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(123)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".to_owned()), None, Some("true".to_owned())];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::Null),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_reverse_order() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        // value = column (reverse order)
        let expr = binary(
            BinaryOp::Equal,
            val_expr(LiteralValue::String("john".to_owned())),
            col_expr(&table, "name"),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // where_expr_evaluate tests
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_simple_equality() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary_expr(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("john".to_owned())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_and_operation_both_true() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(123)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".to_owned())),
            ),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_and_operation_one_false() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(999)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".to_owned())),
            ),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_or_operation_one_true() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary_expr(
            BinaryOp::Or,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(999)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".to_owned())),
            ),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_or_operation_both_false() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary_expr(
            BinaryOp::Or,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(999)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("jane".to_owned())),
            ),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_greater_than() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary_expr(
            BinaryOp::GreaterThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_unsupported_expression_type() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = ResolvedWhereExpr::Function {
            name: EcoString::from("upper"),
            args: vec![],
            agg_star: false,
        };

        assert!(!where_expr_evaluate(&expr, &row_data, table.name.as_str()));
    }

    // ------------------------------------------------------------------
    // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE / IS NULL / IS NOT NULL
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_is_true_with_true_value() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("t".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_true_with_false_value() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("f".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_true_with_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".to_owned()), Some("john".to_owned()), None];

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        // IS TRUE returns false for NULL
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_false_with_false_value() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("f".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsFalse, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_false_with_true_value() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("t".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsFalse, col_expr(&table, "active"));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_true_with_false_value() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("f".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsNotTrue, col_expr(&table, "active"));

        // IS NOT TRUE returns true for FALSE
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_true_with_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".to_owned()), Some("john".to_owned()), None];

        let expr = unary_expr(UnaryOp::IsNotTrue, col_expr(&table, "active"));

        // IS NOT TRUE returns true for NULL
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_false_with_true_value() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("t".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_false_with_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".to_owned()), Some("john".to_owned()), None];

        let expr = unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active"));

        // IS NOT FALSE returns true for NULL
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_null_via_unary() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".to_owned()), Some("john".to_owned()), None];

        let expr = unary_expr(UnaryOp::IsNull, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_null_via_unary() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("t".to_owned()),
        ];

        let expr = unary_expr(UnaryOp::IsNotNull, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Comparison operator coverage
    // ------------------------------------------------------------------

    #[test]
    fn expr_not_equal_evaluate_string_match() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::NotEqual,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("jane".to_owned())),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_not_equal_evaluate_string_no_match() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::NotEqual,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("john".to_owned())),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_evaluate_integer_true() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_equal() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("100".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_less() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_evaluate_integer_true() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_equal() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("100".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::GreaterThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_greater() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::GreaterThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::GreaterThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Type-specific coverage
    // ------------------------------------------------------------------

    #[test]
    fn expr_comparison_evaluate_float_operations() {
        let mut table = test_table_metadata();

        let mut cols: Vec<ColumnMetadata> = table.columns.iter().cloned().collect();
        cols.push(ColumnMetadata {
            name: "price".into(),
            position: 4,
            type_oid: 701,
            data_type: Type::FLOAT8,
            type_name: "double precision".into(),
            cache_type_name: "float8".into(),
            is_primary_key: false,
        });
        table.columns = ColumnStore::new(cols);

        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
            Some("99.50".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "price"),
            val_expr(LiteralValue::Float(NotNan::new(100.0).unwrap())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "price"),
            val_expr(LiteralValue::Float(NotNan::new(50.0).unwrap())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_string_operations() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("zebra".to_owned())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("alice".to_owned())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_null_handling() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".to_owned()), None, Some("true".to_owned())];

        // NULL comparisons other than equality return false
        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("test".to_owned())),
        );
        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));

        // Equality with NULL filter + NULL row value matches
        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::Null),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Cross-table column behavior (new)
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_cross_table_column_returns_false() {
        // Column from "other_table" — row_data belongs to "test_table"
        let table = test_table_metadata();
        let mut other_col = resolved_column(&table, "id");
        other_col.table = "other_table".into();

        let expr = binary_expr(
            BinaryOp::Equal,
            ResolvedWhereExpr::Column(other_col),
            val_expr(LiteralValue::Integer(1)),
        );

        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn unary_expr_evaluate_cross_table_is_null_returns_true() {
        // IS NULL on a cross-table column: column_value_get returns None,
        // so IS NULL evaluates true. Matches the prior evaluator's behavior
        // for a column absent from the passed-in table metadata.
        let table = test_table_metadata();
        let mut other_col = resolved_column(&table, "id");
        other_col.table = "other_table".into();

        let expr = unary_expr(UnaryOp::IsNull, ResolvedWhereExpr::Column(other_col));

        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }
}

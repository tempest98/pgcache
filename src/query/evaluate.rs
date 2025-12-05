#![allow(clippy::wildcard_enum_match_arm)]

use crate::catalog::TableMetadata;

use super::ast::{BinaryExpr, ExprOp, LiteralValue, WhereExpr};

/// Recursively evaluate a WHERE expression against row data.
/// Returns true if the row matches the expression, false otherwise.
pub fn where_expr_evaluate(
    expr: &WhereExpr,
    row_data: &[Option<String>],
    table_metadata: &TableMetadata,
) -> bool {
    match expr {
        WhereExpr::Binary(binary_expr) => {
            match binary_expr.op {
                ExprOp::Equal
                | ExprOp::NotEqual
                | ExprOp::LessThan
                | ExprOp::LessThanOrEqual
                | ExprOp::GreaterThan
                | ExprOp::GreaterThanOrEqual => {
                    expr_comparison_evaluate(binary_expr, row_data, table_metadata)
                }
                ExprOp::And => {
                    // Both sides must be true
                    where_expr_evaluate(&binary_expr.lexpr, row_data, table_metadata)
                        && where_expr_evaluate(&binary_expr.rexpr, row_data, table_metadata)
                }
                ExprOp::Or => {
                    // Either side can be true
                    where_expr_evaluate(&binary_expr.lexpr, row_data, table_metadata)
                        || where_expr_evaluate(&binary_expr.rexpr, row_data, table_metadata)
                }
                ExprOp::Not
                | ExprOp::Like
                | ExprOp::ILike
                | ExprOp::NotLike
                | ExprOp::NotILike
                | ExprOp::In
                | ExprOp::NotIn
                | ExprOp::Between
                | ExprOp::NotBetween
                | ExprOp::IsNull
                | ExprOp::IsNotNull
                | ExprOp::Any
                | ExprOp::All
                | ExprOp::Exists
                | ExprOp::NotExists => {
                    // Unsupported operator - should not reach here if is_cacheable_expr works correctly
                    false
                }
            }
        }
        _ => {
            // Unsupported expression type - should not reach here if is_cacheable_expr works correctly
            false
        }
    }
}

/// Evaluate a comparison expression (column op value) against row data.
fn expr_comparison_evaluate(
    binary_expr: &BinaryExpr,
    row_data: &[Option<String>],
    table_metadata: &TableMetadata,
) -> bool {
    // Extract column and value from the comparison expression
    let (column_ref, value) = match (binary_expr.lexpr.as_ref(), binary_expr.rexpr.as_ref()) {
        (WhereExpr::Column(col), WhereExpr::Value(val)) => (col, val),
        (WhereExpr::Value(val), WhereExpr::Column(col)) => (col, val),
        _ => return false, // Should not happen if is_cacheable_expr works correctly
    };

    // Find column position and get row value
    let row_value = table_metadata
        .columns
        .get1(column_ref.column.as_str())
        .and_then(|col| {
            let pos = col.position as usize - 1;
            row_data.get(pos)
        });

    match row_value {
        Some(Some(row_value_str)) => {
            // Row has non-NULL value, perform comparison
            where_value_compare_string(value, row_value_str, binary_expr.op)
        }
        Some(None) => {
            // Row has NULL value - for equality check if filter is also NULL,
            // for other comparisons NULL always returns false (SQL semantics)
            matches!(binary_expr.op, ExprOp::Equal) && matches!(value, LiteralValue::Null)
        }
        None => {
            // Column not found in table metadata
            false
        }
    }
}

/// Compare a string value from row data with a LiteralValue using the specified operator.
fn where_value_compare_string(
    filter_value: &LiteralValue,
    row_value_str: &str,
    op: ExprOp,
) -> bool {
    use std::cmp::Ordering;

    match filter_value {
        LiteralValue::String(filter_str) => {
            let cmp = row_value_str.cmp(filter_str);
            match op {
                ExprOp::Equal => cmp == Ordering::Equal,
                ExprOp::NotEqual => cmp != Ordering::Equal,
                ExprOp::LessThan => cmp == Ordering::Less,
                ExprOp::LessThanOrEqual => cmp != Ordering::Greater,
                ExprOp::GreaterThan => cmp == Ordering::Greater,
                ExprOp::GreaterThanOrEqual => cmp != Ordering::Less,
                _ => false,
            }
        }
        LiteralValue::StringWithCast(filter_str, _cast) => {
            let cmp = row_value_str.cmp(filter_str);
            match op {
                ExprOp::Equal => cmp == Ordering::Equal,
                ExprOp::NotEqual => cmp != Ordering::Equal,
                ExprOp::LessThan => cmp == Ordering::Less,
                ExprOp::LessThanOrEqual => cmp != Ordering::Greater,
                ExprOp::GreaterThan => cmp == Ordering::Greater,
                ExprOp::GreaterThanOrEqual => cmp != Ordering::Less,
                _ => false,
            }
        }
        LiteralValue::Integer(filter_int) => {
            if let Ok(row_int) = row_value_str.parse::<i64>() {
                let cmp = row_int.cmp(filter_int);
                match op {
                    ExprOp::Equal => cmp == Ordering::Equal,
                    ExprOp::NotEqual => cmp != Ordering::Equal,
                    ExprOp::LessThan => cmp == Ordering::Less,
                    ExprOp::LessThanOrEqual => cmp != Ordering::Greater,
                    ExprOp::GreaterThan => cmp == Ordering::Greater,
                    ExprOp::GreaterThanOrEqual => cmp != Ordering::Less,
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
                    ExprOp::Equal => (row_float - filter_f64).abs() < f64::EPSILON,
                    ExprOp::NotEqual => (row_float - filter_f64).abs() >= f64::EPSILON,
                    ExprOp::LessThan => row_float < filter_f64,
                    ExprOp::LessThanOrEqual => row_float <= filter_f64,
                    ExprOp::GreaterThan => row_float > filter_f64,
                    ExprOp::GreaterThanOrEqual => row_float >= filter_f64,
                    _ => false,
                }
            } else {
                false // Can't parse as float
            }
        }
        LiteralValue::Boolean(filter_bool) => {
            if let Ok(row_bool) = row_value_str.parse::<bool>() {
                match op {
                    ExprOp::Equal => row_bool == *filter_bool,
                    ExprOp::NotEqual => row_bool != *filter_bool,
                    _ => false, // Boolean comparisons other than equality don't make sense
                }
            } else {
                false // Can't parse as boolean
            }
        }
        LiteralValue::Null => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::Parameter(_) => false, // Parameters not supported in cache matching
    }
}

/// Check if a binary expression is a simple comparison (column op value).
pub fn is_simple_comparison(binary_expr: &BinaryExpr) -> bool {
    matches!(
        (binary_expr.lexpr.as_ref(), binary_expr.rexpr.as_ref()),
        (WhereExpr::Column(_), WhereExpr::Value(_)) | (WhereExpr::Value(_), WhereExpr::Column(_))
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::catalog::ColumnMetadata;
    use crate::query::ast::ColumnNode;
    use iddqd::BiHashMap;
    use ordered_float::NotNan;
    use tokio_postgres::types::Type;

    // Tests for where_value_compare_string function
    #[test]
    fn where_value_compare_string_string_match() {
        let filter_value = LiteralValue::String("hello".to_owned());
        assert!(where_value_compare_string(
            &filter_value,
            "hello",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "world",
            ExprOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "world",
            ExprOp::NotEqual
        ));
    }

    #[test]
    fn where_value_compare_string_integer_match() {
        let filter_value = LiteralValue::Integer(123);
        assert!(where_value_compare_string(
            &filter_value,
            "123",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "124",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "abc",
            ExprOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "100",
            ExprOp::LessThan
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "150",
            ExprOp::GreaterThan
        ));
    }

    #[test]
    fn where_value_compare_string_float_match() {
        let filter_value = LiteralValue::Float(NotNan::new(123.45).unwrap());
        assert!(where_value_compare_string(
            &filter_value,
            "123.45",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "123.46",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "invalid",
            ExprOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "100.0",
            ExprOp::LessThan
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "150.0",
            ExprOp::GreaterThan
        ));
    }

    #[test]
    fn where_value_compare_string_boolean_match() {
        let filter_value_true = LiteralValue::Boolean(true);
        let filter_value_false = LiteralValue::Boolean(false);

        assert!(where_value_compare_string(
            &filter_value_true,
            "true",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "false",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "1",
            ExprOp::Equal
        ));

        assert!(where_value_compare_string(
            &filter_value_false,
            "false",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "true",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "0",
            ExprOp::Equal
        ));
    }

    #[test]
    fn where_value_compare_string_null_never_matches() {
        let filter_value = LiteralValue::Null;
        assert!(!where_value_compare_string(
            &filter_value,
            "anything",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "null",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "NULL",
            ExprOp::Equal
        ));
    }

    #[test]
    fn where_value_compare_string_parameter_never_matches() {
        let filter_value = LiteralValue::Parameter("$1".to_owned());
        assert!(!where_value_compare_string(
            &filter_value,
            "$1",
            ExprOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "anything",
            ExprOp::Equal
        ));
    }

    // Helper function to create test table metadata
    fn create_test_table_metadata() -> TableMetadata {
        let mut columns = BiHashMap::new();

        columns.insert_overwrite(ColumnMetadata {
            name: "id".to_owned(),
            position: 1,
            type_oid: 23, // INT4
            data_type: Type::INT4,
            type_name: "integer".to_owned(),
            is_primary_key: true,
        });

        columns.insert_overwrite(ColumnMetadata {
            name: "name".to_owned(),
            position: 2,
            type_oid: 25, // TEXT
            data_type: Type::TEXT,
            type_name: "text".to_owned(),
            is_primary_key: false,
        });

        columns.insert_overwrite(ColumnMetadata {
            name: "active".to_owned(),
            position: 3,
            type_oid: 16, // BOOL
            data_type: Type::BOOL,
            type_name: "boolean".to_owned(),
            is_primary_key: false,
        });

        TableMetadata {
            name: "test_table".to_owned(),
            schema: "public".to_owned(),
            relation_oid: 12345,
            primary_key_columns: vec!["id".to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    // Tests for expr_comparison_evaluate function
    #[test]
    fn expr_comparison_evaluate_string_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_comparison_evaluate_string_no_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("jane".to_owned()))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_comparison_evaluate_integer_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_comparison_evaluate_null_value() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![Some("1".to_owned()), None, Some("true".to_owned())];

        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Null)),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_comparison_evaluate_reverse_order() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        // Test value = column (reverse order)
        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
            rexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_comparison_evaluate_invalid_column() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "nonexistent".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("test".to_owned()))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for where_expr_evaluate function
    #[test]
    fn where_expr_evaluate_simple_equality() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
        });

        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_and_operation_both_true() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: ExprOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
            })),
        });

        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_and_operation_one_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: ExprOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(999))), // Different value
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
            })),
        });

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_or_operation_one_true() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: ExprOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(999))), // False condition
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))), // True condition
            })),
        });

        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_or_operation_both_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: ExprOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(999))), // False condition
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: ExprOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("jane".to_owned()))), // False condition
            })),
        });

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_greater_than() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: ExprOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        });

        // Should return true since 123 > 100
        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_unsupported_expression_type() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let expr = WhereExpr::Function {
            name: "upper".to_owned(),
            args: vec![],
        };

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    // Tests for NotEqual operator
    #[test]
    fn expr_not_equal_evaluate_string_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::NotEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("jane".to_owned()))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_not_equal_evaluate_string_no_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::NotEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for LessThan operator
    #[test]
    fn expr_less_than_evaluate_integer_true() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::LessThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_less_than_evaluate_integer_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::LessThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for LessThanOrEqual operator
    #[test]
    fn expr_less_than_or_equal_evaluate_integer_equal() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("100".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_less() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for GreaterThan operator
    #[test]
    fn expr_greater_than_evaluate_integer_true() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_greater_than_evaluate_integer_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for GreaterThanOrEqual operator
    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_equal() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("100".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_greater() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("150".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("50".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(100))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for float comparisons
    #[test]
    fn expr_comparison_evaluate_float_operations() {
        let table_metadata = create_test_table_metadata();

        // Add a float column to metadata for testing
        let mut columns = BiHashMap::new();
        columns.insert_overwrite(ColumnMetadata {
            name: "price".to_owned(),
            position: 4,
            type_oid: 701, // FLOAT8
            data_type: Type::FLOAT8,
            type_name: "double precision".to_owned(),
            is_primary_key: false,
        });

        let mut table_metadata = table_metadata;
        table_metadata.columns.insert_overwrite(ColumnMetadata {
            name: "price".to_owned(),
            position: 4,
            type_oid: 701,
            data_type: Type::FLOAT8,
            type_name: "double precision".to_owned(),
            is_primary_key: false,
        });

        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
            Some("99.50".to_owned()),
        ];

        // Test less than
        let binary_expr = BinaryExpr {
            op: ExprOp::LessThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "price".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Float(
                NotNan::new(100.0).unwrap(),
            ))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));

        // Test greater than
        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "price".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Float(
                NotNan::new(50.0).unwrap(),
            ))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for string comparisons
    #[test]
    fn expr_comparison_evaluate_string_operations() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_owned()),
            Some("john".to_owned()),
            Some("true".to_owned()),
        ];

        // Test string less than (lexicographic)
        let binary_expr = BinaryExpr {
            op: ExprOp::LessThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("zebra".to_owned()))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));

        // Test string greater than
        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("alice".to_owned()))),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    // Tests for NULL handling
    #[test]
    fn expr_comparison_evaluate_null_handling() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![Some("1".to_owned()), None, Some("true".to_owned())];

        // NULL comparisons should return false (except equality with NULL)
        let binary_expr = BinaryExpr {
            op: ExprOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("test".to_owned()))),
        };

        assert!(!expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));

        // But equality with NULL should work
        let binary_expr = BinaryExpr {
            op: ExprOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "name".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Null)),
        };

        assert!(expr_comparison_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }
}

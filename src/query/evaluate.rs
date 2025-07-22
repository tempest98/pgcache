use crate::cache::TableMetadata;

use super::parse::*;

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
                WhereOp::Equal => expr_equal_evaluate(binary_expr, row_data, table_metadata),
                WhereOp::And => {
                    // Both sides must be true
                    where_expr_evaluate(&binary_expr.lexpr, row_data, table_metadata)
                        && where_expr_evaluate(&binary_expr.rexpr, row_data, table_metadata)
                }
                WhereOp::Or => {
                    // Either side can be true
                    where_expr_evaluate(&binary_expr.lexpr, row_data, table_metadata)
                        || where_expr_evaluate(&binary_expr.rexpr, row_data, table_metadata)
                }
                _ => {
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

/// Evaluate a simple equality expression (column = value) against row data.
fn expr_equal_evaluate(
    binary_expr: &BinaryExpr,
    row_data: &[Option<String>],
    table_metadata: &TableMetadata,
) -> bool {
    // Extract column and value from the equality expression
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
            // Row has non-NULL value, compare with filter value
            where_value_match_string(value, row_value_str)
        }
        Some(None) => {
            // Row has NULL value, check if filter is also NULL
            matches!(value, WhereValue::Null)
        }
        None => {
            // Column not found in table metadata
            false
        }
    }
}

/// Check if a string value from row data matches a WhereValue filter condition.
fn where_value_match_string(filter_value: &WhereValue, row_value_str: &str) -> bool {
    match filter_value {
        WhereValue::String(filter_str) => row_value_str == filter_str,
        WhereValue::Integer(filter_int) => row_value_str.parse::<i64>() == Ok(*filter_int),
        WhereValue::Float(filter_float) => row_value_str
            .parse::<f64>()
            .is_ok_and(|v| (v - filter_float).abs() < f64::EPSILON),
        WhereValue::Boolean(filter_bool) => row_value_str.parse::<bool>() == Ok(*filter_bool),
        WhereValue::Null => false, // Row has non-NULL value, filter expects NULL
        WhereValue::Parameter(_) => false, // Parameters not supported in cache matching
    }
}

/// Check if a binary expression is a simple equality (column = value).
pub fn is_simple_equality(binary_expr: &BinaryExpr) -> bool {
    matches!(
        (binary_expr.lexpr.as_ref(), binary_expr.rexpr.as_ref()),
        (WhereExpr::Column(_), WhereExpr::Value(_)) | (WhereExpr::Value(_), WhereExpr::Column(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ColumnMetadata;
    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    // Tests for where_value_match_string function
    #[test]
    fn where_value_match_string_string_match() {
        let filter_value = WhereValue::String("hello".to_string());
        assert!(where_value_match_string(&filter_value, "hello"));
        assert!(!where_value_match_string(&filter_value, "world"));
    }

    #[test]
    fn where_value_match_string_integer_match() {
        let filter_value = WhereValue::Integer(123);
        assert!(where_value_match_string(&filter_value, "123"));
        assert!(!where_value_match_string(&filter_value, "124"));
        assert!(!where_value_match_string(&filter_value, "abc"));
    }

    #[test]
    fn where_value_match_string_float_match() {
        let filter_value = WhereValue::Float(123.45);
        assert!(where_value_match_string(&filter_value, "123.45"));
        assert!(!where_value_match_string(&filter_value, "123.46"));
        assert!(!where_value_match_string(&filter_value, "invalid"));
    }

    #[test]
    fn where_value_match_string_boolean_match() {
        let filter_value_true = WhereValue::Boolean(true);
        let filter_value_false = WhereValue::Boolean(false);

        assert!(where_value_match_string(&filter_value_true, "true"));
        assert!(!where_value_match_string(&filter_value_true, "false"));
        assert!(!where_value_match_string(&filter_value_true, "1"));

        assert!(where_value_match_string(&filter_value_false, "false"));
        assert!(!where_value_match_string(&filter_value_false, "true"));
        assert!(!where_value_match_string(&filter_value_false, "0"));
    }

    #[test]
    fn where_value_match_string_null_never_matches() {
        let filter_value = WhereValue::Null;
        assert!(!where_value_match_string(&filter_value, "anything"));
        assert!(!where_value_match_string(&filter_value, "null"));
        assert!(!where_value_match_string(&filter_value, "NULL"));
    }

    #[test]
    fn where_value_match_string_parameter_never_matches() {
        let filter_value = WhereValue::Parameter("$1".to_string());
        assert!(!where_value_match_string(&filter_value, "$1"));
        assert!(!where_value_match_string(&filter_value, "anything"));
    }

    // Helper function to create test table metadata
    fn create_test_table_metadata() -> TableMetadata {
        let mut columns = BiHashMap::new();

        columns.insert_overwrite(ColumnMetadata {
            name: "id".to_string(),
            position: 1,
            type_oid: 23, // INT4
            data_type: Type::INT4,
            type_name: "integer".to_string(),
            is_primary_key: true,
        });

        columns.insert_overwrite(ColumnMetadata {
            name: "name".to_string(),
            position: 2,
            type_oid: 25, // TEXT
            data_type: Type::TEXT,
            type_name: "text".to_string(),
            is_primary_key: false,
        });

        columns.insert_overwrite(ColumnMetadata {
            name: "active".to_string(),
            position: 3,
            type_oid: 16, // BOOL
            data_type: Type::BOOL,
            type_name: "boolean".to_string(),
            is_primary_key: false,
        });

        TableMetadata {
            name: "test_table".to_string(),
            schema: "public".to_string(),
            relation_oid: 12345,
            primary_key_columns: vec!["id".to_string()],
            columns,
        }
    }

    // Tests for expr_equal_evaluate function
    #[test]
    fn expr_equal_evaluate_string_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let binary_expr = BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "name".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::String("john".to_string()))),
        };

        assert!(expr_equal_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_equal_evaluate_string_no_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let binary_expr = BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "name".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::String("jane".to_string()))),
        };

        assert!(!expr_equal_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_equal_evaluate_integer_match() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let binary_expr = BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
        };

        assert!(expr_equal_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_equal_evaluate_null_value() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![Some("1".to_string()), None, Some("true".to_string())];

        let binary_expr = BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "name".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Null)),
        };

        assert!(expr_equal_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_equal_evaluate_reverse_order() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        // Test value = column (reverse order)
        let binary_expr = BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Value(WhereValue::String("john".to_string()))),
            rexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "name".to_string(),
            })),
        };

        assert!(expr_equal_evaluate(
            &binary_expr,
            &row_data,
            &table_metadata
        ));
    }

    #[test]
    fn expr_equal_evaluate_invalid_column() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("1".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let binary_expr = BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "nonexistent".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::String("test".to_string()))),
        };

        assert!(!expr_equal_evaluate(
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
            Some("1".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "name".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::String("john".to_string()))),
        });

        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_and_operation_both_true() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: WhereOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("john".to_string()))),
            })),
        });

        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_and_operation_one_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: WhereOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(999))), // Different value
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("john".to_string()))),
            })),
        });

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_or_operation_one_true() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: WhereOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(999))), // False condition
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("john".to_string()))), // True condition
            })),
        });

        assert!(where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_or_operation_both_false() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: WhereOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(999))), // False condition
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("jane".to_string()))), // False condition
            })),
        });

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_unsupported_operator() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Binary(BinaryExpr {
            op: WhereOp::GreaterThan, // Unsupported operator
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(100))),
        });

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }

    #[test]
    fn where_expr_evaluate_unsupported_expression_type() {
        let table_metadata = create_test_table_metadata();
        let row_data = vec![
            Some("123".to_string()),
            Some("john".to_string()),
            Some("true".to_string()),
        ];

        let expr = WhereExpr::Function {
            name: "upper".to_string(),
            args: vec![],
        };

        assert!(!where_expr_evaluate(&expr, &row_data, &table_metadata));
    }
}

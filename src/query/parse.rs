use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use error_set::error_set;
use ordered_float::NotNan;

use pg_query::protobuf::SelectStmt;
use pg_query::protobuf::a_const::Val;
use pg_query::protobuf::node::Node as NodeEnum;
use pg_query::protobuf::{
    AConst, AExpr, AExprKind, BoolExpr, BoolExprType, ColumnRef, FuncCall, NullTest, NullTestType,
    ParamRef, SubLink,
};
use pg_query::{NodeRef, ParseResult};

use super::ast::{
    BinaryExpr, BinaryOp, ColumnNode, LiteralValue, MultiExpr, MultiOp, SubLinkType, UnaryExpr,
    UnaryOp, WhereExpr, select_stmt_to_query_expr,
};

error_set! {
    ParseError := WhereParseError || SqlError

    WhereParseError := {
        #[display("Unsupported WHERE clause pattern")]
        UnsupportedPattern,
        #[display("Unsupported A expression: {expr}")]
        UnsupportedAExpr { expr: String },
        #[display("Unsupported operator: {operator}")]
        UnsupportedOperator { operator: String },
        #[display("Invalid column reference")]
        InvalidColumnRef,
        #[display("Invalid constant value: {value}")]
        InvalidConstValue { value: String },
        #[display("Complex expression not supported: {expr}")]
        ComplexExpression { expr: String },
        #[display("Missing expression")]
        MissingExpression,
        #[display("{error}")]
        Other { error: String },
        #[display("Subquery parse error: {error}")]
        SubqueryError { error: String },
    }

    SqlError := {
        DeparseError(pg_query::Error)
    }
}

pub fn query_fingerprint(ast: &ParseResult) -> Result<u64, SqlError> {
    let query_sql = ast.deparse()?;
    let mut hasher = DefaultHasher::new();
    query_sql.hash(&mut hasher);
    Ok(hasher.finish())
}

pub fn query_select_has_sublink(ast: &ParseResult) -> bool {
    let select_stmt = query_select_statement(ast);

    select_stmt.target_list.iter().any(|target| {
        if let Some(node) = &target.node {
            node.nodes()
                .iter()
                .any(|(node_ref, _, _, _)| matches!(node_ref, NodeRef::SubLink(_)))
        } else {
            false
        }
    })
}

//todo, figure out how to handle subqueries
pub fn _query_select_columns(ast: &ParseResult) -> HashSet<String> {
    let select_stmt = query_select_statement(ast);

    let mut columns = HashSet::new();
    for target in &select_stmt.target_list {
        if let Some(node) = &target.node {
            node.nodes().iter().for_each(|&(node_ref, _, _, _)| {
                if let NodeRef::ColumnRef(column_ref) = node_ref
                    && let Some(field) = column_ref.fields.first()
                    && let Some(NodeEnum::String(column)) = &field.node
                {
                    columns.insert(column.sval.clone());
                }
            });
        }
    }

    columns
}

pub fn query_where_clause_parse(ast: &ParseResult) -> Result<Option<WhereExpr>, WhereParseError> {
    let select_stmt = query_select_statement(ast);
    select_stmt_parse_where(select_stmt)
}

fn query_select_statement(ast: &ParseResult) -> &SelectStmt {
    if ast.protobuf.stmts.len() > 1 {
        todo!("support multiple statements in query");
    }

    let raw_stmt = ast.protobuf.stmts.first().expect("statement in query");

    if let Some(NodeEnum::SelectStmt(select_stmt)) =
        raw_stmt.stmt.as_ref().and_then(|n| n.node.as_ref())
    {
        select_stmt
    } else {
        dbg!(ast);
        todo!();
    }
}

/// Parse a WHERE clause from a pg_query AST
/// Currently supports: equality comparisons (=) and boolean operations (AND, OR)
pub fn select_stmt_parse_where(
    select_stmt: &SelectStmt,
) -> Result<Option<WhereExpr>, WhereParseError> {
    let expr = if let Some(where_node) = &select_stmt.where_clause {
        Some(node_convert_to_expr(where_node)?)
    } else {
        None
    };

    Ok(expr)
}

/// Convert a pg_query Node to our WhereExpr - main entry point for recursion
pub fn node_convert_to_expr(node: &pg_query::Node) -> Result<WhereExpr, WhereParseError> {
    match node.node.as_ref() {
        Some(NodeEnum::AExpr(expr)) => a_expr_convert(expr),
        Some(NodeEnum::BoolExpr(expr)) => bool_expr_convert(expr),
        Some(NodeEnum::ColumnRef(col_ref)) => {
            let column = column_ref_extract(col_ref)?;
            Ok(WhereExpr::Column(column))
        }
        Some(NodeEnum::AConst(const_val)) => {
            let value = const_value_extract(const_val)?;
            Ok(WhereExpr::Value(value))
        }
        Some(NodeEnum::ParamRef(param_ref)) => {
            let value = param_ref_extract(param_ref);
            Ok(WhereExpr::Value(value))
        }
        Some(NodeEnum::SubLink(sub_link)) => sublink_convert(sub_link),
        Some(NodeEnum::NullTest(null_test)) => null_test_convert(null_test),
        Some(NodeEnum::FuncCall(func_call)) => func_call_to_where_expr(func_call),
        unsupported => {
            dbg!(unsupported);
            Err(WhereParseError::UnsupportedPattern)
        }
    }
}

/// Convert pg_query SubLink to WhereExpr::Subquery with properly parsed inner query
fn sublink_convert(sub_link: &SubLink) -> Result<WhereExpr, WhereParseError> {
    // Parse the subquery SELECT statement
    let query = match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
        Some(NodeEnum::SelectStmt(select_stmt)) => select_stmt_to_query_expr(select_stmt)
            .map_err(|e| WhereParseError::SubqueryError {
                error: e.to_string(),
            })?,
        _ => {
            return Err(WhereParseError::Other {
                error: "SubLink missing or invalid subselect".to_owned(),
            })
        }
    };

    // Parse the test expression (left-hand side for IN/ANY/ALL)
    let test_expr = sub_link
        .testexpr
        .as_ref()
        .map(|e| node_convert_to_expr(e))
        .transpose()?
        .map(Box::new);

    // Convert the SubLink type
    let sublink_type =
        SubLinkType::try_from(sub_link.sub_link_type()).map_err(|e| WhereParseError::SubqueryError {
            error: e.to_string(),
        })?;

    Ok(WhereExpr::Subquery {
        query: Box::new(query),
        sublink_type,
        test_expr,
    })
}

/// Convert pg_query NullTest to WhereExpr (IS NULL / IS NOT NULL)
fn null_test_convert(null_test: &NullTest) -> Result<WhereExpr, WhereParseError> {
    let arg = null_test
        .arg
        .as_ref()
        .ok_or(WhereParseError::MissingExpression)?;

    let op = match null_test.nulltesttype() {
        NullTestType::IsNull => UnaryOp::IsNull,
        NullTestType::IsNotNull => UnaryOp::IsNotNull,
        NullTestType::Undefined => {
            return Err(WhereParseError::UnsupportedAExpr {
                expr: "Undefined NullTest type".to_owned(),
            })
        }
    };

    Ok(WhereExpr::Unary(UnaryExpr {
        op,
        expr: Box::new(node_convert_to_expr(arg)?),
    }))
}

/// Convert pg_query FuncCall to WhereExpr::Function
fn func_call_to_where_expr(func_call: &FuncCall) -> Result<WhereExpr, WhereParseError> {
    // Extract function name — last component of qualified name (e.g., "pg_catalog.now" -> "now")
    let name = func_call
        .funcname
        .iter()
        .filter_map(|n| match &n.node {
            Some(NodeEnum::String(s)) => Some(s.sval.clone()),
            _ => None,
        })
        .next_back()
        .ok_or(WhereParseError::UnsupportedPattern)?;

    // Handle COUNT(*) — agg_star means no explicit args
    let args = if func_call.agg_star {
        vec![]
    } else {
        func_call
            .args
            .iter()
            .map(node_convert_to_expr)
            .collect::<Result<Vec<_>, _>>()?
    };

    Ok(WhereExpr::Function { name, args })
}

/// Extract column reference from pg_query ColumnRef
fn column_ref_extract(col_ref: &ColumnRef) -> Result<ColumnNode, WhereParseError> {
    if col_ref.fields.is_empty() {
        return Err(WhereParseError::InvalidColumnRef);
    }

    let mut table: Option<String> = None;
    let mut column: Option<String> = None;

    for field in &col_ref.fields {
        match field.node.as_ref() {
            Some(NodeEnum::String(s)) => {
                if column.is_none() {
                    column = Some(s.sval.clone());
                } else {
                    // If we already have a column, previous value becomes table
                    table = column.clone();
                    column = Some(s.sval.clone());
                }
            }
            _ => return Err(WhereParseError::InvalidColumnRef),
        }
    }

    let column = column.ok_or(WhereParseError::InvalidColumnRef)?;
    Ok(ColumnNode { table, column })
}

/// Extract constant value from pg_query A_Const
pub fn const_value_extract(const_val: &AConst) -> Result<LiteralValue, WhereParseError> {
    // Check for NULL values first
    if const_val.isnull {
        return Ok(LiteralValue::Null);
    }

    match const_val.val.as_ref() {
        Some(Val::Sval(s)) => Ok(LiteralValue::String(s.sval.clone())),
        Some(Val::Ival(i)) => Ok(LiteralValue::Integer(i.ival as i64)),
        Some(Val::Fval(f)) => f
            .fval
            .parse::<f64>()
            .ok()
            .and_then(|v| NotNan::new(v).ok())
            .map(LiteralValue::Float)
            .ok_or_else(|| WhereParseError::InvalidConstValue {
                value: f.fval.clone(),
            }),
        Some(Val::Boolval(b)) => Ok(LiteralValue::Boolean(b.boolval)),
        Some(Val::Bsval(bs)) => Ok(LiteralValue::String(bs.bsval.clone())), // Bit strings as strings for now
        None => Ok(LiteralValue::Null),                                     // Fallback for NULL
    }
}

/// Extract parameter reference from pg_query ParamRef
fn param_ref_extract(param_ref: &ParamRef) -> LiteralValue {
    LiteralValue::Parameter(format!("${}", param_ref.number))
}

/// Convert PostgreSQL A_Expr (expressions like col = value)
#[expect(clippy::wildcard_enum_match_arm)]
fn a_expr_convert(expr: &AExpr) -> Result<WhereExpr, WhereParseError> {
    match expr.kind() {
        AExprKind::AexprOp => {
            // Handle binary operations like =, <, >, etc.
            let op = operator_extract(&expr.name)?;

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            Ok(WhereExpr::Binary(BinaryExpr {
                op,
                lexpr: Box::new(node_convert_to_expr(lexpr)?),
                rexpr: Box::new(node_convert_to_expr(rexpr)?),
            }))
        }
        AExprKind::AexprIn => {
            // Handle IN / NOT IN expressions
            // name: ["="] for IN, ["<>"] for NOT IN
            let op = in_operator_extract(&expr.name)?;

            let lexpr = expr
                .lexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;
            let rexpr = expr
                .rexpr
                .as_ref()
                .ok_or(WhereParseError::MissingExpression)?;

            // Left side is the column/expression being tested
            let left_expr = node_convert_to_expr(lexpr)?;

            // Right side is a List of values
            let values = in_list_extract(rexpr)?;

            // Build MultiExpr: [column, value1, value2, ...]
            let mut exprs = vec![left_expr];
            exprs.extend(values);

            Ok(WhereExpr::Multi(MultiExpr { op, exprs }))
        }
        unsupported_kind => {
            dbg!(unsupported_kind);
            Err(WhereParseError::UnsupportedAExpr {
                expr: format!("{unsupported_kind:?}"),
            })
        }
    }
}

/// Extract IN/NOT IN operator from name nodes
fn in_operator_extract(name_nodes: &[pg_query::Node]) -> Result<MultiOp, WhereParseError> {
    let [name_node] = name_nodes else {
        return Err(WhereParseError::Other {
            error: "IN operator: expected single name node".to_owned(),
        });
    };

    let Some(NodeEnum::String(name_str)) = &name_node.node else {
        return Err(WhereParseError::Other {
            error: "IN operator: expected string node".to_owned(),
        });
    };

    match name_str.sval.as_str() {
        "=" => Ok(MultiOp::In),
        "<>" => Ok(MultiOp::NotIn),
        other => Err(WhereParseError::UnsupportedOperator {
            operator: format!("IN with operator '{other}'"),
        }),
    }
}

/// Extract values from IN list (pg_query List node)
fn in_list_extract(node: &pg_query::Node) -> Result<Vec<WhereExpr>, WhereParseError> {
    let Some(NodeEnum::List(list)) = &node.node else {
        return Err(WhereParseError::Other {
            error: "IN clause: expected List on right side".to_owned(),
        });
    };

    list.items
        .iter()
        .map(node_convert_to_expr)
        .collect()
}

/// Extract operator from pg_query operator name nodes
fn operator_extract(name_nodes: &[pg_query::Node]) -> Result<BinaryOp, WhereParseError> {
    let [name_node] = name_nodes else {
        return Err(WhereParseError::Other {
            error: "Multi-part operator names not supported".to_owned(),
        });
    };

    match name_node.node.as_ref() {
        Some(NodeEnum::String(s)) => match s.sval.as_str() {
            "=" => Ok(BinaryOp::Equal),
            "!=" | "<>" => Ok(BinaryOp::NotEqual),
            "<" => Ok(BinaryOp::LessThan),
            "<=" => Ok(BinaryOp::LessThanOrEqual),
            ">" => Ok(BinaryOp::GreaterThan),
            ">=" => Ok(BinaryOp::GreaterThanOrEqual),

            op => {
                dbg!(op);
                Err(WhereParseError::UnsupportedOperator {
                    operator: op.to_owned(),
                })
            }
        },
        unsupported => {
            dbg!(unsupported);
            Err(WhereParseError::Other {
                error: "Invalid operator name format".to_owned(),
            })
        }
    }
}

/// Convert PostgreSQL BoolExpr (AND, OR, NOT)
fn bool_expr_convert(expr: &BoolExpr) -> Result<WhereExpr, WhereParseError> {
    match expr.boolop() {
        BoolExprType::AndExpr => {
            let [first, second, rest @ ..] = expr.args.as_slice() else {
                return Err(WhereParseError::Other {
                    error: "AND with < 2 arguments not supported".to_owned(),
                });
            };

            // For chained AND expressions (a AND b AND c), build a left-associative tree:
            // ((a AND b) AND c)
            let mut result = WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(node_convert_to_expr(first)?),
                rexpr: Box::new(node_convert_to_expr(second)?),
            });

            // Chain additional arguments
            for arg in rest {
                result = WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::And,
                    lexpr: Box::new(result),
                    rexpr: Box::new(node_convert_to_expr(arg)?),
                });
            }

            Ok(result)
        }
        BoolExprType::OrExpr => {
            let [first, second, rest @ ..] = expr.args.as_slice() else {
                return Err(WhereParseError::Other {
                    error: "OR with < 2 arguments not supported".to_owned(),
                });
            };

            // For chained OR expressions (a OR b OR c), build a left-associative tree:
            // ((a OR b) OR c)
            let mut result = WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Or,
                lexpr: Box::new(node_convert_to_expr(first)?),
                rexpr: Box::new(node_convert_to_expr(second)?),
            });

            // Chain additional arguments
            for arg in rest {
                result = WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Or,
                    lexpr: Box::new(result),
                    rexpr: Box::new(node_convert_to_expr(arg)?),
                });
            }

            Ok(result)
        }
        BoolExprType::NotExpr => {
            let [arg] = expr.args.as_slice() else {
                return Err(WhereParseError::Other {
                    error: "NOT with != 1 argument not supported".to_owned(),
                });
            };

            Ok(WhereExpr::Unary(UnaryExpr {
                op: UnaryOp::Not,
                expr: Box::new(node_convert_to_expr(arg)?),
            }))
        }
        BoolExprType::Undefined => Err(WhereParseError::Other {
            error: "Undefined boolean expression type".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::wildcard_enum_match_arm)]
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn fingerprint_literals_differ() {
        let f1 = query_fingerprint(
            &pg_query::parse("select id, str from test where str = 'hello'").unwrap(),
        )
        .unwrap();
        let f2 = query_fingerprint(
            &pg_query::parse("select id, str from test where str = 'bye'").unwrap(),
        )
        .unwrap();

        assert_ne!(f1, f2);
    }

    #[test]
    fn select_columns() {
        let cols = _query_select_columns(
            &pg_query::parse("select id, str from test where str = 'hello'").unwrap(),
        );
        assert_eq!(cols, HashSet::from(["id".to_owned(), "str".to_owned()]));

        let cols = _query_select_columns(
            &pg_query::parse("select count(id), str from test where str = 'hihi'").unwrap(),
        );
        assert_eq!(cols, HashSet::from(["id".to_owned(), "str".to_owned()]));

        // let cols = _query_select_columns(
        //     &pg_query::parse("select *, count(*) from test where str = 'hihi'").unwrap(),
        // );
        // assert_eq!(cols, HashSet::from(["id".to_owned(), "str".to_owned()]));
    }

    #[test]
    fn where_clause_simple_equality() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id, str FROM test WHERE str = 'hello'").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "str".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("hello".to_owned()))),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_integer_equality() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id = 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_boolean_equality() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE active = true").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "active".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Boolean(true))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_greater_than() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE cnt > 0").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "cnt".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(0))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_and_operation() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE str = 'hello' AND id = 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "str".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("hello".to_owned()))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "id".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_or_operation() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE str = 'hello' OR str = 'world'").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "str".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("hello".to_owned()))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "str".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("world".to_owned()))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_not_operation() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE NOT str = 'hello'").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Unary(UnaryExpr {
            op: UnaryOp::Not,
            expr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "str".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("hello".to_owned()))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_qualified_column() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE test.str = 'hello'").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: Some("test".to_owned()),
                column: "str".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::String("hello".to_owned()))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_null_value() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE data = NULL").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "data".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Null)),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_no_where() {
        let result = query_where_clause_parse(&pg_query::parse("SELECT id FROM test").unwrap());

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        assert_eq!(where_clause, None);
    }

    #[test]
    fn where_clause_not_equal_with_exclamation() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id != 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::NotEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_not_equal_with_angle_brackets() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id <> 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::NotEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_less_than() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id < 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::LessThan,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_less_than_or_equal() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id <= 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_greater_than_or_equal() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id >= 123").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(123))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_unsupported_operator() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id LIKE 'test%'").unwrap(),
        );

        assert!(result.is_err());
        // LIKE uses AexprLike, not AexprOp, so it fails with UnsupportedAExpr
        match result.unwrap_err() {
            WhereParseError::UnsupportedAExpr { .. } => {
                // This is expected for LIKE operations
            }
            other => panic!("Expected UnsupportedAExpr error, got: {other:?}"),
        }
    }

    #[test]
    fn where_clause_chained_and_operation() {
        let result = query_where_clause_parse(
            &pg_query::parse(
                "SELECT id FROM test WHERE name = 'john' AND age > 25 AND active = true",
            )
            .unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        // Should build a left-associative tree: ((name = 'john' AND age > 25) AND active = true)
        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Column(ColumnNode {
                        table: None,
                        column: "name".to_owned(),
                    })),
                    rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
                })),
                rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::GreaterThan,
                    lexpr: Box::new(WhereExpr::Column(ColumnNode {
                        table: None,
                        column: "age".to_owned(),
                    })),
                    rexpr: Box::new(WhereExpr::Value(LiteralValue::Integer(25))),
                })),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "active".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Boolean(true))),
            })),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_chained_or_operation() {
        let result = query_where_clause_parse(
            &pg_query::parse(
                "SELECT id FROM test WHERE name = 'john' OR name = 'jane' OR name = 'bob'",
            )
            .unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        // Should build a left-associative tree: ((name = 'john' OR name = 'jane') OR name = 'bob')
        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Or,
                lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Column(ColumnNode {
                        table: None,
                        column: "name".to_owned(),
                    })),
                    rexpr: Box::new(WhereExpr::Value(LiteralValue::String("john".to_owned()))),
                })),
                rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                    op: BinaryOp::Equal,
                    lexpr: Box::new(WhereExpr::Column(ColumnNode {
                        table: None,
                        column: "name".to_owned(),
                    })),
                    rexpr: Box::new(WhereExpr::Value(LiteralValue::String("jane".to_owned()))),
                })),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::String("bob".to_owned()))),
            })),
        }));

        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_single() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE id = $1").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnNode {
                table: None,
                column: "id".to_owned(),
            })),
            rexpr: Box::new(WhereExpr::Value(LiteralValue::Parameter("$1".to_owned()))),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_multiple() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE name = $1 AND age > $2").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Parameter("$1".to_owned()))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::GreaterThan,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "age".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Parameter("$2".to_owned()))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_parameterized_query_mixed_with_literals() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE name = $1 AND active = true").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "name".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Parameter("$1".to_owned()))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnNode {
                    table: None,
                    column: "active".to_owned(),
                })),
                rexpr: Box::new(WhereExpr::Value(LiteralValue::Boolean(true))),
            })),
        }));
        assert_eq!(where_clause, expected);
    }

    #[test]
    fn where_clause_in_with_strings() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT * FROM t WHERE status IN ('active', 'pending', 'complete')")
                .unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr, got {:?}", where_clause);
        };

        assert_eq!(multi.op, MultiOp::In);
        assert_eq!(multi.exprs.len(), 4); // column + 3 values

        // First element should be the column
        let WhereExpr::Column(col) = &multi.exprs[0] else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "status");

        // Remaining elements should be string values
        let WhereExpr::Value(LiteralValue::String(v1)) = &multi.exprs[1] else {
            panic!("expected string value");
        };
        assert_eq!(v1, "active");
    }

    #[test]
    fn where_clause_not_in() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT * FROM t WHERE id NOT IN (1, 2, 3)").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::NotIn);
        assert_eq!(multi.exprs.len(), 4); // column + 3 values
    }

    #[test]
    fn where_clause_in_with_integers() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Multi(multi) = where_clause else {
            panic!("expected MultiExpr");
        };

        assert_eq!(multi.op, MultiOp::In);

        // Check that values are integers
        let WhereExpr::Value(LiteralValue::Integer(v1)) = &multi.exprs[1] else {
            panic!("expected integer value");
        };
        assert_eq!(*v1, 1);
    }

    #[test]
    fn where_clause_in_combined_with_and() {
        let result = query_where_clause_parse(
            &pg_query::parse(
                "SELECT * FROM t WHERE tenant_id = 1 AND status IN ('active', 'pending')",
            )
            .unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        // Should be AND(tenant_id = 1, status IN (...))
        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        // Right side should be the IN clause
        let WhereExpr::Multi(multi) = binary.rexpr.as_ref() else {
            panic!("expected MultiExpr on right side");
        };
        assert_eq!(multi.op, MultiOp::In);
    }

    #[test]
    fn where_clause_is_null() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE deleted_at IS NULL").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNull);

        let WhereExpr::Column(col) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "deleted_at");
    }

    #[test]
    fn where_clause_is_not_null() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id FROM test WHERE name IS NOT NULL").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        let WhereExpr::Unary(unary) = where_clause else {
            panic!("expected UnaryExpr");
        };

        assert_eq!(unary.op, UnaryOp::IsNotNull);

        let WhereExpr::Column(col) = unary.expr.as_ref() else {
            panic!("expected Column");
        };
        assert_eq!(col.column, "name");
    }

    #[test]
    fn where_clause_is_null_combined_with_and() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT * FROM t WHERE id = 1 AND deleted_at IS NULL").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap().unwrap();

        // Should be AND(id = 1, deleted_at IS NULL)
        let WhereExpr::Binary(binary) = where_clause else {
            panic!("expected BinaryExpr");
        };

        assert_eq!(binary.op, BinaryOp::And);

        // Right side should be IS NULL
        let WhereExpr::Unary(unary) = binary.rexpr.as_ref() else {
            panic!("expected UnaryExpr on right side");
        };
        assert_eq!(unary.op, UnaryOp::IsNull);
    }
}

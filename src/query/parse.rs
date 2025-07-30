use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use error_set::error_set;

use pg_query::protobuf::SelectStmt;
use pg_query::protobuf::node::Node as NodeEnum;
use pg_query::protobuf::{
    AConst, AExpr, AExprKind, BoolExpr, BoolExprType, ColumnRef as PgColumnRef,
};
use pg_query::{NodeRef, ParseResult};

error_set! {
    ParseError = WhereParseError || SqlError;

    WhereParseError = {
        #[display("Unsupported WHERE clause pattern")]
        UnsupportedPattern,
        #[display("Unsupported A expression")]
        UnsupportedAExpr { expr: String },
        #[display("Unsupported operator")]
        UnsupportedOperator { operator: String },
        #[display("Invalid column reference")]
        InvalidColumnRef,
        #[display("Invalid constant value: {value}")]
        InvalidConstValue { value: String },
        #[display("Complex expression not supported: {expr}")]
        ComplexExpression { expr: String },
        #[display("Missing expression")]
        MissingExpression,
        Other { error: String }
    };

    SqlError = {
        DeparseError(pg_query::Error)
    };
}

// Core value types that can appear in WHERE expressions
#[derive(Debug, Clone, PartialEq)]
pub enum WhereValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Parameter(String), // For $1, $2, etc.
}

// Column reference (potentially qualified: table.column)
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

// Operators for WHERE expressions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WhereOp {
    // Logical operators
    And,
    Or,
    Not,

    // Comparison operators
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Pattern matching
    Like,
    ILike,
    NotLike,
    NotILike,

    // Set operations
    In,
    NotIn,

    // Range operations
    Between,
    NotBetween,

    // Null checks
    IsNull,
    IsNotNull,

    // Array operations
    Any,
    All,

    // Existence checks
    Exists,
    NotExists,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub op: WhereOp,
    pub expr: Box<WhereExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub op: WhereOp,
    pub lexpr: Box<WhereExpr>, // left expression
    pub rexpr: Box<WhereExpr>, // right expression
}

// Multi-operand expressions (for IN, BETWEEN, etc.)
#[derive(Debug, Clone, PartialEq)]
pub struct MultiExpr {
    pub op: WhereOp,
    pub exprs: Vec<WhereExpr>,
}

// WHERE expression tree - more abstract and flexible
#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    // Leaf nodes
    Value(WhereValue),
    Column(ColumnRef),

    // Expression nodes
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    Multi(MultiExpr),

    // Function calls (for extensibility)
    Function {
        name: String,
        args: Vec<WhereExpr>,
    },

    // Subqueries (for future support)
    Subquery {
        query: String, // Placeholder for now
    },
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
                if let NodeRef::ColumnRef(column_ref) = node_ref {
                    if let Some(NodeEnum::String(column)) = &column_ref.fields[0].node {
                        columns.insert(column.sval.clone());
                    }
                }
            });
        }
    }

    columns
}

pub fn query_where_clause_parse(ast: &ParseResult) -> Result<Option<WhereExpr>, WhereParseError> {
    let select_stmt = query_select_statement(ast);
    select_stmt_parse(select_stmt)
}

fn query_select_statement(ast: &ParseResult) -> &SelectStmt {
    if ast.protobuf.stmts.len() > 1 {
        todo!("support multiple statements in query");
    }

    let raw_stmt = &ast.protobuf.stmts[0];

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
fn select_stmt_parse(select_stmt: &SelectStmt) -> Result<Option<WhereExpr>, WhereParseError> {
    let expr = if let Some(where_node) = &select_stmt.where_clause {
        Some(node_convert_to_expr(where_node)?)
    } else {
        None
    };

    Ok(expr)
}

/// Convert a pg_query Node to our WhereExpr - main entry point for recursion
fn node_convert_to_expr(node: &pg_query::Node) -> Result<WhereExpr, WhereParseError> {
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
        unsupported => {
            dbg!(unsupported);
            Err(WhereParseError::UnsupportedPattern)
        }
    }
}

/// Extract column reference from pg_query ColumnRef
fn column_ref_extract(col_ref: &PgColumnRef) -> Result<ColumnRef, WhereParseError> {
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
    Ok(ColumnRef { table, column })
}

/// Extract constant value from pg_query A_Const
fn const_value_extract(const_val: &AConst) -> Result<WhereValue, WhereParseError> {
    use pg_query::protobuf::a_const::Val;

    // Check for NULL values first
    if const_val.isnull {
        return Ok(WhereValue::Null);
    }

    match const_val.val.as_ref() {
        Some(Val::Sval(s)) => Ok(WhereValue::String(s.sval.clone())),
        Some(Val::Ival(i)) => Ok(WhereValue::Integer(i.ival as i64)),
        Some(Val::Fval(f)) => f.fval.parse::<f64>().map(WhereValue::Float).map_err(|_| {
            WhereParseError::InvalidConstValue {
                value: f.fval.clone(),
            }
        }),
        Some(Val::Boolval(b)) => Ok(WhereValue::Boolean(b.boolval)),
        Some(Val::Bsval(bs)) => Ok(WhereValue::String(bs.bsval.clone())), // Bit strings as strings for now
        None => Ok(WhereValue::Null),                                     // Fallback for NULL
    }
}

/// Convert PostgreSQL A_Expr (expressions like col = value)
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
        unsupported_kind => {
            dbg!(unsupported_kind);
            Err(WhereParseError::UnsupportedAExpr {
                expr: format!("{unsupported_kind:?}"),
            })
        }
    }
}

/// Extract operator from pg_query operator name nodes
fn operator_extract(name_nodes: &[pg_query::Node]) -> Result<WhereOp, WhereParseError> {
    if name_nodes.len() != 1 {
        return Err(WhereParseError::Other {
            error: "Multi-part operator names not supported".to_string(),
        });
    }

    match name_nodes[0].node.as_ref() {
        Some(NodeEnum::String(s)) => match s.sval.as_str() {
            "=" => Ok(WhereOp::Equal),
            "!=" | "<>" => Ok(WhereOp::NotEqual),
            "<" => Ok(WhereOp::LessThan),
            "<=" => Ok(WhereOp::LessThanOrEqual),
            ">" => Ok(WhereOp::GreaterThan),
            ">=" => Ok(WhereOp::GreaterThanOrEqual),

            op => {
                dbg!(op);
                Err(WhereParseError::UnsupportedOperator {
                    operator: op.to_string(),
                })
            }
        },
        unsupported => {
            dbg!(unsupported);
            Err(WhereParseError::Other {
                error: "Invalid operator name format".to_string(),
            })
        }
    }
}

/// Convert PostgreSQL BoolExpr (AND, OR, NOT)
fn bool_expr_convert(expr: &BoolExpr) -> Result<WhereExpr, WhereParseError> {
    match expr.boolop() {
        BoolExprType::AndExpr => {
            if expr.args.len() != 2 {
                return Err(WhereParseError::Other {
                    error: "AND with != 2 arguments not supported".to_string(),
                });
            }

            Ok(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::And,
                lexpr: Box::new(node_convert_to_expr(&expr.args[0])?),
                rexpr: Box::new(node_convert_to_expr(&expr.args[1])?),
            }))
        }
        BoolExprType::OrExpr => {
            if expr.args.len() != 2 {
                return Err(WhereParseError::Other {
                    error: "OR with != 2 arguments not supported".to_string(),
                });
            }

            Ok(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Or,
                lexpr: Box::new(node_convert_to_expr(&expr.args[0])?),
                rexpr: Box::new(node_convert_to_expr(&expr.args[1])?),
            }))
        }
        BoolExprType::NotExpr => {
            if expr.args.len() != 1 {
                return Err(WhereParseError::Other {
                    error: "NOT with != 1 argument not supported".to_string(),
                });
            }

            Ok(WhereExpr::Unary(UnaryExpr {
                op: WhereOp::Not,
                expr: Box::new(node_convert_to_expr(&expr.args[0])?),
            }))
        }
        BoolExprType::Undefined => Err(WhereParseError::Other {
            error: "Undefined boolean expression type".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(cols, HashSet::from(["id".to_string(), "str".to_string()]));

        let cols = _query_select_columns(
            &pg_query::parse("select count(id), str from test where str = 'hihi'").unwrap(),
        );
        assert_eq!(cols, HashSet::from(["id".to_string(), "str".to_string()]));

        // let cols = _query_select_columns(
        //     &pg_query::parse("select *, count(*) from test where str = 'hihi'").unwrap(),
        // );
        // assert_eq!(cols, HashSet::from(["id".to_string(), "str".to_string()]));
    }

    #[test]
    fn where_clause_simple_equality() {
        let result = query_where_clause_parse(
            &pg_query::parse("SELECT id, str FROM test WHERE str = 'hello'").unwrap(),
        );

        assert!(result.is_ok());
        let where_clause = result.unwrap();

        let expected = Some(WhereExpr::Binary(BinaryExpr {
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "str".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::String("hello".to_string()))),
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
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "active".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Boolean(true))),
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
            op: WhereOp::GreaterThan,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "cnt".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(0))),
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
            op: WhereOp::And,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "str".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("hello".to_string()))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
            op: WhereOp::Or,
            lexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "str".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("hello".to_string()))),
            })),
            rexpr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "str".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("world".to_string()))),
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
            op: WhereOp::Not,
            expr: Box::new(WhereExpr::Binary(BinaryExpr {
                op: WhereOp::Equal,
                lexpr: Box::new(WhereExpr::Column(ColumnRef {
                    table: None,
                    column: "str".to_string(),
                })),
                rexpr: Box::new(WhereExpr::Value(WhereValue::String("hello".to_string()))),
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
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: Some("test".to_string()),
                column: "str".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::String("hello".to_string()))),
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
            op: WhereOp::Equal,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "data".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Null)),
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
            op: WhereOp::NotEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
            op: WhereOp::NotEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
            op: WhereOp::LessThan,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
            op: WhereOp::LessThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
            op: WhereOp::GreaterThanOrEqual,
            lexpr: Box::new(WhereExpr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            })),
            rexpr: Box::new(WhereExpr::Value(WhereValue::Integer(123))),
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
}

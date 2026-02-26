use ordered_float::NotNan;
use postgres_protocol::types as pg_types;
use postgres_types::Type as PgType;
use rootcause::Report;

use crate::cache::{QueryParameter, QueryParameters};
use crate::query::ast::{LiteralValue, QueryBody, QueryExpr, SelectNode, TableSource, WhereExpr};

use super::{AstTransformError, AstTransformResult};

/// Replace parameter placeholders ($1, $2, etc.) in a QueryExpr with actual values.
///
/// This function traverses the AST and replaces all `LiteralValue::Parameter` nodes
/// with appropriate typed `LiteralValue` nodes based on the provided parameter values.
pub fn query_expr_parameters_replace(
    query_expr: &QueryExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<QueryExpr> {
    let mut new_query = query_expr.clone();

    // Replace parameters in CTE definitions
    for cte in &mut new_query.ctes {
        let replaced = query_expr_parameters_replace(&cte.query, parameters)?;
        cte.query = replaced;
    }

    // Replace parameters in body
    query_body_parameters_replace(&mut new_query.body, parameters)?;

    // Replace parameters in LIMIT clause
    if let Some(limit) = &mut new_query.limit {
        if let Some(count) = &mut limit.count {
            literal_value_parameters_replace(count, parameters)?;
        }
        if let Some(offset) = &mut limit.offset {
            literal_value_parameters_replace(offset, parameters)?;
        }
    }

    Ok(new_query)
}

/// Replace parameters in a QueryBody
fn query_body_parameters_replace(
    body: &mut QueryBody,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match body {
        QueryBody::Select(select_node) => {
            select_node_parameters_replace(select_node, parameters)?;
        }
        QueryBody::Values(_) => {
            // Values clauses contain literals, no parameters to replace
        }
        QueryBody::SetOp(set_op) => {
            query_expr_parameters_replace_mut(&mut set_op.left, parameters)?;
            query_expr_parameters_replace_mut(&mut set_op.right, parameters)?;
        }
    }
    Ok(())
}

/// Replace parameters in a QueryExpr (mutable version for recursive calls)
fn query_expr_parameters_replace_mut(
    query_expr: &mut QueryExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    query_body_parameters_replace(&mut query_expr.body, parameters)?;
    if let Some(limit) = &mut query_expr.limit {
        if let Some(count) = &mut limit.count {
            literal_value_parameters_replace(count, parameters)?;
        }
        if let Some(offset) = &mut limit.offset {
            literal_value_parameters_replace(offset, parameters)?;
        }
    }
    Ok(())
}

/// Replace parameters in a SelectNode (mutates in place)
pub fn select_node_parameters_replace(
    select_node: &mut SelectNode,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    // Replace parameters in WHERE clause
    if let Some(where_clause) = &mut select_node.where_clause {
        where_expr_parameters_replace(where_clause, parameters)?;
    }

    // Replace parameters in HAVING clause
    if let Some(having) = &mut select_node.having {
        where_expr_parameters_replace(having, parameters)?;
    }

    // Replace parameters in JOIN conditions
    for table_source in &mut select_node.from {
        table_source_parameters_replace(table_source, parameters)?;
    }

    Ok(())
}

/// Replace parameters in a TableSource (recursively handles JOINs)
fn table_source_parameters_replace(
    table_source: &mut TableSource,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match table_source {
        TableSource::Join(join) => {
            // Replace in JOIN condition
            if let Some(condition) = &mut join.condition {
                where_expr_parameters_replace(condition, parameters)?;
            }
            // Recursively handle nested joins
            table_source_parameters_replace(&mut join.left, parameters)?;
            table_source_parameters_replace(&mut join.right, parameters)?;
        }
        TableSource::Subquery(subquery) => {
            // Replace parameters in subquery
            query_expr_parameters_replace_mut(&mut subquery.query, parameters)?;
        }
        TableSource::CteRef(cte_ref) => {
            // Replace parameters in CTE reference body
            query_expr_parameters_replace_mut(&mut cte_ref.query, parameters)?;
        }
        TableSource::Table(_) => {
            // No parameters in simple table references
        }
    }
    Ok(())
}

/// Replace parameters in a WhereExpr tree (mutates in place)
fn where_expr_parameters_replace(
    expr: &mut WhereExpr,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    match expr {
        WhereExpr::Value(literal) => {
            // Only replace if this is a parameter placeholder
            if let LiteralValue::Parameter(placeholder) = literal {
                // Parse parameter index from placeholder (e.g., "$1" -> 0)
                let index = parameter_index_parse(placeholder)?;

                // Get parameter
                let param = parameters.get(index).ok_or_else(|| {
                    Report::from(AstTransformError::ParameterOutOfBounds {
                        index,
                        count: parameters.len(),
                    })
                })?;

                // Replace the LiteralValue in place
                *literal = parameter_to_literal(&param)?;
            }
        }
        WhereExpr::Column(_) => {
            // No parameters in column references
        }
        WhereExpr::Unary(unary) => {
            where_expr_parameters_replace(&mut unary.expr, parameters)?;
        }
        WhereExpr::Binary(binary) => {
            where_expr_parameters_replace(&mut binary.lexpr, parameters)?;
            where_expr_parameters_replace(&mut binary.rexpr, parameters)?;
        }
        WhereExpr::Multi(multi) => {
            for expr in &mut multi.exprs {
                where_expr_parameters_replace(expr, parameters)?;
            }
        }
        WhereExpr::Array(elems) => {
            for elem in elems {
                where_expr_parameters_replace(elem, parameters)?;
            }
        }
        WhereExpr::Function { args, .. } => {
            for arg in args {
                where_expr_parameters_replace(arg, parameters)?;
            }
        }
        WhereExpr::Subquery {
            query, test_expr, ..
        } => {
            query_expr_parameters_replace_mut(query, parameters)?;
            if let Some(test) = test_expr {
                where_expr_parameters_replace(test, parameters)?;
            }
        }
    }
    Ok(())
}

/// Replace a parameter in a LiteralValue (mutates in place)
fn literal_value_parameters_replace(
    literal: &mut LiteralValue,
    parameters: &QueryParameters,
) -> AstTransformResult<()> {
    if let LiteralValue::Parameter(placeholder) = literal {
        let index = parameter_index_parse(placeholder)?;

        let param = parameters.get(index).ok_or_else(|| {
            Report::from(AstTransformError::ParameterOutOfBounds {
                index,
                count: parameters.len(),
            })
        })?;

        *literal = parameter_to_literal(&param)?;
    }
    Ok(())
}

/// Parse parameter index from placeholder string (e.g., "$1" -> 0, "$2" -> 1)
fn parameter_index_parse(placeholder: &str) -> AstTransformResult<usize> {
    if !placeholder.starts_with('$') {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        }
        .into());
    }

    let index_str = &placeholder[1..];
    let param_num = index_str.parse::<usize>().map_err(|_| {
        Report::from(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        })
    })?;

    if param_num == 0 {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        }
        .into());
    }

    Ok(param_num - 1) // Convert 1-indexed to 0-indexed
}

/// Convert a parameter value (bytes) to a LiteralValue.
///
/// Handles both text format (format=0) and binary format (format=1) parameters
/// from the PostgreSQL extended query protocol. Uses the OID to determine the
/// appropriate type conversion.
fn parameter_to_literal(param: &QueryParameter) -> AstTransformResult<LiteralValue> {
    match &param.value {
        None => Ok(LiteralValue::Null),
        Some(bytes) => {
            if param.format == 0 {
                text_parameter_to_literal(bytes, param.oid)
            } else {
                binary_parameter_to_literal(bytes, param.oid)
            }
        }
    }
}

/// Convert a text format parameter to a LiteralValue based on OID.
fn text_parameter_to_literal(bytes: &[u8], oid: u32) -> AstTransformResult<LiteralValue> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| Report::from(AstTransformError::InvalidUtf8))?;

    // Use postgres_types to identify the type from OID
    let pg_type = PgType::from_oid(oid);

    match pg_type {
        Some(PgType::BOOL) => {
            let value = matches!(s, "t" | "true" | "TRUE" | "1");
            Ok(LiteralValue::Boolean(value))
        }
        Some(PgType::INT2 | PgType::INT4 | PgType::INT8) => {
            let value = s.parse::<i64>().map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid integer '{}': {}", s, e),
                })
            })?;
            Ok(LiteralValue::Integer(value))
        }
        Some(PgType::FLOAT4 | PgType::FLOAT8) => {
            let value = s.parse::<f64>().map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid float '{}': {}", s, e),
                })
            })?;
            let value = NotNan::new(value).map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("NaN is not a valid float value: {}", s),
                })
            })?;
            Ok(LiteralValue::Float(value))
        }
        // String-like types
        Some(
            PgType::TEXT
            | PgType::VARCHAR
            | PgType::BPCHAR
            | PgType::NAME
            | PgType::CHAR
            | PgType::UNKNOWN,
        ) => Ok(LiteralValue::String(s.to_owned())),
        // Types that pass through as strings (UUID, temporal, numeric)
        Some(
            PgType::UUID
            | PgType::TIMESTAMP
            | PgType::TIMESTAMPTZ
            | PgType::DATE
            | PgType::TIME
            | PgType::TIMETZ
            | PgType::INTERVAL
            | PgType::NUMERIC,
        ) => Ok(LiteralValue::String(s.to_owned())),
        // Unknown OID or unhandled type - fallback to string
        _ => Ok(LiteralValue::String(s.to_owned())),
    }
}

/// Convert a binary format parameter to a LiteralValue based on OID.
fn binary_parameter_to_literal(bytes: &[u8], oid: u32) -> AstTransformResult<LiteralValue> {
    let pg_type = PgType::from_oid(oid);

    match pg_type {
        Some(PgType::BOOL) => {
            let value = pg_types::bool_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary bool: {}", e),
                })
            })?;
            Ok(LiteralValue::Boolean(value))
        }
        Some(PgType::INT2) => {
            let value = pg_types::int2_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary int2: {}", e),
                })
            })?;
            Ok(LiteralValue::Integer(value as i64))
        }
        Some(PgType::INT4) => {
            let value = pg_types::int4_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary int4: {}", e),
                })
            })?;
            Ok(LiteralValue::Integer(value as i64))
        }
        Some(PgType::INT8) => {
            let value = pg_types::int8_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary int8: {}", e),
                })
            })?;
            Ok(LiteralValue::Integer(value))
        }
        Some(PgType::FLOAT4) => {
            let value = pg_types::float4_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary float4: {}", e),
                })
            })?;
            let value = NotNan::new(value as f64).map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: "NaN is not a valid float value".to_owned(),
                })
            })?;
            Ok(LiteralValue::Float(value))
        }
        Some(PgType::FLOAT8) => {
            let value = pg_types::float8_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary float8: {}", e),
                })
            })?;
            let value = NotNan::new(value).map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: "NaN is not a valid float value".to_owned(),
                })
            })?;
            Ok(LiteralValue::Float(value))
        }
        Some(
            PgType::TEXT
            | PgType::VARCHAR
            | PgType::BPCHAR
            | PgType::NAME
            | PgType::CHAR
            | PgType::UNKNOWN,
        ) => {
            let value = pg_types::text_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary text: {}", e),
                })
            })?;
            Ok(LiteralValue::String(value.to_owned()))
        }
        Some(PgType::UUID) => {
            // UUID binary format is 16 bytes
            let bytes: &[u8; 16] = bytes.try_into().map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!(
                        "invalid UUID length: expected 16 bytes, got {}",
                        bytes.len()
                    ),
                })
            })?;
            let uuid_str = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0],
                bytes[1],
                bytes[2],
                bytes[3],
                bytes[4],
                bytes[5],
                bytes[6],
                bytes[7],
                bytes[8],
                bytes[9],
                bytes[10],
                bytes[11],
                bytes[12],
                bytes[13],
                bytes[14],
                bytes[15]
            );
            Ok(LiteralValue::String(uuid_str))
        }
        // For known but unhandled types, try to interpret as text
        Some(_) => {
            // Attempt to decode as UTF-8 text (works for enums which use text labels)
            match std::str::from_utf8(bytes) {
                Ok(s) => Ok(LiteralValue::String(s.to_owned())),
                Err(_) => Err(AstTransformError::UnsupportedBinaryFormat { oid }.into()),
            }
        }
        // Unknown OID (custom types like domains or enums) - try text interpretation
        None => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(LiteralValue::String(s.to_owned())),
            Err(_) => Err(AstTransformError::UnsupportedBinaryFormat { oid }.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::wildcard_enum_match_arm)]
    #![allow(clippy::unwrap_used)]

    use tokio_util::bytes::Bytes;

    use crate::query::ast::{Deparse, query_expr_convert};

    use super::*;

    /// Parse SQL and return a SelectNode
    fn parse_select_node(sql: &str) -> SelectNode {
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => node,
            _ => panic!("expected SELECT"),
        }
    }

    /// Helper to create QueryParameters for text format with TEXT OID
    fn text_params(values: Vec<Option<&[u8]>>) -> QueryParameters {
        let len = values.len();
        QueryParameters {
            values: values
                .into_iter()
                .map(|v| v.map(Bytes::copy_from_slice))
                .collect(),
            formats: vec![0; len], // 0 = text format
            oids: vec![PgType::TEXT.oid(); len],
        }
    }

    /// Helper to create QueryParameters with specific OIDs (text format)
    fn typed_text_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), t.oid()))
            .unzip();
        QueryParameters {
            values,
            formats: vec![0; len], // 0 = text format
            oids,
        }
    }

    /// Helper to create QueryParameters with binary format
    fn binary_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), t.oid()))
            .unzip();
        QueryParameters {
            values,
            formats: vec![1; len], // 1 = binary format
            oids,
        }
    }

    #[test]
    fn test_ast_parameters_replace_simple() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");

        // Replace $1 with value "42"
        let params = text_params(vec![Some(b"42")]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE id = '42'");
    }

    #[test]
    fn test_ast_parameters_replace_multiple_params() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1 AND name = $2");

        // Replace $1 with "42", $2 with "alice"
        let params = text_params(vec![Some(b"42"), Some(b"alice")]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id = '42' AND name = 'alice'"
        );
    }

    #[test]
    fn test_ast_parameters_replace_null() {
        let mut node = parse_select_node("SELECT id FROM users WHERE name = $1");

        // Replace $1 with NULL
        let params = text_params(vec![None]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE name = NULL");
    }

    #[test]
    fn test_ast_parameters_replace_out_of_bounds() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $2");

        // Only provide 1 parameter, but query uses $2
        let params = text_params(vec![Some(b"42")]);
        let result = select_node_parameters_replace(&mut node, &params);

        assert!(result.is_err());
        match result.map_err(|e| e.into_current_context()) {
            Err(AstTransformError::ParameterOutOfBounds { index, count }) => {
                assert_eq!(index, 1); // $2 -> index 1
                assert_eq!(count, 1); // Only 1 parameter provided
            }
            _ => panic!("Expected ParameterOutOfBounds error"),
        }
    }

    #[test]
    fn test_ast_parameters_replace_in_join() {
        let mut node = parse_select_node(
            "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > $1",
        );

        // Replace $1 with "100"
        let params = text_params(vec![Some(b"100")]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        node.deparse(&mut buf);

        assert!(buf.contains("WHERE o.total > '100'"));
    }

    // ==================== Text Format Parameter Tests ====================

    #[test]
    fn test_text_parameter_integer() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");

        // Integer type should produce integer literal
        let params = typed_text_params(vec![(Some(b"42"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        // Integer literal renders without quotes
        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_text_parameter_boolean() {
        let mut node = parse_select_node("SELECT id FROM users WHERE active = $1");

        // Boolean 't' -> true
        let params = typed_text_params(vec![(Some(b"t"), PgType::BOOL)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE active = true");
    }

    #[test]
    fn test_text_parameter_float() {
        let mut node = parse_select_node("SELECT id FROM users WHERE score > $1");

        let params = typed_text_params(vec![(Some(b"3.14"), PgType::FLOAT8)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE score > 3.14");
    }

    #[test]
    fn test_text_parameter_invalid_integer() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"not_a_number")),
            format: 0,
            oid: PgType::INT4.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_text_parameter_uuid() {
        let mut node = parse_select_node("SELECT id FROM users WHERE uuid = $1");

        let params = typed_text_params(vec![(
            Some(b"550e8400-e29b-41d4-a716-446655440000"),
            PgType::UUID,
        )]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE uuid = '550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    // ==================== Binary Format Parameter Tests ====================

    #[test]
    fn test_binary_parameter_bool_true() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[1])), // PostgreSQL binary bool: 1 = true
            format: 1,
            oid: PgType::BOOL.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Boolean(true));
    }

    #[test]
    fn test_binary_parameter_bool_false() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0])), // PostgreSQL binary bool: 0 = false
            format: 1,
            oid: PgType::BOOL.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Boolean(false));
    }

    #[test]
    fn test_binary_parameter_int2() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x2A])), // 42 in big-endian i16
            format: 1,
            oid: PgType::INT2.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int4() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x2A])), // 42 in big-endian i32
            format: 1,
            oid: PgType::INT4.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int8() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A])), // 42 in big-endian i64
            format: 1,
            oid: PgType::INT8.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_float4() {
        // 3.14 as f32 in big-endian IEEE 754
        let value: f32 = 2.73;
        let bytes = value.to_be_bytes();

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::FLOAT4.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        match result {
            LiteralValue::Float(f) => {
                assert!((f.into_inner() - 2.73).abs() < 0.001);
            }
            _ => panic!("Expected Float literal"),
        }
    }

    #[test]
    fn test_binary_parameter_float8() {
        let value: f64 = 2.73821;
        let bytes = value.to_be_bytes();

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::FLOAT8.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        match result {
            LiteralValue::Float(f) => {
                assert!((f.into_inner() - 2.73821).abs() < 0.00001);
            }
            _ => panic!("Expected Float literal"),
        }
    }

    #[test]
    fn test_binary_parameter_text() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"hello world")),
            format: 1,
            oid: PgType::TEXT.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::String("hello world".to_owned()));
    }

    #[test]
    fn test_binary_parameter_uuid() {
        // UUID: 550e8400-e29b-41d4-a716-446655440000
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&uuid_bytes)),
            format: 1,
            oid: PgType::UUID.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(
            result,
            LiteralValue::String("550e8400-e29b-41d4-a716-446655440000".to_owned())
        );
    }

    #[test]
    fn test_binary_parameter_unsupported_type_with_invalid_utf8() {
        // Use invalid UTF-8 bytes to test that we get an error when
        // the fallback text interpretation fails
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0xFF, 0xFE])), // Invalid UTF-8 sequence
            format: 1,
            oid: PgType::TIMESTAMP.oid(), // Timestamp not supported in binary
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_unsupported_type_valid_utf8_fallback() {
        // Known but unhandled types with valid UTF-8 should fallback to string
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"2024-01-15")), // Valid UTF-8 text representation
            format: 1,
            oid: PgType::DATE.oid(), // Date not supported in binary
        };

        let result = parameter_to_literal(&param).expect("should fallback to string");
        assert_eq!(result, LiteralValue::String("2024-01-15".to_owned()));
    }

    #[test]
    fn test_binary_parameter_null() {
        let param = QueryParameter {
            value: None,
            format: 1,
            oid: PgType::INT4.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Null);
    }

    #[test]
    fn test_binary_int4_in_query() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");

        // Binary INT4: 42 in big-endian
        let params = binary_params(vec![(Some(&[0x00, 0x00, 0x00, 0x2A]), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    // ==================== WHERE Subquery Parameter Tests ====================

    #[test]
    fn test_parameters_replace_in_subquery() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > $1)",
        );

        let params = typed_text_params(vec![(Some(b"100"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"
        );
    }

    #[test]
    fn test_parameters_replace_exists_subquery() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > $1)",
        );

        let params = typed_text_params(vec![(Some(b"50"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > 50)"
        );
    }

    #[test]
    fn test_parameters_replace_subquery_with_outer_param() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE status = $1 AND id IN (SELECT user_id FROM orders WHERE total > $2)",
        );

        let params = typed_text_params(vec![
            (Some(b"active"), PgType::TEXT),
            (Some(b"200"), PgType::INT4),
        ]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE status = 'active' AND id IN (SELECT user_id FROM orders WHERE total > 200)"
        );
    }

    #[test]
    fn test_parameters_replace_nested_subquery() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > $1))",
        );

        let params = typed_text_params(vec![(Some(b"99"), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > 99))"
        );
    }

    #[test]
    fn test_parameters_replace_scalar_subquery_in_where() {
        let mut node = parse_select_node(
            "SELECT id FROM users WHERE age > (SELECT avg(age) FROM users WHERE status = $1)",
        );

        let params = typed_text_params(vec![(Some(b"active"), PgType::TEXT)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE age > (SELECT AVG(age) FROM users WHERE status = 'active')"
        );
    }

    // ==================== CTE Parameter Replacement Tests ====================

    #[test]
    fn test_cte_parameter_replacement() {
        let sql = "WITH active_users AS (SELECT id, name FROM users WHERE status = $1) \
                   SELECT id FROM active_users WHERE name = $2";
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");

        let params = typed_text_params(vec![
            (Some(b"active"), PgType::TEXT),
            (Some(b"alice"), PgType::TEXT),
        ]);

        let replaced =
            query_expr_parameters_replace(&query_expr, &params).expect("parameter replacement");

        let mut buf = String::new();
        replaced.deparse(&mut buf);

        // Parameters should be replaced in both the CTE body and the main query
        assert!(
            buf.contains("status = 'active'"),
            "CTE body should have $1 replaced: {buf}"
        );
        assert!(
            buf.contains("name = 'alice'"),
            "Main query should have $2 replaced: {buf}"
        );
        assert!(!buf.contains("$1"), "No unreplaced $1 should remain: {buf}");
        assert!(!buf.contains("$2"), "No unreplaced $2 should remain: {buf}");
    }
}

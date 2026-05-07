use fallible_iterator::FallibleIterator;
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
    let Some(index_str) = placeholder.strip_prefix('$') else {
        return Err(AstTransformError::InvalidParameterPlaceholder {
            placeholder: placeholder.to_owned(),
        }
        .into());
    };

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
    let s = std::str::from_utf8(bytes).map_err(|_| Report::from(AstTransformError::InvalidUtf8))?;

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

    // Dispatch by Kind first. PGC-103 was one instance of a class of bug:
    // the previous catch-all UTF-8 fallback silently corrupted SQL
    // whenever a binary wire format happened to be valid UTF-8 (binary
    // arrays were the smoking gun, but composite/range/numeric/jsonb and
    // friends share the same shape — length-prefixed integers and flag
    // bytes that pass UTF-8 validation but break SQL). Anything without
    // explicit handling here returns `UnsupportedBinaryFormat` so the
    // query falls through to origin uncached, rather than being
    // mis-decoded.
    if let Some(ref ty) = pg_type {
        match ty.kind() {
            postgres_types::Kind::Array(_) => {
                return binary_array_to_literal(bytes, oid);
            }
            postgres_types::Kind::Domain(base) => {
                // Domains are transparent wrappers; recurse on the base.
                return binary_parameter_to_literal(bytes, base.oid());
            }
            postgres_types::Kind::Enum(_) => {
                // Enum binary format is the UTF-8 label, by definition.
                let s = std::str::from_utf8(bytes).map_err(|_| {
                    Report::from(AstTransformError::InvalidParameterValue {
                        message: "binary enum value is not valid UTF-8".to_owned(),
                    })
                })?;
                return Ok(LiteralValue::String(s.to_owned()));
            }
            postgres_types::Kind::Composite(_)
            | postgres_types::Kind::Range(_)
            | postgres_types::Kind::Multirange(_)
            | postgres_types::Kind::Pseudo => {
                return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
            }
            postgres_types::Kind::Simple => {
                // Fall through to the per-OID match arms below.
            }
            _ => {
                // `Kind` is `#[non_exhaustive]`; reject anything new
                // conservatively so future variants don't silently slip
                // back into the UTF-8 fallback we just removed.
                return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
            }
        }
    }

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
        Some(PgType::JSON) => {
            // JSON binary format is plain UTF-8 JSON text — no version
            // prefix or other framing — so it round-trips as a string
            // literal, just with an explicit `::json` cast to preserve
            // the column's expected type.
            let value = pg_types::text_from_sql(bytes).map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid binary json: {}", e),
                })
            })?;
            Ok(LiteralValue::StringWithCast(
                value.to_owned(),
                PgType::JSON.name().to_owned(),
            ))
        }
        Some(PgType::JSONB) => {
            // JSONB binary format prepends a 1-byte version (currently
            // 0x01); the rest is UTF-8 JSON text. Strip the prefix so the
            // deparsed SQL is `'<json>'::jsonb` rather than carrying the
            // SOH control byte into the literal.
            match bytes.split_first() {
                Some((&0x01, json)) => {
                    let value = std::str::from_utf8(json).map_err(|_| {
                        Report::from(AstTransformError::InvalidParameterValue {
                            message: "jsonb body is not valid UTF-8".to_owned(),
                        })
                    })?;
                    Ok(LiteralValue::StringWithCast(
                        value.to_owned(),
                        PgType::JSONB.name().to_owned(),
                    ))
                }
                _ => Err(AstTransformError::InvalidParameterValue {
                    message: "missing or unknown jsonb version byte".to_owned(),
                }
                .into()),
            }
        }
        // Fail-closed: any type without an explicit decoder above is
        // rejected rather than coerced via UTF-8. See PGC-103 — the
        // previous fallback silently produced corrupted SQL for any
        // binary wire format whose bytes happened to be valid UTF-8.
        // Unsupported types fall through to origin uncached, which is the
        // correct behavior.
        _ => Err(AstTransformError::UnsupportedBinaryFormat { oid }.into()),
    }
}

/// Decode a binary-format array parameter into a `LiteralValue::StringWithCast`
/// holding a PG text array literal like `'{1,2,NULL,3}'::int4[]`.
///
/// Multi-dim arrays and arrays whose element type isn't in the supported
/// scalar set (the same set `binary_parameter_to_literal` handles directly)
/// return `UnsupportedBinaryFormat` so the caller can fall through to
/// origin uncached.
fn binary_array_to_literal(bytes: &[u8], oid: u32) -> AstTransformResult<LiteralValue> {
    let array = pg_types::array_from_sql(bytes).map_err(|e| {
        Report::from(AstTransformError::InvalidParameterValue {
            message: format!("invalid binary array: {e}"),
        })
    })?;

    // Reject anything other than a single dimension. PG supports N-dim
    // arrays in the wire format and the text format (`{{1,2},{3,4}}`); we
    // can extend later if a workload actually needs it.
    let mut dims = array.dimensions();
    let bad_dim = |_| Report::from(AstTransformError::UnsupportedBinaryFormat { oid });
    let _ = dims.next().map_err(bad_dim)?;
    if dims.next().map_err(bad_dim)?.is_some() {
        return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into());
    }

    let element_type = PgType::from_oid(array.element_type())
        .ok_or_else(|| Report::from(AstTransformError::UnsupportedBinaryFormat { oid }))?;

    // Hoist the per-element decoder out of the loop: the element type is
    // fixed for the whole array, so the type match runs once instead of
    // per element. Typed `LiteralValue` outputs let the constraint
    // analyzer treat `WHERE col = ANY(<array>)` as `InSet(values)` and
    // subsume narrower ANY-queries (PGC-106).
    let decode_element: fn(&[u8]) -> Option<LiteralValue> = match element_type {
        PgType::BOOL => |b| pg_types::bool_from_sql(b).ok().map(LiteralValue::Boolean),
        PgType::INT2 => |b| pg_types::int2_from_sql(b).ok().map(|n| LiteralValue::Integer(n.into())),
        PgType::INT4 => |b| pg_types::int4_from_sql(b).ok().map(|n| LiteralValue::Integer(n.into())),
        PgType::INT8 => |b| pg_types::int8_from_sql(b).ok().map(LiteralValue::Integer),
        PgType::FLOAT4 => |b| {
            pg_types::float4_from_sql(b)
                .ok()
                .and_then(|f| NotNan::new(f64::from(f)).ok())
                .map(LiteralValue::Float)
        },
        PgType::FLOAT8 => |b| {
            pg_types::float8_from_sql(b)
                .ok()
                .and_then(|f| NotNan::new(f).ok())
                .map(LiteralValue::Float)
        },
        PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME | PgType::CHAR => |b| {
            pg_types::text_from_sql(b)
                .ok()
                .map(|s| LiteralValue::String(s.to_owned()))
        },
        _ => return Err(AstTransformError::UnsupportedBinaryFormat { oid }.into()),
    };

    // Build typed elements; deparse will format them as the PG text
    // array literal `'{e1,e2,…}'::cast` at SQL emit time. Pre-size from
    // the iterator's reported remaining count to avoid Vec doubling
    // reallocations.
    let mut values = array.values();
    let mut elements: Vec<LiteralValue> = Vec::with_capacity(values.size_hint().0);
    while let Some(value) = values.next().map_err(bad_dim)? {
        match value {
            None => elements.push(LiteralValue::Null),
            Some(elem_bytes) => {
                let lit = decode_element(elem_bytes).ok_or_else(|| {
                    Report::from(AstTransformError::UnsupportedBinaryFormat { oid })
                })?;
                elements.push(lit);
            }
        }
    }

    let cast = format!("{}[]", element_type.name());
    Ok(LiteralValue::Array(elements, cast))
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use tokio_util::bytes::Bytes;

    use crate::query::ast::{Deparse, query_expr_convert, query_expr_fingerprint};

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
            value: Some(Bytes::from_static(&[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,
            ])), // 42 in big-endian i64
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

    // ==================== Kind-dispatch / fail-closed tests ====================
    //
    // These exercise the `Kind`-based dispatch added on top of
    // `binary_parameter_to_literal` after PGC-103. Builtin types reachable
    // via `PgType::from_oid` only cover `Kind::Simple`, `Array`, `Range`,
    // `Multirange`, and `Pseudo`. Synthesizing a `Kind::Composite`,
    // `Kind::Domain`, or `Kind::Enum` requires `PgType::new(...)` with a
    // custom OID that `from_oid` won't resolve, so those arms are validated
    // by code review only — the dispatch logic for them mirrors the Range
    // / Multirange / Pseudo paths exercised here.

    #[test]
    fn test_binary_parameter_range_rejected() {
        // `Kind::Range(Int4)`: builtin int4range. Even if the binary range
        // wire format happens to be valid UTF-8, the Kind dispatch rejects.
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x01])), // empty-range flags byte
            format: 1,
            oid: PgType::INT4_RANGE.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_multirange_rejected() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x00])), // ranges count = 0
            format: 1,
            oid: PgType::INT4MULTI_RANGE.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_pseudo_rejected() {
        // `record` and `any` are both `Kind::Pseudo`; reject either.
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x01])),
            format: 1,
            oid: PgType::RECORD.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_unknown_oid_rejected() {
        // OID that `PgType::from_oid` can't resolve falls through the Kind
        // dispatch (skipped because pg_type is None) into the new
        // fail-closed catch-all in the OID match.
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"42")),
            format: 1,
            oid: 999_999, // not a builtin OID
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_json() {
        // JSON binary format is plain UTF-8 JSON, no framing.
        let json = br#"{"a":1,"b":[2,3]}"#;
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(json)),
            format: 1,
            oid: PgType::JSON.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode binary json");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(
                r#"{"a":1,"b":[2,3]}"#.to_owned(),
                "json".to_owned()
            )
        );
    }

    #[test]
    fn test_binary_parameter_jsonb_strips_version_byte() {
        // JSONB binary: 0x01 version + UTF-8 JSON. The 0x01 SOH byte must
        // not end up in the deparsed SQL literal.
        let mut bytes = vec![0x01u8];
        bytes.extend_from_slice(br#"{"k":"v"}"#);
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::JSONB.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode binary jsonb");
        assert_eq!(
            literal,
            LiteralValue::StringWithCast(r#"{"k":"v"}"#.to_owned(), "jsonb".to_owned())
        );
    }

    #[test]
    fn test_binary_parameter_jsonb_unknown_version_rejected() {
        // Anything other than the documented 0x01 prefix is malformed.
        let bytes = [0x02, b'{', b'}'];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::JSONB.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_numeric_rejected() {
        // The classic silent-corruption vector before PGC-103: NUMERIC's
        // binary wire format is mostly small big-endian ints that pass
        // UTF-8 validation. Confirm we now reject rather than coerce.
        let bytes = vec![
            0x00, 0x01, // ndigits = 1
            0x00, 0x00, // weight = 0
            0x00, 0x00, // sign = positive
            0x00, 0x00, // dscale = 0
            0x00, 0x2A, // digit = 42
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::NUMERIC.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_timestamp_rejected_valid_utf8() {
        // The text version of this test (`...with_invalid_utf8`) covers the
        // bytes-aren't-valid-UTF-8 path; here the bytes ARE valid UTF-8
        // (the binary timestamp `0x4000000000000000` decodes to the
        // four-byte sequence "@" + three NULs, which is valid UTF-8 but
        // would silently corrupt SQL under the old fallback).
        let param = QueryParameter {
            value: Some(Bytes::from_static(&[
                0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ])),
            format: 1,
            oid: PgType::TIMESTAMP.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
    }

    #[test]
    fn test_binary_parameter_unsupported_type_rejected_even_with_valid_utf8() {
        // Sister test of `test_binary_parameter_unsupported_type_with_invalid_utf8`.
        // Before the fail-closed restructure, valid-UTF-8 binary bytes for an
        // unsupported type (here, DATE) silently fell through to a String
        // literal — that is the bug class we removed (PGC-103). Now the
        // function returns `UnsupportedBinaryFormat` regardless of whether
        // the bytes happen to be valid UTF-8, so the query falls through
        // to origin uncached.
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"2024-01-15")),
            format: 1,
            oid: PgType::DATE.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::UnsupportedBinaryFormat { .. })
        ));
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

    /// Postgres binary `int4[]` wire format for the array `[42, 100]`.
    /// Header is 4 bytes ndim + 4 bytes hasnull + 4 bytes element OID, then
    /// per-dim 4 bytes length + 4 bytes lower bound, then per-element 4 bytes
    /// length-prefix + the 4-byte value. Most bytes are zero — which is the
    /// crux of PGC-103: those zeros are valid UTF-8 and used to slip through
    /// the binary parameter catch-all into a `LiteralValue::String`.
    /// Encode a single binary text-array element: i32 length prefix
    /// followed by the UTF-8 bytes. Used to assemble synthetic `text[]`
    /// payloads in tests.
    fn array_text_element_bytes(s: &str) -> Vec<u8> {
        let len = i32::try_from(s.len()).expect("test element fits in i32");
        let mut buf = Vec::with_capacity(4 + s.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf
    }

    fn binary_int4_array_42_100() -> Vec<u8> {
        vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x02, // dim 0 length = 2
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
            0x00, 0x00, 0x00, 0x04, // element 0 length = 4
            0x00, 0x00, 0x00, 0x2A, // element 0 value = 42
            0x00, 0x00, 0x00, 0x04, // element 1 length = 4
            0x00, 0x00, 0x00, 0x64, // element 1 value = 100
        ]
    }

    #[test]
    fn test_binary_int4_array_decoded() {
        // PGC-103 regression: binary array parameters used to fall through
        // to the UTF-8 catch-all in `binary_parameter_to_literal`, producing
        // a `LiteralValue::String` carrying NUL bytes from the array header
        // and corrupting the deparsed SQL. The fix decodes the binary
        // wire format into a PG text array literal so the query stays
        // cacheable.
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&binary_int4_array_42_100())),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode binary int4[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![LiteralValue::Integer(42), LiteralValue::Integer(100)],
                "int4[]".to_owned()
            )
        );
    }

    #[test]
    fn test_binary_int4_array_distinct_values_produce_distinct_fingerprints() {
        // Sanity check that two different binary int4[] parameter values
        // substituted into the same query template produce different
        // post-substitution fingerprints. Otherwise pgcache would route
        // them to the same cache entry and one query's results would bleed
        // into the other's. This is the unit-level companion to the
        // `binary_array_test::test_binary_int4_array_distinct_cache_entries`
        // integration test.
        let pg_ast = pg_query::parse("SELECT id FROM widgets WHERE id = ANY($1)")
            .expect("parse SQL");
        let q1 = query_expr_convert(&pg_ast).expect("convert to QueryExpr");
        let q2 = q1.clone();

        let arr1 = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02,
        ];
        let arr2 = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x05,
        ];
        let p1 = binary_params(vec![(Some(&arr1), PgType::INT4_ARRAY)]);
        let p2 = binary_params(vec![(Some(&arr2), PgType::INT4_ARRAY)]);

        let r1 = query_expr_parameters_replace(&q1, &p1).expect("replace 1");
        let r2 = query_expr_parameters_replace(&q2, &p2).expect("replace 2");

        let mut s1 = String::new();
        let mut s2 = String::new();
        r1.deparse(&mut s1);
        r2.deparse(&mut s2);
        assert_ne!(s1, s2, "deparsed SQL should differ between arrays");

        let f1 = query_expr_fingerprint(&r1);
        let f2 = query_expr_fingerprint(&r2);
        assert_ne!(
            f1, f2,
            "different binary int4[] values must produce different fingerprints"
        );
    }

    #[test]
    fn test_binary_int4_array_in_query_renders_clean_sql() {
        // PGC-103 integration regression: `WHERE id = ANY($1)` with a
        // binary int4[] must produce a SQL string with no NUL bytes and a
        // properly cast text array literal.
        let mut node = parse_select_node("SELECT id FROM users WHERE id = ANY($1)");

        let array_bytes = binary_int4_array_42_100();
        let params = binary_params(vec![(Some(&array_bytes), PgType::INT4_ARRAY)]);

        select_node_parameters_replace(&mut node, &params).expect("substitute binary int4[]");

        let mut buf = String::new();
        node.deparse(&mut buf);

        assert!(
            !buf.as_bytes().contains(&0),
            "deparsed SQL must not contain NUL bytes; got {buf:?}"
        );
        assert_eq!(buf, "SELECT id FROM users WHERE id = ANY ('{42,100}'::int4[])");
    }

    #[test]
    fn test_binary_int4_array_empty() {
        // Encoded `{}::int4[]`: dimensions=0, hasnull=0, elemtype=23, no
        // dim entries and no elements.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // ndim = 0
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode empty int4[]");
        assert_eq!(
            literal,
            LiteralValue::Array(vec![], "int4[]".to_owned())
        );
    }

    #[test]
    fn test_binary_int4_array_with_null_element() {
        // `{1, NULL, 3}::int4[]`: NULL elements have length-prefix = -1.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x01, // hasnull = 1
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x03, // dim 0 length = 3
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, // 1
            0xFF, 0xFF, 0xFF, 0xFF, // NULL (length = -1)
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03, // 3
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode int4[] with null");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Null,
                    LiteralValue::Integer(3),
                ],
                "int4[]".to_owned()
            )
        );
    }

    #[test]
    fn test_binary_text_array_quoting() {
        // `text[]` whose elements include separator chars, embedded quotes,
        // backslashes, whitespace, an empty string, and the literal "NULL".
        // All require quoting per PG array text format rules.
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x19, // elemtype = 25 (text)
            0x00, 0x00, 0x00, 0x05, // dim 0 length = 5
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
        ];
        for s in ["plain", "with,comma", "has\"quote", "back\\slash", ""] {
            bytes.extend_from_slice(&array_text_element_bytes(s));
        }

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TEXT_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode text[]");
        // Decoded elements are stored as plain `LiteralValue::String` —
        // PG-array-text quoting (`"`-wrap, `\`-escape) is applied at
        // deparse time, not at decode time, so the constraint analyzer
        // can compare element values directly.
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::String("plain".to_owned()),
                    LiteralValue::String("with,comma".to_owned()),
                    LiteralValue::String("has\"quote".to_owned()),
                    LiteralValue::String("back\\slash".to_owned()),
                    LiteralValue::String(String::new()),
                ],
                "text[]".to_owned()
            )
        );

        // Also verify the deparsed SQL still applies array-text-format
        // quoting and SQL string-literal escaping (the byte-identity
        // guarantee from PGC-103's previous representation).
        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(
            buf,
            r#"E'{plain,"with,comma","has\\"quote","back\\\\slash",""}'::text[]"#
        );
    }

    #[test]
    fn test_binary_text_array_with_null_string_element() {
        // The element value `"null"` (case-insensitive) must be quoted so
        // PG doesn't read it as a NULL marker.
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
        ];
        bytes.extend_from_slice(&array_text_element_bytes("NULL"));
        bytes.extend_from_slice(&array_text_element_bytes("a"));

        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TEXT_ARRAY.oid(),
        };

        let literal = parameter_to_literal(&param).expect("decode text[]");
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::String("NULL".to_owned()),
                    LiteralValue::String("a".to_owned()),
                ],
                "text[]".to_owned()
            )
        );

        // Deparse still wraps the literal `"NULL"` so PG doesn't read
        // it as a SQL NULL marker.
        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(buf, r#"'{"NULL",a}'::text[]"#);
    }

    #[test]
    fn test_binary_multidim_array_rejected() {
        // 2-D arrays are deliberately not supported; we want them to fall
        // through to origin uncached rather than emit half-correct text.
        // Encoded as 2-D even though there's only one element so the buffer
        // stays small.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x02, // ndim = 2
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // dim 0: len=1, lb=1
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // dim 1: len=1, lb=1
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A, // single element = 42
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::INT4_ARRAY.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(
            matches!(result, Err(AstTransformError::UnsupportedBinaryFormat { .. })),
            "expected UnsupportedBinaryFormat for 2-D array, got {result:?}"
        );
    }

    #[test]
    fn test_binary_array_unsupported_element_type_rejected() {
        // `timestamp[]` element type isn't in our supported set; should
        // fall through to origin uncached, not emit corrupted text.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x04, 0x5A, // elemtype = 1114 (timestamp)
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x08, // length = 8
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // dummy timestamp bytes
        ];
        let param = QueryParameter {
            value: Some(Bytes::copy_from_slice(&bytes)),
            format: 1,
            oid: PgType::TIMESTAMP_ARRAY.oid(),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(
            matches!(result, Err(AstTransformError::UnsupportedBinaryFormat { .. })),
            "expected UnsupportedBinaryFormat for timestamp[], got {result:?}"
        );
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

use error_set::error_set;
use ordered_float::NotNan;
use postgres_protocol::types as pg_types;
use postgres_types::Type as PgType;
use rootcause::Report;

use crate::{
    cache::{QueryParameter, QueryParameters, UpdateQuerySource, query::CacheableQuery},
    catalog::TableMetadata,
    query::ast::{
        ColumnExpr, LiteralValue, QueryBody, QueryExpr, SelectColumn, SelectColumns, SelectNode,
        TableAlias, TableNode, TableSource, ValuesClause, WhereExpr,
    },
    query::resolved::{
        ResolvedQueryBody, ResolvedQueryExpr, ResolvedSelectColumns, ResolvedSelectNode,
        ResolvedTableSource, ResolvedTableSubqueryNode,
    },
};

error_set! {
    AstTransformError := {
        MissingTable,
        #[display("Parameter index {index} out of bounds (have {count} parameters)")]
        ParameterOutOfBounds { index: usize, count: usize },
        #[display("Invalid parameter placeholder: {placeholder}")]
        InvalidParameterPlaceholder { placeholder: String },
        #[display("Invalid UTF-8 in parameter value")]
        InvalidUtf8,
        #[display("Invalid parameter value: {message}")]
        InvalidParameterValue { message: String },
        #[display("Unsupported binary format for OID {oid}")]
        UnsupportedBinaryFormat { oid: u32 },
    }
}

/// Result type with location-tracking error reports for AST transform operations.
pub type AstTransformResult<T> = Result<T, Report<AstTransformError>>;

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
    let s = String::from_utf8(bytes.to_vec())
        .map_err(|_| Report::from(AstTransformError::InvalidUtf8))?;

    // Use postgres_types to identify the type from OID
    let pg_type = PgType::from_oid(oid);

    match pg_type {
        Some(PgType::BOOL) => {
            let value = matches!(s.as_str(), "t" | "true" | "TRUE" | "1");
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
        ) => Ok(LiteralValue::String(s)),
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
        ) => Ok(LiteralValue::String(s)),
        // Unknown OID or unhandled type - fallback to string
        _ => Ok(LiteralValue::String(s)),
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

/// Replace SELECT columns in a SelectNode.
pub fn select_node_columns_replace(node: &SelectNode, columns: SelectColumns) -> SelectNode {
    SelectNode {
        distinct: node.distinct,
        columns,
        from: node.from.clone(),
        where_clause: node.where_clause.clone(),
        group_by: node.group_by.clone(),
        having: node.having.clone(),
    }
}

/// Replace SELECT columns in a ResolvedSelectNode for cache population fetches.
///
/// This strips aggregation-related clauses (GROUP BY, HAVING) and DISTINCT because
/// we want raw rows for caching, with aggregation performed at cache retrieval time.
pub fn resolved_select_node_replace(
    resolved: &ResolvedSelectNode,
    columns: ResolvedSelectColumns,
) -> ResolvedSelectNode {
    ResolvedSelectNode {
        distinct: false,
        columns,
        from: resolved.from.clone(),
        where_clause: resolved.where_clause.clone(),
        group_by: vec![],
        having: None,
    }
}

/// Replace a table source with a VALUES clause in a ResolvedSelectNode.
///
/// Similar to `resolved_table_replace_with_values` but operates on the new ResolvedSelectNode type.
pub fn resolved_select_node_table_replace_with_values(
    resolved: &ResolvedSelectNode,
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> AstTransformResult<ResolvedSelectNode> {
    let mut resolved_new = resolved.clone();
    let relation_oid = table_metadata.relation_oid;

    // Find first matching table source by relation_oid and get the alias
    let alias = {
        let Some(first_from) = resolved_new.from.first() else {
            return Err(AstTransformError::MissingTable.into());
        };
        let mut frontier = vec![first_from];
        let mut found_alias: Option<String> = None;
        while let Some(cur) = frontier.pop() {
            match cur {
                ResolvedTableSource::Join(join) => {
                    frontier.push(&join.left);
                    frontier.push(&join.right);
                }
                ResolvedTableSource::Table(table) => {
                    if table.relation_oid == relation_oid {
                        found_alias = Some(
                            table
                                .alias
                                .clone()
                                .unwrap_or_else(|| table_metadata.name.clone()),
                        );
                        break;
                    }
                }
                _ => (),
            }
        }
        found_alias.ok_or_else(|| Report::from(AstTransformError::MissingTable))?
    };

    // Update all column references for this table to use the alias
    resolved_select_node_column_alias_update(
        &mut resolved_new,
        &table_metadata.schema,
        &table_metadata.name,
        &alias,
    );

    // Now replace the table source with a VALUES subquery
    let Some(first_from) = resolved_new.from.first_mut() else {
        return Err(AstTransformError::MissingTable.into());
    };
    let mut frontier = vec![first_from];
    let mut source_node: Option<&mut ResolvedTableSource> = None;
    while let Some(cur) = frontier.pop() {
        match cur {
            ResolvedTableSource::Join(join) => {
                frontier.push(&mut join.left);
                frontier.push(&mut join.right);
            }
            ResolvedTableSource::Table(table) => {
                if table.relation_oid == relation_oid {
                    source_node = Some(cur);
                    break;
                }
            }
            _ => (),
        }
    }

    let Some(source_node) = source_node else {
        return Err(AstTransformError::MissingTable.into());
    };

    // Build VALUES clause from row_data and collect column names
    let mut values = Vec::new();
    let mut column_names = Vec::new();
    for column_meta in &table_metadata.columns {
        let position = column_meta.position as usize - 1;
        if let Some(row_value) = row_data.get(position) {
            let value = row_value.as_deref().map_or(LiteralValue::Null, |v| {
                LiteralValue::StringWithCast(v.to_owned(), column_meta.type_name.clone())
            });
            values.push(value);
            column_names.push(column_meta.name.clone());
        }
    }

    *source_node = ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
        query: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Values(ValuesClause { rows: vec![values] }),
            order_by: vec![],
            limit: None,
        }),
        alias: TableAlias {
            name: alias,
            columns: column_names,
        },
    });

    Ok(resolved_new)
}

/// Update all column references for a specific table to use an alias in a ResolvedSelectNode.
fn resolved_select_node_column_alias_update(
    resolved: &mut ResolvedSelectNode,
    schema: &str,
    table: &str,
    alias: &str,
) {
    // Update WHERE clause columns
    if let Some(where_clause) = &mut resolved.where_clause {
        resolved_where_column_alias_update(where_clause, schema, table, alias);
    }
    // Update SELECT columns
    resolved_select_columns_alias_update(&mut resolved.columns, schema, table, alias);
}

/// Update column aliases in a ResolvedQueryExpr
fn resolved_query_expr_column_alias_update(
    query: &mut ResolvedQueryExpr,
    schema: &str,
    table: &str,
    alias: &str,
) {
    match &mut query.body {
        ResolvedQueryBody::Select(select_node) => {
            // Update WHERE clause columns
            if let Some(where_clause) = &mut select_node.where_clause {
                resolved_where_column_alias_update(where_clause, schema, table, alias);
            }
            // Update SELECT columns
            resolved_select_columns_alias_update(&mut select_node.columns, schema, table, alias);
        }
        ResolvedQueryBody::Values(_) => {
            // VALUES clauses don't have column references to update
        }
        ResolvedQueryBody::SetOp(set_op) => {
            resolved_query_expr_column_alias_update(&mut set_op.left, schema, table, alias);
            resolved_query_expr_column_alias_update(&mut set_op.right, schema, table, alias);
        }
    }
    // Update ORDER BY columns at query level
    for order_by in &mut query.order_by {
        resolved_column_expr_alias_update(&mut order_by.expr, schema, table, alias);
    }
}

fn resolved_where_column_alias_update(
    expr: &mut crate::query::resolved::ResolvedWhereExpr,
    schema: &str,
    table: &str,
    alias: &str,
) {
    use crate::query::resolved::ResolvedWhereExpr;

    match expr {
        ResolvedWhereExpr::Column(col) => {
            if col.schema == schema && col.table == table {
                col.table_alias = Some(alias.to_owned());
            }
        }
        ResolvedWhereExpr::Unary(unary) => {
            resolved_where_column_alias_update(&mut unary.expr, schema, table, alias);
        }
        ResolvedWhereExpr::Binary(binary) => {
            resolved_where_column_alias_update(&mut binary.lexpr, schema, table, alias);
            resolved_where_column_alias_update(&mut binary.rexpr, schema, table, alias);
        }
        ResolvedWhereExpr::Multi(multi) => {
            for e in &mut multi.exprs {
                resolved_where_column_alias_update(e, schema, table, alias);
            }
        }
        ResolvedWhereExpr::Function { args, .. } => {
            for arg in args {
                resolved_where_column_alias_update(arg, schema, table, alias);
            }
        }
        ResolvedWhereExpr::Subquery {
            query, test_expr, ..
        } => {
            resolved_query_expr_column_alias_update(query, schema, table, alias);
            if let Some(test) = test_expr {
                resolved_where_column_alias_update(test, schema, table, alias);
            }
        }
        ResolvedWhereExpr::Value(_) => {}
    }
}

fn resolved_select_columns_alias_update(
    columns: &mut crate::query::resolved::ResolvedSelectColumns,
    schema: &str,
    table: &str,
    alias: &str,
) {
    use crate::query::resolved::ResolvedSelectColumns;

    match columns {
        ResolvedSelectColumns::All(cols) => {
            for col in cols {
                if col.schema == schema && col.table == table {
                    col.table_alias = Some(alias.to_owned());
                }
            }
        }
        ResolvedSelectColumns::Columns(cols) => {
            for col in cols {
                resolved_column_expr_alias_update(&mut col.expr, schema, table, alias);
            }
        }
        ResolvedSelectColumns::None => {}
    }
}

fn resolved_column_expr_alias_update(
    expr: &mut crate::query::resolved::ResolvedColumnExpr,
    schema: &str,
    table: &str,
    alias: &str,
) {
    use crate::query::resolved::ResolvedColumnExpr;

    match expr {
        ResolvedColumnExpr::Column(col) => {
            if col.schema == schema && col.table == table {
                col.table_alias = Some(alias.to_owned());
            }
        }
        ResolvedColumnExpr::Function {
            args,
            agg_order,
            over,
            ..
        } => {
            for arg in args {
                resolved_column_expr_alias_update(arg, schema, table, alias);
            }
            for clause in agg_order {
                resolved_column_expr_alias_update(&mut clause.expr, schema, table, alias);
            }
            if let Some(window_spec) = over {
                for col in &mut window_spec.partition_by {
                    resolved_column_expr_alias_update(col, schema, table, alias);
                }
                for clause in &mut window_spec.order_by {
                    resolved_column_expr_alias_update(&mut clause.expr, schema, table, alias);
                }
            }
        }
        ResolvedColumnExpr::Case(case) => {
            if let Some(arg) = &mut case.arg {
                resolved_column_expr_alias_update(arg, schema, table, alias);
            }
            for when in &mut case.whens {
                resolved_where_column_alias_update(&mut when.condition, schema, table, alias);
                resolved_column_expr_alias_update(&mut when.result, schema, table, alias);
            }
            if let Some(default) = &mut case.default {
                resolved_column_expr_alias_update(default, schema, table, alias);
            }
        }
        ResolvedColumnExpr::Arithmetic(arith) => {
            resolved_column_expr_alias_update(&mut arith.left, schema, table, alias);
            resolved_column_expr_alias_update(&mut arith.right, schema, table, alias);
        }
        ResolvedColumnExpr::Subquery(query) => {
            resolved_query_expr_column_alias_update(query, schema, table, alias);
        }
        ResolvedColumnExpr::Identifier(_) | ResolvedColumnExpr::Literal(_) => {}
    }
}

/// Generate queries used to check if a DML statement applies to a given table.
/// Returns one update query per (table, branch) pair.
///
/// For simple SELECT queries, each table gets one update query derived from that SELECT.
/// For set operations (UNION/INTERSECT/EXCEPT), each table gets an update query derived
/// from just the branch that contains it (not the full set operation).
///
/// This approach ensures that CDC handling only checks if a changed row matches the
/// specific branch conditions, not the entire set operation structure.
pub fn query_table_update_queries(
    cacheable_query: &CacheableQuery,
) -> Vec<(&TableNode, QueryExpr, UpdateQuerySource)> {
    let mut result = Vec::new();

    let column = SelectColumn {
        expr: ColumnExpr::Literal(LiteralValue::Boolean(true)),
        alias: None,
    };
    let select_list = SelectColumns::Columns(vec![column]);

    // For each SELECT branch in the query, with its source context
    for (branch, source) in cacheable_query.query.select_nodes_with_source() {
        // For each direct table in this branch (not inside subqueries).
        // Tables inside subqueries are handled by their own inner branch.
        for table in branch.direct_table_nodes() {
            // Create update query from just this branch (not full query)
            let update_select = select_node_columns_replace(branch, select_list.clone());
            let update_query = QueryExpr {
                ctes: vec![],
                body: QueryBody::Select(update_select),
                order_by: vec![],
                limit: None,
            };
            result.push((table, update_query, source));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::wildcard_enum_match_arm)]

    use crate::cache::SubqueryKind;
    use crate::query::ast::{Deparse, query_expr_convert};

    use super::*;

    /// Helper to parse SQL and return a CacheableQuery
    fn parse_cacheable(sql: &str) -> CacheableQuery {
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        CacheableQuery::try_from(&query_expr).expect("query to be cacheable")
    }

    /// Helper to create QueryParameters for text format with TEXT OID
    fn text_params(values: Vec<Option<&[u8]>>) -> QueryParameters {
        let len = values.len();
        QueryParameters {
            values: values.into_iter().map(|v| v.map(|b| b.to_vec())).collect(),
            formats: vec![0; len], // 0 = text format
            oids: vec![PgType::TEXT.oid(); len],
        }
    }

    /// Helper to create QueryParameters with specific OIDs (text format)
    fn typed_text_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(|b| b.to_vec()), t.oid()))
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
            .map(|(v, t)| (v.map(|b| b.to_vec()), t.oid()))
            .unzip();
        QueryParameters {
            values,
            formats: vec![1; len], // 1 = binary format
            oids,
        }
    }

    /// Parse SQL and return a SelectNode (for tests using new types)
    fn parse_select_node(sql: &str) -> SelectNode {
        use crate::query::ast::{query_expr_convert, QueryBody};
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => node,
            _ => panic!("expected SELECT"),
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

    #[test]
    fn test_query_select_replace() {
        // Test replacing specific columns with SELECT *
        let node = parse_select_node("SELECT id, name, email FROM users WHERE id = 1");
        let result = select_node_columns_replace(&node, SelectColumns::All);

        // Deparse to verify the transformation
        let mut buf = String::new();
        let new_sql = result.deparse(&mut buf);

        // Should now be SELECT * FROM users WHERE id = 1
        assert!(
            new_sql.contains("SELECT *"),
            "Query should contain SELECT *"
        );
        assert!(
            new_sql.contains("FROM users"),
            "Query should preserve FROM clause"
        );
        assert!(
            new_sql.contains("WHERE id = 1"),
            "Query should preserve WHERE clause"
        );
        assert!(
            !new_sql.contains("id, name, email"),
            "Query should not contain original columns"
        );
    }

    #[test]
    fn test_query_select_replace_with_complex_where() {
        // Test with more complex query
        let node = parse_select_node(
            "SELECT a.id, b.data FROM table_a a WHERE a.status = 'active' AND b.enabled = true",
        );
        let result = select_node_columns_replace(&node, SelectColumns::All);

        // Deparse to verify the transformation
        let mut buf = String::new();
        let new_sql = result.deparse(&mut buf);

        // Should now contain SELECT *
        assert!(
            new_sql.contains("SELECT *"),
            "Query should contain SELECT *"
        );

        // Verify FROM clause is preserved
        assert!(
            new_sql.contains("FROM table_a a"),
            "Query should preserve FROM clause with alias: {new_sql}"
        );

        // Verify complete WHERE clause is preserved
        assert!(
            new_sql.contains("WHERE a.status = 'active' AND b.enabled = true"),
            "Query should preserve complete WHERE clause: {new_sql}"
        );

        // Verify the original column list is NOT present
        assert!(
            !new_sql.contains("a.id, b.data"),
            "Query should not contain original column list: {new_sql}"
        );

        // Verify the complete expected structure
        let expected_pattern =
            "SELECT * FROM table_a a WHERE a.status = 'active' AND b.enabled = true";
        assert_eq!(
            new_sql, expected_pattern,
            "Query should match expected pattern"
        );
    }

    #[test]
    fn test_query_table_update_query_simple_select() {
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 1);

        let mut buf = String::new();
        let new_sql = result[0].1.deparse(&mut buf);

        assert_eq!(new_sql, "SELECT true FROM users WHERE id = 1");
    }

    #[test]
    fn test_query_table_update_query_simple_join() {
        let original_query = "SELECT id, name, email FROM users \
                            JOIN location ON location.user_id = users.user_id \
                            WHERE users.user_id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 2);

        let mut update1 = String::new();
        result[0].1.deparse(&mut update1);
        assert_eq!(
            update1,
            "SELECT true FROM users JOIN location ON location.user_id = users.user_id WHERE users.user_id = 1"
        );

        let mut update2 = String::new();
        result[1].1.deparse(&mut update2);
        assert_eq!(
            update2,
            "SELECT true FROM users JOIN location ON location.user_id = users.user_id WHERE users.user_id = 1"
        );
    }

    #[test]
    fn test_query_table_update_query_three_table_join() {
        let original_query = "SELECT * FROM users \
                            JOIN location ON location.user_id = users.user_id \
                            JOIN orders ON orders.user_id = users.user_id \
                            WHERE users.user_id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 3, "Should have 3 update queries for 3 tables");

        // All three queries should produce the same SQL (the full join query with SELECT true)
        let mut update1 = String::new();
        result[0].1.deparse(&mut update1);
        assert!(update1.contains("SELECT true"));
        assert!(update1.contains("JOIN"));

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"location"));
        assert!(table_names.contains(&"orders"));
    }

    #[test]
    fn test_query_table_update_query_four_table_join() {
        let original_query = "SELECT * FROM a \
                            JOIN b ON a.id = b.id \
                            JOIN c ON b.id = c.id \
                            JOIN d ON c.id = d.id \
                            WHERE a.id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 4, "Should have 4 update queries for 4 tables");

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.name.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));
        assert!(table_names.contains(&"d"));
    }

    // ==================== Set Operation Update Query Tests ====================
    //
    // Update queries for set operations are now branch-specific:
    // - Each table gets an update query derived from just its branch
    // - No UNION structure is preserved in update queries
    // - This enables CDC to check individual branch predicates

    #[test]
    fn test_query_table_update_query_union() {
        let original_query = "SELECT id FROM users WHERE tenant_id = 1 \
                              UNION SELECT id FROM admins WHERE tenant_id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 2, "Should have 2 update queries for 2 tables");

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"admins"));

        // Update queries should be branch-specific (no UNION)
        for (table, query, _) in &result {
            let mut sql = String::new();
            query.deparse(&mut sql);
            assert!(
                !sql.contains("UNION"),
                "Update query for {} should NOT contain UNION: {}",
                table.name,
                sql
            );
            assert!(
                sql.contains("SELECT true"),
                "Update query should have SELECT true: {}",
                sql
            );
            assert!(
                sql.contains(&table.name),
                "Update query for {} should reference that table: {}",
                table.name,
                sql
            );
        }
    }

    #[test]
    fn test_query_table_update_query_nested_union() {
        let original_query = "SELECT id FROM a WHERE tenant_id = 1 \
                              UNION SELECT id FROM b WHERE tenant_id = 1 \
                              UNION SELECT id FROM c WHERE tenant_id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 3, "Should have 3 update queries for 3 tables");

        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.name.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));

        // Verify each update query is branch-specific (no UNION)
        for (table, query, _) in &result {
            let mut sql = String::new();
            query.deparse(&mut sql);
            assert!(
                !sql.contains("UNION"),
                "Update query for {} should NOT contain UNION: {}",
                table.name,
                sql
            );
        }
    }

    #[test]
    fn test_query_table_update_query_union_with_join() {
        let original_query = "SELECT a.id FROM a JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
                              UNION SELECT id FROM c WHERE tenant_id = 1";
        let cacheable_query = parse_cacheable(original_query);
        let result = query_table_update_queries(&cacheable_query);

        assert_eq!(result.len(), 3, "Should have 3 update queries (a, b, c)");

        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.name.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));
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
            value: Some(b"not_a_number".to_vec()),
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
            value: Some(vec![1]), // PostgreSQL binary bool: 1 = true
            format: 1,
            oid: PgType::BOOL.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Boolean(true));
    }

    #[test]
    fn test_binary_parameter_bool_false() {
        let param = QueryParameter {
            value: Some(vec![0]), // PostgreSQL binary bool: 0 = false
            format: 1,
            oid: PgType::BOOL.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Boolean(false));
    }

    #[test]
    fn test_binary_parameter_int2() {
        let param = QueryParameter {
            value: Some(vec![0x00, 0x2A]), // 42 in big-endian i16
            format: 1,
            oid: PgType::INT2.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int4() {
        let param = QueryParameter {
            value: Some(vec![0x00, 0x00, 0x00, 0x2A]), // 42 in big-endian i32
            format: 1,
            oid: PgType::INT4.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int8() {
        let param = QueryParameter {
            value: Some(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A]), // 42 in big-endian i64
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
            value: Some(bytes.to_vec()),
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
            value: Some(bytes.to_vec()),
            format: 1,
            oid: PgType::FLOAT8.oid(),
        };

        let result = parameter_to_literal(&param).expect("to convert parameter");
        match result {
            LiteralValue::Float(f) => {
                assert!((f.into_inner() - 2.73821).abs() < 0.00001);
            }
            _ => panic!(
                "Expected F
           loat literal"
            ),
        }
    }

    #[test]
    fn test_binary_parameter_text() {
        let param = QueryParameter {
            value: Some(b"hello world".to_vec()),
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
            value: Some(uuid_bytes.to_vec()),
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
            value: Some(vec![0xFF, 0xFE]), // Invalid UTF-8 sequence
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
            value: Some(b"2024-01-15".to_vec()), // Valid UTF-8 text representation
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
        let mut node =
            parse_select_node("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > $1)");

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

    // ==================== Resolved Node Tests ====================

    #[test]
    fn test_resolved_select_node_replace_strips_group_by() {
        use crate::catalog::ColumnMetadata;
        use postgres_types::Type;
        use crate::query::resolved::{
            ResolvedColumnNode, ResolvedSelectColumns, ResolvedSelectNode,
        };

        let col_meta = ColumnMetadata {
            name: "status".to_owned(),
            position: 1,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".to_owned(),
            cache_type_name: "text".to_owned(),
            is_primary_key: false,
        };

        let node = ResolvedSelectNode {
            group_by: vec![ResolvedColumnNode {
                schema: "public".to_owned(),
                table: "orders".to_owned(),
                table_alias: None,
                column: "status".to_owned(),
                column_metadata: col_meta,
            }],
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert!(
            result.group_by.is_empty(),
            "resolved_select_node_replace should strip GROUP BY"
        );
    }

    #[test]
    fn test_resolved_select_node_replace_strips_having() {
        use crate::query::ast::BinaryOp;
        use crate::query::resolved::{
            ResolvedBinaryExpr, ResolvedSelectColumns, ResolvedSelectNode, ResolvedWhereExpr,
        };

        let node = ResolvedSelectNode {
            having: Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: BinaryOp::Equal,
                lexpr: Box::new(ResolvedWhereExpr::Value(LiteralValue::Integer(1))),
                rexpr: Box::new(ResolvedWhereExpr::Value(LiteralValue::Integer(1))),
            })),
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert!(
            result.having.is_none(),
            "resolved_select_node_replace should strip HAVING"
        );
    }

    // Note: ORDER BY and LIMIT tests removed - these fields are now on ResolvedQueryExpr,
    // not ResolvedSelectNode. The resolved_select_node_replace function only operates on
    // ResolvedSelectNode which doesn't have order_by or limit fields.

    #[test]
    fn test_resolved_select_node_replace_preserves_where() {
        use crate::query::ast::BinaryOp;
        use crate::query::resolved::{
            ResolvedBinaryExpr, ResolvedSelectColumns, ResolvedSelectNode, ResolvedWhereExpr,
        };

        let where_clause = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(ResolvedWhereExpr::Value(LiteralValue::Integer(1))),
            rexpr: Box::new(ResolvedWhereExpr::Value(LiteralValue::Integer(1))),
        });

        let node = ResolvedSelectNode {
            where_clause: Some(where_clause),
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert!(
            result.where_clause.is_some(),
            "resolved_select_node_replace should preserve WHERE clause"
        );
    }

    #[test]
    fn test_resolved_select_node_replace_preserves_from() {
        use crate::query::resolved::{
            ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableNode, ResolvedTableSource,
        };

        let node = ResolvedSelectNode {
            from: vec![ResolvedTableSource::Table(ResolvedTableNode {
                schema: "public".to_owned(),
                name: "orders".to_owned(),
                alias: None,
                relation_oid: 12345,
            })],
            ..Default::default()
        };

        let result = resolved_select_node_replace(&node, ResolvedSelectColumns::None);

        assert_eq!(
            result.from.len(),
            1,
            "resolved_select_node_replace should preserve FROM clause"
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

        let replaced = query_expr_parameters_replace(&query_expr, &params)
            .expect("parameter replacement");

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
        assert!(
            !buf.contains("$1"),
            "No unreplaced $1 should remain: {buf}"
        );
        assert!(
            !buf.contains("$2"),
            "No unreplaced $2 should remain: {buf}"
        );
    }

    #[test]
    fn test_cte_update_queries() {
        let sql = "WITH active_users AS (SELECT id, name FROM users WHERE status = 'active') \
                   SELECT active_users.id, orders.total \
                   FROM active_users \
                   JOIN orders ON orders.user_id = active_users.id";
        let cacheable = parse_cacheable(sql);
        let result = query_table_update_queries(&cacheable);

        // The CTE body contains "users" and the main query joins with "orders".
        // select_nodes_with_source traverses into CTE bodies via CteRef,
        // so we should get tables from both the CTE body and the main query.
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.name.as_str()).collect();
        assert!(
            table_names.contains(&"users"),
            "Should have update query for 'users' table from CTE body: {table_names:?}"
        );
        assert!(
            table_names.contains(&"orders"),
            "Should have update query for 'orders' table from main query: {table_names:?}"
        );
    }

    // ==================== UpdateQuerySource Tests ====================

    #[test]
    fn test_update_query_source_simple() {
        let cacheable = parse_cacheable("SELECT id FROM users WHERE id = 1");
        let result = query_table_update_queries(&cacheable);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, UpdateQuerySource::Direct);
    }

    #[test]
    fn test_update_query_source_join() {
        let cacheable = parse_cacheable(
            "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id WHERE a.id = 1",
        );
        let result = query_table_update_queries(&cacheable);

        assert_eq!(result.len(), 2);
        // Both tables in a JOIN are Direct
        assert!(
            result.iter().all(|(_, _, src)| *src == UpdateQuerySource::Direct),
            "All JOIN tables should be Direct"
        );
    }

    #[test]
    fn test_update_query_source_where_in() {
        let cacheable = parse_cacheable(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",
        );
        let result = query_table_update_queries(&cacheable);

        assert_eq!(result.len(), 2);

        let users = result.iter().find(|(t, _, _)| t.name == "users").unwrap();
        assert_eq!(users.2, UpdateQuerySource::Direct);

        let active = result
            .iter()
            .find(|(t, _, _)| t.name == "active_users")
            .unwrap();
        assert_eq!(
            active.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion)
        );
    }

    #[test]
    fn test_update_query_source_not_in() {
        let cacheable = parse_cacheable(
            "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",
        );
        let result = query_table_update_queries(&cacheable);

        assert_eq!(result.len(), 2);

        let banned = result
            .iter()
            .find(|(t, _, _)| t.name == "banned_users")
            .unwrap();
        assert_eq!(
            banned.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT IN subquery table should be Exclusion"
        );
    }

    #[test]
    fn test_update_query_source_cte() {
        let sql = "WITH active AS (SELECT id FROM users WHERE active = true) \
                   SELECT * FROM active";
        let cacheable = parse_cacheable(sql);
        let result = query_table_update_queries(&cacheable);

        // CTE body "users" should be Subquery(Inclusion)
        let users = result.iter().find(|(t, _, _)| t.name == "users").unwrap();
        assert_eq!(
            users.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "CTE body table should be Subquery(Inclusion)"
        );
    }

    #[test]
    fn test_update_query_source_from_subquery() {
        let cacheable = parse_cacheable(
            "SELECT * FROM (SELECT id FROM users WHERE id = 1) sub",
        );
        let result = query_table_update_queries(&cacheable);

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "FROM subquery table should be Subquery(Inclusion)"
        );
    }
}

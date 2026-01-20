use error_set::error_set;
use ordered_float::NotNan;
use postgres_protocol::types as pg_types;
use postgres_types::Type as PgType;
use rootcause::Report;

use crate::{
    cache::{QueryParameter, QueryParameters, query::CacheableQuery},
    catalog::TableMetadata,
    query::ast::{
        ColumnExpr, LiteralValue, SelectColumn, SelectColumns, SelectStatement, TableAlias,
        TableNode, TableSource, TableSubqueryNode, WhereExpr,
    },
    query::resolved::{
        ResolvedSelectColumns, ResolvedSelectStatement, ResolvedTableSource,
        ResolvedTableSubqueryNode,
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

/// Replace parameter placeholders ($1, $2, etc.) in a SelectStatement with actual values.
///
/// This function traverses the AST and replaces all `LiteralValue::Parameter` nodes
/// with appropriate typed `LiteralValue` nodes based on the provided parameter values.
///
/// # Arguments
/// * `select_statement` - The SELECT statement containing parameter placeholders
/// * `parameters` - The parameter values, indexed from 0 (for $1, $2, etc.)
///
/// # Returns
/// A new `SelectStatement` with parameters replaced by literal values
///
/// # Errors
/// Returns `AstTransformError` if:
/// - A parameter index is out of bounds
/// - A parameter placeholder format is invalid
/// - Parameter value contains invalid UTF-8
pub fn ast_parameters_replace(
    select_statement: &SelectStatement,
    parameters: &QueryParameters,
) -> AstTransformResult<SelectStatement> {
    let mut new_stmt = select_statement.clone();

    // Replace parameters in WHERE clause
    if let Some(where_clause) = &mut new_stmt.where_clause {
        where_expr_parameters_replace(where_clause, parameters)?;
    }

    // Replace parameters in HAVING clause
    if let Some(having) = &mut new_stmt.having {
        where_expr_parameters_replace(having, parameters)?;
    }

    // Replace parameters in JOIN conditions
    for table_source in &mut new_stmt.from {
        table_source_parameters_replace(table_source, parameters)?;
    }

    Ok(new_stmt)
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
            *subquery.select = ast_parameters_replace(&subquery.select, parameters)?;
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
        WhereExpr::Subquery { .. } => {
            // Subqueries would need their own parameter replacement
            // but we don't currently support subqueries with parameters
        }
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
        Some(pg_type) => {
            Err(AstTransformError::UnsupportedBinaryFormat { oid: pg_type.oid() }.into())
        }
        None => Err(AstTransformError::UnsupportedBinaryFormat { oid }.into()),
    }
}

pub fn query_select_replace(
    select_statement: &SelectStatement,
    columns: SelectColumns,
) -> SelectStatement {
    let mut new_stmt = select_statement.clone();
    new_stmt.columns = columns;

    new_stmt
}

/// Replace SELECT columns in a resolved statement.
///
/// Similar to `query_select_replace` but operates on the resolved AST.
pub fn resolved_select_replace(
    resolved_statement: &ResolvedSelectStatement,
    columns: ResolvedSelectColumns,
) -> ResolvedSelectStatement {
    let mut new_stmt = resolved_statement.clone();
    new_stmt.columns = columns;

    new_stmt
}

/// Replace a table source with a VALUES clause in a resolved statement.
///
/// Similar to `query_table_replace_with_values` but operates on the resolved AST.
/// Finds the table matching `table_metadata.relation_oid` and replaces it with a subquery
/// containing a VALUES clause built from `row_data`.
///
/// Also updates all column references for the replaced table to use the subquery alias
/// instead of the fully qualified schema.table reference.
pub fn resolved_table_replace_with_values(
    resolved: &ResolvedSelectStatement,
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> AstTransformResult<ResolvedSelectStatement> {
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
    resolved_column_alias_update(
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
        select: Box::new(ResolvedSelectStatement {
            values: vec![values],
            ..Default::default()
        }),
        alias: TableAlias {
            name: alias,
            columns: column_names,
        },
    });

    Ok(resolved_new)
}

/// Update all column references for a specific table to use an alias.
///
/// This is needed when replacing a table with a VALUES subquery - the column
/// references need to use just the alias instead of schema.table.
fn resolved_column_alias_update(
    resolved: &mut ResolvedSelectStatement,
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

    // Update ORDER BY columns
    for order_by in &mut resolved.order_by {
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
        ResolvedWhereExpr::Subquery { query } => {
            resolved_column_alias_update(query, schema, table, alias);
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
        ResolvedColumnExpr::Function { args, .. } => {
            for arg in args {
                resolved_column_expr_alias_update(arg, schema, table, alias);
            }
        }
        ResolvedColumnExpr::Subquery(query) => {
            resolved_column_alias_update(query, schema, table, alias);
        }
        ResolvedColumnExpr::Literal(_) => {}
    }
}

//generate queries used to check if a dml statement applies to a given table
pub fn query_table_update_queries(
    cacheable_query: &CacheableQuery,
) -> Vec<(&TableNode, SelectStatement)> {
    let select = cacheable_query.statement();
    let tables = select.nodes::<TableNode>().collect::<Vec<_>>();

    let column = SelectColumn {
        expr: ColumnExpr::Literal(LiteralValue::Boolean(true)),
        alias: None,
    };

    let select_list = SelectColumns::Columns(vec![column]);

    let mut queries = Vec::new();
    match tables.as_slice() {
        [table] => {
            queries.push((*table, query_select_replace(select, select_list)));
        }
        [table1, table2] => {
            //can use same query for both tables (CacheableQuery guarantees is_supported_from)
            queries.push((*table1, query_select_replace(select, select_list.clone())));
            queries.push((*table2, query_select_replace(select, select_list)));
        }
        _ => {}
    }

    queries
}

pub fn query_table_replace_with_values(
    select: &SelectStatement,
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> AstTransformResult<SelectStatement> {
    let mut select_new = select.clone();

    //find first matching table source
    let Some(first_from) = select_new.from.first_mut() else {
        return Err(AstTransformError::MissingTable.into());
    };
    let mut frontier = vec![first_from];
    let mut source_node: Option<&mut TableSource> = None;
    while let Some(cur) = frontier.pop() {
        match cur {
            TableSource::Join(join) => {
                frontier.push(&mut join.left);
                frontier.push(&mut join.right);
            }
            TableSource::Table(table) => {
                if table.name == table_metadata.name {
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
    let TableSource::Table(table_node) = source_node else {
        return Err(AstTransformError::MissingTable.into());
    };

    let mut column_names = Vec::new();
    let mut values = Vec::new();

    for column_meta in &table_metadata.columns {
        let position = column_meta.position as usize - 1;
        if let Some(row_value) = row_data.get(position) {
            let value = row_value.as_deref().map_or(LiteralValue::Null, |v| {
                //some ugly casting
                LiteralValue::StringWithCast(v.to_owned(), column_meta.type_name.clone())
            });

            column_names.push(column_meta.name.as_str());
            values.push(value);
        }
    }

    let alias = if let Some(table_alias) = &table_node.alias {
        let mut alias = table_alias.clone();
        if table_alias.columns.is_empty() {
            for name in column_names {
                alias.columns.push(name.to_owned());
            }
        }
        alias
    } else {
        let mut alias = TableAlias {
            name: table_metadata.name.clone(),
            columns: Vec::new(),
        };
        for name in column_names {
            alias.columns.push(name.to_owned());
        }
        alias
    };

    *source_node = TableSource::Subquery(TableSubqueryNode {
        lateral: false,
        select: Box::new(SelectStatement {
            values: vec![values],
            ..Default::default()
        }),
        alias: Some(alias),
    });

    Ok(select_new)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    #![allow(clippy::wildcard_enum_match_arm)]

    use crate::query::ast::{Deparse, Statement, sql_query_convert};

    use super::*;

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

    #[test]
    fn test_ast_parameters_replace_simple() {
        let query = "SELECT id FROM users WHERE id = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with value "42"
        let params = text_params(vec![Some(b"42")]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE id = '42'");
    }

    #[test]
    fn test_ast_parameters_replace_multiple_params() {
        let query = "SELECT id FROM users WHERE id = $1 AND name = $2";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with "42", $2 with "alice"
        let params = text_params(vec![Some(b"42"), Some(b"alice")]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(
            buf,
            "SELECT id FROM users WHERE id = '42' AND name = 'alice'"
        );
    }

    #[test]
    fn test_ast_parameters_replace_null() {
        let query = "SELECT id FROM users WHERE name = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with NULL
        let params = text_params(vec![None]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE name = NULL");
    }

    #[test]
    fn test_ast_parameters_replace_out_of_bounds() {
        let query = "SELECT id FROM users WHERE id = $2";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Only provide 1 parameter, but query uses $2
        let params = text_params(vec![Some(b"42")]);
        let result = ast_parameters_replace(stmt, &params);

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
        let query = "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Replace $1 with "100"
        let params = text_params(vec![Some(b"100")]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        // Deparse to verify
        let mut buf = String::new();
        result.deparse(&mut buf);

        assert!(buf.contains("WHERE o.total > '100'"));
    }

    #[test]
    fn test_query_select_replace() {
        // Test replacing specific columns with SELECT *
        let original_query = "SELECT id, name, email FROM users WHERE id = 1";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_select_replace(stmt, SelectColumns::All);

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
        let original_query =
            "SELECT a.id, b.data FROM table_a a WHERE a.status = 'active' AND b.enabled = true";
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;
        let result = query_select_replace(stmt, SelectColumns::All);

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
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let cacheable_query = CacheableQuery::try_from(&sql_query).expect("query to be cacheable");
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
        let ast = pg_query::parse(original_query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let cacheable_query = CacheableQuery::try_from(&sql_query).expect("query to be cacheable");
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

    // ==================== Text Format Parameter Tests ====================

    #[test]
    fn test_text_parameter_integer() {
        let query = "SELECT id FROM users WHERE id = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Integer type should produce integer literal
        let params = typed_text_params(vec![(Some(b"42"), PgType::INT4)]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        let mut buf = String::new();
        result.deparse(&mut buf);

        // Integer literal renders without quotes
        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_text_parameter_boolean() {
        let query = "SELECT id FROM users WHERE active = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Boolean 't' -> true
        let params = typed_text_params(vec![(Some(b"t"), PgType::BOOL)]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE active = true");
    }

    #[test]
    fn test_text_parameter_float() {
        let query = "SELECT id FROM users WHERE score > $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        let params = typed_text_params(vec![(Some(b"3.14"), PgType::FLOAT8)]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        let mut buf = String::new();
        result.deparse(&mut buf);

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
        let query = "SELECT id FROM users WHERE uuid = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        let params = typed_text_params(vec![(
            Some(b"550e8400-e29b-41d4-a716-446655440000"),
            PgType::UUID,
        )]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        let mut buf = String::new();
        result.deparse(&mut buf);

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
    fn test_binary_parameter_unsupported_type() {
        let param = QueryParameter {
            value: Some(vec![0x00]),
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
        let query = "SELECT id FROM users WHERE id = $1";
        let ast = pg_query::parse(query).expect("to parse query");
        let sql_query = sql_query_convert(&ast).expect("to convert to SqlQuery");

        let Statement::Select(stmt) = &sql_query.statement;

        // Binary INT4: 42 in big-endian
        let params = binary_params(vec![(Some(&[0x00, 0x00, 0x00, 0x2A]), PgType::INT4)]);
        let result = ast_parameters_replace(stmt, &params).expect("to replace parameters");

        let mut buf = String::new();
        result.deparse(&mut buf);

        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }
}

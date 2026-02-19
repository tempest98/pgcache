//! Database catalog metadata structures.
//!
//! This module contains metadata about database schemas, tables, and columns.
//! These structures are used by both the cache subsystem (for tracking tables)
//! and the query resolution subsystem (for name resolution and type information).

use std::collections::HashMap;

use ecow::EcoString;
use iddqd::{BiHashItem, BiHashMap, bi_upcast};
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{Client, Error};

use crate::cache::CacheError;

use crate::query::ast::{ColumnExpr, ColumnNode, SelectColumn, SelectColumns, TableAlias};
use crate::query::resolved::{
    ResolvedColumnExpr, ResolvedColumnNode, ResolvedSelectColumn, ResolvedSelectColumns,
};

/// Metadata about a database table.
///
/// Contains schema information, column definitions, and primary key metadata
/// for a PostgreSQL table. This information is fetched from the database
/// information_schema and pg_catalog.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// PostgreSQL relation OID
    pub relation_oid: u32,
    /// Table name (unqualified)
    pub name: EcoString,
    /// Schema name (e.g., "public")
    pub schema: EcoString,
    /// Names of columns that form the primary key
    pub primary_key_columns: Vec<String>,
    /// Column metadata indexed by name and position
    pub columns: BiHashMap<ColumnMetadata>,
    /// Index metadata for non-primary-key indexes
    pub indexes: Vec<IndexMetadata>,
}

impl TableMetadata {
    /// Compare table schema (columns, primary key) without comparing indexes.
    ///
    /// Used to determine if a table needs recreation. Index changes don't
    /// require table recreation.
    pub fn schema_eq(&self, other: &TableMetadata) -> bool {
        self.relation_oid == other.relation_oid
            && self.name == other.name
            && self.schema == other.schema
            && self.primary_key_columns == other.primary_key_columns
            && self.columns == other.columns
    }

    /// Generate SELECT columns for all columns in this table.
    ///
    /// If an alias is provided, column references will use the alias name
    /// and respect any column aliases defined in the TableAlias.
    ///
    pub fn select_columns(&self, alias: Option<&TableAlias>) -> SelectColumns {
        let columns = self
            .columns
            .iter()
            .map(|c| SelectColumn {
                expr: ColumnExpr::Column(ColumnNode {
                    table: if let Some(alias) = alias {
                        Some(alias.name.clone())
                    } else {
                        Some(self.name.to_string())
                    },
                    column: if let Some(alias) = alias {
                        alias
                            .columns
                            .get(c.position as usize - 1)
                            .map(String::as_str)
                            .unwrap_or(c.name.as_str())
                            .to_owned()
                    } else {
                        c.name.to_string()
                    },
                }),
                alias: None,
            })
            .collect();

        SelectColumns::Columns(columns)
    }

    /// Generate resolved SELECT columns for all columns in this table.
    ///
    /// Creates `ResolvedSelectColumns::Columns` with fully qualified column references.
    /// If a table_alias is provided, columns will use that alias for deparsing.
    pub fn resolved_select_columns(&self, table_alias: Option<&str>) -> ResolvedSelectColumns {
        let columns = self
            .columns
            .iter()
            .map(|c| ResolvedSelectColumn {
                expr: ResolvedColumnExpr::Column(ResolvedColumnNode {
                    schema: self.schema.clone(),
                    table: self.name.clone(),
                    table_alias: table_alias.map(EcoString::from),
                    column: c.name.clone(),
                    column_metadata: c.clone(),
                }),
                alias: None,
            })
            .collect();

        ResolvedSelectColumns::Columns(columns)
    }
}

impl BiHashItem for TableMetadata {
    type K1<'a> = u32;
    type K2<'a> = (&'a str, &'a str);

    fn key1(&self) -> Self::K1<'_> {
        self.relation_oid
    }

    fn key2(&self) -> Self::K2<'_> {
        (self.schema.as_str(), self.name.as_str())
    }

    bi_upcast!();
}

/// Metadata about a table column.
///
/// Contains type information and position data for a single column
/// within a table.
///
/// String fields use `EcoString` for reduced clone cost: short names (≤15 bytes)
/// are stored inline (stack-only memcpy), longer names like
/// `"timestamp with time zone"` get a single heap allocation shared via
/// refcount across all clones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Column name
    pub name: EcoString,
    /// 1-based position in table (matches PostgreSQL attnum)
    pub position: i16,
    /// PostgreSQL type OID (original from origin database, used in RowDescription)
    pub type_oid: u32,
    /// Parsed PostgreSQL type (may be Domain, Enum, etc.)
    pub data_type: Type,
    /// Human-readable type name from origin (e.g., "year", "mood")
    pub type_name: EcoString,
    /// Type name for cache table creation (e.g., "integer" for year domain, "text" for enums)
    pub cache_type_name: EcoString,
    /// Whether this column is part of the primary key
    pub is_primary_key: bool,
}

impl std::hash::Hash for ColumnMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash all fields except data_type (which doesn't implement Hash)
        self.name.hash(state);
        self.position.hash(state);
        self.type_oid.hash(state);
        self.type_name.hash(state);
        self.cache_type_name.hash(state);
        self.is_primary_key.hash(state);
    }
}

impl BiHashItem for ColumnMetadata {
    type K1<'a> = &'a str;
    type K2<'a> = i16;

    fn key1(&self) -> Self::K1<'_> {
        self.name.as_str()
    }

    fn key2(&self) -> Self::K2<'_> {
        self.position
    }

    bi_upcast!();
}

/// Metadata about a table index.
///
/// Contains index definition information for recreating indexes
/// on cached tables. Expression indexes and partial indexes are not supported.
/// Primary key indexes are excluded since they are created by the PRIMARY KEY constraint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMetadata {
    /// Index name (for reference/logging, not used in CREATE INDEX)
    pub name: String,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Index method (btree, hash, gist, gin, etc.)
    pub method: String,
    /// Ordered list of column names in the index
    pub columns: Vec<String>,
}

/// PostgreSQL function volatility classification.
///
/// Determines whether a function's result depends only on its inputs (immutable),
/// on inputs plus database state (stable), or can vary between calls (volatile).
/// Used to decide whether functions in WHERE/FROM clauses are safe for caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}

/// Load function volatilities from pg_proc.
///
/// Queries the origin database for all scalar functions and builds a map
/// from unqualified function name to worst-case volatility across all
/// overloads and schemas. Aggregate and window functions are excluded.
///
/// The map is keyed by lowercase function name to match PostgreSQL's
/// case-insensitive identifier handling.
pub async fn function_volatility_map_load(
    client: &Client,
) -> Result<HashMap<String, FunctionVolatility>, Error> {
    let rows = client
        .query(
            "SELECT p.proname,
                    MAX(CASE p.provolatile
                        WHEN 'v' THEN 2
                        WHEN 's' THEN 1
                        ELSE 0
                    END) AS worst_volatility
             FROM pg_proc p
             JOIN pg_namespace n ON p.pronamespace = n.oid
             WHERE p.prokind NOT IN ('a', 'w')
             GROUP BY p.proname",
            &[],
        )
        .await?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in &rows {
        let name: &str = row.get(0);
        let worst: i32 = row.get(1);
        let volatility = match worst {
            0 => FunctionVolatility::Immutable,
            1 => FunctionVolatility::Stable,
            _ => FunctionVolatility::Volatile,
        };
        map.insert(name.to_owned(), volatility);
    }

    Ok(map)
}

/// Resolves a PostgreSQL Type to its cache-compatible storage type name.
///
/// - **Domains** are resolved to their base type recursively (e.g., `year` → `integer`)
/// - **Enums** are stored as `text` (PostgreSQL transmits enum labels as text)
/// - **Composites** return an error (not yet supported)
/// - **Arrays** resolve the element type and append `[]`
/// - **Built-in types** use their PostgreSQL name directly
///
/// # Errors
///
/// Returns `CacheError::UnsupportedType` for composite types.
pub fn cache_type_name_resolve(data_type: &Type) -> Result<String, CacheError> {
    match data_type.kind() {
        Kind::Domain(base) => cache_type_name_resolve(base),
        Kind::Enum(_) => Ok("text".to_owned()),
        Kind::Composite(_) => Err(CacheError::UnsupportedType {
            type_name: data_type.name().to_owned(),
            reason: "composite types not yet supported".to_owned(),
        }),
        Kind::Array(elem) => {
            let elem_name = cache_type_name_resolve(elem)?;
            Ok(format!("{}[]", elem_name))
        }
        // Built-in types use their PostgreSQL name directly
        Kind::Simple | Kind::Pseudo | Kind::Range(_) | Kind::Multirange(_) => {
            Ok(data_type.name().to_owned())
        }
        // Kind is non-exhaustive; treat unknown kinds as their type name
        _ => Ok(data_type.name().to_owned()),
    }
}

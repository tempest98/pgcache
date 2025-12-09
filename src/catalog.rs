//! Database catalog metadata structures.
//!
//! This module contains metadata about database schemas, tables, and columns.
//! These structures are used by both the cache subsystem (for tracking tables)
//! and the query resolution subsystem (for name resolution and type information).

use iddqd::{BiHashItem, BiHashMap, bi_upcast};
use tokio_postgres::types::Type;

use crate::query::ast::{ColumnExpr, ColumnNode, SelectColumn, SelectColumns, TableAlias};

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
    pub name: String,
    /// Schema name (e.g., "public")
    pub schema: String,
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
                        Some(self.name.clone())
                    },
                    column: if let Some(alias) = alias {
                        alias
                            .columns
                            .get(c.position as usize - 1)
                            .unwrap_or(&c.name)
                            .clone()
                    } else {
                        c.name.clone()
                    },
                }),
                alias: None,
            })
            .collect();

        SelectColumns::Columns(columns)
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// 1-based position in table (matches PostgreSQL attnum)
    pub position: i16,
    /// PostgreSQL type OID
    pub type_oid: u32,
    /// Parsed PostgreSQL type
    pub data_type: Type,
    /// Human-readable type name (e.g., "integer", "text")
    pub type_name: String,
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

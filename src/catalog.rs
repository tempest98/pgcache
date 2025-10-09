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
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl TableMetadata {
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
    type K2<'a> = &'a str;

    fn key1(&self) -> Self::K1<'_> {
        self.relation_oid
    }

    fn key2(&self) -> Self::K2<'_> {
        self.name.as_str()
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

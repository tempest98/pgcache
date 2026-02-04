use std::io;

use error_set::error_set;
use rootcause::Report;
use tokio_postgres::Error;

use crate::query::{resolved::ResolveError, transform::AstTransformError};

/// Result type with location-tracking error reports.
/// Use this for functions where you want automatic file:line capture on errors.
pub type CacheResult<T> = Result<T, Report<CacheError>>;

// Re-export result extensions for convenience when using CacheResult
pub use crate::result::{MapIntoReport, ReportExt};

// Module declarations
mod cdc;
mod messages;
pub(crate) mod query;
mod query_cache;
mod runtime;
mod types;
mod worker;
mod writer;

// Re-export public types
pub use messages::{
    CacheMessage, CacheReply, DataStreamState, ProxyMessage, QueryParameter, QueryParameters,
};
pub use runtime::cache_run;
pub use types::{Cache, CachedQuery, CachedQueryState, UpdateQueries, UpdateQuery};

error_set! {
    CacheError := WriteError || ReadError || DbError || ParseError || TableError || SendError || QueryResolutionError

    ReadError := {
        IoError(io::Error),
        InvalidMessage,
    }

    DbError := {
        NoConnection,
        PgError(Error),
        CdcFailure,
        WriterFailure,
        TooManyModifiedRows,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error),
        AstTransformError(AstTransformError),
        /// Query structure is invalid (e.g., expected SELECT but got something else)
        InvalidQuery,
        Other,
    }

    SendError := {
        WorkerSend,
        Reply,
    }

    WriteError := {
        Write,
    }


    TableError := {
        #[display("Oid: {oid:?} Name {name:?}")]
        UnknownTable {
            oid: Option<u32>,
            name: Option<String>,
        },
        #[display("Unknown type OID {type_oid} ('{type_name}') for column '{column_name}' in table '{table_name}'")]
        UnknownType {
            type_oid: u32,
            type_name: String,
            column_name: String,
            table_name: String,
        },
        #[display("Unsupported type '{type_name}': {reason}")]
        UnsupportedType {
            type_name: String,
            reason: String,
        },
        UnknownColumn,
        UnknownSchema,
        NoPrimaryKey,
    }

    QueryResolutionError := {
        ResolveError(ResolveError),
    }
}

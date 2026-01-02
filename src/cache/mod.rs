use std::io;

use error_set::error_set;
use tokio_postgres::Error;

use crate::query::{resolved::ResolveError, transform::AstTransformError};

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
        TooManyModifiedRows,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error),
        AstTransformError(AstTransformError),
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
        UnknownColumn,
        UnknownSchema,
        NoPrimaryKey,
    }

    QueryResolutionError := {
        ResolveError(ResolveError),
    }
}

mod cache_sender;
mod client_stream;
mod connection;
mod query;
pub mod search_path;
mod server;
mod tls_stream;

pub use cache_sender::{CacheSendError, CacheSender, CacheSenderUpdater};

pub use client_stream::{ClientSocket, ClientSocketSource};

use std::io;

use error_set::error_set;
use nix::errno::Errno;
use rootcause::Report;
use tokio::sync::mpsc::Receiver;

use crate::pg::cdc::PgCdcError;
use crate::pg::protocol::ProtocolError;

pub use connection::connection_run;
pub use server::proxy_run;

error_set! {
    ConnectionError := FdError || ConnectError || ReadError || WriteError || DegradedModeExit

    FdError := {
        NixError(Errno),
        FdIoError(io::Error),
    }

    ReadError := {
        ProtocolError(ProtocolError),
        IoError(io::Error),
    }

    WriteError := {
        MpscError,
    }

    ConnectError := {
        NoConnection,
        CdcError(PgCdcError),
        TlsError(io::Error),
    }

    DegradedModeExit := {
        CacheDead,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error)
    }
}

/// Result type with location-tracking error reports for connection operations.
pub type ConnectionResult<T> = Result<T, Report<ConnectionError>>;

// Manual From<io::Error> impl for ConnectionError since error_set doesn't do transitive conversions
impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::FdIoError(e)
    }
}

/// Current proxy operating mode for a connection.
#[derive(Debug)]
pub(crate) enum ProxyMode {
    Read,
    CacheRead(Receiver<CacheReply>),
    CacheWrite(CacheMessage),
}

/// Proxy health status.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProxyStatus {
    Normal,
    Degraded,
}

use crate::cache::{CacheMessage, CacheReply};

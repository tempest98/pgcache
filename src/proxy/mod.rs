mod connection;
mod query;
mod server;

use std::io;

use error_set::error_set;
use nix::errno::Errno;
use tokio::sync::mpsc::Receiver;

use crate::pg::protocol::ProtocolError;

pub use connection::connection_run;
pub use server::proxy_run;

error_set! {
    ConnectionError := FdError || ConnectError || ReadError || WriteError || DegradedModeExit

    FdError := {
        NixError(Errno),
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
    }

    DegradedModeExit := {
        CacheDead,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error)
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
#[derive(Debug)]
pub(crate) enum ProxyStatus {
    Normal,
    Degraded,
}

use crate::cache::{CacheMessage, CacheReply};

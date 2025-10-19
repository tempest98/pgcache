#![allow(dead_code)]
// adapted from https://github.com/sunng87/pgwire
use std::io;

use error_set::error_set;
use tokio_util::bytes::BytesMut;

pub(crate) mod backend;
pub(crate) mod encode;
pub(crate) mod extended;
pub(crate) mod frontend;

error_set! {
    ProtocolError := {
        #[display("Invalid protocal version: {major}.{minor}")]
        InvalidProtocolVersion {
            major: i16,
            minor: i16,
        },
        InvalidStartupFrame,
        #[display("Unrecognized message type: {tag}")]
        UnrecognizedMessageType {
            tag: String,
        },
        IoError(io::Error),
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum PgConnectionState {
    #[default]
    Startup,
    Authentication,
    Query,
    // FunctionCall,
    // Copy,
    // Termination,
    // ReadyForQuery,
    // QueryInProgress,
    // CopyInProgress(bool),
    // AwaitingSync,
}

pub trait PgMessageType {}

#[derive(Debug)]
pub struct PgMessage<T: PgMessageType> {
    pub message_type: T,
    pub data: BytesMut,
}

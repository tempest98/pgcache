#![allow(dead_code)]
// adapted from https://github.com/sunng87/pgwire
use std::io;

use error_set::error_set;
use phf::phf_map;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};
use tracing::{instrument, trace};

error_set! {
    ProtocolError = {
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
    };
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

#[derive(Debug, Clone, Copy)]
pub enum PgFrontendMessageType {
    Startup,
    CancelRequest,
    SslRequest,
    PasswordMessageFamily,

    Query,

    FunctionCall,

    Parse,
    Close,
    Bind,
    Describe,
    Execute,
    Flush,
    Sync,

    Terminate,

    CopyData,
    CopyFail,
    CopyDone,
}

impl PgMessageType for PgFrontendMessageType {}

pub type PgFrontendMessage = PgMessage<PgFrontendMessageType>;

const FRONTEND_MESSAGE_TYPE_MAP: phf::Map<u8, PgFrontendMessageType> = phf_map! {
    b'B' => PgFrontendMessageType::Bind,
    b'C' => PgFrontendMessageType::Close,
    b'd' => PgFrontendMessageType::CopyData,
    b'c' => PgFrontendMessageType::CopyDone,
    b'f' => PgFrontendMessageType::CopyFail,
    b'D' => PgFrontendMessageType::Describe,
    b'E' => PgFrontendMessageType::Execute,
    b'H' => PgFrontendMessageType::Flush,
    b'F' => PgFrontendMessageType::FunctionCall,
    b'p' => PgFrontendMessageType::PasswordMessageFamily,
    b'P' => PgFrontendMessageType::Parse,
    b'Q' => PgFrontendMessageType::Query,
    b'S' => PgFrontendMessageType::Sync,
    b'X' => PgFrontendMessageType::Terminate,

};

#[derive(Debug, Default)]
pub struct PgFrontendMessageCodec {
    pub state: PgConnectionState,
}

impl Decoder for PgFrontendMessageCodec {
    type Item = PgFrontendMessage;
    type Error = ProtocolError;

    #[instrument]
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        match self.state {
            PgConnectionState::Startup => {
                // multiple messages types can be received:
                // * SSLRequest
                // * StartupMessage
                // * CancelRequest
                // * ssl handshake
                //
                // they all start with a 4 byte message length (except ssl handshake)

                const CODE_STARTUP: [i16; 2] = [3, 0];
                const CODE_CANCEL_REQUEST: [i16; 2] = [1234, 5678];
                const CODE_SSL_REQUEST: [i16; 2] = [1234, 5679];

                const MIN_MESSAGE_LEN: usize = 4;
                if buf.remaining() < MIN_MESSAGE_LEN {
                    return Ok(None);
                }
                let msg_len = (&buf[0..4]).get_i32() as usize;
                if buf.remaining() < msg_len {
                    return Ok(None);
                }

                let code = [(&buf[4..6]).get_i16(), (&buf[6..8]).get_i16()];

                match code {
                    CODE_STARTUP => {
                        //validate message ends with double nulls
                        let end = (&buf[(msg_len - 2)..msg_len]).get_i16();
                        if end != 0 {
                            return Err(ProtocolError::InvalidStartupFrame);
                        }

                        self.state = PgConnectionState::Query;
                        Ok(Some(PgFrontendMessage {
                            message_type: PgFrontendMessageType::Startup,
                            data: buf.split_to(msg_len),
                        }))
                    }
                    CODE_CANCEL_REQUEST => Ok(Some(PgFrontendMessage {
                        message_type: PgFrontendMessageType::CancelRequest,
                        data: buf.split_to(msg_len),
                    })),
                    CODE_SSL_REQUEST => Ok(Some(PgFrontendMessage {
                        message_type: PgFrontendMessageType::SslRequest,
                        data: buf.split_to(msg_len),
                    })),
                    _ => Err(ProtocolError::InvalidProtocolVersion {
                        major: code[0],
                        minor: code[1],
                    }),
                }
            }
            _ => {
                if let Some(msg_type) = FRONTEND_MESSAGE_TYPE_MAP.get(&buf[0]) {
                    const MIN_MESSAGE_LEN: usize = 5;
                    if buf.remaining() < MIN_MESSAGE_LEN {
                        return Ok(None);
                    }

                    let msg_len = (&buf[1..5]).get_i32() as usize + 1;
                    if buf.remaining() < msg_len {
                        return Ok(None);
                    }

                    Ok(Some(PgFrontendMessage {
                        message_type: *msg_type,
                        data: buf.split_to(msg_len),
                    }))
                } else {
                    trace!("{:?}", buf);
                    Err(ProtocolError::UnrecognizedMessageType {
                        tag: buf[0].escape_ascii().to_string(),
                    })
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PgBackendMessageType {
    // startup
    SslRequestResponse,
    Authentication,
    ParameterStatus,
    BackendKeyData,
    NegotiateProtocolVersion,

    // extended query
    ParseComplete,
    CloseComplete,
    BindComplete,
    PortalSuspended,

    // command response
    CommandComplete,
    EmptyQueryResponse,
    ReadyForQuery,
    ErrorResponse,
    NoticeResponse,
    NotificationResponse,
    FunctionCallResponse,

    // data
    ParameterDescription,
    RowDescription,
    DataRow,
    NoData,

    // copy
    CopyData,
    CopyDone,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,

    //ugly?
    Multi, //used for sending a buf that has multiple messages encoded into it
}

impl PgMessageType for PgBackendMessageType {}

pub(crate) type PgBackendMessage = PgMessage<PgBackendMessageType>;

pub const AUTHENTICATION_TAG: u8 = b'R'; // => PgBackendMessageType::Authentication,
pub const BACKEND_KEY_DATA_TAG: u8 = b'K'; // => PgBackendMessageType::BackendKeyData,
pub const BIND_COMPLETE_TAG: u8 = b'2'; // => PgBackendMessageType::BindComplete,
pub const CLOSE_COMPLETE_TAG: u8 = b'3'; // => PgBackendMessageType::CloseComplete,
pub const COMMAND_COMPLETE_TAG: u8 = b'C'; // => PgBackendMessageType::CommandComplete,
pub const COPY_DATA_TAG: u8 = b'd'; // => PgBackendMessageType::CopyData,
pub const COPY_DONE_TAG: u8 = b'c'; // => PgBackendMessageType::CopyDone,
pub const COPY_IN_RESPONSE_TAG: u8 = b'G'; // => PgBackendMessageType::CopyInResponse,
pub const COPY_OUT_RESPONSE_TAG: u8 = b'H'; // => PgBackendMessageType::CopyOutResponse,
pub const COPY_BOHT_RESPONSE_TAG: u8 = b'W'; // => PgBackendMessageType::CopyBothResponse,
pub const DATA_ROW_TAG: u8 = b'D'; // => PgBackendMessageType::DataRow,
pub const EMPTY_QUERY_RESPONSE_TAG: u8 = b'I'; // => PgBackendMessageType::EmptyQueryResponse,
pub const ERROR_RESPONSE_TAG: u8 = b'E'; // => PgBackendMessageType::ErrorResponse,
pub const FUNCTION_CALL_RESPONSE_TAG: u8 = b'V'; // => PgBackendMessageType::FunctionCallResponse,
pub const NEGOTIATE_PROTOCOL_VERSION_TAG: u8 = b'v'; // => PgBackendMessageType::NegotiateProtocolVersion,
pub const NO_DATA_TAG: u8 = b'n'; // => PgBackendMessageType::NoData,
pub const NOTICE_RESPONSE_TAG: u8 = b'N'; // => PgBackendMessageType::NoticeResponse,
pub const NOTIFICATION_RESONPSE_TAG: u8 = b'A'; // => PgBackendMessageType::NotificationResponse,
pub const PARAMETER_DESCRIPTION_TAG: u8 = b't'; // => PgBackendMessageType::ParameterDescription,
pub const PARAMETER_STATUS_TAG: u8 = b'S'; // => PgBackendMessageType::ParameterStatus,
pub const PARSE_COMPLETE_TAG: u8 = b'1'; // => PgBackendMessageType::ParseComplete,
pub const PORTAL_SUSPENDED_TAG: u8 = b's'; // => PgBackendMessageType::PortalSuspended,
pub const READY_FOR_QUERY_TAG: u8 = b'Z'; // => PgBackendMessageType::ReadyForQuery,
pub const ROW_DESCRIPTION_TAG: u8 = b'T'; // => PgBackendMessageType::RowDescription,

const BACKEND_MESSAGE_TYPE_MAP: phf::Map<u8, PgBackendMessageType> = phf_map! {
    b'R' => PgBackendMessageType::Authentication,
    b'K' => PgBackendMessageType::BackendKeyData,
    b'2' => PgBackendMessageType::BindComplete,
    b'3' => PgBackendMessageType::CloseComplete,
    b'C' => PgBackendMessageType::CommandComplete,
    b'd' => PgBackendMessageType::CopyData,
    b'c' => PgBackendMessageType::CopyDone,
    b'G' => PgBackendMessageType::CopyInResponse,
    b'H' => PgBackendMessageType::CopyOutResponse,
    b'W' => PgBackendMessageType::CopyBothResponse,
    b'D' => PgBackendMessageType::DataRow,
    b'I' => PgBackendMessageType::EmptyQueryResponse,
    b'E' => PgBackendMessageType::ErrorResponse,
    b'V' => PgBackendMessageType::FunctionCallResponse,
    b'v' => PgBackendMessageType::NegotiateProtocolVersion,
    b'n' => PgBackendMessageType::NoData,
    b'N' => PgBackendMessageType::NoticeResponse,
    b'A' => PgBackendMessageType::NotificationResponse,
    b't' => PgBackendMessageType::ParameterDescription,
    b'S' => PgBackendMessageType::ParameterStatus,
    b'1' => PgBackendMessageType::ParseComplete,
    b's' => PgBackendMessageType::PortalSuspended,
    b'Z' => PgBackendMessageType::ReadyForQuery,
    b'T' => PgBackendMessageType::RowDescription,
};

#[derive(Debug, Default)]
pub struct PgBackendMessageCodec {
    pub state: PgConnectionState,
}

impl PgBackendMessageCodec {
    fn handle_authentication_message(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<PgMessage<PgBackendMessageType>>, ProtocolError> {
        const MIN_AUTHENTICATION_LEN: usize = 9;
        const AUTHENTICATION_OK: i32 = 0;
        if buf.remaining() < MIN_AUTHENTICATION_LEN {
            return Ok(None);
        }

        let msg_len = (&buf[1..5]).get_i32() as usize + 1;
        if buf.remaining() < msg_len {
            return Ok(None);
        }

        self.state = if (&buf[5..9]).get_i32() == AUTHENTICATION_OK {
            PgConnectionState::Query
        } else {
            PgConnectionState::Authentication
        };

        Ok(Some(PgBackendMessage {
            message_type: PgBackendMessageType::Authentication,
            data: buf.split_to(msg_len),
        }))
    }
}

impl Decoder for PgBackendMessageCodec {
    type Item = PgMessage<PgBackendMessageType>;
    type Error = ProtocolError;

    #[instrument]
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        match self.state {
            PgConnectionState::Startup => {
                const MIN_MESSAGE_LEN: usize = 1;

                if buf.remaining() < MIN_MESSAGE_LEN {
                    return Ok(None);
                }

                match buf[0] {
                    b'S' | b'N' => Ok(Some(PgBackendMessage {
                        message_type: PgBackendMessageType::SslRequestResponse,
                        data: buf.split_to(1),
                    })),
                    b'R' => self.handle_authentication_message(buf),
                    _ => Err(ProtocolError::InvalidStartupFrame),
                }
            }
            PgConnectionState::Authentication => self.handle_authentication_message(buf),
            _ => {
                if let Some(msg_type) = BACKEND_MESSAGE_TYPE_MAP.get(&buf[0]) {
                    const MIN_MESSAGE_LEN: usize = 5;
                    if buf.remaining() < MIN_MESSAGE_LEN {
                        return Ok(None);
                    }

                    let msg_len = (&buf[1..5]).get_i32() as usize + 1;
                    if buf.remaining() < msg_len {
                        return Ok(None);
                    }

                    Ok(Some(PgBackendMessage {
                        message_type: *msg_type,
                        data: buf.split_to(msg_len),
                    }))
                } else {
                    trace!("{:?}", buf);
                    Err(ProtocolError::UnrecognizedMessageType {
                        tag: buf[0].escape_ascii().to_string(),
                    })
                }
            }
        }
    }
}

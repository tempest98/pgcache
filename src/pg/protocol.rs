// adapted from https://github.com/sunng87/pgwire
use std::{error::Error, fmt, io, ops::Range};

use phf::phf_map;
use tokio_util::{
    bytes::{self, Buf, BytesMut},
    codec::Decoder,
};

#[derive(Debug, Clone)]
pub enum ProtocolError {
    InvalidProtocolVersion(i16, i16),
    InvalidSslRequest(i16, i16),
    InvalidStartupFrame,
    IoError,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "protocol error")
    }
}
impl Error for ProtocolError {}

impl From<ProtocolError> for io::Error {
    fn from(error: ProtocolError) -> Self {
        io::Error::other(error)
    }
}

impl From<io::Error> for ProtocolError {
    fn from(_: io::Error) -> Self {
        Self::IoError
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum PgConnectionState {
    #[default]
    AwaitingSslRequest,
    AwaitingStartup,
    AuthenticationInProgress,
    ReadyForQuery,
    QueryInProgress,
    CopyInProgress(bool),
    AwaitingSync,
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
    b'P' => PgFrontendMessageType::Parse,
    b'Q' => PgFrontendMessageType::Query,
    b'S' => PgFrontendMessageType::Sync,
    b'X' => PgFrontendMessageType::Terminate,

};

#[derive(Debug, Default)]
pub struct PgFrontendMessageCodec {
    pub state: PgConnectionState,
}

impl PgFrontendMessageCodec {
    fn extract_ssl_request(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<PgFrontendMessage>, ProtocolError> {
        const SSL_REQUEST_MESSAGE_LEN: usize = 8;
        const CODE_MAJOR: i16 = 1234;
        const CODE_MINOR: i16 = 5679;

        if buf.remaining() < SSL_REQUEST_MESSAGE_LEN {
            return Ok(None);
        }

        let code_major = (&buf[4..6]).get_i16();
        let code_minor = (&buf[6..8]).get_i16();
        if code_major != CODE_MAJOR || code_minor != CODE_MINOR {
            return Err(ProtocolError::InvalidSslRequest(code_major, code_minor));
        }

        self.state = PgConnectionState::AwaitingStartup;
        Ok(Some(PgFrontendMessage {
            message_type: PgFrontendMessageType::SslRequest,
            data: buf.split_to(SSL_REQUEST_MESSAGE_LEN),
        }))
    }

    fn extract_startup(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<PgFrontendMessage>, ProtocolError> {
        const MINIMUM_STARTUP_MESSAGE_LEN: usize = 8;
        const PROTOCOL_VERSION_MAJOR: i16 = 3;
        const PROTOCOL_VERSION_MINOR: i16 = 0;

        if buf.remaining() < MINIMUM_STARTUP_MESSAGE_LEN {
            return Ok(None);
        }

        let packet_version_major = (&buf[4..6]).get_i16();
        let packet_version_minor = (&buf[6..8]).get_i16();
        if packet_version_major != PROTOCOL_VERSION_MAJOR
            || packet_version_minor != PROTOCOL_VERSION_MINOR
        {
            return Err(ProtocolError::InvalidProtocolVersion(
                packet_version_major,
                packet_version_minor,
            ));
        }

        let msg_len = (&buf[0..4]).get_i32() as usize;
        dbg!(msg_len);
        if buf.remaining() < msg_len {
            buf.reserve(msg_len);
            return Ok(None);
        }

        //validate message ends with double nulls
        let end = (&buf[(msg_len - 2)..msg_len]).get_i16();
        if end != 0 {
            return Err(ProtocolError::InvalidStartupFrame);
        }

        self.state = PgConnectionState::AuthenticationInProgress;
        Ok(Some(PgFrontendMessage {
            message_type: PgFrontendMessageType::Startup,
            data: buf.split_to(msg_len),
        }))
    }
}

impl Decoder for PgFrontendMessageCodec {
    type Item = PgFrontendMessage;
    type Error = ProtocolError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            PgConnectionState::AwaitingSslRequest => {
                //todo: check for cancel request

                self.extract_ssl_request(buf)
            }

            PgConnectionState::AwaitingStartup => self.extract_startup(buf),

            _ => {
                if !buf.has_remaining() {
                    return Ok(None);
                }
                dbg!(&buf);

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
                    todo!()
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
}

impl PgMessageType for PgBackendMessageType {}

pub(crate) type PgBackendMessage = PgMessage<PgBackendMessageType>;

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

impl Decoder for PgBackendMessageCodec {
    type Item = PgMessage<PgBackendMessageType>;
    type Error = ProtocolError;

    fn decode(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            PgConnectionState::AwaitingSslRequest => {
                const MESSAGE_LEN: usize = 1;

                if buf.remaining() < MESSAGE_LEN {
                    return Ok(None);
                }

                self.state = PgConnectionState::AwaitingStartup;
                Ok(Some(PgBackendMessage {
                    message_type: PgBackendMessageType::SslRequestResponse,
                    data: buf.split_to(MESSAGE_LEN),
                }))
            }

            PgConnectionState::AwaitingStartup => {
                const MIN_MESSAGE_LEN: usize = 5;

                if buf.remaining() < MIN_MESSAGE_LEN {
                    return Ok(None);
                }

                if buf[0] != b'R' {
                    return Err(ProtocolError::InvalidStartupFrame);
                }

                let msg_len = (&buf[1..5]).get_i32() as usize + 1;
                if buf.remaining() < msg_len {
                    return Ok(None);
                }

                self.state = PgConnectionState::AuthenticationInProgress;
                Ok(Some(PgBackendMessage {
                    message_type: PgBackendMessageType::Authentication,
                    data: buf.split_to(msg_len),
                }))
            }
            _ => {
                if !buf.has_remaining() {
                    return Ok(None);
                }
                dbg!(&buf);

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
                    todo!()
                }
            }
        }
    }
}

// struct MessageDescriptor<T: PgMessageType> {
//     msg_type: T,
//     min_msg_len: usize,
//     msg_len_loc: Option<Range<usize>>,
// }

// type FrontendMessageDescriptor = MessageDescriptor<PgFrontendMessageType>;
// type BackendMessageDescriptor = MessageDescriptor<PgBackendMessageType>;

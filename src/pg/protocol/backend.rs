use phf::phf_map;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};

use super::*;

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
                    Err(ProtocolError::UnrecognizedMessageType {
                        tag: buf[0].escape_ascii().to_string(),
                    })
                }
            }
        }
    }
}

use std::sync::Arc;

use tokio_postgres::{SimpleColumn, SimpleQueryRow};
use tokio_util::bytes::{BufMut, BytesMut};
use tracing::instrument;

use crate::pg::protocol::backend::{
    COMMAND_COMPLETE_TAG, DATA_ROW_TAG, READY_FOR_QUERY_TAG, ROW_DESCRIPTION_TAG,
};

#[instrument]
pub fn row_description_encode(desc: &Arc<[SimpleColumn]>, buf: &mut BytesMut) {
    let cnt = desc.len() as i16;
    let string_len = desc.iter().fold(0, |acc, col| acc + col.name().len() + 1);

    buf.put_u8(ROW_DESCRIPTION_TAG);
    buf.put_i32(6 + (18 * cnt as i32) + string_len as i32);
    buf.put_i16(cnt);
    for col in desc.iter() {
        buf.put_slice(col.name().as_bytes());
        buf.put_u8(0);
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i32(0);
        buf.put_i16(-1);
        buf.put_i32(-1);
        buf.put_i16(0);
    }
}

#[instrument]
pub fn simple_query_row_encode(row: &SimpleQueryRow, buf: &mut BytesMut) {
    let cnt = row.len() as i16;
    let mut value_len = 0;
    for i in 0..cnt {
        let value = row.get(i as usize).unwrap_or_default();
        value_len += value.len();
    }

    buf.put_u8(DATA_ROW_TAG);
    buf.put_i32(6 + (4 * cnt as i32) + value_len as i32);
    buf.put_i16(cnt);
    for i in 0..cnt {
        let data = row.get(i as usize).unwrap_or_default().as_bytes();
        buf.put_i32(data.len() as i32);
        buf.put_slice(data);
    }
}

#[instrument]
pub fn command_complete_encode(cnt: u64, buf: &mut BytesMut) {
    let msg = format!("SELECT {cnt}");

    buf.put_u8(COMMAND_COMPLETE_TAG);
    buf.put_i32((4 + msg.len() + 1) as i32);
    buf.put_slice(msg.as_bytes());
    buf.put_u8(0);
}

#[instrument]
pub fn ready_for_query_encode(buf: &mut BytesMut) {
    buf.put_u8(READY_FOR_QUERY_TAG);
    buf.put_i32(5);
    buf.put_u8(b'I');
}

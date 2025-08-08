use std::{io::Read, process::Child};

use tokio_util::bytes::{Buf, BytesMut};

pub fn proxy_wait_for_ready(pgcache: &mut Child) -> Result<(), String> {
    const NEEDLE: &str = "Listening to";
    //wait to listening message from proxy before proceeding
    let mut buf = BytesMut::new();
    let mut read_buf = [0u8; 1024];

    let mut stdout = pgcache.stdout.take().unwrap();
    while !String::from_utf8_lossy(&buf).contains(NEEDLE) {
        if buf.len() > NEEDLE.len() {
            buf.advance(buf.len() - NEEDLE.len());
        }
        let cnt = stdout.read(&mut read_buf).unwrap_or_default();
        if cnt == 0 {
            return Err("Unexpected end of stdout".to_owned());
        }
        buf.extend_from_slice(&read_buf[0..cnt]);
    }
    pgcache.stdout = Some(stdout);

    Ok(())
}

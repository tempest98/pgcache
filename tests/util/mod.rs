use std::{io::Read, process::Child};

use tokio_util::bytes::{Buf, BytesMut};

pub fn proxy_wait_for_ready(pgcache: &mut Child) {
    const NEEDLE: &str = "Listening to";
    //wait to listening message from proxy before proceeding
    let mut buf = BytesMut::zeroed(1024);

    let mut stdout = pgcache.stdout.take().unwrap();
    while !String::from_utf8_lossy(&buf).contains(NEEDLE) {
        if buf.len() > NEEDLE.len() {
            buf.advance(buf.len() - NEEDLE.len());
        }
        let _ = stdout.read(buf.as_mut());
    }
}

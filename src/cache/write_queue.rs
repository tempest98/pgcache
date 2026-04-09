use std::collections::VecDeque;
use std::io::IoSlice;

use tokio_util::bytes::{Buf, Bytes};

/// Queue of `Bytes` chunks for zero-copy vectored writes.
///
/// Instead of copying frame data into a contiguous write buffer,
/// push zero-copy `BytesMut` splits directly. When the writer
/// supports vectored I/O (`is_write_vectored`), `tokio::io::write_buf`
/// uses `chunks_vectored` to issue a single `writev` syscall across
/// all queued chunks.
///
/// Accepts both `BytesMut` (frame data from the codec) and `Bytes`
/// (e.g. `Bytes::from_static` for fixed protocol messages).
pub struct WriteQueue {
    bufs: VecDeque<Bytes>,
}

impl WriteQueue {
    /// Pre-allocates space for 16 chunks, enough for a typical response
    /// (prefix + RowDescription + DataRow batches + CommandComplete)
    /// without reallocation.
    pub fn new() -> Self {
        Self {
            bufs: VecDeque::with_capacity(16),
        }
    }

    /// Push a chunk onto the queue. Empty chunks are silently ignored.
    ///
    /// Accepts `BytesMut` (zero-cost freeze) or `Bytes` (e.g. `from_static`).
    pub fn push(&mut self, buf: impl Into<Bytes>) {
        let buf = buf.into();
        if buf.has_remaining() {
            self.bufs.push_back(buf);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bufs.is_empty()
    }
}

impl Buf for WriteQueue {
    fn remaining(&self) -> usize {
        self.bufs.iter().map(Buf::remaining).sum()
    }

    fn chunk(&self) -> &[u8] {
        self.bufs.front().map_or(&[], Buf::chunk)
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let front = self
                .bufs
                .front_mut()
                .expect("advance called with cnt > remaining");
            let n = cnt.min(front.remaining());
            front.advance(n);
            cnt -= n;
            if !front.has_remaining() {
                self.bufs.pop_front();
            }
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let filled = dst.len().min(self.bufs.len());
        for (slot, buf) in dst.iter_mut().zip(self.bufs.iter()) {
            *slot = IoSlice::new(buf.chunk());
        }
        filled
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::bytes::BytesMut;

    use super::*;

    #[test]
    fn empty_queue() {
        let q = WriteQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.remaining(), 0);
        assert!(q.chunk().is_empty());
    }

    #[test]
    fn push_bytes_mut() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"hello"[..]));
        q.push(BytesMut::from(&b"world"[..]));
        assert!(!q.is_empty());
        assert_eq!(q.remaining(), 10);
        assert_eq!(q.chunk(), b"hello");
    }

    #[test]
    fn push_static() {
        let mut q = WriteQueue::new();
        q.push(Bytes::from_static(b"static"));
        assert_eq!(q.remaining(), 6);
        assert_eq!(q.chunk(), b"static");
    }

    #[test]
    fn push_empty_ignored() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::new());
        q.push(Bytes::new());
        assert!(q.is_empty());
    }

    #[test]
    fn advance_within_chunk() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"hello"[..]));
        q.advance(3);
        assert_eq!(q.remaining(), 2);
        assert_eq!(q.chunk(), b"lo");
    }

    #[test]
    fn advance_across_chunks() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"ab"[..]));
        q.push(Bytes::from_static(b"cde"));
        q.advance(3); // consumes "ab" + "c"
        assert_eq!(q.remaining(), 2);
        assert_eq!(q.chunk(), b"de");
    }

    #[test]
    fn advance_drains_all() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"abc"[..]));
        q.advance(3);
        assert!(q.is_empty());
        assert_eq!(q.remaining(), 0);
    }

    #[test]
    fn chunks_vectored_fills_dst() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"aa"[..]));
        q.push(Bytes::from_static(b"bb"));
        q.push(BytesMut::from(&b"cc"[..]));

        let mut slices = [IoSlice::new(&[]); 4];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, 3);
        assert_eq!(&*slices[0], b"aa");
        assert_eq!(&*slices[1], b"bb");
        assert_eq!(&*slices[2], b"cc");
    }

    #[test]
    fn chunks_vectored_limited_by_dst_len() {
        let mut q = WriteQueue::new();
        q.push(BytesMut::from(&b"aa"[..]));
        q.push(BytesMut::from(&b"bb"[..]));
        q.push(BytesMut::from(&b"cc"[..]));

        let mut slices = [IoSlice::new(&[]); 2];
        let n = q.chunks_vectored(&mut slices);
        assert_eq!(n, 2);
        assert_eq!(&*slices[0], b"aa");
        assert_eq!(&*slices[1], b"bb");
    }
}

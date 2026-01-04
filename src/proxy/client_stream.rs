//! Client stream types for handling both plain TCP and TLS connections.
//!
//! This module provides the client-specific stream types that extend the generic
//! TLS stream infrastructure with fd duplication for the cache worker.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use nix::libc;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;

use super::tls_stream::{TlsReadHalf, TlsStream, TlsWriteHalf, tls_poll_shutdown, tls_poll_write};
use crate::tls::ClientTlsState;

// ============================================================================
// ClientStream - type alias for client-to-proxy connections
// ============================================================================

/// Client connection stream, either plain TCP or TLS-encrypted.
///
/// Uses rustls::ServerConnection because the proxy acts as a TLS server
/// for incoming client connections.
pub type ClientStream = TlsStream<rustls::ServerConnection>;

/// Borrowed read half of a ClientStream.
pub type ClientReadHalf<'a> = TlsReadHalf<'a, rustls::ServerConnection>;

/// Borrowed write half of a ClientStream.
pub type ClientWriteHalf<'a> = TlsWriteHalf<'a, rustls::ServerConnection>;

// ============================================================================
// ClientStream extension methods
// ============================================================================

impl ClientStream {
    /// Create a ClientSocketSource for creating ClientSocket instances on demand.
    ///
    /// Call this BEFORE splitting the stream to capture the raw fd and TLS state.
    /// The returned source can be cloned and used to create multiple ClientSocket
    /// instances by duplicating the file descriptor.
    pub fn socket_source_create(&self) -> ClientSocketSource {
        match self {
            TlsStream::Plain(tcp) => ClientSocketSource::Plain(tcp.as_raw_fd()),
            TlsStream::Tls { tcp, tls_state } => ClientSocketSource::Tls {
                fd: tcp.as_raw_fd(),
                tls_state: Arc::clone(tls_state),
            },
        }
    }
}

// ============================================================================
// ClientSocket - owned socket for cache worker async writes
// ============================================================================

/// Owned client socket that can be sent to the cache worker.
///
/// Created by duplicating the file descriptor from a ClientStream.
/// Implements AsyncWrite for standard async write operations.
pub enum ClientSocket {
    /// Plain TCP connection
    Plain(TcpStream),
    /// TLS connection with shared encryption state
    Tls {
        tcp: TcpStream,
        tls_state: ClientTlsState,
    },
}

impl AsyncWrite for ClientSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            ClientSocket::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            ClientSocket::Tls { tcp, tls_state } => {
                tls_poll_write(Pin::new(tcp), tls_state, cx, buf)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            ClientSocket::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            ClientSocket::Tls { tcp, .. } => Pin::new(tcp).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            ClientSocket::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            ClientSocket::Tls { tcp, tls_state } => tls_poll_shutdown(Pin::new(tcp), tls_state, cx),
        }
    }
}

impl std::fmt::Debug for ClientSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientSocket::Plain(tcp) => f
                .debug_struct("ClientSocket::Plain")
                .field("fd", &tcp.as_raw_fd())
                .finish(),
            ClientSocket::Tls { tcp, .. } => f
                .debug_struct("ClientSocket::Tls")
                .field("fd", &tcp.as_raw_fd())
                .finish(),
        }
    }
}

// ============================================================================
// ClientSocketSource - factory for creating ClientSocket instances
// ============================================================================

/// Source for creating ClientSocket instances on demand.
///
/// This type stores the raw fd and optional TLS state, and can create
/// new ClientSocket instances by duplicating the file descriptor.
/// This is necessary because we need to create multiple sockets for
/// different cache queries, but the original stream is borrowed by split().
#[derive(Clone)]
pub enum ClientSocketSource {
    /// Plain TCP - stores the raw fd
    Plain(RawFd),
    /// TLS - stores raw fd and shared TLS state
    Tls {
        fd: RawFd,
        tls_state: ClientTlsState,
    },
}

impl ClientSocketSource {
    /// Create a new ClientSocket by duplicating the file descriptor.
    pub fn socket_create(&self) -> io::Result<ClientSocket> {
        match self {
            ClientSocketSource::Plain(fd) => {
                let dup_fd = dup_fd(*fd)?;
                let tcp = fd_to_tcp_stream(dup_fd)?;
                Ok(ClientSocket::Plain(tcp))
            }
            ClientSocketSource::Tls { fd, tls_state } => {
                let dup_fd = dup_fd(*fd)?;
                let tcp = fd_to_tcp_stream(dup_fd)?;
                Ok(ClientSocket::Tls {
                    tcp,
                    tls_state: Arc::clone(tls_state),
                })
            }
        }
    }
}

impl std::fmt::Debug for ClientSocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientSocketSource::Plain(fd) => f.debug_tuple("Plain").field(fd).finish(),
            ClientSocketSource::Tls { fd, .. } => f.debug_struct("Tls").field("fd", fd).finish(),
        }
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Convert an OwnedFd to a tokio TcpStream.
fn fd_to_tcp_stream(fd: OwnedFd) -> io::Result<TcpStream> {
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd.into_raw_fd()) };
    std_stream.set_nonblocking(true)?;
    TcpStream::from_std(std_stream)
}

/// Duplicate a file descriptor.
fn dup_fd(fd: RawFd) -> io::Result<OwnedFd> {
    let duped = unsafe { libc::dup(fd) };
    if duped < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(duped) })
}

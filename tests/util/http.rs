use std::io::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Make a raw HTTP GET request and return (status_code, body).
pub async fn http_get(port: u16, path: &str) -> Result<(u16, String), Error> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .map_err(Error::other)?;

    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::other)?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(Error::other)?;

    // Parse status line: "HTTP/1.1 200 OK\r\n..."
    let status_code = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);

    // Body starts after "\r\n\r\n"
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();

    Ok((status_code, body))
}

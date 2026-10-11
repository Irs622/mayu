//! A minimal HTTP/1.1 endpoint for Prometheus: `GET /metrics`, one request per connection.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Produces the response body for one scrape.
pub(crate) type Render = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

const MAX_REQUEST: usize = 8 * 1024;
/// A connection that has not finished within this is dropped (slow or idle clients).
const CONN_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn serve(listener: TcpListener, render: Render) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let render = render.clone();
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(CONN_TIMEOUT, handle(stream, render)).await;
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "accepting a metrics connection failed");
                tokio::time::sleep(Duration::from_millis(100)).await; // e.g. EMFILE: don't spin
            }
        }
    }
}

async fn handle(mut s: TcpStream, render: Render) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        if buf.len() > MAX_REQUEST {
            return respond(&mut s, "431 Request Header Fields Too Large", "").await;
        }
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let line = buf.split(|&b| b == b'\r').next().unwrap_or_default();
    let mut parts = line.split(|&b| b == b' ');
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let path = target.split(|&b| b == b'?').next().unwrap_or_default();
    match (method, path) {
        (b"GET", b"/metrics") => {
            let body = render().await;
            respond(&mut s, "200 OK", &body).await
        }
        (b"GET", _) => respond(&mut s, "404 Not Found", "not found\n").await,
        _ => respond(&mut s, "405 Method Not Allowed", "").await,
    }
}

async fn respond(s: &mut TcpStream, status: &str, body: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).await?;
    s.write_all(body.as_bytes()).await?;
    s.shutdown().await
}

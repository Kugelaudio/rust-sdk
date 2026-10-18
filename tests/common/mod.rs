//! A scripted local WebSocket server for the session tests.

#![allow(dead_code)]
// The handshake callback signature is tungstenite's; its error type is large by design.
#![allow(clippy::result_large_err)]

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// One accepted connection.
pub struct Conn {
    pub ws: WebSocketStream<TcpStream>,
    /// Path and query of the upgrade request.
    pub uri: String,
    /// Zero-based index of this connection.
    pub index: usize,
}

impl Conn {
    /// The next text frame as JSON (skips pings).
    pub async fn recv_json(&mut self) -> Value {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                other => panic!("expected a text frame, got {other:?}"),
            }
        }
    }

    /// The next frame of any kind, `None` when the socket ended.
    pub async fn recv(&mut self) -> Option<Message> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(message)) => return Some(message),
                _ => return None,
            }
        }
    }

    pub async fn send_json(&mut self, value: Value) {
        self.ws
            .send(Message::text(value.to_string()))
            .await
            .unwrap();
    }

    pub async fn send_binary(&mut self, data: Vec<u8>) {
        self.ws.send(Message::binary(data)).await.unwrap();
    }

    pub async fn close(&mut self, code: u16, reason: &str) {
        let _ = self
            .ws
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.to_string().into(),
            })))
            .await;
        // Wait for the client's reply so the close is clean.
        while let Some(Ok(_)) = self.ws.next().await {}
    }
}

/// An audio frame with `samples` PCM16 samples of value `value`.
pub fn audio_frame(index: u64, samples: usize, value: i16) -> Value {
    let pcm: Vec<u8> = std::iter::repeat_n(value.to_le_bytes(), samples)
        .flatten()
        .collect();
    json!({
        "audio": BASE64.encode(pcm),
        "enc": "pcm_s16le",
        "idx": index,
        "sr": 24000,
        "samples": samples,
        "chunk_id": 0,
    })
}

/// A local WebSocket server; every connection runs `handler`.
pub struct Server {
    pub addr: String,
    pub connections: Arc<AtomicUsize>,
    pub uris: Arc<Mutex<Vec<String>>>,
}

impl Server {
    /// The `http://` base URL to pass as `tts_url` / `api_url`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

/// Serve WebSocket connections with `handler`.
pub async fn serve<F, Fut>(handler: F) -> Server
where
    F: Fn(Conn) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    serve_with_status(None, handler).await
}

/// Serve connections, refusing every upgrade with `refuse` when set.
pub async fn serve_with_status<F, Fut>(refuse: Option<(u16, &'static str)>, handler: F) -> Server
where
    F: Fn(Conn) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let connections = Arc::new(AtomicUsize::new(0));
    let uris = Arc::new(Mutex::new(Vec::new()));
    let handler = Arc::new(handler);
    let (count, seen) = (connections.clone(), uris.clone());
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let index = count.fetch_add(1, Ordering::SeqCst);
            let handler = handler.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let uri = Arc::new(Mutex::new(String::new()));
                let captured = uri.clone();
                let callback = move |request: &Request, response: Response| {
                    *captured.lock().unwrap() = request.uri().to_string();
                    if let Some((status, body)) = refuse {
                        let mut error = ErrorResponse::new(Some(body.to_string()));
                        *error.status_mut() = StatusCode::from_u16(status).unwrap();
                        error
                            .headers_mut()
                            .insert("x-request-id", "req-handshake".parse().unwrap());
                        return Err(error);
                    }
                    Ok(response)
                };
                let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
                    return;
                };
                let uri = uri.lock().unwrap().clone();
                seen.lock().unwrap().push(uri.clone());
                handler(Conn { ws, uri, index }).await;
            });
        }
    });
    Server {
        addr,
        connections,
        uris,
    }
}

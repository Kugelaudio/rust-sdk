//! One WebSocket connection driven by a background task.
//!
//! The task owns the socket: it writes what the session sends, forwards every
//! received frame to an inbox, answers pings, and sends keepalive pings of its
//! own so NAT and proxy idle timeouts do not drop a quiet connection. When the
//! socket ends, the task records the close code and reason; every later
//! receive or send reports them.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::error::{classify_handshake, classify_ws_close, Error, Result};
use crate::metadata::sdk_headers;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Abnormal closure: the socket ended without a close frame.
pub(crate) const CLOSE_ABNORMAL: u16 = 1006;
/// How long a graceful close waits for the server's close frame.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Why the socket ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloseInfo {
    pub code: Option<u16>,
    pub reason: String,
}

impl CloseInfo {
    fn abnormal(reason: impl Into<String>) -> Self {
        Self {
            code: Some(CLOSE_ABNORMAL),
            reason: reason.into(),
        }
    }

    /// The typed error for this close.
    pub fn error(&self) -> Error {
        classify_ws_close(self.code, &self.reason)
    }
}

/// A received data frame.
#[derive(Debug)]
pub(crate) enum Inbound {
    Text(String),
    Binary(Vec<u8>),
}

/// Why a receive returned without a frame.
#[derive(Debug)]
pub(crate) enum RecvError {
    Closed(CloseInfo),
    Timeout,
}

/// The URL of a WebSocket endpoint: `http(s)` becomes `ws(s)`, `path` is
/// appended, and `query` plus the SDK metadata are added.
pub(crate) fn ws_url(base: &str, path: &str, query: &[(&str, &str)]) -> Result<reqwest::Url> {
    let base = base.trim_end_matches('/');
    let base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base.to_string()
    };
    let mut url = reqwest::Url::parse(&format!("{base}{path}")).map_err(|_| {
        Error::validation("The KugelAudio API URL does not form a valid WebSocket URL.")
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
        for (key, value) in crate::metadata::sdk_query() {
            pairs.append_pair(key, value);
        }
    }
    Ok(url)
}

#[derive(Default)]
struct Shared {
    closed: Mutex<Option<CloseInfo>>,
}

impl Shared {
    fn get(&self) -> Option<CloseInfo> {
        // KEEP-JUSTIFIED: a poisoned lock still holds a valid Option.
        self.closed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn set(&self, info: CloseInfo) {
        let mut slot = self.closed.lock().unwrap_or_else(|p| p.into_inner());
        slot.get_or_insert(info);
    }
}

/// A connected WebSocket.
pub(crate) struct WsConn {
    out: mpsc::UnboundedSender<Message>,
    inbox: mpsc::UnboundedReceiver<Inbound>,
    shared: Arc<Shared>,
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for WsConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsConn")
            .field("closed", &self.shared.get())
            .finish()
    }
}

impl WsConn {
    /// Open `url`, sending the SDK headers in the handshake. A refused
    /// upgrade becomes the typed error for its HTTP status.
    pub async fn connect(
        url: &reqwest::Url,
        timeout: Duration,
        keepalive: Option<Duration>,
    ) -> Result<Self> {
        let mut request = url.as_str().into_client_request().map_err(|_| {
            Error::validation("The KugelAudio API URL does not form a valid WebSocket URL.")
        })?;
        for (name, value) in sdk_headers() {
            if let Ok(value) = HeaderValue::from_str(&value) {
                request.headers_mut().insert(name, value);
            }
        }
        let connecting = tokio_tungstenite::connect_async(request);
        let (socket, _response) = match tokio::time::timeout(timeout, connecting).await {
            Err(_) => {
                return Err(Error::connection(format!(
                    "KugelAudio WebSocket handshake timed out after {}s.",
                    timeout.as_secs_f64()
                )))
            }
            Ok(Err(WsError::Http(response))) => {
                let status = response.status().as_u16();
                let body = response.body().clone().unwrap_or_default();
                return Err(classify_handshake(status, response.headers(), &body));
            }
            Ok(Err(WsError::Url(_))) => {
                // The URL error's text may hold the URL, whose query carries the key.
                return Err(Error::validation(
                    "The KugelAudio API URL does not form a valid WebSocket URL.",
                ));
            }
            Ok(Err(e)) => {
                return Err(Error::connection(format!(
                    "KugelAudio WebSocket handshake failed: {e}."
                ))
                .with_source(e))
            }
            Ok(Ok(pair)) => pair,
        };
        Ok(Self::spawn(socket, keepalive))
    }

    fn spawn(socket: Socket, keepalive: Option<Duration>) -> Self {
        let (out, out_rx) = mpsc::unbounded_channel();
        let (in_tx, inbox) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared::default());
        let task = tokio::spawn(run(socket, out_rx, in_tx, shared.clone(), keepalive));
        Self {
            out,
            inbox,
            shared,
            task: Some(task),
        }
    }

    /// True until the socket ended.
    pub fn is_open(&self) -> bool {
        self.shared.get().is_none() && self.task.as_ref().is_some_and(|t| !t.is_finished())
    }

    /// The close that ended the socket, if it ended.
    pub fn close_info(&self) -> Option<CloseInfo> {
        self.shared.get()
    }

    fn closed_info(&self) -> CloseInfo {
        self.shared
            .get()
            .unwrap_or_else(|| CloseInfo::abnormal("connection lost"))
    }

    /// Queue a JSON text frame. Fails with the close that ended the socket.
    pub fn send_json(&self, value: &Value) -> std::result::Result<(), CloseInfo> {
        self.send(Message::text(value.to_string()))
    }

    fn send(&self, message: Message) -> std::result::Result<(), CloseInfo> {
        if let Some(info) = self.shared.get() {
            return Err(info);
        }
        self.out.send(message).map_err(|_| self.closed_info())
    }

    /// A sender usable from another task (enhancement input pumps).
    pub fn sender(&self) -> WsSender {
        WsSender {
            out: self.out.clone(),
            shared: self.shared.clone(),
        }
    }

    /// The next data frame. Frames received before a close are delivered
    /// before the close is reported.
    pub async fn recv(&mut self, timeout: Duration) -> std::result::Result<Inbound, RecvError> {
        match tokio::time::timeout(timeout, self.inbox.recv()).await {
            Err(_) => Err(RecvError::Timeout),
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(RecvError::Closed(self.closed_info())),
        }
    }

    /// The next text frame parsed as JSON; binary frames are skipped.
    pub async fn recv_json(&mut self, timeout: Duration) -> std::result::Result<Value, RecvError> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.recv(left).await? {
                Inbound::Text(text) => match serde_json::from_str(&text) {
                    Ok(value) => return Ok(value),
                    Err(_) => {
                        tracing::warn!(
                            len = text.len(),
                            "ignoring a non-JSON text frame from KugelAudio"
                        );
                    }
                },
                Inbound::Binary(_) => {
                    tracing::warn!("ignoring an unexpected binary frame from KugelAudio");
                }
            }
        }
    }

    /// Close gracefully (code 1000) and wait for the socket to finish.
    pub async fn close(mut self) {
        let _ = self.out.send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })));
        if let Some(task) = self.task.take() {
            let abort = task.abort_handle();
            if tokio::time::timeout(CLOSE_TIMEOUT + Duration::from_secs(1), task)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
    }
}

/// A cloneable handle that writes to a [`WsConn`].
#[derive(Clone)]
pub(crate) struct WsSender {
    out: mpsc::UnboundedSender<Message>,
    shared: Arc<Shared>,
}

impl WsSender {
    /// Queue a frame; fails once the socket ended.
    pub fn send(&self, message: Message) -> std::result::Result<(), CloseInfo> {
        if let Some(info) = self.shared.get() {
            return Err(info);
        }
        self.out.send(message).map_err(|_| {
            self.shared
                .get()
                .unwrap_or_else(|| CloseInfo::abnormal("connection lost"))
        })
    }
}

async fn tick(interval: &mut Option<Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn info_from_error(error: &WsError) -> CloseInfo {
    match error {
        WsError::ConnectionClosed | WsError::AlreadyClosed => CloseInfo {
            code: Some(1000),
            reason: String::new(),
        },
        // An I/O, protocol or TLS failure ends the socket without a close frame.
        other => CloseInfo::abnormal(other.to_string()),
    }
}

fn info_from_frame(frame: Option<CloseFrame>) -> CloseInfo {
    match frame {
        Some(frame) => CloseInfo {
            code: Some(u16::from(frame.code)),
            reason: frame.reason.to_string(),
        },
        // A close frame without a code is 1005 (no status received).
        None => CloseInfo {
            code: Some(1005),
            reason: String::new(),
        },
    }
}

async fn run(
    mut socket: Socket,
    mut out_rx: mpsc::UnboundedReceiver<Message>,
    in_tx: mpsc::UnboundedSender<Inbound>,
    shared: Arc<Shared>,
    keepalive: Option<Duration>,
) {
    let mut ping = keepalive.map(|every| tokio::time::interval_at(Instant::now() + every, every));
    let mut ping_sent: Option<Instant> = None;
    let info = loop {
        tokio::select! {
            outgoing = out_rx.recv() => {
                let closing = matches!(outgoing, None | Some(Message::Close(_)));
                let message = outgoing.unwrap_or(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "".into(),
                })));
                if let Err(e) = socket.send(message).await {
                    break info_from_error(&e);
                }
                if closing {
                    // Wait (bounded) for the server's close frame.
                    let server = tokio::time::timeout(CLOSE_TIMEOUT, async {
                        while let Some(frame) = socket.next().await {
                            if let Ok(Message::Close(frame)) = frame {
                                return Some(info_from_frame(frame));
                            }
                            if frame.is_err() {
                                return None;
                            }
                        }
                        None
                    })
                    .await;
                    break server.ok().flatten().unwrap_or(CloseInfo {
                        code: Some(1000),
                        reason: "closed by client".to_string(),
                    });
                }
            }
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    let _ = in_tx.send(Inbound::Text(text.to_string()));
                }
                Some(Ok(Message::Binary(data))) => {
                    let _ = in_tx.send(Inbound::Binary(data.to_vec()));
                }
                Some(Ok(Message::Pong(_))) => ping_sent = None,
                Some(Ok(Message::Ping(_) | Message::Frame(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    let info = info_from_frame(frame);
                    // Let tungstenite send its close reply (bounded).
                    let _ = tokio::time::timeout(CLOSE_TIMEOUT, async {
                        while socket.next().await.is_some() {}
                    })
                    .await;
                    break info;
                }
                Some(Err(e)) => break info_from_error(&e),
                None => break CloseInfo::abnormal("connection closed without a close frame"),
            },
            _ = tick(&mut ping) => {
                if let Some(sent) = ping_sent {
                    if let Some(every) = keepalive {
                        if sent.elapsed() >= every {
                            break CloseInfo::abnormal("keepalive ping timeout");
                        }
                    }
                    continue;
                }
                if let Err(e) = socket.send(Message::Ping(Vec::new().into())).await {
                    break info_from_error(&e);
                }
                ping_sent = Some(Instant::now());
            }
        }
    };
    shared.set(info);
    // `in_tx` drops here: receivers drain buffered frames, then see the close.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_switches_scheme_and_adds_metadata() {
        let url = ws_url(
            "https://api.kugelaudio.com/",
            "/ws/tts",
            &[("api_key", "k&1"), ("model_id", "kugel-3.5")],
        )
        .unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.path(), "/ws/tts");
        let query: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        assert_eq!(query[0], ("api_key".into(), "k&1".into()));
        assert_eq!(query[2], ("sdk".into(), "rust".into()));
        let local = ws_url("http://localhost:8080", "/ws/tts/stream", &[]).unwrap();
        assert_eq!(local.scheme(), "ws");
    }

    #[test]
    fn close_info_classifies() {
        let info = CloseInfo {
            code: Some(1012),
            reason: String::new(),
        };
        assert!(matches!(info.error(), Error::ServerRestarting(_)));
        assert_eq!(CloseInfo::abnormal("x").code, Some(1006));
    }
}

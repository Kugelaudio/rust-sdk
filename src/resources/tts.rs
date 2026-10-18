//! Text to speech: REST generation, the `/ws/tts` request stream, and the
//! constructors of the streaming sessions.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::Method;

use crate::audio::{AudioEncoding, OutputFormat};
use crate::client::{ClientInner, KugelAudio, PooledWs};
use crate::diagnostics::{
    Operation, OP_GENERATE, OP_STREAM, STAGE_AWAITING_FIRST_AUDIO, STAGE_CONNECTING,
    STAGE_HANDSHAKE, STAGE_SENDING_REQUEST, TRANSPORT_HTTP, TRANSPORT_WEBSOCKET,
};
use crate::error::{classify_ws_frame, header_value, Error, Result};
use crate::request::{
    warn_if_no_language, MultiContextConfig, StreamConfig, TtsRequest, DEFAULT_MODEL,
};
use crate::response::{AudioChunk, AudioResponse, FinalStats, WordTimestamp};
use crate::streaming::ws::{ws_url, RecvError, WsConn};
use crate::streaming::{truthy, word_timestamps, MultiContextSession, StreamingSession};

/// One event of a [`TtsStream`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum StreamEvent {
    /// A chunk of audio.
    Audio(AudioChunk),
    /// Word timestamps (when the request set `word_timestamps`).
    WordTimestamps(Vec<WordTimestamp>),
    /// The request finished: stats and usage. Always the last event.
    Final(FinalStats),
}

/// The audio of one `/ws/tts` request, as a [`Stream`] of [`StreamEvent`]s.
///
/// Dropping it before the [`StreamEvent::Final`] event closes its socket, so
/// no stale audio can reach a later request.
pub struct TtsStream {
    events: BoxStream<'static, Result<StreamEvent>>,
    requested_rate: u32,
    requested_encoding: AudioEncoding,
}

impl std::fmt::Debug for TtsStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TtsStream").finish_non_exhaustive()
    }
}

impl Stream for TtsStream {
    type Item = Result<StreamEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.poll_next_unpin(cx)
    }
}

impl TtsStream {
    /// Drain the stream into one [`AudioResponse`] with every chunk, the word
    /// timestamps and the final stats and usage.
    pub async fn collect_response(mut self) -> Result<AudioResponse> {
        let mut audio = Vec::new();
        let mut rate = None;
        let mut encoding = None;
        let mut stamps = Vec::new();
        let mut last = None;
        while let Some(event) = self.events.next().await {
            match event? {
                StreamEvent::Audio(chunk) => {
                    rate.get_or_insert(chunk.sample_rate);
                    encoding.get_or_insert(chunk.encoding);
                    audio.extend_from_slice(&chunk.audio);
                }
                StreamEvent::WordTimestamps(words) => stamps.extend(words),
                StreamEvent::Final(stats) => last = Some(stats),
            }
        }
        // Without any chunk the rate and encoding are the requested ones.
        let mut response = AudioResponse::from_bytes(
            audio,
            rate.unwrap_or(self.requested_rate),
            encoding.unwrap_or(self.requested_encoding),
            None,
        );
        if let Some(stats) = last {
            if let Some(total) = stats.total_samples {
                response.samples = total;
            }
            response.duration_ms = stats.duration_ms;
            response.generation_ms = stats.generation_ms;
            response.rtf = stats.rtf;
            response.usage = stats.usage;
        }
        response.word_timestamps = stamps;
        Ok(response)
    }
}

/// Text to speech, from [`KugelAudio::tts`].
#[derive(Debug, Clone, Copy)]
pub struct TtsResource<'a> {
    client: &'a KugelAudio,
}

impl<'a> TtsResource<'a> {
    pub(crate) fn new(client: &'a KugelAudio) -> Self {
        Self { client }
    }

    /// Generate the whole audio in one REST request (`POST /v1/tts/generate`).
    ///
    /// The REST endpoint has no word timestamps; a request with
    /// `word_timestamps` set is rejected here. Stream it instead and call
    /// [`TtsStream::collect_response`].
    ///
    /// ```no_run
    /// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
    /// use kugelaudio::TtsRequest;
    ///
    /// let audio = client
    ///     .tts()
    ///     .generate(&TtsRequest::new("Hallo Welt!").voice_id(1071).language("de"))
    ///     .await?;
    /// audio.save_wav("hallo.wav")?;
    /// # Ok(()) }
    /// ```
    pub async fn generate(&self, request: &TtsRequest) -> Result<AudioResponse> {
        if request.word_timestamps {
            return Err(Error::validation(
                "word_timestamps is available on the WebSocket endpoints only: use \
                 client.tts().stream(&request).await?.collect_response().await",
            ));
        }
        let inner = &self.client.inner;
        let mut op = inner
            .diagnostics
            .operation(Some(OP_GENERATE), TRANSPORT_HTTP);
        let outcome = generate_rest(inner, request).await;
        match &outcome {
            Ok(_) => op.succeed(),
            Err(e) => op.fail(e),
        }
        outcome
    }

    /// Stream one request over the pooled `/ws/tts` socket.
    ///
    /// Connects (or reuses the socket a previous request left open), sends the
    /// request and returns the stream of its events. Requests may run
    /// concurrently: each borrows its own socket.
    ///
    /// ```no_run
    /// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
    /// use futures_util::StreamExt;
    /// use kugelaudio::{StreamEvent, TtsRequest};
    ///
    /// let mut stream = client.tts().stream(&TtsRequest::new("Hello!").voice_id(1071)).await?;
    /// while let Some(event) = stream.next().await {
    ///     match event? {
    ///         StreamEvent::Audio(chunk) => println!("{} bytes", chunk.audio.len()),
    ///         StreamEvent::Final(stats) => println!("usage: {:?}", stats.usage),
    ///         _ => {}
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn stream(&self, request: &TtsRequest) -> Result<TtsStream> {
        let inner = self.client.inner.clone();
        let mut op = inner
            .diagnostics
            .operation(Some(OP_STREAM), TRANSPORT_WEBSOCKET);
        warn_if_no_language(request.language.as_deref(), request.normalize);
        op.set_stage(STAGE_CONNECTING);
        let conn = match pooled_or_connect(&inner, &request.model_id, &mut op).await {
            Ok(conn) => conn,
            Err(e) => {
                op.fail(&e);
                return Err(e);
            }
        };
        op.set_stage(STAGE_SENDING_REQUEST);
        if let Err(info) = conn.send_json(&request.to_json(true)) {
            let e = info.error();
            op.fail(&e);
            return Err(e);
        }
        op.set_stage(STAGE_AWAITING_FIRST_AUDIO);
        Ok(TtsStream {
            events: Box::pin(request_events(inner, conn, request.model_id.clone(), op)),
            requested_rate: request.effective_sample_rate(),
            requested_encoding: request
                .output_format
                .map(OutputFormat::encoding)
                .unwrap_or_default(),
        })
    }

    /// Open the `/ws/tts` socket for `model_id` now, so the first
    /// [`stream`](Self::stream) skips the connection setup (300-600 ms).
    pub async fn connect(&self, model_id: Option<&str>) -> Result<()> {
        let inner = &self.client.inner;
        let model_id = model_id.unwrap_or(DEFAULT_MODEL);
        let mut op = inner
            .diagnostics
            .operation(Some(OP_STREAM), TRANSPORT_WEBSOCKET);
        match pooled_or_connect(inner, model_id, &mut op).await {
            Ok(conn) => {
                op.succeed();
                return_to_pool(inner, model_id.to_string(), conn).await;
                Ok(())
            }
            Err(e) => {
                op.fail(&e);
                Err(e)
            }
        }
    }

    /// True when a pooled `/ws/tts` socket is open.
    pub fn is_connected(&self) -> bool {
        self.client.inner.has_open_pooled()
    }

    /// A session for streaming text in (e.g. LLM tokens) and audio out
    /// (`/ws/tts/stream`). Call [`StreamingSession::connect`] to open it early,
    /// or just [`send`](StreamingSession::send).
    pub fn streaming_session(&self, config: StreamConfig) -> StreamingSession {
        StreamingSession::new(self.client.inner.clone(), config)
    }

    /// A session with up to 20 independent contexts on one socket (`/ws/tts/multi`).
    pub fn multi_context_session(&self, config: MultiContextConfig) -> MultiContextSession {
        MultiContextSession::new(self.client.inner.clone(), config)
    }
}

async fn generate_rest(inner: &ClientInner, request: &TtsRequest) -> Result<AudioResponse> {
    const PATH: &str = "/v1/tts/generate";
    warn_if_no_language(request.language.as_deref(), request.normalize);
    let builder = inner
        .http
        .request(Method::POST, inner.url(PATH))
        .json(&request.to_json(false));
    let response = inner.send(&Method::POST, PATH, builder).await?;
    let headers = response.headers().clone();
    let request_id = header_value(&headers, "x-request-id");
    // The response is produced at the requested rate and format; the headers
    // confirm it, and win when present.
    let sample_rate = match header_value(&headers, "x-sample-rate") {
        Some(value) => value.parse::<u32>().map_err(|_| {
            Error::api(format!(
                "Unexpected X-Sample-Rate '{value}' from KugelAudio."
            ))
        })?,
        None => request.effective_sample_rate(),
    };
    let encoding = match header_value(&headers, "x-audio-format") {
        Some(value) => AudioEncoding::parse(&value).ok_or_else(|| {
            Error::api(format!(
                "Unexpected X-Audio-Format '{value}' from KugelAudio; upgrade the SDK."
            ))
        })?,
        None => request
            .output_format
            .map(OutputFormat::encoding)
            .unwrap_or_default(),
    };
    let audio = response.bytes().await.map_err(|e| {
        Error::connection(format!("Reading the audio from {PATH} failed: {e}.")).with_source(e)
    })?;
    Ok(AudioResponse::from_bytes(
        audio.to_vec(),
        sample_rate,
        encoding,
        request_id,
    ))
}

/// The pooled socket for `model_id` if it is still open, else a new one.
async fn pooled_or_connect(
    inner: &Arc<ClientInner>,
    model_id: &str,
    op: &mut Operation,
) -> Result<WsConn> {
    if let Some(pooled) = inner.take_pooled() {
        if pooled.model_id == model_id && pooled.conn.is_open() {
            return Ok(pooled.conn);
        }
        pooled.conn.close().await;
    }
    op.set_stage(STAGE_HANDSHAKE);
    let url = ws_url(
        &inner.tts_url,
        "/ws/tts",
        &[("api_key", inner.api_key.as_str()), ("model_id", model_id)],
    )?;
    WsConn::connect(&url, inner.timeout, inner.keepalive).await
}

async fn return_to_pool(inner: &ClientInner, model_id: String, conn: WsConn) {
    if let Some(extra) = inner.put_pooled(PooledWs { model_id, conn }) {
        extra.conn.close().await;
    }
}

fn request_events(
    inner: Arc<ClientInner>,
    conn: WsConn,
    model_id: String,
    mut op: Operation,
) -> impl Stream<Item = Result<StreamEvent>> + Send + 'static {
    let timeout: Duration = inner.timeout;
    async_stream::stream! {
        let mut conn = Some(conn);
        while let Some(socket) = conn.as_mut() {
            let frame = match socket.recv_json(timeout).await {
                Ok(frame) => frame,
                Err(RecvError::Closed(info)) => {
                    let e = info.error();
                    op.fail(&e);
                    yield Err(e);
                    break;
                }
                Err(RecvError::Timeout) => {
                    let e = Error::connection(format!(
                        "Timed out after {}s waiting for audio from KugelAudio.",
                        timeout.as_secs_f64()
                    ));
                    op.fail(&e);
                    yield Err(e);
                    break;
                }
            };
            if truthy(&frame, "error") {
                // An error frame ends the request; the socket stays usable.
                op.mark_server_error();
                let e = classify_ws_frame(&frame);
                op.fail(&e);
                if let Some(socket) = conn.take() {
                    return_to_pool(&inner, model_id.clone(), socket).await;
                }
                yield Err(e);
                break;
            }
            if truthy(&frame, "final") {
                op.succeed();
                if let Some(socket) = conn.take() {
                    return_to_pool(&inner, model_id.clone(), socket).await;
                }
                yield Ok(StreamEvent::Final(FinalStats::from_frame(&frame)));
                break;
            }
            if truthy(&frame, "audio") {
                match AudioChunk::from_frame(&frame) {
                    Ok(chunk) => {
                        op.record_chunk(chunk.audio.len());
                        yield Ok(StreamEvent::Audio(chunk));
                    }
                    Err(e) => {
                        op.fail(&e);
                        yield Err(e);
                        break;
                    }
                }
            }
            match word_timestamps(&frame) {
                Ok(Some(words)) => yield Ok(StreamEvent::WordTimestamps(words)),
                Ok(None) => {}
                Err(e) => {
                    op.fail(&e);
                    yield Err(e);
                    break;
                }
            }
        }
    }
}

//! Speech enhancement: remove background noise, or keep only one voice.
//!
//! One rule everywhere: enhancement removes noise; a `speaker` sample keeps
//! only that voice. Results are mono PCM16 at 24 kHz with the input's duration.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_core::stream::BoxStream;
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::multipart::{Form, Part};
use reqwest::Method;
use serde_json::{json, Value};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::audio::{parse_wav, wav_bytes, Audio, AudioEncoding, AudioStream};
use crate::client::{ClientInner, KugelAudio};
use crate::error::{classify_ws_frame, Error, Result};
use crate::streaming::ws::{ws_url, RecvError, WsConn};

const TASK_NOISE_REMOVAL: &str = "noise_removal";
const TASK_TARGET_SPEAKER_EXTRACTION: &str = "target_speaker_extraction";
const ENHANCE_PATH: &str = "/v1/audio/enhance";
const ENHANCE_STREAM_PATH: &str = "/v1/audio/enhance/stream";
const ENHANCE_WARMUP_PATH: &str = "/v1/audio/enhance/warmup";
const NOT_READY: &str = "The enhancement stream did not become ready in time.";
/// The server closes a session after 60 s without a config and at 1 h; a
/// socket is not reused past these, a little before the server's limits.
const SESSION_IDLE: Duration = Duration::from_secs(55);
const SESSION_LIFETIME: Duration = Duration::from_secs(3540);
/// What a server without session mode sends on a socket left without a
/// config, before closing it: that socket is stale, not the config wrong.
const IDLE_FRAME_CODE: u64 = 408;
/// The server caps one stream at an hour of audio.
const STREAM_RECV_TIMEOUT: Duration = Duration::from_secs(3600);
const STREAM_PING_INTERVAL: Duration = Duration::from_secs(20);

/// Model and optional speaker sample of an enhancement request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EnhanceOptions {
    /// Enhancement model, e.g. `clarity-1`.
    pub model: String,
    /// A clean 2-8 s sample of one voice; when set, only that voice is kept.
    pub speaker: Option<Audio>,
}

impl EnhanceOptions {
    /// Remove noise with `model`.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            speaker: None,
        }
    }

    /// Keep only the voice of `speaker` (target speaker extraction).
    pub fn speaker(mut self, speaker: Audio) -> Self {
        self.speaker = Some(speaker);
        self
    }

    fn check_model(&self) -> Result<()> {
        if self.model.trim().is_empty() {
            return Err(Error::validation(
                "model must be a non-empty string, e.g. \"clarity-1\".",
            ));
        }
        Ok(())
    }

    fn task(&self) -> &'static str {
        if self.speaker.is_some() {
            TASK_TARGET_SPEAKER_EXTRACTION
        } else {
            TASK_NOISE_REMOVAL
        }
    }
}

/// Enhanced audio: mono PCM16 little-endian, 24 kHz.
#[derive(Clone, PartialEq, Eq)]
pub struct EnhancedAudio {
    /// Raw mono PCM16 samples.
    pub audio: Vec<u8>,
    /// Sample rate in Hz.
    pub sample_rate: u32,
}

impl std::fmt::Debug for EnhancedAudio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnhancedAudio")
            .field("bytes", &self.audio.len())
            .field("sample_rate", &self.sample_rate)
            .finish()
    }
}

impl EnhancedAudio {
    /// Parse a mono PCM16 WAV file.
    pub fn from_wav(data: &[u8]) -> Result<Self> {
        let info = parse_wav(data).map_err(|e| {
            Error::api(format!(
                "Unexpected enhancement response: not a WAV file ({}).",
                e.message()
            ))
        })?;
        if info.channels != 1 || info.bits_per_sample != 16 || info.format_tag != 1 {
            return Err(Error::api(
                "Unexpected enhancement response: expected mono PCM16 WAV.",
            ));
        }
        Ok(Self {
            audio: data[info.data].to_vec(),
            sample_rate: info.sample_rate,
        })
    }

    /// Duration in seconds.
    pub fn duration(&self) -> f64 {
        if self.sample_rate == 0 {
            return 0.0;
        }
        (self.audio.len() / 2) as f64 / f64::from(self.sample_rate)
    }

    /// The audio as WAV file bytes.
    pub fn to_wav(&self) -> Vec<u8> {
        wav_bytes(&self.audio, self.sample_rate, AudioEncoding::PcmS16le)
    }

    /// Write the audio to `path` as a WAV file.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        std::fs::write(path, self.to_wav()).map_err(|e| {
            Error::validation(format!("Cannot write {}: {e}", path.display())).with_source(e)
        })
    }
}

/// Mono PCM16 input for [`EnhanceResource::stream`]: a stream of chunks
/// (ideally at most one second each; longer ones are split) and their rate.
pub struct PcmInput {
    chunks: BoxStream<'static, Vec<u8>>,
    sample_rate: u32,
}

impl std::fmt::Debug for PcmInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PcmInput")
            .field("sample_rate", &self.sample_rate)
            .finish_non_exhaustive()
    }
}

impl PcmInput {
    /// Raw PCM16 chunks arriving over time (e.g. from a microphone), at
    /// `sample_rate` Hz (8-48 kHz).
    pub fn new(chunks: impl Stream<Item = Vec<u8>> + Send + 'static, sample_rate: u32) -> Self {
        Self {
            chunks: Box::pin(chunks),
            sample_rate,
        }
    }

    /// Raw PCM16 chunks already in memory.
    pub fn from_chunks(chunks: impl IntoIterator<Item = Vec<u8>>, sample_rate: u32) -> Self {
        let chunks: Vec<Vec<u8>> = chunks.into_iter().collect();
        Self::new(futures_util::stream::iter(chunks), sample_rate)
    }

    /// Sample rate in Hz.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }
}

impl From<AudioStream> for PcmInput {
    fn from(audio: AudioStream) -> Self {
        let rate = audio.sample_rate();
        let chunks: Vec<Vec<u8>> = audio.chunks().map(<[u8]>::to_vec).collect();
        Self::from_chunks(chunks, rate)
    }
}

fn stream_config(options: &EnhanceOptions, sample_rate: u32) -> Value {
    let mut config = json!({
        "type": "config",
        "model": options.model,
        "task": options.task(),
        "sample_rate_hz": sample_rate,
        "encoding": "pcm_s16le",
    });
    if let Some(speaker) = &options.speaker {
        config["speaker_wav_b64"] = Value::String(BASE64.encode(&speaker.data));
    }
    config
}

fn stream_url(inner: &ClientInner, session: bool) -> Result<reqwest::Url> {
    let mut query = vec![("api_key", inner.api_key.as_str())];
    if session {
        query.push(("session", "1"));
    }
    ws_url(&inner.api_url, ENHANCE_STREAM_PATH, &query)
}

/// The `ready` frame; an `error` frame or anything else is an error.
fn check_ready(frame: Value) -> Result<Value> {
    match frame.get("type").and_then(Value::as_str) {
        Some("error") => Err(classify_ws_frame(&frame)),
        Some("ready") => Ok(frame),
        _ => Err(Error::connection(
            "Unexpected first message from the enhancement stream.",
        )),
    }
}

/// Why a config was not accepted.
enum BeginError {
    /// A reused session socket turned out closed before it took the config.
    Stale,
    Failed(Error),
}

/// Send `config` and wait for `ready`.
async fn begin(
    ws: &mut WsConn,
    config: &Value,
    timeout: Duration,
    reused: bool,
) -> std::result::Result<Value, BeginError> {
    if let Err(info) = ws.send_json(config) {
        return Err(if reused {
            BeginError::Stale
        } else {
            BeginError::Failed(info.error())
        });
    }
    let frame = match ws.recv_json(timeout).await {
        Ok(frame) => frame,
        Err(RecvError::Timeout) => return Err(BeginError::Failed(Error::connection(NOT_READY))),
        Err(RecvError::Closed(info)) => {
            return Err(if reused {
                BeginError::Stale
            } else {
                BeginError::Failed(info.error())
            })
        }
    };
    if reused
        && frame.get("type").and_then(Value::as_str) == Some("error")
        && frame.get("code").and_then(Value::as_u64) == Some(IDLE_FRAME_CODE)
    {
        return Err(BeginError::Stale);
    }
    check_ready(frame).map_err(BeginError::Failed)
}

/// After `ready`: send the audio and `end` from a task and yield the enhanced
/// chunks until `done`. The socket stays open; `finished` is set on `done`.
fn exchange<'a>(
    ws: &'a mut WsConn,
    mut input: BoxStream<'static, Vec<u8>>,
    sample_rate: u32,
    finished: &'a mut bool,
) -> impl Stream<Item = Result<Vec<u8>>> + Send + 'a {
    async_stream::stream! {
        let sender = ws.sender();
        let max_bytes = (sample_rate as usize * 2).max(2);
        let pump = tokio::spawn(async move {
            while let Some(chunk) = input.next().await {
                for part in chunk.chunks(max_bytes) {
                    if sender.send(Message::binary(part.to_vec())).is_err() {
                        return; // the receive side reports why the socket closed
                    }
                }
            }
            let _ = sender.send(Message::text(json!({"type": "end"}).to_string()));
        });
        let abort = pump.abort_handle();
        // Stops the input pump however this stream ends (done, error, drop).
        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _guard = AbortOnDrop(abort);
        loop {
            match ws.recv(STREAM_RECV_TIMEOUT).await {
                Ok(crate::streaming::ws::Inbound::Binary(data)) => yield Ok(data),
                Ok(crate::streaming::ws::Inbound::Text(text)) => {
                    let frame: Value = match serde_json::from_str(&text) {
                        Ok(frame) => frame,
                        Err(e) => {
                            yield Err(Error::api(format!("Malformed message from the enhancement stream: {e}")));
                            return;
                        }
                    };
                    match frame.get("type").and_then(Value::as_str) {
                        Some("error") => {
                            yield Err(classify_ws_frame(&frame));
                            return;
                        }
                        Some("done") => {
                            *finished = true;
                            return;
                        }
                        _ => {}
                    }
                }
                Err(RecvError::Closed(info)) => {
                    yield Err(info.error());
                    return;
                }
                Err(RecvError::Timeout) => {
                    yield Err(Error::connection("The enhancement stream sent nothing for an hour."));
                    return;
                }
            }
        }
    }
}

/// One stream on a connection of its own.
fn one_shot_stream(
    inner: Arc<ClientInner>,
    config: Value,
    input: PcmInput,
) -> impl Stream<Item = Result<Vec<u8>>> + Send + 'static {
    async_stream::stream! {
        let url = match stream_url(&inner, false) {
            Ok(url) => url,
            Err(e) => {
                yield Err(e);
                return;
            }
        };
        let mut ws = match WsConn::connect(&url, inner.timeout, Some(STREAM_PING_INTERVAL)).await {
            Ok(ws) => ws,
            Err(e) => {
                yield Err(e);
                return;
            }
        };
        match begin(&mut ws, &config, inner.timeout, false).await {
            Ok(_) => {}
            Err(BeginError::Failed(e)) => {
                yield Err(e);
                ws.close().await;
                return;
            }
            Err(BeginError::Stale) => {
                yield Err(Error::connection(NOT_READY));
                return;
            }
        }
        let mut finished = false;
        {
            let mut chunks = Box::pin(exchange(&mut ws, input.chunks, input.sample_rate, &mut finished));
            while let Some(item) = chunks.next().await {
                yield item;
            }
        }
        ws.close().await;
    }
}

/// Speech enhancement, from [`KugelAudio::enhance`].
#[derive(Debug, Clone, Copy)]
pub struct EnhanceResource<'a> {
    client: &'a KugelAudio,
}

impl<'a> EnhanceResource<'a> {
    pub(crate) fn new(client: &'a KugelAudio) -> Self {
        Self { client }
    }

    /// Enhance a recording (`POST /v1/audio/enhance`): remove noise, or keep
    /// only the speaker in `options.speaker`.
    ///
    /// ```no_run
    /// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
    /// use kugelaudio::{load_audio, EnhanceOptions};
    ///
    /// let options = EnhanceOptions::new("clarity-1").speaker(load_audio("speaker.wav")?);
    /// let clean = client.enhance().generate(&load_audio("meeting.wav")?, &options).await?;
    /// clean.save("one-voice.wav")?;
    /// # Ok(()) }
    /// ```
    pub async fn generate(&self, audio: &Audio, options: &EnhanceOptions) -> Result<EnhancedAudio> {
        options.check_model()?;
        let inner = &self.client.inner;
        let part = |a: &Audio| {
            Part::bytes(a.data.clone())
                .file_name(a.filename.clone())
                .mime_str("audio/wav")
                .map_err(|e| Error::validation(e.to_string()))
        };
        let mut form = Form::new()
            .part("file", part(audio)?)
            .text("model", options.model.clone())
            .text("task", options.task());
        if let Some(speaker) = &options.speaker {
            form = form.part("speaker", part(speaker)?);
        }
        let request = inner
            .http
            .request(Method::POST, inner.url(ENHANCE_PATH))
            .multipart(form);
        let response = inner.send(&Method::POST, ENHANCE_PATH, request).await?;
        let body = response.bytes().await.map_err(|e| {
            Error::connection(format!("Reading the enhanced audio failed: {e}.")).with_source(e)
        })?;
        EnhancedAudio::from_wav(&body)
    }

    /// Ready enhancement before the first request: the server starts scaling
    /// up (not billed, no rate limit) and the HTTP connection opens. Never
    /// fails: a network error or a refused warm-up is logged at WARN and the
    /// first real request then reports its own error.
    pub async fn prewarm(&self) {
        let inner = &self.client.inner;
        let url = inner.url(ENHANCE_WARMUP_PATH);
        match inner.http.post(&url).send().await {
            // KEEP-JUSTIFIED: warming is an optimisation; the first real
            // request connects on its own and raises its own typed error.
            Err(e) => tracing::warn!(
                error = %e,
                "could not prewarm the enhancement connection; the first request will connect instead"
            ),
            Ok(response) if !response.status().is_success() => tracing::warn!(
                status = response.status().as_u16(),
                "the enhancement warm-up was refused; the first request may wait for a cold start"
            ),
            Ok(_) => {}
        }
    }

    /// Enhance audio in real time (`/v1/audio/enhance/stream`) and yield the
    /// enhanced mono PCM16 (24 kHz) chunks as they arrive. Input is sent from
    /// a background task while you read; the stream ends once the last input
    /// was enhanced. Dropping it closes the connection.
    ///
    /// ```no_run
    /// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
    /// use futures_util::StreamExt;
    /// use kugelaudio::{load_audio_stream, EnhanceOptions};
    ///
    /// let input = load_audio_stream("meeting.wav", 0.1)?;
    /// let mut out = client.enhance().stream(input, &EnhanceOptions::new("clarity-1"));
    /// while let Some(chunk) = out.next().await {
    ///     let pcm = chunk?;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn stream(
        &self,
        input: impl Into<PcmInput>,
        options: &EnhanceOptions,
    ) -> BoxStream<'static, Result<Vec<u8>>> {
        if let Err(e) = options.check_model() {
            return Box::pin(futures_util::stream::once(async move { Err(e) }));
        }
        let input = input.into();
        let config = stream_config(options, input.sample_rate);
        Box::pin(one_shot_stream(self.client.inner.clone(), config, input))
    }

    /// A session: one warm WebSocket that carries many streams, one after
    /// another. Opening a connection costs TCP, TLS and WebSocket handshakes
    /// plus admission; a session pays that once, ahead of the first audio.
    pub fn session(&self) -> EnhanceSession {
        EnhanceSession {
            inner: self.client.inner.clone(),
            ws: None,
            opened_at: Instant::now(),
            idle_since: Instant::now(),
            one_shot: false,
            closed: false,
        }
    }
}

/// One warm WebSocket for many enhancement streams, from
/// [`EnhanceResource::session`].
///
/// Each [`stream`](Self::stream) sends its own config (task, speaker and rate
/// may change) and is billed as its own request. A socket the server closed
/// (60 s idle, one-hour lifetime, restart) is replaced without an error; a
/// server without session support gets one connection per stream.
pub struct EnhanceSession {
    inner: Arc<ClientInner>,
    ws: Option<WsConn>,
    opened_at: Instant,
    idle_since: Instant,
    one_shot: bool,
    closed: bool,
}

impl std::fmt::Debug for EnhanceSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnhanceSession")
            .field("connected", &self.ws.as_ref().is_some_and(WsConn::is_open))
            .field("one_shot", &self.one_shot)
            .field("closed", &self.closed)
            .finish()
    }
}

impl EnhanceSession {
    /// Open the session socket now so the next stream skips the setup. Does
    /// nothing while an open socket is reusable.
    pub async fn connect(&mut self) -> Result<()> {
        self.socket().await.map(|_| ())
    }

    fn reusable(&self) -> bool {
        let now = Instant::now();
        self.ws.as_ref().is_some_and(WsConn::is_open)
            && now.duration_since(self.idle_since) < SESSION_IDLE
            && now.duration_since(self.opened_at) < SESSION_LIFETIME
    }

    async fn drop_socket(&mut self) {
        if let Some(ws) = self.ws.take() {
            ws.close().await;
        }
    }

    /// The session socket and whether it was already open; `None` when this
    /// stream must use a connection of its own.
    async fn socket(&mut self) -> Result<Option<bool>> {
        if self.closed {
            return Err(Error::api("The enhancement session is closed."));
        }
        if self.one_shot {
            return Ok(None);
        }
        if self.reusable() {
            return Ok(Some(true));
        }
        self.drop_socket().await;
        let url = stream_url(&self.inner, true)?;
        match WsConn::connect(&url, self.inner.timeout, Some(STREAM_PING_INTERVAL)).await {
            Ok(ws) => {
                self.ws = Some(ws);
                self.opened_at = Instant::now();
                self.idle_since = self.opened_at;
                Ok(Some(false))
            }
            Err(Error::RateLimit(e)) if e.retry_after.is_some() => Err(Error::RateLimit(e)),
            // The open-session limit (429 without Retry-After) or a server
            // that does not know sessions (400): stream on a connection of its own.
            Err(e @ (Error::RateLimit(_) | Error::Validation(_))) => {
                if matches!(e, Error::Validation(_)) {
                    self.one_shot = true;
                }
                tracing::info!(error = %e, "the enhancement session was refused; streaming on a connection of its own");
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Enhance one audio on the session socket; same input and output as
    /// [`EnhanceResource::stream`]. Dropping the stream early ends this audio
    /// and closes the socket (the next stream opens a new one).
    pub fn stream(
        &mut self,
        input: impl Into<PcmInput>,
        options: &EnhanceOptions,
    ) -> BoxStream<'_, Result<Vec<u8>>> {
        let input = input.into();
        let checked = options.check_model();
        let config = stream_config(options, input.sample_rate);
        Box::pin(async_stream::stream! {
            if let Err(e) = checked {
                yield Err(e);
                return;
            }
            let reused = match self.socket().await {
                Ok(Some(reused)) => reused,
                Ok(None) => {
                    let mut chunks = Box::pin(one_shot_stream(self.inner.clone(), config, input));
                    while let Some(item) = chunks.next().await {
                        yield item;
                    }
                    return;
                }
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            let timeout = self.inner.timeout;
            // Taken out while in use: a stream dropped mid-audio drops (and
            // closes) the socket instead of leaving its frames to the next one.
            let Some(mut ws) = self.ws.take() else { return };
            let ready = match begin(&mut ws, &config, timeout, reused).await {
                Ok(ready) => ready,
                Err(BeginError::Stale) => {
                    drop(ws);
                    match self.socket().await {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            yield Err(Error::connection("The enhancement session could not be reopened."));
                            return;
                        }
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                    let Some(fresh) = self.ws.take() else { return };
                    ws = fresh;
                    match begin(&mut ws, &config, timeout, false).await {
                        Ok(ready) => ready,
                        Err(BeginError::Failed(e)) => {
                            yield Err(e);
                            return;
                        }
                        Err(BeginError::Stale) => {
                            yield Err(Error::connection(NOT_READY));
                            return;
                        }
                    }
                }
                Err(BeginError::Failed(e)) => {
                    // A rate-limit refusal of the config leaves the socket open
                    // and reusable; any other refusal leaves its state unknown.
                    if matches!(e, Error::RateLimit(_)) && ws.is_open() {
                        self.ws = Some(ws);
                    }
                    yield Err(e);
                    return;
                }
            };
            if ready.get("request_id").is_none() {
                // The server ignored session=1: it closes after this audio.
                tracing::info!("the enhancement server does not keep sessions open; using one connection per stream");
                self.one_shot = true;
            }
            let mut finished = false;
            {
                let mut chunks = Box::pin(exchange(&mut ws, input.chunks, input.sample_rate, &mut finished));
                while let Some(item) = chunks.next().await {
                    yield item;
                }
            }
            if finished && !self.one_shot {
                self.idle_since = Instant::now();
                self.ws = Some(ws);
            } else {
                ws.close().await;
            }
        })
    }

    /// Close the socket; the session cannot stream afterwards.
    pub async fn close(&mut self) {
        self.closed = true;
        self.drop_socket().await;
    }
}

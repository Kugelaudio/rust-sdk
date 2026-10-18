//! `/ws/tts/stream`: text in token by token, audio out, one turn at a time.

use std::sync::Arc;

use futures_core::stream::BoxStream;
use serde_json::{json, Map, Value};

use super::ws::{ws_url, CloseInfo, RecvError, WsConn};
use super::{
    is_error_close, is_restart_close, restart_delay, truthy, word_timestamps, DEFAULT_RECV_TIMEOUT,
    INTER_CHUNK_IDLE, POLL_TIMEOUT, SESSION_PING_INTERVAL,
};
use crate::client::ClientInner;
use crate::diagnostics::{
    Operation, OP_STREAM_SESSION, STAGE_FINALIZING, STAGE_HANDSHAKE, STAGE_SENDING_REQUEST,
    TRANSPORT_WEBSOCKET,
};
use crate::error::{classify_ws_frame, Error, Result};
use crate::request::{warn_if_no_language, GenerationSettings, SettingsUpdate, StreamConfig};
use crate::response::{AudioChunk, FinalStats, SessionUsage, WordTimestamp};

type WordCallback = Box<dyn FnMut(&[WordTimestamp]) + Send>;

/// Receive state of one `send` / `flush` exchange.
struct IdleState {
    wait_for_first: bool,
    generation_active: bool,
    last_chunk_complete: bool,
}

/// A WebSocket session for streaming text in (e.g. LLM tokens) and audio out
/// (`/ws/tts/stream`), from [`TtsResource::streaming_session`](crate::TtsResource::streaming_session).
///
/// The server buffers text and synthesizes at sentence boundaries. End every
/// turn with [`flush`](Self::flush) (or `send(.., true)`); the socket stays
/// open for the next turn. A rolling-deploy close (1012/1013) is absorbed
/// while the turn is still silent: the session reconnects and replays it.
///
/// ```no_run
/// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
/// use futures_util::StreamExt;
/// use kugelaudio::StreamConfig;
///
/// let mut session = client.tts().streaming_session(StreamConfig::new(1071).language("en"));
/// session.connect().await?;
/// for token in ["Hello, ", "this is ", "streamed ", "speech."] {
///     let mut audio = session.send(token, false);
///     while let Some(chunk) = audio.next().await {
///         let chunk = chunk?; // play chunk.audio
///     }
/// }
/// let mut tail = session.flush();
/// while let Some(chunk) = tail.next().await {
///     let chunk = chunk?;
/// }
/// drop(tail);
/// session.close().await?;
/// # Ok(()) }
/// ```
pub struct StreamingSession {
    inner: Arc<ClientInner>,
    config: StreamConfig,
    ws: Option<WsConn>,
    config_sent: bool,
    session_ended: bool,
    session_stats: Option<FinalStats>,
    last_word_timestamps: Vec<WordTimestamp>,
    last_usage: Option<SessionUsage>,
    last_final: Option<FinalStats>,
    on_word_timestamps: Option<WordCallback>,
    turn_frames: Vec<Map<String, Value>>,
    turn_audio: bool,
    turn_replayed: bool,
    turn_op: Option<Operation>,
}

impl std::fmt::Debug for StreamingSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingSession")
            .field("config", &self.config)
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

impl StreamingSession {
    pub(crate) fn new(inner: Arc<ClientInner>, config: StreamConfig) -> Self {
        Self {
            inner,
            config,
            ws: None,
            config_sent: false,
            session_ended: false,
            session_stats: None,
            last_word_timestamps: Vec::new(),
            last_usage: None,
            last_final: None,
            on_word_timestamps: None,
            turn_frames: Vec::new(),
            turn_audio: false,
            turn_replayed: false,
            turn_op: None,
        }
    }

    /// Call `callback` every time the server sends word timestamps.
    pub fn on_word_timestamps(&mut self, callback: impl FnMut(&[WordTimestamp]) + Send + 'static) {
        self.on_word_timestamps = Some(Box::new(callback));
    }

    /// The session configuration (sent with the first text of each turn).
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    /// True while the socket is open.
    pub fn is_connected(&self) -> bool {
        self.ws.as_ref().is_some_and(WsConn::is_open)
    }

    /// The most recently received word timestamps.
    pub fn last_word_timestamps(&self) -> &[WordTimestamp] {
        &self.last_word_timestamps
    }

    /// Usage of the most recently closed turn (`session_closed`). Use it to
    /// bill your own customers per conversation.
    pub fn last_usage(&self) -> Option<&SessionUsage> {
        self.last_usage.as_ref()
    }

    /// End-of-audio stats of the most recently completed turn (`final`);
    /// not updated by a barge-in cancel.
    pub fn last_final(&self) -> Option<&FinalStats> {
        self.last_final.as_ref()
    }

    /// Open the socket now, so the first [`send`](Self::send) skips the handshake.
    pub async fn connect(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        let mut op = self
            .inner
            .diagnostics
            .operation(Some(OP_STREAM_SESSION), TRANSPORT_WEBSOCKET);
        op.set_stage(STAGE_HANDSHAKE);
        match self.connect_socket().await {
            Ok(()) => {
                op.succeed();
                Ok(())
            }
            Err(e) => {
                op.fail(&e);
                Err(e)
            }
        }
    }

    async fn connect_socket(&mut self) -> Result<()> {
        let url = ws_url(
            &self.inner.tts_url,
            "/ws/tts/stream",
            &[("api_key", self.inner.api_key.as_str())],
        )?;
        let conn = WsConn::connect(&url, self.inner.timeout, Some(SESSION_PING_INTERVAL)).await?;
        self.ws = Some(conn);
        self.config_sent = false;
        Ok(())
    }

    /// The live turn, minting one when the last turn settled.
    fn turn(&mut self) -> &mut Operation {
        if self.turn_op.as_ref().is_some_and(Operation::settled) {
            self.turn_op = None;
        }
        let diagnostics = &self.inner.diagnostics;
        self.turn_op.get_or_insert_with(|| {
            diagnostics.operation_at(
                OP_STREAM_SESSION,
                TRANSPORT_WEBSOCKET,
                STAGE_SENDING_REQUEST,
            )
        })
    }

    fn live_turn(&mut self) -> Option<&mut Operation> {
        self.turn_op.as_mut().filter(|op| !op.settled())
    }

    fn finish_turn(&mut self) {
        if let Some(mut op) = self.turn_op.take() {
            op.succeed();
        }
    }

    fn cancel_turn(&mut self) {
        if let Some(mut op) = self.turn_op.take() {
            op.cancel();
        }
    }

    fn fail_turn(&mut self, error: &Error) {
        if let Some(op) = self.live_turn() {
            op.fail(error);
        }
    }

    fn reset_turn(&mut self) {
        self.turn_frames.clear();
        self.turn_audio = false;
        self.turn_replayed = false;
    }

    /// Drop a dead socket so the next `send` reconnects.
    fn forget_socket(&mut self) {
        self.ws = None;
        self.config_sent = false;
    }

    fn frame_error(&mut self, frame: &Value) -> Error {
        if let Some(op) = self.live_turn() {
            op.mark_server_error();
        }
        classify_ws_frame(frame)
    }

    /// Send one turn frame, remembering it for a possible replay. The config
    /// rides along with the first frame of a turn.
    async fn send_frame(&mut self, frame: Map<String, Value>) -> Result<()> {
        match self.write_frame(frame) {
            Ok(()) => Ok(()),
            Err(info) if is_restart_close(info.code) => {
                self.recover_from_restart(info).await.map(|_| ())
            }
            Err(info) => {
                self.forget_socket();
                Err(info.error())
            }
        }
    }

    fn write_frame(&mut self, frame: Map<String, Value>) -> std::result::Result<(), CloseInfo> {
        self.turn_frames.push(frame.clone());
        let mut message = frame;
        if !self.config_sent {
            warn_if_no_language(self.config.language.as_deref(), self.config.normalize);
            message.extend(self.config.to_json());
            self.config_sent = true;
        }
        match &self.ws {
            Some(ws) => ws.send_json(&Value::Object(message)),
            None => Err(CloseInfo {
                code: None,
                reason: "not connected".to_string(),
            }),
        }
    }

    /// Absorb a rolling-deploy close. While no audio of the turn was
    /// delivered, wait the retry hint, reconnect and resend the turn's frames
    /// once. Returns `true` when frames were replayed. Fails with
    /// [`Error::ServerRestarting`] when audio already went out or the turn was
    /// replayed once already.
    async fn recover_from_restart(&mut self, info: CloseInfo) -> Result<bool> {
        let err = info.error();
        self.forget_socket();
        if self.turn_audio || self.turn_replayed {
            self.reset_turn();
            return Err(err);
        }
        let frames = std::mem::take(&mut self.turn_frames);
        if frames.is_empty() {
            return Ok(false);
        }
        let delay = restart_delay(&err);
        tracing::info!(
            code = ?info.code,
            delay_s = delay.as_secs_f64(),
            frames = frames.len(),
            "server restarting; reconnecting and replaying the turn"
        );
        if let Some(op) = self.live_turn() {
            op.record_retry();
            op.set_stage(STAGE_HANDSHAKE);
        }
        tokio::time::sleep(delay).await;
        self.connect_socket().await?;
        self.turn_replayed = true;
        for frame in frames {
            if let Err(info) = self.write_frame(frame) {
                self.forget_socket();
                return Err(info.error());
            }
        }
        if let Some(op) = self.live_turn() {
            op.expect_audio();
        }
        Ok(true)
    }

    fn note_word_timestamps(&mut self, words: Vec<WordTimestamp>) {
        if let Some(callback) = self.on_word_timestamps.as_mut() {
            callback(&words);
        }
        self.last_word_timestamps = words;
    }

    fn capture_session_stats(&mut self, frame: &Value) {
        self.finish_turn();
        let stats = FinalStats::from_frame(frame);
        if let Some(usage) = stats.usage.clone() {
            self.last_usage = Some(usage);
        }
        self.session_stats = Some(stats);
    }

    /// The next audio chunk of the current exchange, or `None` once the
    /// server went idle or closed the turn.
    async fn next_chunk(&mut self, state: &mut IdleState) -> Result<Option<AudioChunk>> {
        loop {
            let timeout = if state.last_chunk_complete {
                INTER_CHUNK_IDLE
            } else if state.generation_active || state.wait_for_first {
                DEFAULT_RECV_TIMEOUT
            } else {
                POLL_TIMEOUT
            };
            let Some(ws) = self.ws.as_mut() else {
                return Ok(None);
            };
            let frame = match ws.recv_json(timeout).await {
                Ok(frame) => frame,
                Err(RecvError::Timeout) => {
                    if state.generation_active && !state.last_chunk_complete {
                        tracing::warn!(
                            "timed out waiting for chunk_complete after {}s (generation was active)",
                            DEFAULT_RECV_TIMEOUT.as_secs()
                        );
                    }
                    return Ok(None);
                }
                Err(RecvError::Closed(info)) => {
                    if is_restart_close(info.code) {
                        if !self.recover_from_restart(info).await? {
                            return Ok(None);
                        }
                        state.generation_active = false;
                        state.last_chunk_complete = false;
                        continue;
                    }
                    self.forget_socket();
                    if is_error_close(info.code) {
                        return Err(info.error());
                    }
                    return Ok(None);
                }
            };
            if truthy(&frame, "error") {
                return Err(self.frame_error(&frame));
            }
            if truthy(&frame, "generation_started") {
                state.generation_active = true;
                state.last_chunk_complete = false;
                continue;
            }
            let chunk = if truthy(&frame, "audio") {
                state.generation_active = true;
                state.last_chunk_complete = false;
                self.turn_audio = true;
                Some(AudioChunk::from_frame(&frame)?)
            } else {
                None
            };
            if let Some(words) = word_timestamps(&frame)? {
                self.note_word_timestamps(words);
            }
            if chunk.is_some() {
                return Ok(chunk);
            }
            if let Some(warning) = frame.get("warning").and_then(Value::as_str) {
                tracing::warn!(warning, "KugelAudio streaming warning");
            }
            if truthy(&frame, "final") {
                // End of audio for the turn; `session_closed` follows with usage.
                self.last_final = Some(FinalStats::from_frame(&frame));
                self.finish_turn();
                continue;
            }
            if truthy(&frame, "session_closed") {
                self.capture_session_stats(&frame);
                self.reset_turn();
                return Ok(None);
            }
            if truthy(&frame, "chunk_complete") {
                state.last_chunk_complete = true;
            }
        }
    }

    /// Run one text exchange: send `frame`, then yield the audio until idle.
    fn exchange(
        &mut self,
        frame: Map<String, Value>,
        wait_for_first: bool,
    ) -> BoxStream<'_, Result<AudioChunk>> {
        Box::pin(async_stream::stream! {
            self.turn().set_stage(STAGE_SENDING_REQUEST);
            if let Err(e) = self.send_frame(frame).await {
                self.fail_turn(&e);
                yield Err(e);
                return;
            }
            self.turn().expect_audio();
            let mut state = IdleState {
                wait_for_first,
                generation_active: false,
                last_chunk_complete: false,
            };
            loop {
                match self.next_chunk(&mut state).await {
                    Ok(Some(chunk)) => {
                        if let Some(op) = self.live_turn() {
                            op.record_chunk(chunk.audio.len());
                        }
                        yield Ok(chunk);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        self.fail_turn(&e);
                        yield Err(e);
                        break;
                    }
                }
            }
        })
    }

    /// Send text and yield the audio generated so far.
    ///
    /// With `flush` false the server keeps buffering until a sentence ends:
    /// the stream may end without audio after a 50 ms poll, so keep sending.
    /// With `flush` true the turn ends and the stream yields all its audio.
    pub fn send(&mut self, text: &str, flush: bool) -> BoxStream<'_, Result<AudioChunk>> {
        let text = text.to_string();
        Box::pin(async_stream::stream! {
            // A socket that died while idle between turns (proxy timeout,
            // network blip) is replaced before a new turn starts. A rolling
            // deploy close is left to the replay path, which resends the turn.
            let idle_drop = self.ws.as_ref().and_then(WsConn::close_info).is_some_and(|info| {
                self.turn_frames.is_empty() && !is_restart_close(info.code)
            });
            if idle_drop {
                self.forget_socket();
            }
            if self.ws.is_none() {
                if let Err(e) = self.connect().await {
                    yield Err(e);
                    return;
                }
            }
            let mut frame = Map::new();
            frame.insert("text".into(), Value::String(text));
            frame.insert("flush".into(), Value::Bool(flush));
            let mut audio = self.exchange(frame, flush);
            while let Some(item) = futures_util::StreamExt::next(&mut audio).await {
                yield item;
            }
        })
    }

    /// End the turn: synthesize the buffered text and yield its audio.
    pub fn flush(&mut self) -> BoxStream<'_, Result<AudioChunk>> {
        if self.ws.is_none() {
            return Box::pin(futures_util::stream::empty());
        }
        let mut frame = Map::new();
        frame.insert("flush".into(), Value::Bool(true));
        self.exchange(frame, true)
    }

    /// Signal end of input (`close`) and yield the remaining audio until the
    /// server sends `session_closed`. The socket stays open; the next
    /// [`send`](Self::send) starts a new turn. Use it before
    /// [`close`](Self::close) when you need the tail audio.
    pub fn drain(&mut self) -> BoxStream<'_, Result<AudioChunk>> {
        Box::pin(async_stream::stream! {
            loop {
                if self.ws.is_none() || self.session_ended {
                    return;
                }
                if let Some(op) = self.live_turn() {
                    op.set_stage(STAGE_FINALIZING);
                }
                self.session_ended = true;
                let mut replay = false;
                let sent = match &self.ws {
                    Some(ws) => ws.send_json(&json!({"close": true})),
                    None => return,
                };
                let mut closed: Option<CloseInfo> = sent.err();
                while closed.is_none() {
                    let Some(ws) = self.ws.as_mut() else { return };
                    match ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                        Ok(frame) => {
                            if truthy(&frame, "error") {
                                let e = self.frame_error(&frame);
                                self.fail_turn(&e);
                                yield Err(e);
                                return;
                            }
                            if truthy(&frame, "audio") {
                                self.turn_audio = true;
                                match AudioChunk::from_frame(&frame) {
                                    Ok(chunk) => {
                                        if let Some(op) = self.live_turn() {
                                            op.record_chunk(chunk.audio.len());
                                        }
                                        yield Ok(chunk);
                                    }
                                    Err(e) => {
                                        self.fail_turn(&e);
                                        yield Err(e);
                                        return;
                                    }
                                }
                            }
                            match word_timestamps(&frame) {
                                Ok(Some(words)) => self.note_word_timestamps(words),
                                Ok(None) => {}
                                Err(e) => {
                                    yield Err(e);
                                    return;
                                }
                            }
                            if truthy(&frame, "final") {
                                self.last_final = Some(FinalStats::from_frame(&frame));
                            }
                            if truthy(&frame, "session_closed") {
                                self.capture_session_stats(&frame);
                                self.reset_turn();
                                return;
                            }
                        }
                        Err(RecvError::Timeout) => {
                            tracing::warn!("no session_closed within {}s of close; dropping the socket", DEFAULT_RECV_TIMEOUT.as_secs());
                            self.forget_socket();
                            return;
                        }
                        Err(RecvError::Closed(info)) => closed = Some(info),
                    }
                }
                if let Some(info) = closed {
                    self.forget_socket();
                    if is_restart_close(info.code) {
                        // The close handshake never completed: replay the
                        // unspoken turn on a fresh socket and drain again.
                        self.session_ended = false;
                        match self.recover_from_restart(info).await {
                            Ok(replayed) => replay = replayed,
                            Err(e) => {
                                self.fail_turn(&e);
                                yield Err(e);
                                return;
                            }
                        }
                    } else if is_error_close(info.code) {
                        let e = info.error();
                        self.fail_turn(&e);
                        yield Err(e);
                        return;
                    }
                }
                if !replay {
                    return;
                }
            }
        })
    }

    /// End the current turn but keep the socket open. Audio still in flight is
    /// discarded (use [`drain`](Self::drain) to receive it). Returns the
    /// turn's `session_closed` stats, or those [`drain`](Self::drain) captured.
    pub async fn end_session(&mut self) -> Result<Option<FinalStats>> {
        if self.session_ended {
            self.session_ended = false;
            self.config_sent = false;
            self.last_word_timestamps.clear();
            return Ok(self.session_stats.take());
        }
        let mut stats = None;
        let sent = match &self.ws {
            Some(ws) => ws.send_json(&json!({"close": true})),
            None => return Ok(None),
        };
        if sent.is_err() {
            self.forget_socket();
        }
        while let Some(ws) = self.ws.as_mut() {
            match ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                Ok(frame) => {
                    if truthy(&frame, "error") {
                        let e = self.frame_error(&frame);
                        self.fail_turn(&e);
                        return Err(e);
                    }
                    if truthy(&frame, "session_closed") {
                        self.capture_session_stats(&frame);
                        stats = self.session_stats.take();
                        break;
                    }
                }
                // A dead socket (or a rolling-deploy close, whose in-flight
                // audio end_session discards anyway): reconnect on next send.
                Err(_) => self.forget_socket(),
            }
        }
        self.config_sent = false;
        self.last_word_timestamps.clear();
        self.reset_turn();
        Ok(stats)
    }

    /// Barge-in: stop the current turn now and drop its buffered text. The
    /// socket stays open; the next [`send`](Self::send) starts a fresh turn.
    /// Returns once the server acknowledged with `interrupted` (late audio of
    /// the cancelled turn is discarded) or after 30 s of silence.
    pub async fn cancel(&mut self) -> Result<()> {
        if self.ws.is_none() {
            return Ok(());
        }
        self.cancel_turn();
        self.session_ended = false;
        self.session_stats = None;
        self.reset_turn();
        let sent = self
            .ws
            .as_ref()
            .map(|ws| ws.send_json(&json!({"cancel": true})));
        if matches!(sent, Some(Err(_))) {
            self.forget_socket();
        }
        while let Some(ws) = self.ws.as_mut() {
            match ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                Ok(frame) => {
                    if truthy(&frame, "error") {
                        return Err(classify_ws_frame(&frame));
                    }
                    if truthy(&frame, "interrupted") {
                        break;
                    }
                }
                // The server stayed silent: treat the barge-in as acknowledged.
                Err(RecvError::Timeout) => break,
                Err(RecvError::Closed(_)) => self.forget_socket(),
            }
        }
        self.config_sent = false;
        self.last_word_timestamps.clear();
        Ok(())
    }

    /// Replace the configuration for the next turn (e.g. another voice). Call
    /// it between turns; the socket is kept.
    pub fn update_config(&mut self, config: StreamConfig) {
        self.config = config;
        self.config_sent = false;
        self.reset_turn();
    }

    /// Change generation parameters on the open connection (`update_settings`)
    /// and return the parameters now in effect. Applies from the next turn.
    pub async fn update_settings(&mut self, update: &SettingsUpdate) -> Result<GenerationSettings> {
        let body = update.to_body()?;
        if !self.is_connected() {
            self.connect().await?;
        }
        let ws = self
            .ws
            .as_mut()
            .ok_or_else(|| Error::connection("KugelAudio WebSocket is not connected."))?;
        if let Err(info) = ws.send_json(&json!({"update_settings": body})) {
            self.forget_socket();
            return Err(info.error());
        }
        let settings = loop {
            let Some(ws) = self.ws.as_mut() else {
                return Err(Error::connection("KugelAudio WebSocket is not connected."));
            };
            match ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                Ok(frame) => {
                    if truthy(&frame, "error") {
                        return Err(classify_ws_frame(&frame));
                    }
                    if truthy(&frame, "settings_updated") {
                        break frame.get("settings").cloned().unwrap_or(Value::Null);
                    }
                }
                Err(RecvError::Timeout) => {
                    return Err(Error::api(
                        "Timed out waiting for settings_updated acknowledgement",
                    ))
                }
                Err(RecvError::Closed(info)) => {
                    self.forget_socket();
                    return Err(info.error());
                }
            }
        };
        // Mirror the accepted change locally so later config re-sends agree.
        if let Some(v) = update.cfg_scale {
            self.config.cfg_scale = v;
        }
        if let Some(v) = update.temperature {
            self.config.temperature = Some(v);
        }
        if let Some(v) = update.speed {
            self.config.speed = v;
        }
        if let Some(v) = update.max_new_tokens {
            self.config.max_new_tokens = v;
        }
        if let Some(v) = &update.language {
            self.config.language = Some(v.clone());
        }
        if let Some(v) = update.normalize {
            self.config.normalize = v;
        }
        if settings.is_null() {
            return Ok(GenerationSettings::default());
        }
        serde_json::from_value(settings).map_err(|e| {
            Error::api(format!(
                "Malformed settings_updated frame from KugelAudio: {e}"
            ))
        })
    }

    /// End the turn and close the socket. Audio still in flight is discarded;
    /// call [`drain`](Self::drain) first if you need it. Returns the last
    /// turn's stats.
    pub async fn close(&mut self) -> Result<Option<FinalStats>> {
        if let Some(op) = self.live_turn() {
            op.set_stage(STAGE_FINALIZING);
        }
        let stats = match self.end_session().await {
            Ok(stats) => stats,
            Err(e) => {
                self.fail_turn(&e);
                return Err(e);
            }
        };
        if let Some(ws) = self.ws.take() {
            ws.close().await;
        }
        self.config_sent = false;
        // A turn the server never closed was abandoned by this close.
        self.cancel_turn();
        Ok(stats)
    }
}

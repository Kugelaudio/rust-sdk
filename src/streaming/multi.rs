//! `/ws/tts/multi`: up to 20 independent contexts over one socket.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_core::stream::BoxStream;
use serde_json::{json, Map, Value};

use super::ws::{ws_url, CloseInfo, RecvError, WsConn};
use super::{
    is_error_close, is_restart_close, restart_delay, truthy, word_timestamps, DEFAULT_RECV_TIMEOUT,
    INTER_CHUNK_IDLE, POLL_TIMEOUT, SESSION_PING_INTERVAL,
};
use crate::client::ClientInner;
use crate::diagnostics::{
    Operation, OP_MULTI_CONTEXT, STAGE_FINALIZING, STAGE_HANDSHAKE, STAGE_SENDING_REQUEST,
    TRANSPORT_WEBSOCKET,
};
use crate::error::{classify_ws_frame, Error, Result};
use crate::models::VoiceId;
use crate::request::{
    clamp_cfg_scale, warn_if_no_language, GenerationSettings, MultiContextConfig, SettingsUpdate,
};
use crate::response::{AudioChunk, FinalStats, SessionUsage, WordTimestamp};

type WordCallback = Box<dyn FnMut(&str, &[WordTimestamp]) + Send>;

/// How one receive loop ends.
#[derive(Clone, Copy)]
struct ReceiveMode {
    /// Run until `context_closed` (closing a context).
    wait_for_close: bool,
    /// Wait the full timeout for the first audio (a flushed send).
    wait_for_chunk_complete: bool,
    /// Linger after `chunk_complete` for the next sentence.
    idle_after_chunk_complete: Duration,
}

struct ReceiveState {
    received_any_audio: bool,
    last_chunk_complete: bool,
}

/// A session with up to 20 independent audio contexts over one WebSocket
/// (`/ws/tts/multi`), from [`TtsResource::multi_context_session`](crate::TtsResource::multi_context_session).
///
/// Each context has its own voice, text buffer and generation queue: use it
/// for several speakers, or to pre-buffer one reply while another plays.
/// Contexts close on their own after 20 s without text; see
/// [`keep_alive`](Self::keep_alive).
///
/// ```no_run
/// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
/// use futures_util::StreamExt;
/// use kugelaudio::MultiContextConfig;
///
/// let mut session = client.tts().multi_context_session(MultiContextConfig::default().language("en"));
/// session.connect().await?;
/// session.create_context("narrator", Some(1071.into())).await?;
/// session.create_context("character", Some(1072.into())).await?;
/// let mut audio = session.send("narrator", "The story begins.", true, None);
/// while let Some(chunk) = audio.next().await {
///     let chunk = chunk?;
/// }
/// drop(audio);
/// let mut tail = session.close_context("narrator", false);
/// while let Some(chunk) = tail.next().await {
///     chunk?;
/// }
/// drop(tail);
/// println!("{:?}", session.usage_for("narrator"));
/// session.close().await;
/// # Ok(()) }
/// ```
pub struct MultiContextSession {
    inner: Arc<ClientInner>,
    config: MultiContextConfig,
    ws: Option<WsConn>,
    is_started: bool,
    contexts: HashSet<String>,
    /// Frames of other contexts received while reading for one context,
    /// delivered when that context is read.
    pending: Vec<Value>,
    on_word_timestamps: Option<WordCallback>,
    last_word_timestamps: HashMap<String, Vec<WordTimestamp>>,
    context_usage: HashMap<String, SessionUsage>,
    /// Every context created on this socket with its voice, in creation order.
    context_voices: Vec<(String, Option<VoiceId>)>,
    turn_frames: HashMap<String, Vec<Value>>,
    turn_audio: HashSet<String>,
    turn_replayed: HashSet<String>,
    turn_ops: HashMap<String, Operation>,
}

impl std::fmt::Debug for MultiContextSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiContextSession")
            .field("config", &self.config)
            .field("contexts", &self.contexts)
            .field("alive", &self.is_alive())
            .finish_non_exhaustive()
    }
}

impl MultiContextSession {
    pub(crate) fn new(inner: Arc<ClientInner>, config: MultiContextConfig) -> Self {
        Self {
            inner,
            config,
            ws: None,
            is_started: false,
            contexts: HashSet::new(),
            pending: Vec::new(),
            on_word_timestamps: None,
            last_word_timestamps: HashMap::new(),
            context_usage: HashMap::new(),
            context_voices: Vec::new(),
            turn_frames: HashMap::new(),
            turn_audio: HashSet::new(),
            turn_replayed: HashSet::new(),
            turn_ops: HashMap::new(),
        }
    }

    /// Call `callback(context_id, words)` every time word timestamps arrive.
    pub fn on_word_timestamps(
        &mut self,
        callback: impl FnMut(&str, &[WordTimestamp]) + Send + 'static,
    ) {
        self.on_word_timestamps = Some(Box::new(callback));
    }

    /// True while the socket is usable. Check it before reusing a cached
    /// session; a dropped socket reconnects on the next [`send`](Self::send).
    pub fn is_alive(&self) -> bool {
        self.ws.as_ref().is_some_and(WsConn::is_open)
    }

    /// The contexts currently open.
    pub fn active_contexts(&self) -> HashSet<String> {
        self.contexts.clone()
    }

    /// The most recent word timestamps of `context_id`.
    pub fn word_timestamps(&self, context_id: &str) -> &[WordTimestamp] {
        self.last_word_timestamps
            .get(context_id)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// Usage (audio time and charge) of a closed context, from its
    /// `context_closed` frame; `None` until it closed.
    pub fn usage_for(&self, context_id: &str) -> Option<&SessionUsage> {
        self.context_usage.get(context_id)
    }

    /// Usage of every closed context, by context id.
    pub fn context_usage(&self) -> &HashMap<String, SessionUsage> {
        &self.context_usage
    }

    /// Open the socket now.
    pub async fn connect(&mut self) -> Result<()> {
        if self.is_alive() {
            return Ok(());
        }
        let mut op = self
            .inner
            .diagnostics
            .operation(Some(OP_MULTI_CONTEXT), TRANSPORT_WEBSOCKET);
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
            "/ws/tts/multi",
            &[("api_key", self.inner.api_key.as_str())],
        )?;
        self.ws =
            Some(WsConn::connect(&url, self.inner.timeout, Some(SESSION_PING_INTERVAL)).await?);
        Ok(())
    }

    fn turn(&mut self, context_id: &str) -> &mut Operation {
        if self
            .turn_ops
            .get(context_id)
            .is_some_and(Operation::settled)
        {
            self.turn_ops.remove(context_id);
        }
        let diagnostics = &self.inner.diagnostics;
        self.turn_ops
            .entry(context_id.to_string())
            .or_insert_with(|| {
                diagnostics.operation_at(
                    OP_MULTI_CONTEXT,
                    TRANSPORT_WEBSOCKET,
                    STAGE_SENDING_REQUEST,
                )
            })
    }

    fn live_turn(&mut self, context_id: &str) -> Option<&mut Operation> {
        self.turn_ops.get_mut(context_id).filter(|op| !op.settled())
    }

    fn finish_turn(&mut self, context_id: &str) {
        if let Some(mut op) = self.turn_ops.remove(context_id) {
            op.succeed();
        }
    }

    fn cancel_turn(&mut self, context_id: &str) {
        if let Some(mut op) = self.turn_ops.remove(context_id) {
            op.cancel();
        }
    }

    fn fail_turn(&mut self, context_id: &str, error: &Error) {
        if let Some(op) = self.live_turn(context_id) {
            op.fail(error);
        }
    }

    fn end_turn(&mut self, context_id: &str) {
        self.turn_frames.remove(context_id);
        self.turn_audio.remove(context_id);
        self.turn_replayed.remove(context_id);
    }

    fn forget_context(&mut self, context_id: &str) {
        self.contexts.remove(context_id);
        self.context_voices.retain(|(id, _)| id != context_id);
        self.pending
            .retain(|frame| frame.get("context_id").and_then(Value::as_str) != Some(context_id));
        self.end_turn(context_id);
    }

    /// Track turn boundaries from a frame of any context.
    fn note_turn_event(&mut self, frame: &Value) {
        let Some(context_id) = frame.get("context_id").and_then(Value::as_str) else {
            return;
        };
        let context_id = context_id.to_string();
        if truthy(frame, "audio") {
            self.turn_audio.insert(context_id.clone());
        }
        if truthy(frame, "final") || truthy(frame, "context_closed") {
            self.end_turn(&context_id);
            self.finish_turn(&context_id);
        }
    }

    fn capture_context_usage(&mut self, frame: &Value) {
        let context_id = frame.get("context_id").and_then(Value::as_str);
        if let (Some(id), Some(usage)) = (context_id, SessionUsage::from_payload(frame)) {
            self.context_usage.insert(id.to_string(), usage);
        }
    }

    /// Drop every reference to a dead socket. Idempotent.
    fn reset_dead_session(&mut self) {
        self.ws = None;
        self.is_started = false;
        self.contexts.clear();
        self.pending.clear();
        self.context_voices.clear();
        self.turn_frames.clear();
        self.turn_audio.clear();
        self.turn_replayed.clear();
    }

    fn frame_error(&mut self, frame: &Value, context_id: &str) -> Error {
        if let Some(op) = self.live_turn(context_id) {
            op.mark_server_error();
        }
        classify_ws_frame(frame)
    }

    fn remember_voice(&mut self, context_id: &str, voice_id: Option<VoiceId>) {
        if !self.context_voices.iter().any(|(id, _)| id == context_id) {
            self.context_voices.push((context_id.to_string(), voice_id));
        }
    }

    /// Write a frame. A rolling-deploy close is absorbed when `recover` is
    /// set: the socket is replaced, every context re-created and unspoken
    /// turns replayed; `Ok(true)` then tells the caller that happened.
    async fn ws_send(&mut self, payload: &Value, recover: bool) -> Result<bool> {
        let sent = match &self.ws {
            Some(ws) => ws.send_json(payload),
            None => Err(CloseInfo {
                code: None,
                reason: "not connected".to_string(),
            }),
        };
        let Err(info) = sent else {
            return Ok(false);
        };
        if recover && is_restart_close(info.code) {
            let context_id = payload
                .get("context_id")
                .and_then(Value::as_str)
                .map(str::to_string);
            // Boxed: recovery re-creates contexts, which sends again.
            Box::pin(self.recover_from_restart(context_id.as_deref(), info)).await?;
            return Ok(true);
        }
        self.reset_dead_session();
        if is_error_close(info.code) {
            return Err(info.error());
        }
        Err(Error::connection(format!(
            "KugelAudio WebSocket dropped while sending (close code={}). Retry on a fresh session.",
            info.code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "none".to_string())
        )))
    }

    /// Absorb a rolling-deploy close (1012/1013): reconnect after the retry
    /// hint, re-create every context with its voice, and resend the unspoken
    /// turn of each context that has not received audio yet. A context whose
    /// turn was already partly spoken loses the rest of it (logged). Returns
    /// `true` when `context_id`'s frames were replayed; fails with
    /// [`Error::ServerRestarting`] when its turn already produced audio or was
    /// replayed once.
    async fn recover_from_restart(
        &mut self,
        context_id: Option<&str>,
        info: CloseInfo,
    ) -> Result<bool> {
        let err = info.error();
        if let Some(id) = context_id {
            if self.turn_audio.contains(id) || self.turn_replayed.contains(id) {
                self.reset_dead_session();
                return Err(err);
            }
        }
        let contexts = std::mem::take(&mut self.context_voices);
        let frames = std::mem::take(&mut self.turn_frames);
        let spoken = std::mem::take(&mut self.turn_audio);
        self.reset_dead_session();
        if let Some(op) = context_id.and_then(|id| self.live_turn(id)) {
            op.record_retry();
            op.set_stage(STAGE_HANDSHAKE);
        }
        let delay = restart_delay(&err);
        tracing::info!(
            code = ?info.code,
            delay_s = delay.as_secs_f64(),
            contexts = contexts.len(),
            "server restarting; reconnecting and restoring contexts"
        );
        tokio::time::sleep(delay).await;
        self.connect_socket().await?;
        // Re-create every context before any replay can produce audio.
        for (id, voice) in &contexts {
            self.open_context(id, voice.clone(), false).await?;
        }
        let mut replayed = false;
        for (id, _) in &contexts {
            if spoken.contains(id) {
                tracing::warn!(context_id = %id, "context lost the rest of its in-flight turn to a server restart; resend it");
                continue;
            }
            let Some(turn) = frames.get(id).filter(|t| !t.is_empty()) else {
                continue;
            };
            self.turn_replayed.insert(id.clone());
            if Some(id.as_str()) != context_id {
                if let Some(op) = self.live_turn(id) {
                    op.record_retry();
                }
            }
            for frame in turn {
                self.turn_frames
                    .entry(id.clone())
                    .or_default()
                    .push(frame.clone());
                self.ws_send(frame, false).await?;
            }
            if let Some(op) = self.live_turn(id) {
                op.expect_audio();
            }
            if Some(id.as_str()) == context_id {
                replayed = true;
            }
        }
        if let Some(op) = context_id.and_then(|id| self.live_turn(id)) {
            op.expect_audio();
        }
        Ok(replayed)
    }

    fn voice_settings(&self, voice_id: Option<VoiceId>) -> Option<Value> {
        voice_id
            .or_else(|| self.config.default_voice_id.clone())
            .map(|voice| {
                json!({
                    "voice_id": voice,
                    "cfg_scale": clamp_cfg_scale(self.config.cfg_scale),
                    "max_new_tokens": self.config.max_new_tokens,
                })
            })
    }

    /// Send a context's creation message and wait for `context_created`.
    /// The first context also carries the session configuration.
    async fn open_context(
        &mut self,
        context_id: &str,
        voice_id: Option<VoiceId>,
        recover: bool,
    ) -> Result<()> {
        if self.ws.is_none() {
            self.connect().await?;
        }
        if self.is_started && self.contexts.contains(context_id) {
            return Ok(());
        }
        self.remember_voice(context_id, voice_id.clone());
        let mut message = Map::new();
        message.insert("text".into(), json!(" "));
        message.insert("context_id".into(), json!(context_id));
        let config = &self.config;
        if !self.is_started {
            warn_if_no_language(config.language.as_deref(), config.normalize);
            message.insert("sample_rate".into(), json!(config.sample_rate));
            message.insert("normalize".into(), json!(config.normalize));
            message.insert("word_timestamps".into(), json!(config.word_timestamps));
            if let Some(v) = config.output_format {
                message.insert("output_format".into(), json!(v));
            }
            if let Some(v) = &config.language {
                message.insert("language".into(), json!(v));
            }
            if let Some(v) = config.temperature {
                message.insert("temperature".into(), json!(v));
            }
            if let Some(v) = config.project_id {
                message.insert("project_id".into(), json!(v));
            }
            if let Some(v) = &config.dictionary_ids {
                message.insert("dictionary_ids".into(), json!(v));
            }
        }
        if let Some(v) = &config.model_id {
            message.insert("model_id".into(), json!(v));
        }
        if let Some(settings) = self.voice_settings(voice_id) {
            message.insert("voice_settings".into(), settings);
        }
        if self.ws_send(&Value::Object(message), recover).await? {
            return Ok(()); // the restart recovery re-created every context
        }
        loop {
            let Some(ws) = self.ws.as_mut() else {
                return Err(Error::connection("KugelAudio WebSocket is not connected."));
            };
            match ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                Ok(frame) => {
                    let frame_context = frame
                        .get("context_id")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    if truthy(&frame, "error")
                        && frame_context.as_deref().is_none_or(|c| c == context_id)
                    {
                        return Err(self.frame_error(&frame, context_id));
                    }
                    if truthy(&frame, "context_created")
                        && frame_context.as_deref() == Some(context_id)
                    {
                        self.contexts.insert(context_id.to_string());
                        self.is_started = true;
                        return Ok(());
                    }
                    if frame_context.is_some() {
                        self.note_turn_event(&frame);
                        self.pending.push(frame);
                    }
                }
                Err(RecvError::Timeout) => {
                    return Err(Error::connection(format!(
                        "KugelAudio did not confirm context '{context_id}' within {}s.",
                        DEFAULT_RECV_TIMEOUT.as_secs()
                    )))
                }
                Err(RecvError::Closed(info)) => {
                    self.reset_dead_session();
                    return Err(info.error());
                }
            }
        }
    }

    /// Create a context, with its own voice or the session default.
    /// Creating an existing context does nothing.
    pub async fn create_context(
        &mut self,
        context_id: &str,
        voice_id: Option<VoiceId>,
    ) -> Result<()> {
        self.open_context(context_id, voice_id, true).await
    }

    /// Change the session's generation parameters (`update_settings`); applies
    /// to contexts started afterwards. Returns the parameters now in effect.
    pub async fn update_settings(&mut self, update: &SettingsUpdate) -> Result<GenerationSettings> {
        let body = json!({"update_settings": update.to_body()?});
        if self.ws.is_none() {
            self.connect().await?;
        }
        if self.ws_send(&body, true).await? {
            self.ws_send(&body, false).await?;
        }
        let settings = loop {
            let Some(ws) = self.ws.as_mut() else {
                return Err(Error::connection("KugelAudio WebSocket is not connected."));
            };
            match ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                Ok(frame) => {
                    if truthy(&frame, "error") && frame.get("context_id").is_none() {
                        return Err(classify_ws_frame(&frame));
                    }
                    if truthy(&frame, "settings_updated") {
                        break frame.get("settings").cloned().unwrap_or(Value::Null);
                    }
                    if frame.get("context_id").is_some() {
                        self.note_turn_event(&frame);
                        self.pending.push(frame);
                    }
                }
                Err(RecvError::Timeout) => {
                    return Err(Error::api(
                        "Timed out waiting for settings_updated acknowledgement",
                    ))
                }
                Err(RecvError::Closed(info)) => {
                    self.reset_dead_session();
                    return Err(info.error());
                }
            }
        };
        if let Some(v) = update.cfg_scale {
            self.config.cfg_scale = v;
        }
        if let Some(v) = update.temperature {
            self.config.temperature = Some(v);
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

    /// The next frame for `context_id`: a stashed one first, else the socket.
    async fn next_frame(
        &mut self,
        context_id: &str,
        timeout: Duration,
    ) -> std::result::Result<Value, RecvError> {
        let stashed = self
            .pending
            .iter()
            .position(|f| f.get("context_id").and_then(Value::as_str) == Some(context_id));
        if let Some(index) = stashed {
            return Ok(self.pending.remove(index));
        }
        match self.ws.as_mut() {
            Some(ws) => ws.recv_json(timeout).await,
            None => Err(RecvError::Closed(CloseInfo {
                code: None,
                reason: "not connected".to_string(),
            })),
        }
    }

    /// The next audio chunk of `context_id`, or `None` when the exchange ended.
    async fn next_chunk(
        &mut self,
        context_id: &str,
        mode: ReceiveMode,
        state: &mut ReceiveState,
    ) -> Result<Option<AudioChunk>> {
        loop {
            let timeout = if mode.wait_for_close {
                DEFAULT_RECV_TIMEOUT
            } else if state.last_chunk_complete {
                mode.idle_after_chunk_complete
            } else if mode.wait_for_chunk_complete || !state.received_any_audio {
                DEFAULT_RECV_TIMEOUT
            } else {
                POLL_TIMEOUT
            };
            let frame = match self.next_frame(context_id, timeout).await {
                Ok(frame) => frame,
                // Idle after chunk_complete: the text is rendered; idle on a
                // poll: nothing more yet.
                Err(RecvError::Timeout) => return Ok(None),
                Err(RecvError::Closed(info)) => {
                    if is_restart_close(info.code) {
                        if state.last_chunk_complete && !mode.wait_for_close {
                            // Every admitted chunk was delivered: nothing to replay.
                            self.end_turn(context_id);
                        }
                        if !self.recover_from_restart(Some(context_id), info).await? {
                            return Ok(None);
                        }
                        state.received_any_audio = false;
                        state.last_chunk_complete = false;
                        continue;
                    }
                    self.reset_dead_session();
                    if state.last_chunk_complete && !mode.wait_for_close {
                        tracing::warn!(code = ?info.code, "KugelAudio WebSocket closed after chunk_complete; the next turn reconnects");
                        return Ok(None);
                    }
                    if is_error_close(info.code) {
                        return Err(info.error());
                    }
                    return Err(Error::connection(format!(
                        "KugelAudio WebSocket dropped mid-stream (close code={}). Retry on a fresh session.",
                        info.code.map(|c| c.to_string()).unwrap_or_else(|| "none".to_string())
                    )));
                }
            };
            let frame_context = frame
                .get("context_id")
                .and_then(Value::as_str)
                .map(str::to_string);
            if truthy(&frame, "error") && frame_context.as_deref().is_none_or(|c| c == context_id) {
                return Err(self.frame_error(&frame, context_id));
            }
            self.note_turn_event(&frame);
            match frame_context.as_deref() {
                Some(c) if c == context_id => {}
                Some(_) => {
                    self.pending.push(frame);
                    continue;
                }
                None => {
                    if let Some(warning) = frame.get("warning").and_then(Value::as_str) {
                        tracing::warn!(warning, "KugelAudio streaming warning");
                    }
                    continue;
                }
            }
            if truthy(&frame, "generation_started") {
                state.received_any_audio = false;
                state.last_chunk_complete = false;
                continue;
            }
            let chunk = if truthy(&frame, "audio") {
                state.received_any_audio = true;
                state.last_chunk_complete = false;
                Some(AudioChunk::from_frame(&frame)?)
            } else {
                None
            };
            if let Some(words) = word_timestamps(&frame)? {
                if let Some(callback) = self.on_word_timestamps.as_mut() {
                    callback(context_id, &words);
                }
                self.last_word_timestamps
                    .insert(context_id.to_string(), words);
            }
            if chunk.is_some() {
                return Ok(chunk);
            }
            if truthy(&frame, "chunk_complete") {
                if !mode.wait_for_close && mode.idle_after_chunk_complete.is_zero() {
                    return Ok(None);
                }
                state.last_chunk_complete = true;
                continue;
            }
            // `final`: every audio frame admitted before the flush was
            // delivered. On a close drain the terminal signal is context_closed.
            if truthy(&frame, "final") && !mode.wait_for_close {
                return Ok(None);
            }
            if truthy(&frame, "context_closed") {
                self.capture_context_usage(&frame);
                if mode.wait_for_close {
                    return Ok(None);
                }
            }
        }
    }

    /// Yield the audio of `context_id` until the exchange ends.
    fn receive(
        &mut self,
        context_id: String,
        mode: ReceiveMode,
        track: bool,
    ) -> BoxStream<'_, Result<AudioChunk>> {
        Box::pin(async_stream::stream! {
            let mut state = ReceiveState { received_any_audio: false, last_chunk_complete: false };
            loop {
                match self.next_chunk(&context_id, mode, &mut state).await {
                    Ok(Some(chunk)) => {
                        if track {
                            if let Some(op) = self.live_turn(&context_id) {
                                op.record_chunk(chunk.audio.len());
                            }
                        }
                        yield Ok(chunk);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        if track {
                            self.fail_turn(&context_id, &e);
                        }
                        yield Err(e);
                        break;
                    }
                }
            }
        })
    }

    /// Send text to a context (creating it with the default voice if needed)
    /// and yield its audio.
    ///
    /// With `flush` true, waits for the audio of the flushed text.
    /// `chunk_complete_idle` overrides how long to wait after a
    /// `chunk_complete` for the next sentence (default 2 s; zero returns at
    /// the first completion).
    pub fn send<'s>(
        &'s mut self,
        context_id: &str,
        text: &str,
        flush: bool,
        chunk_complete_idle: Option<Duration>,
    ) -> BoxStream<'s, Result<AudioChunk>> {
        let context_id = context_id.to_string();
        let text = text.to_string();
        Box::pin(async_stream::stream! {
            self.turn(&context_id).set_stage(STAGE_SENDING_REQUEST);
            let prepared = self.prepare_send(&context_id).await;
            if let Err(e) = prepared {
                self.fail_turn(&context_id, &e);
                yield Err(e);
                return;
            }
            let frame = json!({"text": text, "context_id": context_id, "flush": flush});
            self.turn_frames.entry(context_id.clone()).or_default().push(frame.clone());
            if let Err(e) = self.ws_send(&frame, true).await {
                self.fail_turn(&context_id, &e);
                yield Err(e);
                return;
            }
            self.turn(&context_id).expect_audio();
            let mode = ReceiveMode {
                wait_for_close: false,
                wait_for_chunk_complete: flush,
                idle_after_chunk_complete: chunk_complete_idle.unwrap_or(INTER_CHUNK_IDLE),
            };
            let mut audio = self.receive(context_id, mode, true);
            while let Some(item) = futures_util::StreamExt::next(&mut audio).await {
                yield item;
            }
        })
    }

    async fn prepare_send(&mut self, context_id: &str) -> Result<()> {
        if let Some(ws) = &self.ws {
            if !ws.is_open() {
                let info = ws.close_info().unwrap_or(CloseInfo {
                    code: None,
                    reason: String::new(),
                });
                if is_restart_close(info.code) {
                    // The server closed an idle socket for a rolling deploy:
                    // reconnect and restore the contexts before sending.
                    self.recover_from_restart(Some(context_id), info).await?;
                } else {
                    self.reset_dead_session();
                }
            }
        }
        if self.ws.is_none() {
            self.connect().await?;
        }
        if !self.contexts.contains(context_id) {
            self.open_context(context_id, None, true).await?;
        }
        Ok(())
    }

    /// Flush a context's buffer and yield the remaining audio.
    pub fn flush(&mut self, context_id: &str) -> BoxStream<'_, Result<AudioChunk>> {
        let context_id = context_id.to_string();
        Box::pin(async_stream::stream! {
            if !self.is_started || !self.contexts.contains(&context_id) {
                return;
            }
            let frame = json!({"flush": true, "context_id": context_id});
            self.turn_frames.entry(context_id.clone()).or_default().push(frame.clone());
            if let Err(e) = self.ws_send(&frame, true).await {
                self.fail_turn(&context_id, &e);
                yield Err(e);
                return;
            }
            if let Some(op) = self.live_turn(&context_id) {
                op.expect_audio();
            }
            let mode = ReceiveMode {
                wait_for_close: false,
                wait_for_chunk_complete: true,
                idle_after_chunk_complete: INTER_CHUNK_IDLE,
            };
            let mut audio = self.receive(context_id, mode, true);
            while let Some(item) = futures_util::StreamExt::next(&mut audio).await {
                yield item;
            }
        })
    }

    /// Close a context and yield its remaining audio until `context_closed`
    /// (whose usage is then in [`usage_for`](Self::usage_for)).
    ///
    /// With `immediate` true this is a barge-in: the server cancels the
    /// context's generation at once and drops its buffered text.
    pub fn close_context(
        &mut self,
        context_id: &str,
        immediate: bool,
    ) -> BoxStream<'_, Result<AudioChunk>> {
        let context_id = context_id.to_string();
        Box::pin(async_stream::stream! {
            if !self.is_started || !self.contexts.contains(&context_id) {
                return;
            }
            if immediate {
                // A caller-initiated cancellation of this context's turn only.
                self.cancel_turn(&context_id);
            } else if let Some(op) = self.live_turn(&context_id) {
                op.set_stage(STAGE_FINALIZING);
            }
            let mut message = json!({"close_context": true, "context_id": context_id});
            if immediate {
                message["immediate"] = Value::Bool(true);
            }
            let resent = match self.ws_send(&message, true).await {
                Ok(true) => self.ws_send(&message, false).await.map(|_| ()),
                Ok(false) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = resent {
                self.fail_turn(&context_id, &e);
                yield Err(e);
                return;
            }
            let mode = ReceiveMode {
                wait_for_close: true,
                wait_for_chunk_complete: false,
                idle_after_chunk_complete: INTER_CHUNK_IDLE,
            };
            {
                let mut audio = self.receive(context_id.clone(), mode, !immediate);
                while let Some(item) = futures_util::StreamExt::next(&mut audio).await {
                    yield item;
                }
            }
            self.forget_context(&context_id);
        })
    }

    /// Reset a context's 20 s inactivity timeout without generating audio.
    pub async fn keep_alive(&mut self, context_id: &str) -> Result<()> {
        if self.is_started && self.contexts.contains(context_id) {
            self.ws_send(&json!({"text": "", "context_id": context_id}), false)
                .await?;
        }
        Ok(())
    }

    /// Close every context and the socket. Per-context usage seen while
    /// closing is recorded; returns the session's `session_closed` stats.
    pub async fn close(&mut self) -> Option<FinalStats> {
        let mut stats = None;
        if let Some(ws) = self.ws.as_mut() {
            if ws.send_json(&json!({"close_socket": true})).is_ok() {
                // KEEP-JUSTIFIED: the socket is being torn down; a timeout or a
                // close here only ends the drain early, there is nothing to report.
                while let Ok(frame) = ws.recv_json(DEFAULT_RECV_TIMEOUT).await {
                    if truthy(&frame, "context_closed") {
                        let context_id = frame
                            .get("context_id")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        if let (Some(id), Some(usage)) =
                            (context_id, SessionUsage::from_payload(&frame))
                        {
                            self.context_usage.insert(id, usage);
                        }
                        let id = frame
                            .get("context_id")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        if let Some(id) = id {
                            if let Some(mut op) = self.turn_ops.remove(&id) {
                                op.succeed();
                            }
                        }
                    }
                    if truthy(&frame, "session_closed") {
                        stats = Some(FinalStats::from_frame(&frame));
                        break;
                    }
                }
            }
        }
        if let Some(ws) = self.ws.take() {
            ws.close().await;
        }
        self.reset_dead_session();
        // Turns the server never finished were abandoned by this close.
        for (_, mut op) in self.turn_ops.drain() {
            op.cancel();
        }
        stats
    }
}

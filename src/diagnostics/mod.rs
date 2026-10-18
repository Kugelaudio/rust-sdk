//! Client-error diagnostics: a bounded, off-path OTLP/HTTP-JSON log reporter
//! plus the [`Operation`] seam every public entry point wraps itself in.
//!
//! The wire format, attribute allowlist, enablement order and delivery bounds
//! are shared with the Python, JavaScript and Java SDKs. Every unit of work
//! mints an [`Operation`]; on failure it reports exactly one event. A
//! caller-initiated cancellation is not a fault: it only bumps a counter
//! reported in `sdk_stats` when the client closes. Events never contain text,
//! audio, URLs, keys or error messages: only allowlisted attributes are encoded.
//!
//! All network I/O happens on a background task, so telemetry never blocks a
//! synthesis call and never surfaces an error to the caller.

pub(crate) mod config;
#[cfg(test)]
mod golden_tests;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::error::{BoxError, Error};
use crate::metadata::{runtime_string, SDK_NAME, SDK_VERSION};
pub(crate) use config::DiagnosticsConfig;

const NOT_FOUND_STATUS: u16 = 404;
const MAX_CONSECUTIVE_NOT_FOUND: u32 = 3;
const PAYLOAD_TOO_LARGE_STATUS: u16 = 413;
const QUEUE_CAPACITY: usize = 64;
const BATCH_SIZE: usize = 8;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_SEND_ATTEMPTS: usize = 2;
/// Closing the client waits at most this long for pending reports.
pub(crate) const CLOSE_DEADLINE: Duration = Duration::from_secs(1);

const EVENT_CONNECTION_FAILED: &str = "connection_failed";
const EVENT_REQUEST_FAILED: &str = "request_failed";
const EVENT_STREAM_INTERRUPTED: &str = "stream_interrupted";
const EVENT_RETRY_EXHAUSTED: &str = "retry_exhausted";
const EVENT_SDK_STATS: &str = "sdk_stats";

pub(crate) const OP_GENERATE: &str = "generate";
pub(crate) const OP_STREAM: &str = "stream";
pub(crate) const OP_STREAM_SESSION: &str = "stream_session";
pub(crate) const OP_MULTI_CONTEXT: &str = "multi_context";
pub(crate) const OP_VOICES: &str = "voices";
pub(crate) const OP_DICTIONARIES: &str = "dictionaries";
pub(crate) const OP_MODELS: &str = "models";

pub(crate) const STAGE_CONNECTING: &str = "connecting";
pub(crate) const STAGE_HANDSHAKE: &str = "handshake";
pub(crate) const STAGE_SENDING_REQUEST: &str = "sending_request";
pub(crate) const STAGE_AWAITING_FIRST_AUDIO: &str = "awaiting_first_audio";
pub(crate) const STAGE_RECEIVING_AUDIO: &str = "receiving_audio";
pub(crate) const STAGE_FINALIZING: &str = "finalizing";

pub(crate) const TRANSPORT_HTTP: &str = "http";
pub(crate) const TRANSPORT_WEBSOCKET: &str = "websocket";

const INTEGRATION_NONE: &str = "none";
const OUTCOME_FAILED: &str = "failed";

/// The exact attribute allowlist; `true` marks int-valued keys.
const ATTRIBUTE_TYPES: &[(&str, bool)] = &[
    ("kugel.event", false),
    ("kugel.event_id", false),
    ("kugel.operation_id", false),
    ("kugel.operation", false),
    ("kugel.sdk.name", false),
    ("kugel.sdk.version", false),
    ("kugel.runtime", false),
    ("kugel.integration", false),
    ("kugel.transport", false),
    ("kugel.failure_stage", false),
    ("kugel.error_type", false),
    ("kugel.error_code", false),
    ("kugel.http_status", true),
    ("kugel.ws_close_code", true),
    ("kugel.server_request_id", false),
    ("kugel.elapsed_ms", true),
    ("kugel.audio_chunks", true),
    ("kugel.audio_bytes", true),
    ("kugel.retry_count", true),
    ("kugel.outcome", false),
    ("kugel.endpoint_kind", false),
    ("kugel.success_count", true),
    ("kugel.failure_count", true),
    ("kugel.cancelled_count", true),
];

/// One attribute value.
#[derive(Debug, Clone)]
pub(crate) enum Attr {
    Str(String),
    Int(i64),
}

impl From<&str> for Attr {
    fn from(value: &str) -> Self {
        Attr::Str(value.to_string())
    }
}

impl From<String> for Attr {
    fn from(value: String) -> Self {
        Attr::Str(value)
    }
}

macro_rules! int_attr {
    ($($t:ty),*) => {$(
        impl From<$t> for Attr {
            fn from(value: $t) -> Self {
                Attr::Int(i64::try_from(value).unwrap_or(i64::MAX))
            }
        }
    )*};
}
int_attr!(u16, u32, u64, u128, usize);

type Attributes = Vec<(&'static str, Attr)>;

/// Encode attributes as OTLP JSON, dropping every key outside the allowlist.
fn encode_attributes(attributes: &Attributes) -> Vec<Value> {
    attributes
        .iter()
        .filter_map(|(key, value)| {
            let is_int = ATTRIBUTE_TYPES.iter().find(|(k, _)| k == key)?.1;
            let encoded = match (is_int, value) {
                (true, Attr::Int(i)) => json!({"intValue": i.to_string()}),
                (false, Attr::Str(s)) => json!({"stringValue": s}),
                (false, Attr::Int(i)) => json!({"stringValue": i.to_string()}),
                (true, Attr::Str(_)) => return None,
            };
            Some(json!({"key": key, "value": encoded}))
        })
        .collect()
}

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// 32 lowercase hex characters.
pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// One OTLP log record. `sdk_stats` is INFO, every failure is ERROR.
fn build_log_record(event: &str, attributes: &Attributes, time_unix_nanos: u128) -> Value {
    let (number, text) = if event == EVENT_SDK_STATS {
        (9, "INFO")
    } else {
        (17, "ERROR")
    };
    json!({
        "timeUnixNano": time_unix_nanos.to_string(),
        "severityNumber": number,
        "severityText": text,
        "body": {"stringValue": event},
        "attributes": encode_attributes(attributes),
    })
}

fn build_payload(records: &[Value], version: &str) -> Vec<u8> {
    let body = json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "kugelaudio-sdk"}},
                {"key": "service.version", "value": {"stringValue": version}},
                {"key": "telemetry.sdk.language", "value": {"stringValue": SDK_NAME}},
            ]},
            "scopeLogs": [{
                "scope": {"name": "kugelaudio.diagnostics", "version": "1"},
                "logRecords": records,
            }],
        }]
    });
    serde_json::to_vec(&body).unwrap_or_default()
}

/// The future a [`Sender`] returns: the HTTP status, or a transport failure.
pub(crate) type SendFuture = Pin<Box<dyn Future<Output = Result<u16, BoxError>> + Send>>;

/// The injectable transport. Every HTTP response, 2xx or not, comes back as its
/// status code: the reporter must see a 404 or 413 to apply the contract.
pub(crate) trait Sender: Send + Sync {
    fn send(&self, url: String, headers: Vec<(String, String)>, payload: Vec<u8>) -> SendFuture;
}

/// The default transport: the client's own `reqwest` connection pool.
pub(crate) struct HttpSender(pub reqwest::Client);

impl Sender for HttpSender {
    fn send(&self, url: String, headers: Vec<(String, String)>, payload: Vec<u8>) -> SendFuture {
        let mut request = self.0.post(url).timeout(REQUEST_TIMEOUT).body(payload);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        Box::pin(async move {
            let response = request.send().await?;
            Ok(response.status().as_u16())
        })
    }
}

#[derive(Default)]
struct State {
    records: VecDeque<Value>,
    deadline: Option<Instant>,
    flush_requested: bool,
    closed: bool,
    in_flight: bool,
    worker_running: bool,
}

struct Shared {
    config: DiagnosticsConfig,
    /// `kugel.sdk.version` and `service.version`: this crate's version.
    version: String,
    sender: Arc<dyn Sender>,
    state: Mutex<State>,
    wake: Notify,
    drained: Notify,
    success: AtomicU64,
    failure: AtomicU64,
    cancelled: AtomicU64,
    consecutive_not_found: AtomicU32,
    route_missing: AtomicBool,
}

/// The per-client reporter. Cheap to clone.
#[derive(Clone)]
pub(crate) struct Diagnostics {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Diagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Diagnostics")
            .field("enabled", &self.shared.config.enabled)
            .field("endpoint_kind", &self.shared.config.endpoint_kind)
            .finish()
    }
}

impl Diagnostics {
    pub fn new(config: DiagnosticsConfig, sender: Arc<dyn Sender>) -> Self {
        Self::with_version(config, sender, SDK_VERSION)
    }

    fn with_version(config: DiagnosticsConfig, sender: Arc<dyn Sender>, version: &str) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                version: version.to_string(),
                sender,
                state: Mutex::new(State::default()),
                wake: Notify::new(),
                drained: Notify::new(),
                success: AtomicU64::new(0),
                failure: AtomicU64::new(0),
                cancelled: AtomicU64::new(0),
                consecutive_not_found: AtomicU32::new(0),
                route_missing: AtomicBool::new(false),
            }),
        }
    }

    /// True when telemetry is enabled for this client.
    pub fn enabled(&self) -> bool {
        self.shared.config.enabled
    }

    fn inert(&self) -> bool {
        self.shared.config.inert() || self.shared.route_missing.load(Ordering::SeqCst)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // KEEP-JUSTIFIED: a poisoned lock only means a panic elsewhere while
        // holding it; the queue is still structurally valid and telemetry must
        // never panic the caller.
        self.shared.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Mint an operation. Its id exists before anything connects.
    pub fn operation(&self, name: Option<&'static str>, transport: &'static str) -> Operation {
        Operation::new(self.clone(), name, transport, None)
    }

    /// Mint an operation that starts at `stage`.
    pub fn operation_at(
        &self,
        name: &'static str,
        transport: &'static str,
        stage: &'static str,
    ) -> Operation {
        Operation::new(self.clone(), Some(name), transport, Some(stage))
    }

    #[cfg(test)]
    pub fn counters(&self) -> (u64, u64, u64) {
        (
            self.shared.success.load(Ordering::SeqCst),
            self.shared.failure.load(Ordering::SeqCst),
            self.shared.cancelled.load(Ordering::SeqCst),
        )
    }

    #[cfg(test)]
    pub fn pending(&self) -> usize {
        self.lock().records.len()
    }

    fn record(&self, event: &str, attributes: Attributes) -> Value {
        self.record_at(event, attributes, now_unix_nanos())
    }

    /// The OTLP record for `event`. The event name is always set; the
    /// required attributes and this client's identity (ids, SDK name and
    /// version, runtime, integration, endpoint kind) are added only where
    /// `attributes` does not carry them. Pure: queues nothing.
    fn record_at(&self, event: &str, mut attributes: Attributes, time_unix_nanos: u128) -> Value {
        attributes.retain(|(k, _)| *k != "kugel.event");
        attributes.push(("kugel.event", event.into()));
        let mut defaults: Attributes = vec![
            ("kugel.event_id", new_id().into()),
            ("kugel.operation_id", new_id().into()),
            ("kugel.sdk.name", SDK_NAME.into()),
            ("kugel.sdk.version", self.shared.version.clone().into()),
            ("kugel.integration", INTEGRATION_NONE.into()),
            (
                "kugel.endpoint_kind",
                self.shared.config.endpoint_kind.into(),
            ),
        ];
        if let Some(runtime) = runtime_string() {
            defaults.push(("kugel.runtime", runtime.into()));
        }
        for (key, value) in defaults {
            if !attributes.iter().any(|(k, _)| *k == key) {
                attributes.push((key, value));
            }
        }
        build_log_record(event, &attributes, time_unix_nanos)
    }

    /// Queue one event. Never blocks on the network, never fails.
    fn report(&self, event: &str, attributes: Attributes) {
        if self.inert() {
            return;
        }
        let record = self.record(event, attributes);
        {
            let mut state = self.lock();
            if state.closed && event != EVENT_SDK_STATS {
                return;
            }
            if state.records.len() >= QUEUE_CAPACITY {
                state.records.pop_front();
            }
            state.records.push_back(record);
            if state.deadline.is_none() {
                state.deadline = Some(Instant::now() + FLUSH_INTERVAL);
            }
        }
        self.ensure_worker();
        self.shared.wake.notify_one();
    }

    fn ensure_worker(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // KEEP-JUSTIFIED: every SDK operation runs inside a Tokio runtime;
            // outside one there is nothing to deliver on and telemetry must
            // stay silent. The record stays queued for a later flush.
            return;
        };
        let mut state = self.lock();
        if state.worker_running {
            return;
        }
        state.worker_running = true;
        drop(state);
        let this = self.clone();
        handle.spawn(async move { this.run_worker().await });
    }

    async fn run_worker(self) {
        loop {
            let batch = loop {
                let wait = {
                    let mut state = self.lock();
                    if self.shared.route_missing.load(Ordering::SeqCst) || state.records.is_empty()
                    {
                        state.records.clear();
                        state.flush_requested = false;
                        state.deadline = None;
                        state.worker_running = false;
                        drop(state);
                        self.shared.drained.notify_waiters();
                        return;
                    }
                    let now = Instant::now();
                    let due = state.records.len() >= BATCH_SIZE
                        || state.flush_requested
                        || state.closed
                        || state.deadline.is_some_and(|d| now >= d);
                    if due {
                        let size = state.records.len().min(BATCH_SIZE);
                        let batch: Vec<Value> = state.records.drain(..size).collect();
                        state.deadline = if state.records.is_empty() {
                            None
                        } else {
                            Some(now + FLUSH_INTERVAL)
                        };
                        state.in_flight = true;
                        break batch;
                    }
                    state
                        .deadline
                        .map(|d| d.saturating_duration_since(now))
                        .unwrap_or(FLUSH_INTERVAL)
                };
                // A notify_one permit is stored when nobody waits, so a wake
                // between the check above and this await is not lost.
                let _ = tokio::time::timeout(wait, self.shared.wake.notified()).await;
            };
            self.deliver(batch).await;
            self.lock().in_flight = false;
            self.shared.drained.notify_waiters();
        }
    }

    async fn deliver(&self, batch: Vec<Value>) {
        let payload = build_payload(&batch, &self.shared.version);
        let mut headers = self.shared.config.auth_headers.clone();
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
        for _ in 0..MAX_SEND_ATTEMPTS {
            let attempt = self.shared.sender.send(
                self.shared.config.url.clone(),
                headers.clone(),
                payload.clone(),
            );
            match attempt.await {
                // KEEP-JUSTIFIED: a transport failure is retried once, then the
                // batch is dropped; telemetry never surfaces to the caller.
                Err(_) => continue,
                Ok(status) if (200..300).contains(&status) => {
                    self.shared.consecutive_not_found.store(0, Ordering::SeqCst);
                    return;
                }
                Ok(NOT_FOUND_STATUS) => {
                    let streak = self
                        .shared
                        .consecutive_not_found
                        .fetch_add(1, Ordering::SeqCst)
                        + 1;
                    if streak >= MAX_CONSECUTIVE_NOT_FOUND {
                        self.shared.route_missing.store(true, Ordering::SeqCst);
                        self.lock().records.clear();
                    }
                    return;
                }
                Ok(PAYLOAD_TOO_LARGE_STATUS) => {
                    self.shared.consecutive_not_found.store(0, Ordering::SeqCst);
                    return;
                }
                Ok(_) => {
                    self.shared.consecutive_not_found.store(0, Ordering::SeqCst);
                }
            }
        }
    }

    /// Ask the worker to send everything now and wait at most `timeout`.
    pub async fn flush(&self, timeout: Duration) {
        if self.inert() {
            return;
        }
        {
            let mut state = self.lock();
            if state.records.is_empty() && !state.in_flight {
                return;
            }
            state.flush_requested = true;
        }
        self.ensure_worker();
        self.shared.wake.notify_one();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let drained = self.shared.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            {
                let state = self.lock();
                if state.records.is_empty() && !state.in_flight {
                    return;
                }
            }
            if tokio::time::timeout_at(deadline, drained).await.is_err() {
                return;
            }
        }
    }

    /// Queue `sdk_stats`, hand everything to the worker and wait at most
    /// `timeout`. Idempotent.
    pub async fn close(&self, timeout: Duration) {
        {
            let mut state = self.lock();
            if state.closed {
                return;
            }
            state.closed = true;
        }
        let success = self.shared.success.load(Ordering::SeqCst);
        let failure = self.shared.failure.load(Ordering::SeqCst);
        let cancelled = self.shared.cancelled.load(Ordering::SeqCst);
        if success + failure + cancelled > 0 {
            self.report(
                EVENT_SDK_STATS,
                vec![
                    ("kugel.success_count", success.into()),
                    ("kugel.failure_count", failure.into()),
                    // The only place a cancellation is ever reported.
                    ("kugel.cancelled_count", cancelled.into()),
                ],
            );
        }
        self.flush(timeout).await;
    }
}

/// One unit of work that independently succeeds or fails: a one-shot call, an
/// HTTP request, one session turn, or one session connect. Reports at most one
/// event: the first `succeed`, `fail` or `cancel` settles it.
#[derive(Debug)]
pub(crate) struct Operation {
    diagnostics: Diagnostics,
    name: Option<&'static str>,
    transport: &'static str,
    id: String,
    started: Instant,
    stage: &'static str,
    audio_chunks: u64,
    audio_bytes: u64,
    retry_count: u32,
    server_error: bool,
    settled: bool,
}

impl Operation {
    fn new(
        diagnostics: Diagnostics,
        name: Option<&'static str>,
        transport: &'static str,
        stage: Option<&'static str>,
    ) -> Self {
        let stage = stage.unwrap_or(if transport == TRANSPORT_HTTP {
            STAGE_SENDING_REQUEST
        } else {
            STAGE_CONNECTING
        });
        Self {
            diagnostics,
            name,
            transport,
            id: new_id(),
            started: Instant::now(),
            stage,
            audio_chunks: 0,
            audio_bytes: 0,
            retry_count: 0,
            server_error: false,
            settled: false,
        }
    }

    pub fn settled(&self) -> bool {
        self.settled
    }

    pub fn set_stage(&mut self, stage: &'static str) {
        self.stage = stage;
    }

    /// Enter the receive phase: awaiting first audio, or receiving more.
    pub fn expect_audio(&mut self) {
        self.stage = if self.audio_chunks > 0 {
            STAGE_RECEIVING_AUDIO
        } else {
            STAGE_AWAITING_FIRST_AUDIO
        };
    }

    pub fn record_chunk(&mut self, size_bytes: usize) {
        self.audio_chunks += 1;
        self.audio_bytes += size_bytes as u64;
        if self.stage == STAGE_AWAITING_FIRST_AUDIO {
            self.stage = STAGE_RECEIVING_AUDIO;
        }
    }

    /// A new attempt of this operation: earlier attempt details reset.
    pub fn record_retry(&mut self) {
        self.retry_count += 1;
        self.server_error = false;
    }

    /// The server answered this operation with an error frame: the event is
    /// `request_failed` (`retry_exhausted` after a retry).
    pub fn mark_server_error(&mut self) {
        self.server_error = true;
    }

    pub fn succeed(&mut self) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.diagnostics
            .shared
            .success
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Caller-initiated cancellation: counted in `sdk_stats`, never an event.
    pub fn cancel(&mut self) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.diagnostics
            .shared
            .cancelled
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Report one event for `error`.
    pub fn fail(&mut self, error: &Error) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.diagnostics
            .shared
            .failure
            .fetch_add(1, Ordering::SeqCst);
        let event = self.event_name();
        let attributes = self.attributes(error);
        self.diagnostics.report(event, attributes);
    }

    fn event_name(&self) -> &'static str {
        if self.retry_count > 0 {
            EVENT_RETRY_EXHAUSTED
        } else if self.server_error {
            EVENT_REQUEST_FAILED
        } else if matches!(self.stage, STAGE_CONNECTING | STAGE_HANDSHAKE) {
            EVENT_CONNECTION_FAILED
        } else if matches!(
            self.stage,
            STAGE_AWAITING_FIRST_AUDIO | STAGE_RECEIVING_AUDIO | STAGE_FINALIZING
        ) {
            EVENT_STREAM_INTERRUPTED
        } else {
            EVENT_REQUEST_FAILED
        }
    }

    fn attributes(&self, error: &Error) -> Attributes {
        let elapsed_ms = self.started.elapsed().as_millis();
        let mut attributes: Attributes = vec![
            ("kugel.operation_id", self.id.clone().into()),
            ("kugel.transport", self.transport.into()),
            ("kugel.failure_stage", self.stage.into()),
            ("kugel.elapsed_ms", elapsed_ms.into()),
            ("kugel.audio_chunks", self.audio_chunks.into()),
            ("kugel.audio_bytes", self.audio_bytes.into()),
            ("kugel.retry_count", self.retry_count.into()),
            ("kugel.outcome", OUTCOME_FAILED.into()),
            ("kugel.error_type", error.type_name().into()),
        ];
        if let Some(name) = self.name {
            attributes.push(("kugel.operation", name.into()));
        }
        let details = error.details();
        if let Some(code) = &details.error_code {
            attributes.push(("kugel.error_code", code.clone().into()));
        }
        if let Some(status) = details.status_code {
            attributes.push(("kugel.http_status", status.into()));
        }
        if let Some(id) = &details.request_id {
            attributes.push(("kugel.server_request_id", id.clone().into()));
        }
        if let Some(code) = details.ws_close_code {
            attributes.push(("kugel.ws_close_code", code.into()));
        }
        attributes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Call = (String, Vec<(String, String)>, Value);

    #[derive(Default)]
    struct FakeSender {
        statuses: Mutex<VecDeque<u16>>,
        calls: Mutex<Vec<Call>>,
    }

    impl Sender for Arc<FakeSender> {
        fn send(
            &self,
            url: String,
            headers: Vec<(String, String)>,
            payload: Vec<u8>,
        ) -> SendFuture {
            let body: Value = serde_json::from_slice(&payload).unwrap();
            self.calls.lock().unwrap().push((url, headers, body));
            let status = self.statuses.lock().unwrap().pop_front().unwrap_or(200);
            Box::pin(async move { Ok(status) })
        }
    }

    fn reporter(sender: Arc<FakeSender>) -> Diagnostics {
        let config = DiagnosticsConfig::resolve(
            "https://api.kugelaudio.com",
            Some(true),
            None,
            vec![("Authorization".into(), "Bearer k".into())],
        );
        Diagnostics::new(config, Arc::new(sender))
    }

    fn attr<'a>(record: &'a Value, key: &str) -> Option<&'a Value> {
        record["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == key)
            .map(|a| &a["value"])
    }

    #[tokio::test]
    async fn failure_reports_one_allowlisted_event_and_close_sends_stats() {
        let sender = Arc::new(FakeSender::default());
        let diagnostics = reporter(sender.clone());
        let mut op = diagnostics.operation(Some(OP_STREAM), TRANSPORT_WEBSOCKET);
        op.expect_audio();
        op.record_chunk(100);
        let err = crate::error::classify_ws_close(Some(1011), "secret text");
        op.fail(&err);
        op.fail(&err);
        let mut ok = diagnostics.operation(Some(OP_MODELS), TRANSPORT_HTTP);
        ok.succeed();
        let mut barge = diagnostics.operation(Some(OP_STREAM_SESSION), TRANSPORT_WEBSOCKET);
        barge.cancel();
        diagnostics.close(Duration::from_secs(2)).await;

        let calls = sender.calls.lock().unwrap();
        let records: Vec<&Value> = calls
            .iter()
            .flat_map(|(_, _, body)| {
                body["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
                    .as_array()
                    .unwrap()
                    .iter()
            })
            .collect();
        assert_eq!(records.len(), 2, "one failure plus sdk_stats");
        let failure = records[0];
        assert_eq!(failure["body"]["stringValue"], "stream_interrupted");
        assert_eq!(
            attr(failure, "kugel.ws_close_code").unwrap()["intValue"],
            "1011"
        );
        assert_eq!(
            attr(failure, "kugel.error_type").unwrap()["stringValue"],
            "ConnectionError"
        );
        assert_eq!(
            attr(failure, "kugel.sdk.name").unwrap()["stringValue"],
            "rust"
        );
        assert!(
            !failure.to_string().contains("secret text"),
            "messages never leave the SDK"
        );
        let stats = records[1];
        assert_eq!(stats["severityText"], "INFO");
        assert_eq!(attr(stats, "kugel.success_count").unwrap()["intValue"], "1");
        assert_eq!(attr(stats, "kugel.failure_count").unwrap()["intValue"], "1");
        assert_eq!(
            attr(stats, "kugel.cancelled_count").unwrap()["intValue"],
            "1"
        );
        let (url, headers, _) = &calls[0];
        assert_eq!(url, "https://api.kugelaudio.com/v1/sdk-diagnostics");
        assert!(headers.contains(&("Authorization".into(), "Bearer k".into())));
    }

    #[tokio::test]
    async fn three_not_found_switch_the_reporter_off() {
        let sender = Arc::new(FakeSender::default());
        sender.statuses.lock().unwrap().extend([404, 404, 404]);
        let diagnostics = reporter(sender.clone());
        for _ in 0..3 {
            let mut op = diagnostics.operation(Some(OP_VOICES), TRANSPORT_HTTP);
            op.fail(&Error::connection("down"));
            diagnostics.flush(Duration::from_secs(2)).await;
        }
        assert_eq!(sender.calls.lock().unwrap().len(), 3);
        let mut op = diagnostics.operation(Some(OP_VOICES), TRANSPORT_HTTP);
        op.fail(&Error::connection("down"));
        assert_eq!(diagnostics.pending(), 0, "route missing: nothing is queued");
    }

    #[test]
    fn disabled_reporter_queues_nothing() {
        let diagnostics = Diagnostics::new(
            DiagnosticsConfig::disabled(),
            Arc::new(Arc::new(FakeSender::default())),
        );
        let mut op = diagnostics.operation(Some(OP_MODELS), TRANSPORT_HTTP);
        op.fail(&Error::connection("down"));
        assert_eq!(diagnostics.pending(), 0);
        assert_eq!(diagnostics.counters(), (0, 1, 0));
    }

    #[test]
    fn event_names_follow_the_stage() {
        let diagnostics = Diagnostics::new(
            DiagnosticsConfig::disabled(),
            Arc::new(Arc::new(FakeSender::default())),
        );
        let mut op = diagnostics.operation(Some(OP_STREAM), TRANSPORT_WEBSOCKET);
        assert_eq!(op.event_name(), EVENT_CONNECTION_FAILED);
        op.expect_audio();
        assert_eq!(op.event_name(), EVENT_STREAM_INTERRUPTED);
        op.mark_server_error();
        assert_eq!(op.event_name(), EVENT_REQUEST_FAILED);
        op.record_retry();
        assert_eq!(op.event_name(), EVENT_RETRY_EXHAUSTED);
        let http = diagnostics.operation(Some(OP_MODELS), TRANSPORT_HTTP);
        assert_eq!(http.event_name(), EVENT_REQUEST_FAILED);
    }

    #[test]
    fn unknown_attributes_are_dropped() {
        let encoded = encode_attributes(&vec![
            ("kugel.event", "x".into()),
            ("text", "secret".into()),
            ("kugel.http_status", Attr::Int(500)),
        ]);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[1]["value"]["intValue"], "500");
    }
}

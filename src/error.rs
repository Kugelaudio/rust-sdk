//! Typed errors, mapped from HTTP status, the server's `error_code`, refused
//! WebSocket upgrades and WebSocket close codes.
//!
//! Every SDK call returns [`Result<T>`](crate::Result). Match on [`Error`] to
//! react to a category without parsing message text:
//!
//! ```no_run
//! # async fn demo(client: kugelaudio::KugelAudio) {
//! use kugelaudio::{Error, TtsRequest};
//!
//! match client.tts().generate(&TtsRequest::new("Hallo").voice_id(1071)).await {
//!     Ok(audio) => println!("{} bytes", audio.audio.len()),
//!     Err(Error::RateLimit(e)) => println!("retry after {:?}s", e.retry_after),
//!     Err(Error::Authentication(e)) => println!("bad key: {e}"),
//!     Err(other) => println!("request {:?} failed: {other}", other.request_id()),
//! }
//! # }
//! ```

use std::fmt;

use serde_json::Value;

/// The result type of every SDK call.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A boxed error kept as the [`std::error::Error::source`] of an SDK error.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `error_code`: the API key was missing, invalid or not accepted.
pub const ERROR_CODE_UNAUTHORIZED: &str = "UNAUTHORIZED";
/// `error_code`: the organization exceeded its rate or concurrency limit.
pub const ERROR_CODE_RATE_LIMITED: &str = "RATE_LIMITED";
/// `error_code`: the organization is out of credits.
pub const ERROR_CODE_INSUFFICIENT_CREDITS: &str = "INSUFFICIENT_CREDITS";
/// `error_code`: the model is temporarily unavailable.
pub const ERROR_CODE_MODEL_UNAVAILABLE: &str = "MODEL_UNAVAILABLE";
/// `error_code`: the request was malformed or invalid.
pub const ERROR_CODE_VALIDATION: &str = "VALIDATION_ERROR";
/// `error_code`: the server failed while handling the request.
pub const ERROR_CODE_INTERNAL: &str = "INTERNAL_ERROR";
/// `error_code`: the referenced resource does not exist or is not visible.
pub const ERROR_CODE_NOT_FOUND: &str = "NOT_FOUND";
/// `error_code`: no `voice_id` was supplied for synthesis.
pub const ERROR_CODE_MISSING_VOICE_ID: &str = "MISSING_VOICE_ID";
/// `error_code`: a multi-context socket already has 20 live contexts.
pub const ERROR_CODE_TOO_MANY_CONTEXTS: &str = "TOO_MANY_CONTEXTS";

/// WebSocket close code: the API key is not accepted for this connection.
pub const WS_CLOSE_UNAUTHORIZED: u16 = 4001;
/// WebSocket close code: the organization ran out of credits.
pub const WS_CLOSE_INSUFFICIENT_CREDITS: u16 = 4003;
/// WebSocket close code: the organization exceeded its rate limit.
pub const WS_CLOSE_RATE_LIMITED: u16 = 4029;
/// WebSocket close code: the model is temporarily unavailable.
pub const WS_CLOSE_MODEL_UNAVAILABLE: u16 = 4500;
/// WebSocket close code: the replica is restarting (rolling deploy).
pub const WS_CLOSE_SERVICE_RESTART: u16 = 1012;
/// WebSocket close code: the replica accepted the handshake only to report it is draining.
pub const WS_CLOSE_TRY_AGAIN_LATER: u16 = 1013;

const API_KEYS_URL: &str = "https://www.kugelaudio.com/dashboard";
const BILLING_URL: &str = "https://www.kugelaudio.com/dashboard";

/// The details every SDK error carries.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub struct ApiError {
    /// Human-readable, actionable text. Do not parse it for program logic.
    pub message: String,
    /// HTTP status (or the HTTP-style `code` of a WebSocket error frame), when known.
    pub status_code: Option<u16>,
    /// Machine-readable `error_code` mirrored from the server, when known.
    pub error_code: Option<String>,
    /// Correlation id echoed by the server (`x-request-id` header or the
    /// `request_id` field of a WebSocket error frame). Quote it to support.
    pub request_id: Option<String>,
    /// Seconds to wait before retrying, when the server said so.
    pub retry_after: Option<u64>,
    /// The WebSocket close code that ended the connection, when one did.
    pub ws_close_code: Option<u16>,
    #[source]
    source: Option<BoxError>,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.request_id {
            Some(id) => write!(f, "{} (request_id: {id})", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl ApiError {
    /// An error with only a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status_code: None,
            error_code: None,
            request_id: None,
            retry_after: None,
            ws_close_code: None,
            source: None,
        }
    }

    pub(crate) fn status(mut self, status: Option<u16>) -> Self {
        self.status_code = status;
        self
    }

    pub(crate) fn code(mut self, code: Option<String>) -> Self {
        self.error_code = code;
        self
    }

    pub(crate) fn request_id(mut self, id: Option<String>) -> Self {
        self.request_id = id;
        self
    }

    pub(crate) fn retry_after(mut self, seconds: Option<u64>) -> Self {
        self.retry_after = seconds;
        self
    }

    fn default_status(mut self, status: u16) -> Self {
        self.status_code.get_or_insert(status);
        self
    }

    fn default_code(mut self, code: &str) -> Self {
        self.error_code.get_or_insert_with(|| code.to_string());
        self
    }
}

/// Every error the SDK returns. The variants mirror the error classes of the
/// Python SDK; [`Error::ServerRestarting`] is a kind of connection error
/// ([`Error::is_connection`] is true for both).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The API key was missing, malformed or rejected (HTTP 401, a refused
    /// WebSocket upgrade with 403, or close code 4001).
    #[error(transparent)]
    Authentication(ApiError),
    /// The account is out of credits (HTTP 402, close code 4003).
    #[error(transparent)]
    InsufficientCredits(ApiError),
    /// A referenced resource does not exist or is not visible (HTTP 404).
    #[error(transparent)]
    NotFound(ApiError),
    /// The per-organization rate or concurrency limit was hit (HTTP 429,
    /// close code 4029, `TOO_MANY_CONTEXTS`). The wait is in `retry_after`.
    #[error(transparent)]
    RateLimit(ApiError),
    /// The request was rejected as invalid, or an argument failed a local check.
    #[error(transparent)]
    Validation(ApiError),
    /// The replica serving this WebSocket is restarting (close code 1012 or
    /// 1013). Audio received before it is complete up to the last chunk.
    /// Streaming sessions absorb this while a turn is still silent; you see it
    /// only when replaying would repeat audio. Reconnect and resend the turn.
    #[error(transparent)]
    ServerRestarting(ApiError),
    /// KugelAudio could not be reached, timed out, closed the socket, or the
    /// model is temporarily unavailable (HTTP 503, close code 4500).
    #[error(transparent)]
    Connection(ApiError),
    /// Any other API error.
    #[error(transparent)]
    Api(ApiError),
}

impl Error {
    /// The details shared by every variant.
    pub fn details(&self) -> &ApiError {
        match self {
            Error::Authentication(e)
            | Error::InsufficientCredits(e)
            | Error::NotFound(e)
            | Error::RateLimit(e)
            | Error::Validation(e)
            | Error::ServerRestarting(e)
            | Error::Connection(e)
            | Error::Api(e) => e,
        }
    }

    fn details_mut(&mut self) -> &mut ApiError {
        match self {
            Error::Authentication(e)
            | Error::InsufficientCredits(e)
            | Error::NotFound(e)
            | Error::RateLimit(e)
            | Error::Validation(e)
            | Error::ServerRestarting(e)
            | Error::Connection(e)
            | Error::Api(e) => e,
        }
    }

    /// The human-readable message, without the request id.
    pub fn message(&self) -> &str {
        &self.details().message
    }

    /// HTTP status, when known.
    pub fn status_code(&self) -> Option<u16> {
        self.details().status_code
    }

    /// The server's machine-readable `error_code`, when known.
    pub fn error_code(&self) -> Option<&str> {
        self.details().error_code.as_deref()
    }

    /// The server's correlation id for this call, when known.
    pub fn request_id(&self) -> Option<&str> {
        self.details().request_id.as_deref()
    }

    /// Seconds to wait before retrying, when the server said so.
    pub fn retry_after(&self) -> Option<u64> {
        self.details().retry_after
    }

    /// True for [`Error::Connection`] and [`Error::ServerRestarting`].
    pub fn is_connection(&self) -> bool {
        matches!(self, Error::Connection(_) | Error::ServerRestarting(_))
    }

    /// The Python SDK's class name for this error, used as the diagnostics
    /// `kugel.error_type` so every SDK reports the same vocabulary.
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Error::Authentication(_) => "AuthenticationError",
            Error::InsufficientCredits(_) => "InsufficientCreditsError",
            Error::NotFound(_) => "NotFoundError",
            Error::RateLimit(_) => "RateLimitError",
            Error::Validation(_) => "ValidationError",
            Error::ServerRestarting(_) => "ServerRestartingError",
            Error::Connection(_) => "ConnectionError",
            Error::Api(_) => "KugelAudioError",
        }
    }

    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Error::Validation(ApiError::new(message).default_code(ERROR_CODE_VALIDATION))
    }

    pub(crate) fn connection(message: impl Into<String>) -> Self {
        Error::Connection(ApiError::new(message).default_status(503))
    }

    pub(crate) fn api(message: impl Into<String>) -> Self {
        Error::Api(ApiError::new(message))
    }

    pub(crate) fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.details_mut().source = Some(source.into());
        self
    }

    pub(crate) fn server_restarting() -> Self {
        Error::ServerRestarting(
            ApiError::new(
                "KugelAudio replica is restarting (rolling deploy). Reconnect and resend \
                 the current turn; audio already received is complete up to the last chunk.",
            )
            .default_status(503)
            .retry_after(Some(1)),
        )
    }
}

/// Pick the variant for a status / `error_code` pair (Python `_build`).
pub(crate) fn build(
    status: Option<u16>,
    error_code: Option<String>,
    message: &str,
    request_id: Option<String>,
    retry_after: Option<u64>,
) -> Error {
    let code = error_code.as_deref();
    let base = ApiError::new(message)
        .status(status)
        .code(error_code.clone())
        .request_id(request_id)
        .retry_after(retry_after);
    let text = |fallback: String| {
        if message.is_empty() {
            fallback
        } else {
            message.to_string()
        }
    };

    if code == Some(ERROR_CODE_UNAUTHORIZED) || status == Some(401) {
        let message = text(format!(
            "KugelAudio rejected the API key. Check it is current at {API_KEYS_URL}."
        ));
        return Error::Authentication(
            ApiError { message, ..base }
                .default_status(401)
                .default_code(ERROR_CODE_UNAUTHORIZED),
        );
    }
    if code == Some(ERROR_CODE_INSUFFICIENT_CREDITS) || status == Some(402) {
        let message = text(format!(
            "Your KugelAudio account is out of credits. Top up at {BILLING_URL}."
        ));
        return Error::InsufficientCredits(
            ApiError { message, ..base }
                .default_status(402)
                .default_code(ERROR_CODE_INSUFFICIENT_CREDITS),
        );
    }
    if matches!(
        code,
        Some(ERROR_CODE_RATE_LIMITED | ERROR_CODE_TOO_MANY_CONTEXTS)
    ) || status == Some(429)
    {
        let message = text(match retry_after {
            Some(seconds) if seconds > 0 => {
                format!("KugelAudio rate limit hit; retry after {seconds}s.")
            }
            _ => "KugelAudio rate limit hit; retry shortly.".to_string(),
        });
        return Error::RateLimit(
            ApiError { message, ..base }
                .default_status(429)
                .default_code(ERROR_CODE_RATE_LIMITED),
        );
    }
    if matches!(
        code,
        Some(ERROR_CODE_VALIDATION | ERROR_CODE_MISSING_VOICE_ID)
    ) || status == Some(400)
    {
        let message = text("Request validation failed.".to_string());
        return Error::Validation(
            ApiError { message, ..base }
                .default_status(400)
                .default_code(ERROR_CODE_VALIDATION),
        );
    }
    if code == Some(ERROR_CODE_MODEL_UNAVAILABLE) || status == Some(503) {
        let detail = text("service temporarily unavailable".to_string());
        let message = format!("KugelAudio is temporarily unavailable: {detail}. Retry shortly.");
        return Error::Connection(ApiError { message, ..base }.default_status(503));
    }
    if code == Some(ERROR_CODE_NOT_FOUND) || status == Some(404) {
        let message = text("Not found.".to_string());
        return Error::NotFound(
            ApiError { message, ..base }
                .default_status(404)
                .default_code(ERROR_CODE_NOT_FOUND),
        );
    }
    let message = text(match status {
        Some(s) => format!("HTTP {s}"),
        None => "KugelAudio request failed.".to_string(),
    });
    Error::Api(ApiError { message, ..base })
}

/// A header lookup that works for both reqwest and tungstenite header maps.
pub(crate) fn header_value(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// The typed error for an HTTP error response (Python `classify_http_response`).
///
/// Reads the structured `{error, error_code, retry_after}` body, falls back to
/// FastAPI's `detail` field, and finally to the raw body text.
pub(crate) fn classify_http(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
) -> Error {
    let mut error_code = None;
    let mut message: Option<String> = None;
    let mut retry_after = None;
    if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(body) {
        error_code = map
            .get("error_code")
            .and_then(Value::as_str)
            .map(str::to_string);
        message = match map
            .get("error")
            .filter(|v| !v.is_null())
            .or(map.get("detail"))
        {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Array(items)) => Some(
                items
                    .iter()
                    .map(|item| match item {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
            Some(Value::Null) | None => None,
            Some(other) => Some(other.to_string()),
        };
        retry_after = map.get("retry_after").and_then(Value::as_u64);
    }
    if retry_after.is_none() {
        retry_after = header_value(headers, "retry-after").and_then(|v| v.parse().ok());
    }
    let request_id = header_value(headers, "x-request-id");
    let message = message
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| String::from_utf8_lossy(body).trim().to_string());
    build(Some(status), error_code, &message, request_id, retry_after)
}

/// The typed error for a server-sent WebSocket error frame
/// (`{error, error_code, code, request_id, retry_after}`).
pub(crate) fn classify_ws_frame(frame: &Value) -> Error {
    let error_code = frame
        .get("error_code")
        .and_then(Value::as_str)
        .map(str::to_string);
    let message = match frame.get("error") {
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => "Server reported an error.".to_string(),
    };
    let status = frame
        .get("code")
        .and_then(Value::as_u64)
        .and_then(|c| u16::try_from(c).ok());
    let retry_after = frame.get("retry_after").and_then(Value::as_u64);
    let request_id = frame
        .get("request_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    build(status, error_code, &message, request_id, retry_after)
}

/// The typed error for a refused WebSocket upgrade.
///
/// The TTS server rejects upgrades with a bad key using HTTP 403, so a 403
/// during a handshake is an [`Error::Authentication`].
pub(crate) fn classify_handshake(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
) -> Error {
    if status == 403 {
        let request_id = header_value(headers, "x-request-id");
        let retry_after = header_value(headers, "retry-after").and_then(|v| v.parse().ok());
        return build(
            None,
            Some(ERROR_CODE_UNAUTHORIZED.to_string()),
            "",
            request_id,
            retry_after,
        )
        .with_status(403);
    }
    classify_http(status, headers, body)
}

impl Error {
    fn with_status(mut self, status: u16) -> Self {
        self.details_mut().status_code = Some(status);
        self
    }
}

/// The typed error for a WebSocket close code and reason (Python `classify_ws_close`).
pub(crate) fn classify_ws_close(code: Option<u16>, reason: &str) -> Error {
    let reason = reason.trim();
    let suffix = if reason.is_empty() {
        String::new()
    } else {
        format!(" ({reason})")
    };
    let mut err = match code {
        Some(WS_CLOSE_UNAUTHORIZED) => Error::Authentication(
            ApiError::new(format!(
                "KugelAudio rejected the API key. Check it is current at {API_KEYS_URL}.{suffix}"
            ))
            .default_status(401)
            .default_code(ERROR_CODE_UNAUTHORIZED),
        ),
        Some(WS_CLOSE_INSUFFICIENT_CREDITS) => build(
            None,
            Some(ERROR_CODE_INSUFFICIENT_CREDITS.to_string()),
            "",
            None,
            None,
        ),
        Some(WS_CLOSE_RATE_LIMITED) => build(
            None,
            Some(ERROR_CODE_RATE_LIMITED.to_string()),
            "",
            None,
            None,
        ),
        Some(WS_CLOSE_MODEL_UNAVAILABLE) => Error::connection(format!(
            "KugelAudio model is temporarily unavailable. Retry shortly.{suffix}"
        )),
        Some(WS_CLOSE_SERVICE_RESTART | WS_CLOSE_TRY_AGAIN_LATER) => Error::server_restarting(),
        _ => {
            let detail = if reason.is_empty() {
                "no reason given"
            } else {
                reason
            };
            let code_text = code.map(|c| format!(" (code {c})")).unwrap_or_default();
            Error::connection(format!(
                "KugelAudio WebSocket closed by server: {detail}{code_text}."
            ))
        }
    };
    err.details_mut().ws_close_code = code;
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(*k, HeaderValue::from_static(v));
        }
        map
    }

    #[test]
    fn http_status_maps_to_variant() {
        let empty = HeaderMap::new();
        assert!(matches!(
            classify_http(401, &empty, b""),
            Error::Authentication(_)
        ));
        assert!(matches!(
            classify_http(402, &empty, b""),
            Error::InsufficientCredits(_)
        ));
        assert!(matches!(
            classify_http(404, &empty, b""),
            Error::NotFound(_)
        ));
        assert!(matches!(
            classify_http(429, &empty, b""),
            Error::RateLimit(_)
        ));
        assert!(matches!(
            classify_http(400, &empty, b""),
            Error::Validation(_)
        ));
        assert!(matches!(
            classify_http(503, &empty, b""),
            Error::Connection(_)
        ));
        assert!(matches!(classify_http(500, &empty, b"boom"), Error::Api(_)));
        assert!(matches!(classify_http(403, &empty, b""), Error::Api(_)));
    }

    #[test]
    fn error_code_wins_over_status() {
        let body = br#"{"error":"Too many concurrent contexts","error_code":"TOO_MANY_CONTEXTS","code":429}"#;
        let err = classify_http(200, &HeaderMap::new(), body);
        assert!(matches!(err, Error::RateLimit(_)));
        assert_eq!(err.error_code(), Some("TOO_MANY_CONTEXTS"));
        let body = br#"{"error":"voice_id is required","error_code":"MISSING_VOICE_ID"}"#;
        let err = classify_http(422, &HeaderMap::new(), body);
        assert!(matches!(err, Error::Validation(_)));
        assert_eq!(err.message(), "voice_id is required");
    }

    #[test]
    fn http_reads_request_id_retry_after_and_detail() {
        let h = headers(&[("x-request-id", "abc123"), ("retry-after", "7")]);
        let err = classify_http(429, &h, br#"{"detail":"slow down"}"#);
        assert_eq!(err.request_id(), Some("abc123"));
        assert_eq!(err.retry_after(), Some(7));
        assert_eq!(err.to_string(), "slow down (request_id: abc123)");
        let err = classify_http(422, &HeaderMap::new(), br#"{"detail":["a","b"]}"#);
        assert_eq!(err.message(), "a; b");
    }

    #[test]
    fn http_defaults_give_actionable_messages() {
        let err = classify_http(401, &HeaderMap::new(), b"");
        assert!(err.message().contains("www.kugelaudio.com/dashboard"));
        assert_eq!(err.error_code(), Some(ERROR_CODE_UNAUTHORIZED));
        let err = classify_http(
            503,
            &HeaderMap::new(),
            br#"{"error":"busy","error_code":"MODEL_UNAVAILABLE"}"#,
        );
        assert_eq!(
            err.message(),
            "KugelAudio is temporarily unavailable: busy. Retry shortly."
        );
        let err = classify_http(429, &headers(&[("retry-after", "3")]), b"");
        assert_eq!(err.message(), "KugelAudio rate limit hit; retry after 3s.");
    }

    #[test]
    fn ws_frame_maps_fields() {
        let frame = serde_json::json!({
            "error": "Rate limit exceeded", "error_code": "RATE_LIMITED",
            "code": 429, "request_id": "r-1", "retry_after": 4
        });
        let err = classify_ws_frame(&frame);
        assert!(matches!(err, Error::RateLimit(_)));
        assert_eq!(err.request_id(), Some("r-1"));
        assert_eq!(err.retry_after(), Some(4));
        assert_eq!(err.status_code(), Some(429));
        let err = classify_ws_frame(
            &serde_json::json!({"error": "x", "error_code": "INTERNAL_ERROR", "code": 500}),
        );
        assert!(matches!(err, Error::Api(_)));
    }

    #[test]
    fn handshake_403_is_authentication() {
        let h = headers(&[("x-request-id", "hs-1")]);
        let err = classify_handshake(403, &h, b"Forbidden");
        assert!(matches!(err, Error::Authentication(_)));
        assert_eq!(err.status_code(), Some(403));
        assert_eq!(err.request_id(), Some("hs-1"));
        assert!(matches!(
            classify_handshake(429, &HeaderMap::new(), b""),
            Error::RateLimit(_)
        ));
    }

    #[test]
    fn close_codes_map_to_variants() {
        assert!(matches!(
            classify_ws_close(Some(4001), ""),
            Error::Authentication(_)
        ));
        assert!(matches!(
            classify_ws_close(Some(4003), ""),
            Error::InsufficientCredits(_)
        ));
        assert!(matches!(
            classify_ws_close(Some(4029), ""),
            Error::RateLimit(_)
        ));
        assert!(matches!(
            classify_ws_close(Some(4500), ""),
            Error::Connection(_)
        ));
        let restart = classify_ws_close(Some(1012), "");
        assert!(matches!(restart, Error::ServerRestarting(_)));
        assert!(restart.is_connection());
        assert_eq!(restart.retry_after(), Some(1));
        assert_eq!(restart.details().ws_close_code, Some(1012));
        assert!(matches!(
            classify_ws_close(Some(1013), ""),
            Error::ServerRestarting(_)
        ));
        let generic = classify_ws_close(Some(1011), "boom");
        assert_eq!(
            generic.message(),
            "KugelAudio WebSocket closed by server: boom (code 1011)."
        );
    }
}

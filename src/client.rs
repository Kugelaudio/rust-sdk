//! The client, its configuration, region resolution and the shared HTTP layer.

use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::diagnostics::config::ENV_TELEMETRY;
use crate::diagnostics::{
    Diagnostics, DiagnosticsConfig, HttpSender, OP_DICTIONARIES, OP_MODELS, OP_VOICES,
    TRANSPORT_HTTP,
};
use crate::error::{classify_http, header_value, Error, Result};
use crate::metadata::sdk_headers;
use crate::resources::{
    DictionariesResource, EnhanceResource, ModelsResource, TtsResource, VoicesResource,
};
use crate::streaming::ws::WsConn;

/// The canonical, geo-routed API.
pub const DEFAULT_API_URL: &str = "https://api.kugelaudio.com";
/// The direct EU endpoint.
pub const EU_API_URL: &str = "https://api.eu.kugelaudio.com";
/// Default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
/// Default interval of keepalive pings on the pooled `/ws/tts` socket.
pub const DEFAULT_KEEPALIVE_PING_INTERVAL: Duration = Duration::from_secs(20);
/// Idle pooled HTTP connections stay open this long (under the 60 s idle
/// timeout of the load balancers in front of the API).
const HTTP_KEEPALIVE: Duration = Duration::from_secs(50);

/// A deployment region hint.
///
/// Only [`Region::Eu`] has a dedicated endpoint (`api.eu.kugelaudio.com`);
/// `us` and `global` are accepted compatibility hints for the canonical API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    /// The direct EU endpoint.
    Eu,
    /// Compatibility hint: the canonical geo-routed API.
    Us,
    /// Compatibility hint: the canonical geo-routed API.
    Global,
}

impl Region {
    /// The hint as written in a key prefix (`eu`, `us`, `global`).
    pub fn as_str(self) -> &'static str {
        match self {
            Region::Eu => "eu",
            Region::Us => "us",
            Region::Global => "global",
        }
    }

    /// The API base URL this hint selects.
    pub fn api_url(self) -> &'static str {
        match self {
            Region::Eu => EU_API_URL,
            Region::Us | Region::Global => DEFAULT_API_URL,
        }
    }

    const ALL: [Region; 3] = [Region::Eu, Region::Us, Region::Global];
}

impl FromStr for Region {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Region::ALL
            .into_iter()
            .find(|region| region.as_str() == value)
            .ok_or_else(|| {
                Error::validation(format!(
                    "Invalid region '{value}'. Must be one of: eu, us, global"
                ))
            })
    }
}

/// Strip a region prefix (`eu-`, `us-`, `global-`) from an API key.
///
/// Returns the key to authenticate with and the region the prefix selected.
/// Project keys (`sk-kug-proj_...`) and every other key without a prefix are
/// returned unchanged.
pub fn parse_api_key(api_key: &str) -> (String, Option<Region>) {
    for region in Region::ALL {
        if let Some(rest) = api_key
            .strip_prefix(region.as_str())
            .and_then(|r| r.strip_prefix('-'))
        {
            return (rest.to_string(), Some(region));
        }
    }
    (api_key.to_string(), None)
}

/// Resolve the API base URL: an explicit URL wins, then the region option,
/// then the key prefix, then the canonical endpoint.
pub(crate) fn resolve_api_url(
    api_url: Option<&str>,
    region: Option<Region>,
    key_region: Option<Region>,
) -> String {
    match api_url {
        Some(url) => url.trim_end_matches('/').to_string(),
        None => region
            .or(key_region)
            .map(Region::api_url)
            .unwrap_or(DEFAULT_API_URL)
            .to_string(),
    }
}

/// Builder for [`KugelAudio`].
///
/// ```no_run
/// use std::time::Duration;
/// use kugelaudio::{KugelAudio, Region};
///
/// # fn demo() -> kugelaudio::Result<()> {
/// let client = KugelAudio::builder(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())
///     .region(Region::Eu)
///     .timeout(Duration::from_secs(30))
///     .telemetry(false)
///     .build()?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct KugelAudioBuilder {
    api_key: String,
    api_url: Option<String>,
    tts_url: Option<String>,
    region: Option<Region>,
    timeout: Duration,
    keepalive_ping_interval: Option<Duration>,
    telemetry: Option<bool>,
}

impl fmt::Debug for KugelAudioBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KugelAudioBuilder")
            .field("api_url", &self.api_url)
            .field("tts_url", &self.tts_url)
            .field("region", &self.region)
            .field("timeout", &self.timeout)
            .field("keepalive_ping_interval", &self.keepalive_ping_interval)
            .field("telemetry", &self.telemetry)
            .finish_non_exhaustive()
    }
}

impl KugelAudioBuilder {
    /// API base URL. Takes precedence over [`region`](Self::region) and any
    /// key prefix (default `https://api.kugelaudio.com`).
    pub fn api_url(mut self, api_url: impl Into<String>) -> Self {
        self.api_url = Some(api_url.into());
        self
    }

    /// Base URL for the TTS WebSockets (default: the API URL, which proxies them).
    pub fn tts_url(mut self, tts_url: impl Into<String>) -> Self {
        self.tts_url = Some(tts_url.into());
        self
    }

    /// Region hint. Takes precedence over a key prefix, not over [`api_url`](Self::api_url).
    pub fn region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    /// Request timeout, also used for WebSocket handshakes (default 60 s).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Interval of keepalive pings on the pooled `/ws/tts` socket (default
    /// 20 s); `None` disables them.
    pub fn keepalive_ping_interval(mut self, interval: Option<Duration>) -> Self {
        self.keepalive_ping_interval = interval;
        self
    }

    /// Opt in or out of anonymous client-error diagnostics. Unset, they are on
    /// only for the hosted `*.kugelaudio.com` API. The `KUGELAUDIO_TELEMETRY`
    /// environment variable (`1/true/on/yes` or `0/false/off/no`) overrides
    /// this in both directions. Reports never contain text, audio, URLs, keys
    /// or error messages.
    pub fn telemetry(mut self, enabled: bool) -> Self {
        self.telemetry = Some(enabled);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<KugelAudio> {
        if self.api_key.trim().is_empty() {
            return Err(Error::validation(
                "KugelAudio API key is missing. Pass it to KugelAudio::new(..). Get a key at \
                 https://app.kugelaudio.com/settings/api-keys.",
            ));
        }
        let (api_key, key_region) = parse_api_key(&self.api_key);
        let api_url = resolve_api_url(self.api_url.as_deref(), self.region, key_region);
        let tts_url = self
            .tts_url
            .as_deref()
            .unwrap_or(&api_url)
            .trim_end_matches('/')
            .to_string();
        let auth = auth_headers(&api_key);
        let mut headers = HeaderMap::new();
        for (name, value) in auth.iter().cloned().chain(
            sdk_headers()
                .into_iter()
                .map(|(name, value)| (name.to_string(), value)),
        ) {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| Error::validation(format!("Invalid header name: {e}")))?;
            let value = HeaderValue::from_str(&value).map_err(|_| {
                Error::validation("The API key contains characters not allowed in an HTTP header.")
            })?;
            headers.insert(name, value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(self.timeout)
            .pool_idle_timeout(HTTP_KEEPALIVE)
            .build()
            .map_err(|e| {
                Error::api(format!("Could not build the HTTP client: {e}")).with_source(e)
            })?;
        let env = std::env::var(ENV_TELEMETRY).ok();
        let config = DiagnosticsConfig::resolve(&api_url, self.telemetry, env.as_deref(), auth);
        let diagnostics = Diagnostics::new(config, Arc::new(HttpSender(http.clone())));
        Ok(KugelAudio {
            inner: Arc::new(ClientInner {
                api_key,
                api_url,
                tts_url,
                timeout: self.timeout,
                keepalive: self.keepalive_ping_interval,
                http,
                diagnostics,
                tts_pool: Mutex::new(None),
            }),
        })
    }
}

/// The SDK's auth headers. The HTTP client and the diagnostics reporter
/// present the same credential.
fn auth_headers(api_key: &str) -> Vec<(String, String)> {
    vec![
        ("Authorization".to_string(), format!("Bearer {api_key}")),
        ("X-API-Key".to_string(), api_key.to_string()),
    ]
}

/// The KugelAudio API client. Cheap to clone: clones share connections.
///
/// ```no_run
/// use kugelaudio::{KugelAudio, TtsRequest};
///
/// # async fn demo() -> kugelaudio::Result<()> {
/// let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;
/// let voices = client.voices().list(&Default::default()).await?;
/// let audio = client
///     .tts()
///     .generate(&TtsRequest::new("Hello from KugelAudio!").voice_id(voices.voices[0].id))
///     .await?;
/// audio.save_wav("hello.wav")?;
/// client.close().await;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct KugelAudio {
    pub(crate) inner: Arc<ClientInner>,
}

impl fmt::Debug for KugelAudio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KugelAudio")
            .field("api_url", &self.inner.api_url)
            .field("tts_url", &self.inner.tts_url)
            .field("telemetry", &self.inner.diagnostics.enabled())
            .finish_non_exhaustive()
    }
}

impl KugelAudio {
    /// A client with default options. An `eu-` key prefix selects the EU
    /// endpoint and is stripped before authenticating.
    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::builder(api_key).build()
    }

    /// A builder for a client with custom options.
    pub fn builder(api_key: impl Into<String>) -> KugelAudioBuilder {
        KugelAudioBuilder {
            api_key: api_key.into(),
            api_url: None,
            tts_url: None,
            region: None,
            timeout: DEFAULT_TIMEOUT,
            keepalive_ping_interval: Some(DEFAULT_KEEPALIVE_PING_INTERVAL),
            telemetry: None,
        }
    }

    /// The effective API base URL.
    pub fn api_url(&self) -> &str {
        &self.inner.api_url
    }

    /// The effective base URL of the TTS WebSockets.
    pub fn tts_url(&self) -> &str {
        &self.inner.tts_url
    }

    /// The model catalog.
    pub fn models(&self) -> ModelsResource<'_> {
        ModelsResource::new(self)
    }

    /// Voice listing and management.
    pub fn voices(&self) -> VoicesResource<'_> {
        VoicesResource::new(self)
    }

    /// Pronunciation dictionaries.
    pub fn dictionaries(&self) -> DictionariesResource<'_> {
        DictionariesResource::new(self)
    }

    /// Text to speech: one-shot, streamed and session-based.
    pub fn tts(&self) -> TtsResource<'_> {
        TtsResource::new(self)
    }

    /// Speech enhancement: noise removal and target speaker extraction.
    pub fn enhance(&self) -> EnhanceResource<'_> {
        EnhanceResource::new(self)
    }

    /// Close the pooled WebSocket and deliver pending diagnostics (waiting at
    /// most one second). Sessions you created close on their own `close()`.
    /// The client stays usable; a later request reconnects.
    pub async fn close(&self) {
        let pooled = self.inner.take_pooled();
        if let Some(pooled) = pooled {
            pooled.conn.close().await;
        }
        self.inner
            .diagnostics
            .close(crate::diagnostics::CLOSE_DEADLINE)
            .await;
    }
}

/// The `/ws/tts` socket kept for the next request.
pub(crate) struct PooledWs {
    pub model_id: String,
    pub conn: WsConn,
}

pub(crate) struct ClientInner {
    pub api_key: String,
    pub api_url: String,
    pub tts_url: String,
    pub timeout: Duration,
    pub keepalive: Option<Duration>,
    pub http: reqwest::Client,
    pub diagnostics: Diagnostics,
    tts_pool: Mutex<Option<PooledWs>>,
}

/// `kugel.operation` for a REST path: `/v1/voices/12/references` -> `voices`.
fn operation_for_path(path: &str) -> Option<&'static str> {
    let trimmed = path.trim_start_matches('/');
    let trimmed = trimmed.strip_prefix("v1/").unwrap_or(trimmed);
    match trimmed.split('/').next() {
        Some("models") => Some(OP_MODELS),
        Some("voices") => Some(OP_VOICES),
        Some("dictionaries") => Some(OP_DICTIONARIES),
        _ => None,
    }
}

impl ClientInner {
    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_url, path.trim_start_matches('/'))
    }

    pub fn take_pooled(&self) -> Option<PooledWs> {
        // KEEP-JUSTIFIED: a poisoned lock still holds a valid Option.
        self.tts_pool
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    }

    /// Keep `conn` for the next request; returns the socket that could not be
    /// kept (the pool already holds one), for the caller to close.
    pub fn put_pooled(&self, pooled: PooledWs) -> Option<PooledWs> {
        let mut slot = self.tts_pool.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Some(pooled);
        }
        *slot = Some(pooled);
        None
    }

    pub fn has_open_pooled(&self) -> bool {
        self.tts_pool
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|p| p.conn.is_open())
    }

    /// Send a request and classify transport failures and error statuses.
    pub async fn send(
        &self,
        method: &Method,
        path: &str,
        request: RequestBuilder,
    ) -> Result<Response> {
        let response = request.send().await.map_err(|e| {
            if e.is_timeout() {
                Error::connection(format!(
                    "Request to {method} {path} timed out after {}s.",
                    self.timeout.as_secs_f64()
                ))
                .with_source(e)
            } else {
                Error::connection(format!(
                    "Could not reach KugelAudio at {}: {e}. Check network connectivity.",
                    self.url(path)
                ))
                .with_source(e)
            }
        })?;
        let status = response.status().as_u16();
        if status >= 400 {
            let headers = response.headers().clone();
            let body = response
                .bytes()
                .await
                .map(|b| b.to_vec())
                .unwrap_or_default();
            return Err(classify_http(status, &headers, &body));
        }
        Ok(response)
    }

    /// One REST call returning the raw response body, wrapped in a
    /// diagnostics operation named after the path.
    async fn call(
        &self,
        method: Method,
        path: &str,
        build: impl FnOnce(RequestBuilder) -> RequestBuilder,
    ) -> Result<(Vec<u8>, Option<String>)> {
        let mut op = self
            .diagnostics
            .operation(operation_for_path(path), TRANSPORT_HTTP);
        let request = build(self.http.request(method.clone(), self.url(path)));
        let outcome = async {
            let response = self.send(&method, path, request).await?;
            let request_id = header_value(response.headers(), "x-request-id");
            let body = response.bytes().await.map_err(|e| {
                Error::connection(format!(
                    "Reading the response to {method} {path} failed: {e}."
                ))
                .with_source(e)
            })?;
            Ok((body.to_vec(), request_id))
        }
        .await;
        match &outcome {
            Ok(_) => op.succeed(),
            Err(e) => op.fail(e),
        }
        outcome
    }

    /// A JSON REST call.
    pub async fn json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<T> {
        let (bytes, request_id) = self
            .call(method.clone(), path, |mut request| {
                if !query.is_empty() {
                    request = request.query(query);
                }
                if let Some(body) = body {
                    request = request.json(body);
                }
                request
            })
            .await?;
        decode(&method, path, &bytes, request_id)
    }

    /// A REST call whose response body is ignored (DELETE).
    pub async fn empty(&self, method: Method, path: &str, query: &[(&str, String)]) -> Result<()> {
        self.call(method, path, |request| {
            if query.is_empty() {
                request
            } else {
                request.query(query)
            }
        })
        .await
        .map(|_| ())
    }

    /// A multipart REST call returning JSON.
    pub async fn multipart<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        form: reqwest::multipart::Form,
    ) -> Result<T> {
        let (bytes, request_id) = self
            .call(method.clone(), path, |request| request.multipart(form))
            .await?;
        decode(&method, path, &bytes, request_id)
    }
}

fn decode<T: DeserializeOwned>(
    method: &Method,
    path: &str,
    bytes: &[u8],
    request_id: Option<String>,
) -> Result<T> {
    let bytes: &[u8] = if bytes.is_empty() { b"null" } else { bytes };
    serde_json::from_slice(bytes).map_err(|e| {
        Error::Api(
            crate::error::ApiError::new(format!(
                "Unexpected response to {method} {path} from KugelAudio: {e}"
            ))
            .request_id(request_id),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_prefix_selects_region_and_is_stripped() {
        assert_eq!(
            parse_api_key("eu-abc"),
            ("abc".to_string(), Some(Region::Eu))
        );
        assert_eq!(
            parse_api_key("global-abc"),
            ("abc".to_string(), Some(Region::Global))
        );
        assert_eq!(
            parse_api_key("sk-kug-proj_abc123"),
            ("sk-kug-proj_abc123".to_string(), None)
        );
        assert_eq!(
            parse_api_key("eu-sk-kug-proj_abc123"),
            ("sk-kug-proj_abc123".to_string(), Some(Region::Eu))
        );
        assert_eq!(parse_api_key("euabc"), ("euabc".to_string(), None));
    }

    #[test]
    fn region_url_priority() {
        assert_eq!(resolve_api_url(None, None, None), DEFAULT_API_URL);
        assert_eq!(resolve_api_url(None, None, Some(Region::Eu)), EU_API_URL);
        assert_eq!(
            resolve_api_url(None, Some(Region::Us), Some(Region::Eu)),
            DEFAULT_API_URL
        );
        assert_eq!(resolve_api_url(None, Some(Region::Eu), None), EU_API_URL);
        assert_eq!(
            resolve_api_url(
                Some("http://localhost:9000/"),
                Some(Region::Eu),
                Some(Region::Eu)
            ),
            "http://localhost:9000"
        );
        assert_eq!(Region::Global.api_url(), DEFAULT_API_URL);
    }

    #[test]
    fn region_from_str_validates() {
        assert_eq!("eu".parse::<Region>().unwrap(), Region::Eu);
        let err = "mars".parse::<Region>().unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
    }

    #[test]
    fn builder_rejects_empty_key_and_resolves_urls() {
        assert!(matches!(KugelAudio::new("  "), Err(Error::Validation(_))));
        let client = KugelAudio::builder("eu-key")
            .telemetry(false)
            .build()
            .unwrap();
        assert_eq!(client.api_url(), EU_API_URL);
        assert_eq!(client.tts_url(), EU_API_URL);
        assert_eq!(client.inner.api_key, "key");
        let client = KugelAudio::builder("key")
            .api_url("http://localhost:1/")
            .tts_url("http://localhost:2/")
            .build()
            .unwrap();
        assert_eq!(client.api_url(), "http://localhost:1");
        assert_eq!(client.tts_url(), "http://localhost:2");
    }

    #[test]
    fn operation_names_follow_the_path() {
        assert_eq!(
            operation_for_path("/v1/voices/12/references"),
            Some(OP_VOICES)
        );
        assert_eq!(operation_for_path("/v1/models"), Some(OP_MODELS));
        assert_eq!(
            operation_for_path("/v1/dictionaries/1/entries"),
            Some(OP_DICTIONARIES)
        );
        assert_eq!(operation_for_path("/v1/tts/generate"), None);
    }
}

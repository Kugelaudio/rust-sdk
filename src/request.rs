//! Generation parameters for REST, `/ws/tts`, `/ws/tts/stream` and `/ws/tts/multi`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::audio::{OutputFormat, DEFAULT_SAMPLE_RATE};
use crate::error::{Error, Result};
use crate::models::VoiceId;

/// The default model.
pub const DEFAULT_MODEL: &str = "kugel-3.5";
/// Lowest accepted classifier-free guidance scale.
pub const MIN_CFG_SCALE: f64 = 1.2;
/// Highest accepted classifier-free guidance scale.
pub const MAX_CFG_SCALE: f64 = 2.5;

/// Clamp `cfg_scale` into `[1.2, 2.5]`, as the server does.
pub fn clamp_cfg_scale(cfg_scale: f64) -> f64 {
    cfg_scale.clamp(MIN_CFG_SCALE, MAX_CFG_SCALE)
}

static LANGUAGE_WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Log once per process when normalization runs without an explicit language.
pub(crate) fn warn_if_no_language(language: Option<&str>, normalize: bool) {
    use std::sync::atomic::Ordering;
    if language.is_none() && normalize && !LANGUAGE_WARNED.swap(true, Ordering::SeqCst) {
        tracing::warn!(
            "No language set with normalization enabled: the server normalizes in the voice's \
             primary language (English if it has none). Set language (e.g. \"de\") when the \
             text is in another language."
        );
    }
}

fn insert(map: &mut Map<String, Value>, key: &str, value: impl Serialize) {
    if let Ok(value) = serde_json::to_value(value) {
        map.insert(key.to_string(), value);
    }
}

/// One text-to-speech request, for [`TtsResource::generate`](crate::TtsResource::generate)
/// (REST) and [`TtsResource::stream`](crate::TtsResource::stream) (WebSocket).
///
/// ```
/// use kugelaudio::{OutputFormat, TtsRequest};
///
/// let request = TtsRequest::new("Your code is 4 8 1 5.")
///     .voice_id(1071)
///     .language("en")
///     .output_format(OutputFormat::Ulaw8000);
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TtsRequest {
    /// Text to synthesize (2 to 10,000 characters for a one-shot request).
    pub text: String,
    /// Model id (default `kugel-3.5`).
    pub model_id: String,
    /// Voice to use. Required by the API: there is no default voice.
    pub voice_id: Option<VoiceId>,
    /// Classifier-free guidance scale, clamped to `[1.2, 2.5]` (default 2.0).
    pub cfg_scale: f64,
    /// Sampling variance in `[0.0, 1.0]`; `None` uses the engine default.
    pub temperature: Option<f64>,
    /// Maximum tokens to generate, 1-2048 (default 2048).
    pub max_new_tokens: u32,
    /// Output sample rate (default 24000); see [`SUPPORTED_SAMPLE_RATES`](crate::SUPPORTED_SAMPLE_RATES).
    pub sample_rate: u32,
    /// Combined codec + rate token; authoritative when set.
    pub output_format: Option<OutputFormat>,
    /// Text normalization (numbers, dates, symbols to words; default true).
    pub normalize: bool,
    /// ISO 639-1 language for normalization. Without it the server uses the
    /// voice's primary language; it does not detect the language from the text.
    pub language: Option<String>,
    /// Word-level timestamps. WebSocket only: [`TtsResource::generate`](crate::TtsResource::generate)
    /// rejects a request with this set; use [`TtsResource::stream`](crate::TtsResource::stream).
    pub word_timestamps: bool,
    /// Playback speed multiplier, 0.8-1.2 (default 1.0), pitch-preserving.
    pub speed: f64,
    /// Dictionary selection: `None` applies every active dictionary of the
    /// project, `Some(vec![])` none, a list exactly those.
    pub dictionary_ids: Option<Vec<i64>>,
    /// Project whose dictionaries apply; `None` uses the API key's project.
    pub project_id: Option<i64>,
}

impl TtsRequest {
    /// A request for `text` with every other parameter at its default.
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            model_id: DEFAULT_MODEL.to_string(),
            voice_id: None,
            cfg_scale: 2.0,
            temperature: None,
            max_new_tokens: 2048,
            sample_rate: DEFAULT_SAMPLE_RATE,
            output_format: None,
            normalize: true,
            language: None,
            word_timestamps: false,
            speed: 1.0,
            dictionary_ids: None,
            project_id: None,
        }
    }

    /// Set the model.
    pub fn model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = model_id.into();
        self
    }

    /// Set the voice (numeric id or handle).
    pub fn voice_id(mut self, voice_id: impl Into<VoiceId>) -> Self {
        self.voice_id = Some(voice_id.into());
        self
    }

    /// Set the classifier-free guidance scale (clamped to `[1.2, 2.5]`).
    pub fn cfg_scale(mut self, cfg_scale: f64) -> Self {
        self.cfg_scale = cfg_scale;
        self
    }

    /// Set the sampling temperature.
    pub fn temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set the maximum number of generated tokens.
    pub fn max_new_tokens(mut self, max_new_tokens: u32) -> Self {
        self.max_new_tokens = max_new_tokens;
        self
    }

    /// Set the output sample rate.
    pub fn sample_rate(mut self, sample_rate: u32) -> Self {
        self.sample_rate = sample_rate;
        self
    }

    /// Set the combined codec + rate token.
    pub fn output_format(mut self, output_format: OutputFormat) -> Self {
        self.output_format = Some(output_format);
        self
    }

    /// Enable or disable text normalization.
    pub fn normalize(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }

    /// Set the normalization language (ISO 639-1, e.g. `de`).
    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Request word timestamps (WebSocket streaming only).
    pub fn word_timestamps(mut self, word_timestamps: bool) -> Self {
        self.word_timestamps = word_timestamps;
        self
    }

    /// Set the speed multiplier.
    pub fn speed(mut self, speed: f64) -> Self {
        self.speed = speed;
        self
    }

    /// Select dictionaries (`vec![]` disables them for this request).
    pub fn dictionary_ids(mut self, dictionary_ids: Vec<i64>) -> Self {
        self.dictionary_ids = Some(dictionary_ids);
        self
    }

    /// Set the project whose dictionaries apply.
    pub fn project_id(mut self, project_id: i64) -> Self {
        self.project_id = Some(project_id);
        self
    }

    /// The sample rate the response will have.
    pub(crate) fn effective_sample_rate(&self) -> u32 {
        self.output_format
            .map(OutputFormat::sample_rate)
            .unwrap_or(self.sample_rate)
    }

    /// The JSON body. `word_timestamps` is sent only on WebSocket requests:
    /// the REST endpoint validates strictly and rejects the field.
    pub(crate) fn to_json(&self, websocket: bool) -> Value {
        let mut map = Map::new();
        insert(&mut map, "text", &self.text);
        insert(&mut map, "model_id", &self.model_id);
        insert(&mut map, "cfg_scale", clamp_cfg_scale(self.cfg_scale));
        insert(&mut map, "max_new_tokens", self.max_new_tokens);
        insert(&mut map, "sample_rate", self.sample_rate);
        insert(&mut map, "normalize", self.normalize);
        insert(&mut map, "speed", self.speed);
        if websocket {
            insert(&mut map, "word_timestamps", self.word_timestamps);
        }
        if let Some(voice_id) = &self.voice_id {
            insert(&mut map, "voice_id", voice_id);
        }
        if let Some(output_format) = self.output_format {
            insert(&mut map, "output_format", output_format);
        }
        if let Some(language) = &self.language {
            insert(&mut map, "language", language);
        }
        if let Some(temperature) = self.temperature {
            insert(&mut map, "temperature", temperature);
        }
        if let Some(project_id) = self.project_id {
            insert(&mut map, "project_id", project_id);
        }
        // `[]` is an explicit opt-out and must be sent; only `None` is omitted.
        if let Some(ids) = &self.dictionary_ids {
            insert(&mut map, "dictionary_ids", ids);
        }
        Value::Object(map)
    }
}

/// Configuration of a [`StreamingSession`](crate::StreamingSession), sent with
/// the first text of each turn.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct StreamConfig {
    /// Voice to use.
    pub voice_id: Option<VoiceId>,
    /// Model id; `None` uses the server default (`kugel-3.5`).
    pub model_id: Option<String>,
    /// Classifier-free guidance scale, clamped to `[1.2, 2.5]` (default 2.0).
    pub cfg_scale: f64,
    /// Sampling variance in `[0.0, 1.0]`; `None` uses the engine default.
    pub temperature: Option<f64>,
    /// Maximum tokens per generation (default 2048).
    pub max_new_tokens: u32,
    /// Output sample rate (default 24000).
    pub sample_rate: u32,
    /// Combined codec + rate token, set once per session.
    pub output_format: Option<OutputFormat>,
    /// Longest wait for the token after an ambiguous period (default 500 ms).
    pub flush_timeout_ms: u32,
    /// Maximum buffered characters before a forced flush (default 1000).
    pub max_buffer_length: u32,
    /// Text normalization (default true).
    pub normalize: bool,
    /// Normalization language (ISO 639-1).
    pub language: Option<String>,
    /// Per-chunk word timestamps.
    pub word_timestamps: bool,
    /// Minimum buffer sizes (characters) before each successive chunk is
    /// synthesized; the last value repeats. `None` uses the server schedule.
    pub chunk_length_schedule: Option<Vec<u32>>,
    /// Start generating at the first clean sentence boundary (lower latency).
    pub auto_mode: Option<bool>,
    /// Speed multiplier, 0.8-1.2 (default 1.0).
    pub speed: f64,
    /// Dictionary selection, sticky for the session.
    pub dictionary_ids: Option<Vec<i64>>,
    /// Project whose dictionaries apply.
    pub project_id: Option<i64>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            voice_id: None,
            model_id: None,
            cfg_scale: 2.0,
            temperature: None,
            max_new_tokens: 2048,
            sample_rate: DEFAULT_SAMPLE_RATE,
            output_format: None,
            flush_timeout_ms: 500,
            max_buffer_length: 1000,
            normalize: true,
            language: None,
            word_timestamps: false,
            chunk_length_schedule: None,
            auto_mode: None,
            speed: 1.0,
            dictionary_ids: None,
            project_id: None,
        }
    }
}

impl StreamConfig {
    /// A configuration for `voice_id` with every other field at its default.
    pub fn new(voice_id: impl Into<VoiceId>) -> Self {
        Self {
            voice_id: Some(voice_id.into()),
            ..Self::default()
        }
    }

    /// Set the model.
    pub fn model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }

    /// Set the voice.
    pub fn voice_id(mut self, voice_id: impl Into<VoiceId>) -> Self {
        self.voice_id = Some(voice_id.into());
        self
    }

    /// Set the classifier-free guidance scale.
    pub fn cfg_scale(mut self, cfg_scale: f64) -> Self {
        self.cfg_scale = cfg_scale;
        self
    }

    /// Set the sampling temperature.
    pub fn temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set the maximum tokens per generation.
    pub fn max_new_tokens(mut self, max_new_tokens: u32) -> Self {
        self.max_new_tokens = max_new_tokens;
        self
    }

    /// Set the output sample rate.
    pub fn sample_rate(mut self, sample_rate: u32) -> Self {
        self.sample_rate = sample_rate;
        self
    }

    /// Set the combined codec + rate token.
    pub fn output_format(mut self, output_format: OutputFormat) -> Self {
        self.output_format = Some(output_format);
        self
    }

    /// Set the ambiguous-period flush timeout.
    pub fn flush_timeout_ms(mut self, flush_timeout_ms: u32) -> Self {
        self.flush_timeout_ms = flush_timeout_ms;
        self
    }

    /// Set the forced-flush buffer length.
    pub fn max_buffer_length(mut self, max_buffer_length: u32) -> Self {
        self.max_buffer_length = max_buffer_length;
        self
    }

    /// Enable or disable normalization.
    pub fn normalize(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }

    /// Set the normalization language.
    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Request word timestamps.
    pub fn word_timestamps(mut self, word_timestamps: bool) -> Self {
        self.word_timestamps = word_timestamps;
        self
    }

    /// Set the chunk length schedule.
    pub fn chunk_length_schedule(mut self, schedule: Vec<u32>) -> Self {
        self.chunk_length_schedule = Some(schedule);
        self
    }

    /// Enable or disable auto mode.
    pub fn auto_mode(mut self, auto_mode: bool) -> Self {
        self.auto_mode = Some(auto_mode);
        self
    }

    /// Set the speed multiplier.
    pub fn speed(mut self, speed: f64) -> Self {
        self.speed = speed;
        self
    }

    /// Select dictionaries.
    pub fn dictionary_ids(mut self, dictionary_ids: Vec<i64>) -> Self {
        self.dictionary_ids = Some(dictionary_ids);
        self
    }

    /// Set the project whose dictionaries apply.
    pub fn project_id(mut self, project_id: i64) -> Self {
        self.project_id = Some(project_id);
        self
    }

    pub(crate) fn to_json(&self) -> Map<String, Value> {
        let mut map = Map::new();
        insert(&mut map, "cfg_scale", clamp_cfg_scale(self.cfg_scale));
        insert(&mut map, "max_new_tokens", self.max_new_tokens);
        insert(&mut map, "sample_rate", self.sample_rate);
        insert(&mut map, "flush_timeout_ms", self.flush_timeout_ms);
        insert(&mut map, "max_buffer_length", self.max_buffer_length);
        insert(&mut map, "normalize", self.normalize);
        insert(&mut map, "word_timestamps", self.word_timestamps);
        insert(&mut map, "speed", self.speed);
        if let Some(v) = &self.voice_id {
            insert(&mut map, "voice_id", v);
        }
        if let Some(v) = &self.model_id {
            insert(&mut map, "model_id", v);
        }
        if let Some(v) = self.output_format {
            insert(&mut map, "output_format", v);
        }
        if let Some(v) = &self.language {
            insert(&mut map, "language", v);
        }
        if let Some(v) = &self.chunk_length_schedule {
            insert(&mut map, "chunk_length_schedule", v);
        }
        if let Some(v) = self.auto_mode {
            insert(&mut map, "auto_mode", v);
        }
        if let Some(v) = self.temperature {
            insert(&mut map, "temperature", v);
        }
        if let Some(v) = self.project_id {
            insert(&mut map, "project_id", v);
        }
        if let Some(v) = &self.dictionary_ids {
            insert(&mut map, "dictionary_ids", v);
        }
        map
    }
}

/// Configuration of a [`MultiContextSession`](crate::MultiContextSession).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MultiContextConfig {
    /// Voice for contexts created without their own voice.
    pub default_voice_id: Option<VoiceId>,
    /// Model id; `None` uses the server default.
    pub model_id: Option<String>,
    /// Output sample rate (default 24000).
    pub sample_rate: u32,
    /// Combined codec + rate token, set once per session.
    pub output_format: Option<OutputFormat>,
    /// Classifier-free guidance scale, clamped to `[1.2, 2.5]` (default 2.0).
    pub cfg_scale: f64,
    /// Sampling variance; `None` uses the engine default.
    pub temperature: Option<f64>,
    /// Maximum tokens per generation (default 2048).
    pub max_new_tokens: u32,
    /// Text normalization (default true).
    pub normalize: bool,
    /// Normalization language.
    pub language: Option<String>,
    /// Dictionary selection for the session.
    pub dictionary_ids: Option<Vec<i64>>,
    /// Project whose dictionaries apply.
    pub project_id: Option<i64>,
    /// Per-chunk word timestamps.
    pub word_timestamps: bool,
}

impl Default for MultiContextConfig {
    fn default() -> Self {
        Self {
            default_voice_id: None,
            model_id: None,
            sample_rate: DEFAULT_SAMPLE_RATE,
            output_format: None,
            cfg_scale: 2.0,
            temperature: None,
            max_new_tokens: 2048,
            normalize: true,
            language: None,
            dictionary_ids: None,
            project_id: None,
            word_timestamps: false,
        }
    }
}

impl MultiContextConfig {
    /// Set the default voice.
    pub fn default_voice_id(mut self, voice_id: impl Into<VoiceId>) -> Self {
        self.default_voice_id = Some(voice_id.into());
        self
    }

    /// Set the model.
    pub fn model_id(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }

    /// Set the output sample rate.
    pub fn sample_rate(mut self, sample_rate: u32) -> Self {
        self.sample_rate = sample_rate;
        self
    }

    /// Set the combined codec + rate token.
    pub fn output_format(mut self, output_format: OutputFormat) -> Self {
        self.output_format = Some(output_format);
        self
    }

    /// Set the classifier-free guidance scale.
    pub fn cfg_scale(mut self, cfg_scale: f64) -> Self {
        self.cfg_scale = cfg_scale;
        self
    }

    /// Set the sampling temperature.
    pub fn temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set the maximum tokens per generation.
    pub fn max_new_tokens(mut self, max_new_tokens: u32) -> Self {
        self.max_new_tokens = max_new_tokens;
        self
    }

    /// Enable or disable normalization.
    pub fn normalize(mut self, normalize: bool) -> Self {
        self.normalize = normalize;
        self
    }

    /// Set the normalization language.
    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Select dictionaries.
    pub fn dictionary_ids(mut self, dictionary_ids: Vec<i64>) -> Self {
        self.dictionary_ids = Some(dictionary_ids);
        self
    }

    /// Set the project whose dictionaries apply.
    pub fn project_id(mut self, project_id: i64) -> Self {
        self.project_id = Some(project_id);
        self
    }

    /// Request word timestamps.
    pub fn word_timestamps(mut self, word_timestamps: bool) -> Self {
        self.word_timestamps = word_timestamps;
        self
    }
}

/// Generation parameters changeable mid-connection with `update_settings`.
/// Identity and audio-format fields are fixed for the connection.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[non_exhaustive]
pub struct SettingsUpdate {
    /// Classifier-free guidance scale.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cfg_scale: Option<f64>,
    /// Sampling variance (0.0-1.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// Speed multiplier (0.8-1.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,
    /// Maximum tokens per generation (1-2048).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_new_tokens: Option<u32>,
    /// Normalization language.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Text normalization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
}

impl SettingsUpdate {
    /// An empty update; set at least one field before sending it.
    pub fn new() -> Self {
        Self::default()
    }

    /// Change the guidance scale.
    pub fn cfg_scale(mut self, value: f64) -> Self {
        self.cfg_scale = Some(value);
        self
    }

    /// Change the temperature.
    pub fn temperature(mut self, value: f64) -> Self {
        self.temperature = Some(value);
        self
    }

    /// Change the speed.
    pub fn speed(mut self, value: f64) -> Self {
        self.speed = Some(value);
        self
    }

    /// Change the token limit.
    pub fn max_new_tokens(mut self, value: u32) -> Self {
        self.max_new_tokens = Some(value);
        self
    }

    /// Change the normalization language.
    pub fn language(mut self, value: impl Into<String>) -> Self {
        self.language = Some(value.into());
        self
    }

    /// Change normalization.
    pub fn normalize(mut self, value: bool) -> Self {
        self.normalize = Some(value);
        self
    }

    /// The `update_settings` body; an empty update is a caller mistake.
    pub(crate) fn to_body(&self) -> Result<Value> {
        let body = serde_json::to_value(self).map_err(|e| Error::validation(e.to_string()))?;
        if body.as_object().is_none_or(Map::is_empty) {
            return Err(Error::validation(
                "update_settings requires at least one parameter to change (one of cfg_scale, \
                 temperature, max_new_tokens, language, normalize, speed)",
            ));
        }
        Ok(body)
    }
}

/// The generation parameters in effect after an `update_settings`, as echoed
/// by the server.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct GenerationSettings {
    /// Classifier-free guidance scale.
    #[serde(default)]
    pub cfg_scale: Option<f64>,
    /// Sampling variance.
    #[serde(default)]
    pub temperature: Option<f64>,
    /// Speed multiplier.
    #[serde(default)]
    pub speed: Option<f64>,
    /// Maximum tokens per generation.
    #[serde(default)]
    pub max_new_tokens: Option<u32>,
    /// Normalization language.
    #[serde(default)]
    pub language: Option<String>,
    /// Text normalization.
    #[serde(default)]
    pub normalize: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_json_matches_the_python_sdk() {
        let body = TtsRequest::new("Hallo Welt")
            .voice_id(1071)
            .cfg_scale(9.0)
            .language("de")
            .dictionary_ids(vec![])
            .to_json(true);
        assert_eq!(body["cfg_scale"], 2.5);
        assert_eq!(body["model_id"], "kugel-3.5");
        assert_eq!(body["voice_id"], 1071);
        assert_eq!(body["dictionary_ids"], serde_json::json!([]));
        assert_eq!(body["word_timestamps"], false);
        assert!(body.get("temperature").is_none());
        assert!(body.get("project_id").is_none());
        let rest = TtsRequest::new("x").to_json(false);
        assert!(
            rest.get("word_timestamps").is_none(),
            "REST rejects the field"
        );
    }

    #[test]
    fn effective_rate_follows_output_format() {
        let request = TtsRequest::new("x").output_format(OutputFormat::Ulaw8000);
        assert_eq!(request.effective_sample_rate(), 8000);
        assert_eq!(
            TtsRequest::new("x")
                .sample_rate(16000)
                .effective_sample_rate(),
            16000
        );
    }

    #[test]
    fn stream_config_json() {
        let map = StreamConfig::new(7)
            .auto_mode(true)
            .chunk_length_schedule(vec![50, 100])
            .to_json();
        assert_eq!(map["voice_id"], 7);
        assert_eq!(map["auto_mode"], true);
        assert_eq!(map["flush_timeout_ms"], 500);
        assert!(!map.contains_key("model_id"));
    }

    #[test]
    fn empty_settings_update_is_rejected() {
        assert!(SettingsUpdate::new().to_body().is_err());
        let body = SettingsUpdate::new().speed(1.1).to_body().unwrap();
        assert_eq!(body, serde_json::json!({"speed": 1.1}));
    }
}

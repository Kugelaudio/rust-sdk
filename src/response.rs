//! Audio, timestamps and usage returned by the TTS endpoints.

use std::path::Path;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::audio::{pcm16_to_f32, wav_bytes, AudioEncoding};
use crate::error::{Error, Result};

/// Accept any JSON number for a millisecond field.
fn de_ms<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<i64, D::Error> {
    let value = f64::deserialize(deserializer)?;
    Ok(value.round() as i64)
}

fn default_score() -> f64 {
    1.0
}

/// One word with its time boundaries, sent when word timestamps are requested.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct WordTimestamp {
    /// The word.
    pub word: String,
    /// Start, in milliseconds from the start of the turn's audio.
    #[serde(deserialize_with = "de_ms")]
    pub start_ms: i64,
    /// End, in milliseconds.
    #[serde(deserialize_with = "de_ms")]
    pub end_ms: i64,
    /// Start offset of the word in the input text (characters).
    pub char_start: i64,
    /// End offset of the word in the input text (characters).
    pub char_end: i64,
    /// Compatibility field, currently always 1.0.
    #[serde(default = "default_score")]
    pub score: f64,
}

impl WordTimestamp {
    /// Duration of this word in milliseconds.
    pub fn duration_ms(&self) -> i64 {
        self.end_ms - self.start_ms
    }
}

/// What a request, turn or context consumed and was charged.
///
/// `cost_cents` is the actual charge in EUR cents. When it could not be
/// determined it is `None` and [`SessionUsage::cost_available`] is false,
/// never a misleading 0. `audio_seconds` is always reported.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct SessionUsage {
    /// Audio generated, in seconds (the billing unit).
    #[serde(default)]
    pub audio_seconds: f64,
    /// Actual charge in EUR cents, or `None` when it could not be determined.
    #[serde(default)]
    pub cost_cents: Option<f64>,
    /// Currency of `cost_cents` (`eur`), present only with a charge.
    #[serde(default)]
    pub currency: Option<String>,
    /// Input characters submitted.
    #[serde(default)]
    pub characters: Option<u64>,
    /// Model that produced the audio.
    #[serde(default)]
    pub model_id: Option<String>,
}

impl SessionUsage {
    /// True when an authoritative charge was returned.
    pub fn cost_available(&self) -> bool {
        self.cost_cents.is_some()
    }

    /// Typed usage from a `final`, `session_closed` or `context_closed` frame:
    /// the nested `usage` block, else the top-level `total_audio_seconds` an
    /// older server sends, else `None`.
    pub(crate) fn from_payload(frame: &Value) -> Option<Self> {
        if let Some(usage) = frame.get("usage").filter(|u| u.is_object()) {
            return serde_json::from_value(usage.clone()).ok();
        }
        frame
            .get("total_audio_seconds")
            .map(|seconds| SessionUsage {
                audio_seconds: seconds.as_f64().unwrap_or(0.0),
                cost_cents: None,
                currency: None,
                characters: None,
                model_id: None,
            })
    }
}

/// A chunk of streamed audio.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct AudioChunk {
    /// The audio bytes, encoded as [`AudioChunk::encoding`] (PCM16 LE by default).
    pub audio: Vec<u8>,
    /// Chunk index (0-based).
    pub index: u64,
    /// Sample rate in Hz.
    pub sample_rate: u32,
    /// Number of samples in this chunk.
    pub samples: u64,
    /// Encoding of `audio`.
    pub encoding: AudioEncoding,
    /// The server's text chunk id, when sent.
    pub chunk_id: Option<u64>,
    /// The context this chunk belongs to (multi-context sessions).
    pub context_id: Option<String>,
}

#[derive(Deserialize)]
struct AudioFrame {
    audio: String,
    idx: u64,
    sr: u32,
    samples: u64,
    #[serde(default)]
    enc: Option<String>,
    #[serde(default)]
    chunk_id: Option<u64>,
    #[serde(default)]
    context_id: Option<String>,
}

impl AudioChunk {
    /// Decode an audio frame (`{audio, enc, idx, sr, samples, chunk_id}`).
    pub(crate) fn from_frame(frame: &Value) -> Result<Self> {
        let parsed: AudioFrame = serde_json::from_value(frame.clone())
            .map_err(|e| Error::api(format!("Malformed audio frame from KugelAudio: {e}")))?;
        let audio = BASE64
            .decode(parsed.audio.as_bytes())
            .map_err(|e| Error::api(format!("Audio frame is not valid base64: {e}")))?;
        let encoding = match parsed.enc.as_deref() {
            // Frames without `enc` carry the documented default format.
            None => AudioEncoding::PcmS16le,
            Some(enc) => AudioEncoding::parse(enc).ok_or_else(|| {
                Error::api(format!(
                    "Audio frame has an unknown encoding '{enc}'; upgrade the SDK."
                ))
            })?,
        };
        Ok(Self {
            audio,
            index: parsed.idx,
            sample_rate: parsed.sr,
            samples: parsed.samples,
            encoding,
            chunk_id: parsed.chunk_id,
            context_id: parsed.context_id,
        })
    }

    /// Duration in seconds.
    pub fn duration_seconds(&self) -> f64 {
        if self.sample_rate == 0 {
            return 0.0;
        }
        self.samples as f64 / f64::from(self.sample_rate)
    }

    /// PCM16 samples as `f32` in `[-1.0, 1.0)`. Fails for G.711 audio.
    pub fn to_f32(&self) -> Result<Vec<f32>> {
        to_f32(&self.audio, self.encoding)
    }
}

fn to_f32(audio: &[u8], encoding: AudioEncoding) -> Result<Vec<f32>> {
    if encoding != AudioEncoding::PcmS16le {
        return Err(Error::validation(format!(
            "to_f32 needs PCM16 audio, this is {}.",
            encoding.as_str()
        )));
    }
    Ok(pcm16_to_f32(audio))
}

/// End-of-request, end-of-turn or end-of-context statistics: the `final`,
/// `session_closed` and `context_closed` frames. Fields a frame does not carry
/// are `None`; [`FinalStats::raw`] holds the whole frame.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FinalStats {
    /// Number of audio chunks (`/ws/tts` `final`).
    pub chunks: Option<u64>,
    /// Total samples (`/ws/tts` `final`).
    pub total_samples: Option<u64>,
    /// Audio duration in ms (`/ws/tts` `final`).
    pub duration_ms: Option<f64>,
    /// Generation time in ms (`/ws/tts` `final`).
    pub generation_ms: Option<f64>,
    /// Real-time factor (`/ws/tts` `final`).
    pub rtf: Option<f64>,
    /// Audio seconds of the turn (streaming `final` / `session_closed`).
    pub total_audio_seconds: Option<f64>,
    /// Text chunks of the turn.
    pub total_text_chunks: Option<u64>,
    /// Audio chunks of the turn.
    pub total_audio_chunks: Option<u64>,
    /// The context of a multi-context frame.
    pub context_id: Option<String>,
    /// Usage and charge, when the frame carries it.
    pub usage: Option<SessionUsage>,
    /// The whole frame as received.
    pub raw: Value,
}

impl FinalStats {
    pub(crate) fn from_frame(frame: &Value) -> Self {
        let u = |k: &str| frame.get(k).and_then(Value::as_u64);
        let f = |k: &str| frame.get(k).and_then(Value::as_f64);
        Self {
            chunks: u("chunks"),
            total_samples: u("total_samples"),
            duration_ms: f("dur_ms"),
            generation_ms: f("gen_ms"),
            rtf: f("rtf"),
            total_audio_seconds: f("total_audio_seconds"),
            total_text_chunks: u("total_text_chunks"),
            total_audio_chunks: u("total_audio_chunks"),
            context_id: frame
                .get("context_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            usage: SessionUsage::from_payload(frame),
            raw: frame.clone(),
        }
    }
}

/// Complete audio from [`TtsResource::generate`](crate::TtsResource::generate)
/// or a collected [`TtsStream`](crate::TtsStream).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct AudioResponse {
    /// The audio bytes, encoded as [`AudioResponse::encoding`].
    pub audio: Vec<u8>,
    /// Sample rate in Hz.
    pub sample_rate: u32,
    /// Encoding of `audio`.
    pub encoding: AudioEncoding,
    /// Number of samples.
    pub samples: u64,
    /// Audio duration in ms, when the server reported it.
    pub duration_ms: Option<f64>,
    /// Generation time in ms, when the server reported it.
    pub generation_ms: Option<f64>,
    /// Real-time factor, when the server reported it.
    pub rtf: Option<f64>,
    /// Word timestamps (streamed requests with `word_timestamps` set).
    pub word_timestamps: Vec<WordTimestamp>,
    /// Usage and charge, when reported (streamed requests).
    pub usage: Option<SessionUsage>,
    /// The server's `x-request-id` (REST requests).
    pub request_id: Option<String>,
}

impl AudioResponse {
    pub(crate) fn from_bytes(
        audio: Vec<u8>,
        sample_rate: u32,
        encoding: AudioEncoding,
        request_id: Option<String>,
    ) -> Self {
        let samples = (audio.len() / encoding.bytes_per_sample()) as u64;
        Self {
            audio,
            sample_rate,
            encoding,
            samples,
            duration_ms: None,
            generation_ms: None,
            rtf: None,
            word_timestamps: Vec::new(),
            usage: None,
            request_id,
        }
    }

    /// Duration in seconds.
    pub fn duration_seconds(&self) -> f64 {
        if self.sample_rate == 0 {
            return 0.0;
        }
        self.samples as f64 / f64::from(self.sample_rate)
    }

    /// PCM16 samples as `f32` in `[-1.0, 1.0)`. Fails for G.711 audio.
    pub fn to_f32(&self) -> Result<Vec<f32>> {
        to_f32(&self.audio, self.encoding)
    }

    /// The audio as a WAV file.
    pub fn to_wav(&self) -> Vec<u8> {
        wav_bytes(&self.audio, self.sample_rate, self.encoding)
    }

    /// Write the audio to `path` as a WAV file.
    pub fn save_wav(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        std::fs::write(path, self.to_wav()).map_err(|e| {
            Error::validation(format!("Cannot write {}: {e}", path.display())).with_source(e)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn audio_frame_decodes() {
        let chunk = AudioChunk::from_frame(&json!({
            "audio": BASE64.encode([1u8, 0, 2, 0]), "enc": "pcm_s16le",
            "idx": 3, "sr": 24000, "samples": 2, "chunk_id": 0
        }))
        .unwrap();
        assert_eq!(chunk.audio, vec![1, 0, 2, 0]);
        assert_eq!(
            (chunk.index, chunk.sample_rate, chunk.samples),
            (3, 24000, 2)
        );
        assert_eq!(chunk.chunk_id, Some(0));
        let mulaw = AudioChunk::from_frame(&json!({
            "audio": BASE64.encode([0xFFu8]), "enc": "mulaw", "idx": 0, "sr": 8000, "samples": 1
        }))
        .unwrap();
        assert_eq!(mulaw.encoding, AudioEncoding::Mulaw);
        assert!(mulaw.to_f32().is_err());
    }

    #[test]
    fn malformed_frames_are_errors() {
        assert!(AudioChunk::from_frame(&json!({"audio": "AAAA", "idx": 0, "samples": 1})).is_err());
        assert!(
            AudioChunk::from_frame(&json!({"audio": "!!", "idx": 0, "sr": 1, "samples": 1}))
                .is_err()
        );
        assert!(AudioChunk::from_frame(
            &json!({"audio": "AAAA", "enc": "opus", "idx": 0, "sr": 1, "samples": 1})
        )
        .is_err());
    }

    #[test]
    fn usage_prefers_nested_block() {
        let usage = SessionUsage::from_payload(&json!({
            "session_closed": true, "total_audio_seconds": 5.4,
            "usage": {"audio_seconds": 5.4, "characters": 142, "cost_cents": null, "cost_unavailable": true}
        }))
        .unwrap();
        assert_eq!(usage.characters, Some(142));
        assert!(!usage.cost_available());
        let legacy = SessionUsage::from_payload(&json!({"total_audio_seconds": 2.5})).unwrap();
        assert_eq!(legacy.audio_seconds, 2.5);
        assert!(SessionUsage::from_payload(&json!({"final": true})).is_none());
    }

    #[test]
    fn word_timestamps_accept_float_ms() {
        let ts: WordTimestamp = serde_json::from_value(json!({
            "word": "Hello", "start_ms": 0, "end_ms": 320.4, "char_start": 0, "char_end": 5
        }))
        .unwrap();
        assert_eq!(ts.duration_ms(), 320);
        assert_eq!(ts.score, 1.0);
    }

    #[test]
    fn final_stats_fields() {
        let stats = FinalStats::from_frame(&json!({
            "final": true, "chunks": 10, "total_samples": 48000, "dur_ms": 2000,
            "gen_ms": 150, "rtf": 0.075, "usage": {"audio_seconds": 2.0, "cost_cents": 0.18, "currency": "eur"}
        }));
        assert_eq!(stats.total_samples, Some(48000));
        assert_eq!(stats.rtf, Some(0.075));
        assert_eq!(stats.usage.unwrap().cost_cents, Some(0.18));
    }
}

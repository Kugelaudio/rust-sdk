//! Official Rust SDK for the [KugelAudio](https://kugelaudio.com) text-to-speech
//! and speech enhancement API.
//!
//! - [`TtsResource::generate`]: one REST request, the whole audio.
//! - [`TtsResource::stream`]: one request over a pooled WebSocket, audio chunks
//!   as they are generated, word timestamps and usage.
//! - [`StreamingSession`]: text in token by token (LLM output), audio out, with
//!   turns, flush, barge-in and settings updates.
//! - [`MultiContextSession`]: up to 20 independent voices on one socket.
//! - [`EnhanceResource`]: noise removal and target speaker extraction.
//! - Voices, models and pronunciation dictionaries.
//!
//! ```no_run
//! use kugelaudio::{KugelAudio, TtsRequest};
//!
//! #[tokio::main]
//! async fn main() -> kugelaudio::Result<()> {
//!     let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;
//!     let audio = client
//!         .tts()
//!         .generate(&TtsRequest::new("Hello from KugelAudio!").voice_id(1071).language("en"))
//!         .await?;
//!     audio.save_wav("hello.wav")?;
//!     client.close().await;
//!     Ok(())
//! }
//! ```
//!
//! Every call is async and needs a [Tokio](https://tokio.rs) runtime. Errors
//! are typed ([`Error`]); see [`error`] for the mapping from HTTP statuses and
//! WebSocket close codes.
//!
//! # Diagnostics
//!
//! The SDK reports its own failures (error category, stage, timings, the
//! server request id; never text, audio, URLs, keys or messages) to the API
//! host it already calls. It is on for the hosted API and off for custom
//! endpoints. Turn it off with [`KugelAudioBuilder::telemetry`] or
//! `KUGELAUDIO_TELEMETRY=0`.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod audio;
mod client;
mod diagnostics;
pub mod error;
mod metadata;
mod models;
mod request;
mod resources;
mod response;
mod streaming;

pub use audio::{
    load_audio, load_audio_stream, parse_wav, pcm16_to_f32, wav_bytes, Audio, AudioEncoding,
    AudioStream, OutputFormat, WavInfo, DEFAULT_SAMPLE_RATE, ENHANCED_SAMPLE_RATE,
    SUPPORTED_SAMPLE_RATES,
};
pub use client::{
    parse_api_key, KugelAudio, KugelAudioBuilder, Region, DEFAULT_API_URL,
    DEFAULT_KEEPALIVE_PING_INTERVAL, DEFAULT_TIMEOUT, EU_API_URL,
};
pub use diagnostics::config::ENV_TELEMETRY;
pub use error::{ApiError, Error, Result};
pub use models::{
    BulkReplaceResult, Dictionary, DictionaryEntry, DictionaryEntryInput, DictionaryEntryList,
    GeneratedSample, Model, Voice, VoiceAge, VoiceCategory, VoiceDetail, VoiceId,
    VoiceListResponse, VoiceReference, VoiceSex,
};
pub use request::{
    clamp_cfg_scale, GenerationSettings, MultiContextConfig, SettingsUpdate, StreamConfig,
    TtsRequest, DEFAULT_MODEL, MAX_CFG_SCALE, MIN_CFG_SCALE,
};
pub use resources::{
    DictionariesResource, DictionaryCreate, DictionaryEntriesResource, DictionaryEntryUpdate,
    DictionaryUpdate, EnhanceOptions, EnhanceResource, EnhanceSession, EnhancedAudio,
    EntryListOptions, ModelsResource, NewDictionaryEntry, PcmInput, ReferenceFile, StreamEvent,
    TtsResource, TtsStream, VoiceCreate, VoiceListOptions, VoiceUpdate, VoicesResource,
};
pub use response::{AudioChunk, AudioResponse, FinalStats, SessionUsage, WordTimestamp};
pub use streaming::{MultiContextSession, StreamingSession};

#[cfg(test)]
mod tests {
    use super::*;

    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}

    #[test]
    fn public_types_cross_threads() {
        send_sync::<KugelAudio>();
        send_sync::<Error>();
        send_sync::<AudioResponse>();
        send::<TtsStream>();
        send::<StreamingSession>();
        send::<MultiContextSession>();
        send::<EnhanceSession>();
        send::<PcmInput>();
    }
}

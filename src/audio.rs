//! Audio formats, WAV helpers and the loaders used by speech enhancement.
//!
//! Text to speech returns mono PCM16 little-endian at 24 kHz unless you ask
//! for another `sample_rate` or an [`OutputFormat`]. Speech enhancement returns
//! mono PCM16 at 24 kHz.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// The native generation rate of every TTS model, and the default output rate.
pub const DEFAULT_SAMPLE_RATE: u32 = 24_000;

/// The `sample_rate` values the TTS endpoints accept. Rates other than 24 kHz
/// are produced by server-side resampling.
pub const SUPPORTED_SAMPLE_RATES: [u32; 5] = [8_000, 16_000, 22_050, 24_000, 44_100];

/// The sample rate of enhanced audio.
pub const ENHANCED_SAMPLE_RATE: u32 = 24_000;

/// The encoding of audio bytes (`enc` on WebSocket frames, `X-Audio-Format` on REST).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub enum AudioEncoding {
    /// Linear PCM, 16-bit signed little-endian, 2 bytes per sample.
    #[default]
    #[serde(rename = "pcm_s16le")]
    PcmS16le,
    /// G.711 mu-law, 1 byte per sample.
    #[serde(rename = "mulaw")]
    Mulaw,
    /// G.711 a-law, 1 byte per sample.
    #[serde(rename = "alaw")]
    Alaw,
}

impl AudioEncoding {
    /// The wire name (`pcm_s16le`, `mulaw`, `alaw`).
    pub fn as_str(self) -> &'static str {
        match self {
            AudioEncoding::PcmS16le => "pcm_s16le",
            AudioEncoding::Mulaw => "mulaw",
            AudioEncoding::Alaw => "alaw",
        }
    }

    /// Bytes per sample (2 for PCM16, 1 for G.711).
    pub fn bytes_per_sample(self) -> usize {
        match self {
            AudioEncoding::PcmS16le => 2,
            AudioEncoding::Mulaw | AudioEncoding::Alaw => 1,
        }
    }

    /// Parse a wire name; `None` for an encoding this SDK version does not know.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pcm_s16le" | "pcm" => Some(AudioEncoding::PcmS16le),
            "mulaw" | "ulaw" => Some(AudioEncoding::Mulaw),
            "alaw" => Some(AudioEncoding::Alaw),
            _ => None,
        }
    }
}

/// The combined codec + rate token (`output_format`). Opt-in: without it the
/// API returns PCM16 at `sample_rate`. When set it is authoritative and must
/// not contradict a non-default `sample_rate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OutputFormat {
    /// Linear PCM16 at 8 kHz.
    #[serde(rename = "pcm_8000")]
    Pcm8000,
    /// Linear PCM16 at 16 kHz.
    #[serde(rename = "pcm_16000")]
    Pcm16000,
    /// Linear PCM16 at 22.05 kHz.
    #[serde(rename = "pcm_22050")]
    Pcm22050,
    /// Linear PCM16 at 24 kHz.
    #[serde(rename = "pcm_24000")]
    Pcm24000,
    /// G.711 mu-law at 8 kHz (telephony).
    #[serde(rename = "ulaw_8000")]
    Ulaw8000,
    /// G.711 a-law at 8 kHz (telephony).
    #[serde(rename = "alaw_8000")]
    Alaw8000,
}

impl OutputFormat {
    /// The wire token, e.g. `ulaw_8000`.
    pub fn as_str(self) -> &'static str {
        match self {
            OutputFormat::Pcm8000 => "pcm_8000",
            OutputFormat::Pcm16000 => "pcm_16000",
            OutputFormat::Pcm22050 => "pcm_22050",
            OutputFormat::Pcm24000 => "pcm_24000",
            OutputFormat::Ulaw8000 => "ulaw_8000",
            OutputFormat::Alaw8000 => "alaw_8000",
        }
    }

    /// The sample rate the token selects.
    pub fn sample_rate(self) -> u32 {
        match self {
            OutputFormat::Pcm8000 | OutputFormat::Ulaw8000 | OutputFormat::Alaw8000 => 8_000,
            OutputFormat::Pcm16000 => 16_000,
            OutputFormat::Pcm22050 => 22_050,
            OutputFormat::Pcm24000 => 24_000,
        }
    }

    /// The encoding of the audio bytes the token selects.
    pub fn encoding(self) -> AudioEncoding {
        match self {
            OutputFormat::Ulaw8000 => AudioEncoding::Mulaw,
            OutputFormat::Alaw8000 => AudioEncoding::Alaw,
            _ => AudioEncoding::PcmS16le,
        }
    }
}

/// Convert PCM16 little-endian bytes to `f32` samples in `[-1.0, 1.0)`.
/// A trailing odd byte (half a sample) is ignored.
pub fn pcm16_to_f32(pcm: &[u8]) -> Vec<f32> {
    let (samples, _) = pcm.as_chunks::<2>();
    samples
        .iter()
        .map(|b| f32::from(i16::from_le_bytes(*b)) / 32768.0)
        .collect()
}

/// Wrap mono audio bytes in a WAV container.
///
/// PCM16 is written as a standard PCM WAV; G.711 mu-law and a-law are written
/// with their WAV format tags (7 and 6), which common players decode.
pub fn wav_bytes(audio: &[u8], sample_rate: u32, encoding: AudioEncoding) -> Vec<u8> {
    let (format_tag, bits): (u16, u16) = match encoding {
        AudioEncoding::PcmS16le => (1, 16),
        AudioEncoding::Mulaw => (7, 8),
        AudioEncoding::Alaw => (6, 8),
    };
    let block_align = bits / 8;
    let byte_rate = sample_rate * u32::from(block_align);
    let pcm = encoding == AudioEncoding::PcmS16le;
    let fmt_size: u32 = if pcm { 16 } else { 18 };
    let fact_size: u32 = if pcm { 0 } else { 12 };
    let data_len = audio.len() as u32;
    let mut out = Vec::with_capacity(audio.len() + 58);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(4 + 8 + fmt_size + fact_size + 8 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&fmt_size.to_le_bytes());
    out.extend_from_slice(&format_tag.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&bits.to_le_bytes());
    if !pcm {
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(b"fact");
        out.extend_from_slice(&4u32.to_le_bytes());
        out.extend_from_slice(&data_len.to_le_bytes());
    }
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(audio);
    out
}

/// The parts of a WAV header this SDK needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WavInfo {
    /// WAV format tag (1 PCM, 3 IEEE float, 6 a-law, 7 mu-law); the sub-format
    /// for `WAVE_FORMAT_EXTENSIBLE` files.
    pub format_tag: u16,
    /// Number of interleaved channels.
    pub channels: u16,
    /// Sample rate in Hz.
    pub sample_rate: u32,
    /// Bits per sample.
    pub bits_per_sample: u16,
    /// Byte range of the `data` chunk inside the file.
    pub data: std::ops::Range<usize>,
}

impl WavInfo {
    /// Duration in seconds, or `None` when the header describes no audio rate.
    pub fn duration(&self) -> Option<f64> {
        let frame = u64::from(self.channels) * u64::from(self.bits_per_sample / 8);
        if frame == 0 || self.sample_rate == 0 {
            return None;
        }
        Some((self.data.len() as u64 / frame) as f64 / f64::from(self.sample_rate))
    }
}

/// Parse a RIFF/WAVE header.
pub fn parse_wav(bytes: &[u8]) -> Result<WavInfo> {
    let invalid = |why: &str| Error::validation(format!("Not a readable WAV file: {why}."));
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return Err(invalid("missing the RIFF/WAVE header"));
    }
    let u16_at = |i: usize| u16::from_le_bytes([bytes[i], bytes[i + 1]]);
    let u32_at =
        |i: usize| u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    let mut fmt: Option<(u16, u16, u32, u16)> = None;
    let mut pos = 12;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let size = u32_at(pos + 4) as usize;
        let body = pos + 8;
        if id == b"fmt " {
            if size < 16 || body + 16 > bytes.len() {
                return Err(invalid("truncated fmt chunk"));
            }
            let mut tag = u16_at(body);
            if tag == 0xFFFE && size >= 26 && body + 26 <= bytes.len() {
                tag = u16_at(body + 24);
            }
            fmt = Some((tag, u16_at(body + 2), u32_at(body + 4), u16_at(body + 14)));
        } else if id == b"data" {
            let (format_tag, channels, sample_rate, bits_per_sample) =
                fmt.ok_or_else(|| invalid("data chunk before fmt chunk"))?;
            let end = body.saturating_add(size).min(bytes.len());
            return Ok(WavInfo {
                format_tag,
                channels,
                sample_rate,
                bits_per_sample,
                data: body..end,
            });
        }
        pos = body.saturating_add(size).saturating_add(size & 1);
    }
    Err(invalid("no data chunk"))
}

fn read_file(path: &Path) -> Result<(String, Vec<u8>)> {
    let data = std::fs::read(path).map_err(|e| {
        Error::validation(format!("Cannot read audio file {}: {e}", path.display())).with_source(e)
    })?;
    if data.is_empty() {
        return Err(Error::validation("Audio must not be empty."));
    }
    let filename = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "audio.wav".to_string());
    Ok((filename, data))
}

/// A WAV recording for [`EnhanceResource::generate`](crate::EnhanceResource::generate),
/// sent to the API as-is (any WAV the API accepts: PCM 16/24/32-bit or float,
/// mono or stereo, 8-48 kHz, at most 300 s).
#[derive(Clone, PartialEq, Eq)]
pub struct Audio {
    /// The WAV file bytes.
    pub data: Vec<u8>,
    /// File name sent with the upload.
    pub filename: String,
}

impl std::fmt::Debug for Audio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Audio")
            .field("filename", &self.filename)
            .field("bytes", &self.data.len())
            .field("duration", &self.duration())
            .finish()
    }
}

impl Audio {
    /// Wrap WAV bytes. Fails on empty input.
    pub fn from_wav_bytes(data: impl Into<Vec<u8>>, filename: impl Into<String>) -> Result<Self> {
        let data = data.into();
        if data.is_empty() {
            return Err(Error::validation("Audio must not be empty."));
        }
        Ok(Self {
            data,
            filename: filename.into(),
        })
    }

    /// Duration in seconds when the WAV header is readable here, else `None`
    /// (the server decodes the file either way).
    pub fn duration(&self) -> Option<f64> {
        parse_wav(&self.data).ok().and_then(|info| info.duration())
    }
}

/// Load a WAV file for [`EnhanceResource::generate`](crate::EnhanceResource::generate).
///
/// ```no_run
/// # async fn demo(client: kugelaudio::KugelAudio) -> kugelaudio::Result<()> {
/// use kugelaudio::{load_audio, EnhanceOptions};
///
/// let audio = load_audio("meeting.wav")?;
/// let clean = client.enhance().generate(&audio, &EnhanceOptions::new("clarity-1")).await?;
/// clean.save("clean.wav")?;
/// # Ok(()) }
/// ```
pub fn load_audio(path: impl AsRef<Path>) -> Result<Audio> {
    let (filename, data) = read_file(path.as_ref())?;
    Ok(Audio { data, filename })
}

/// Mono PCM16 audio served in fixed-length chunks, for
/// [`EnhanceResource::stream`](crate::EnhanceResource::stream).
#[derive(Clone, PartialEq, Eq)]
pub struct AudioStream {
    pcm: Vec<u8>,
    sample_rate: u32,
    chunk_bytes: usize,
}

impl std::fmt::Debug for AudioStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioStream")
            .field("sample_rate", &self.sample_rate)
            .field("duration", &self.duration())
            .field("chunk_bytes", &self.chunk_bytes)
            .finish()
    }
}

impl AudioStream {
    /// Wrap raw mono PCM16 little-endian audio.
    ///
    /// `chunk_seconds` must be greater than 0 and at most 1.
    pub fn from_pcm(pcm: Vec<u8>, sample_rate: u32, chunk_seconds: f64) -> Result<Self> {
        if !(chunk_seconds > 0.0 && chunk_seconds <= 1.0) {
            return Err(Error::validation(
                "chunk_seconds must be greater than 0 and at most 1.",
            ));
        }
        if pcm.is_empty() {
            return Err(Error::validation("Audio must not be empty."));
        }
        let frames = ((f64::from(sample_rate) * chunk_seconds) as usize).max(1);
        Ok(Self {
            pcm,
            sample_rate,
            chunk_bytes: frames * 2,
        })
    }

    /// Parse a 16-bit PCM WAV; multi-channel audio is downmixed to mono.
    pub fn from_wav_bytes(wav: &[u8], chunk_seconds: f64) -> Result<Self> {
        const HINT: &str = "Use a 16-bit PCM WAV, or client.enhance().generate(load_audio(..)), \
                            which accepts any supported WAV.";
        let info =
            parse_wav(wav).map_err(|e| Error::validation(format!("{} {HINT}", e.message())))?;
        if info.format_tag != 1 || info.bits_per_sample != 16 {
            return Err(Error::validation(format!(
                "The WAV file is not 16-bit PCM ({}-bit, format {}). {HINT}",
                info.bits_per_sample, info.format_tag
            )));
        }
        let frames = &wav[info.data.clone()];
        if frames.is_empty() || info.channels == 0 {
            return Err(Error::validation("Audio must not be empty."));
        }
        let pcm = if info.channels > 1 {
            downmix(frames, usize::from(info.channels))
        } else {
            frames.to_vec()
        };
        Self::from_pcm(pcm, info.sample_rate, chunk_seconds)
    }

    /// Sample rate in Hz.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Duration in seconds.
    pub fn duration(&self) -> f64 {
        (self.pcm.len() / 2) as f64 / f64::from(self.sample_rate)
    }

    /// The audio as chunks of `chunk_seconds` each (the last may be shorter).
    pub fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        self.pcm.chunks(self.chunk_bytes)
    }

    /// The whole mono PCM16 audio.
    pub fn pcm(&self) -> &[u8] {
        &self.pcm
    }
}

/// Average interleaved PCM16 channels into mono (floor division, as the Python SDK).
fn downmix(frames: &[u8], channels: usize) -> Vec<u8> {
    let frame_bytes = channels * 2;
    let mut out = Vec::with_capacity(frames.len() / channels);
    for frame in frames.chunks_exact(frame_bytes) {
        let sum: i32 = frame
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| i32::from(i16::from_le_bytes(*b)))
            .sum();
        let mono = sum.div_euclid(channels as i32) as i16;
        out.extend_from_slice(&mono.to_le_bytes());
    }
    out
}

/// Load a 16-bit PCM WAV file as chunks for
/// [`EnhanceResource::stream`](crate::EnhanceResource::stream). Stereo is
/// downmixed to mono; the sample rate is read from the file.
///
/// `chunk_seconds` (greater than 0, at most 1; the Python SDK's default is
/// 0.1) sets the length of each chunk. Small chunks let enhanced audio start
/// coming back sooner.
pub fn load_audio_stream(path: impl AsRef<Path>, chunk_seconds: f64) -> Result<AudioStream> {
    let (_, data) = read_file(path.as_ref())?;
    AudioStream::from_wav_bytes(&data, chunk_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_round_trip_pcm16() {
        let pcm: Vec<u8> = [0i16, 1000, -1000, i16::MAX]
            .iter()
            .flat_map(|s| s.to_le_bytes())
            .collect();
        let wav = wav_bytes(&pcm, 24_000, AudioEncoding::PcmS16le);
        assert_eq!(wav.len(), 44 + pcm.len());
        let info = parse_wav(&wav).unwrap();
        assert_eq!(
            (
                info.format_tag,
                info.channels,
                info.sample_rate,
                info.bits_per_sample
            ),
            (1, 1, 24_000, 16)
        );
        assert_eq!(&wav[info.data.clone()], &pcm[..]);
        assert!((info.duration().unwrap() - 4.0 / 24_000.0).abs() < 1e-12);
    }

    #[test]
    fn wav_mulaw_has_format_tag_7() {
        let wav = wav_bytes(&[0xFF; 80], 8_000, AudioEncoding::Mulaw);
        let info = parse_wav(&wav).unwrap();
        assert_eq!(
            (info.format_tag, info.bits_per_sample, info.sample_rate),
            (7, 8, 8_000)
        );
        assert_eq!(info.data.len(), 80);
    }

    #[test]
    fn pcm_to_float() {
        let samples = pcm16_to_f32(&[0x00, 0x80, 0x00, 0x40, 0x01]);
        assert_eq!(samples, vec![-1.0, 0.5]);
    }

    #[test]
    fn stereo_is_downmixed_with_floor_division() {
        let stereo: Vec<u8> = [(-3i16, 0i16), (100, 200)]
            .iter()
            .flat_map(|(l, r)| [l.to_le_bytes(), r.to_le_bytes()].concat())
            .collect();
        let mut wav = wav_bytes(&stereo, 16_000, AudioEncoding::PcmS16le);
        wav[22] = 2; // channels
        wav[32] = 4; // block align
        let stream = AudioStream::from_wav_bytes(&wav, 0.1).unwrap();
        let mono: Vec<i16> = stream
            .pcm()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| i16::from_le_bytes(*b))
            .collect();
        assert_eq!(mono, vec![-2, 150]);
        assert_eq!(stream.sample_rate(), 16_000);
    }

    #[test]
    fn stream_chunks_and_validation() {
        let stream = AudioStream::from_pcm(vec![0; 16_000 * 2], 16_000, 0.25).unwrap();
        assert_eq!(stream.chunks().count(), 4);
        assert!((stream.duration() - 1.0).abs() < 1e-9);
        assert!(AudioStream::from_pcm(vec![0; 2], 16_000, 0.0).is_err());
        assert!(AudioStream::from_pcm(vec![0; 2], 16_000, 1.5).is_err());
        let float_wav = {
            let mut w = wav_bytes(&[0; 8], 16_000, AudioEncoding::PcmS16le);
            w[20] = 3;
            w
        };
        assert!(AudioStream::from_wav_bytes(&float_wav, 0.1).is_err());
        assert!(parse_wav(b"not a wav").is_err());
    }

    #[test]
    fn output_format_tokens() {
        assert_eq!(
            serde_json::to_string(&OutputFormat::Ulaw8000).unwrap(),
            "\"ulaw_8000\""
        );
        assert_eq!(OutputFormat::Alaw8000.encoding(), AudioEncoding::Alaw);
        assert_eq!(OutputFormat::Pcm22050.sample_rate(), 22_050);
        assert_eq!(AudioEncoding::parse("mulaw"), Some(AudioEncoding::Mulaw));
        assert_eq!(AudioEncoding::parse("opus"), None);
    }
}

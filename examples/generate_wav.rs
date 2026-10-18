//! Generate one sentence over REST and save it as a WAV file.
//!
//! `KUGELAUDIO_API_KEY=... cargo run --example generate_wav -- 1071 "Hello there!"`

use kugelaudio::{KugelAudio, TtsRequest};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let mut args = std::env::args().skip(1);
    let voice_id: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(1071);
    let text = args
        .next()
        .unwrap_or_else(|| "Hello from the KugelAudio Rust SDK!".to_string());

    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;
    let audio = client
        .tts()
        .generate(&TtsRequest::new(text).voice_id(voice_id).language("en"))
        .await?;
    audio.save_wav("output.wav")?;
    println!(
        "Wrote output.wav: {:.2}s at {} Hz (request {:?})",
        audio.duration_seconds(),
        audio.sample_rate,
        audio.request_id
    );
    client.close().await;
    Ok(())
}

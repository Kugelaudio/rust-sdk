//! Remove background noise from a WAV file, over REST and over the stream.
//!
//! `KUGELAUDIO_API_KEY=... cargo run --example enhance_file -- noisy.wav`

use futures_util::StreamExt;
use kugelaudio::{load_audio, load_audio_stream, EnhanceOptions, KugelAudio};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "noisy.wav".to_string());
    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;
    let options = EnhanceOptions::new("clarity-1");

    // Optional: start capacity before the first request.
    client.enhance().prewarm().await;

    // The whole recording in one request.
    let clean = client
        .enhance()
        .generate(&load_audio(&path)?, &options)
        .await?;
    clean.save("clean.wav")?;
    println!("clean.wav: {:.2}s", clean.duration());

    // The same file in real time, 100 ms per chunk (16-bit PCM WAV only).
    let mut stream = client
        .enhance()
        .stream(load_audio_stream(&path, 0.1)?, &options);
    let mut bytes = 0;
    while let Some(chunk) = stream.next().await {
        bytes += chunk?.len();
    }
    println!("streamed {bytes} bytes of enhanced 24 kHz PCM16");
    client.close().await;
    Ok(())
}

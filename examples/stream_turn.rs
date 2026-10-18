//! Stream one request over the pooled WebSocket and print each event.
//!
//! `KUGELAUDIO_API_KEY=... cargo run --example stream_turn -- 1071`

use futures_util::StreamExt;
use kugelaudio::{KugelAudio, StreamEvent, TtsRequest};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let voice_id: i64 = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1071);
    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;

    // Optional warm-up: open the socket before the first request.
    client.tts().connect(None).await?;

    let request = TtsRequest::new("Streaming starts playing before the sentence is finished.")
        .voice_id(voice_id)
        .language("en")
        .word_timestamps(true);
    let mut stream = client.tts().stream(&request).await?;
    while let Some(event) = stream.next().await {
        match event? {
            StreamEvent::Audio(chunk) => {
                println!("chunk {}: {} samples", chunk.index, chunk.samples)
            }
            StreamEvent::WordTimestamps(words) => {
                for word in words {
                    println!("  {} {}-{} ms", word.word, word.start_ms, word.end_ms);
                }
            }
            StreamEvent::Final(stats) => println!(
                "done: {:?} ms audio, usage {:?}",
                stats.duration_ms, stats.usage
            ),
            _ => {}
        }
    }
    client.close().await;
    Ok(())
}

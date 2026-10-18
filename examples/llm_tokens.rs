//! Feed LLM-style tokens into a streaming session, then flush the turn.
//!
//! `KUGELAUDIO_API_KEY=... cargo run --example llm_tokens -- 1071`

use futures_util::StreamExt;
use kugelaudio::{KugelAudio, StreamConfig};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let voice_id: i64 = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1071);
    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;
    let mut session = client
        .tts()
        .streaming_session(StreamConfig::new(voice_id).language("en"));
    session.connect().await?;

    // Stand-in for an LLM: tokens arrive one by one. Send them as they come;
    // the server synthesizes at sentence boundaries.
    let tokens = [
        "Sure",
        "! ",
        "Your ",
        "order ",
        "ships ",
        "tomorrow",
        ". ",
        "Anything ",
        "else",
        "?",
    ];
    let mut pcm = Vec::new();
    for token in tokens {
        let mut audio = session.send(token, false);
        while let Some(chunk) = audio.next().await {
            pcm.extend_from_slice(&chunk?.audio); // play it here
        }
    }
    // End the turn: the rest of the buffer is synthesized now.
    {
        let mut tail = session.flush();
        while let Some(chunk) = tail.next().await {
            pcm.extend_from_slice(&chunk?.audio);
        }
    }
    println!(
        "{} bytes of audio; usage {:?}",
        pcm.len(),
        session.last_usage()
    );
    std::fs::write(
        "llm_turn.wav",
        kugelaudio::wav_bytes(&pcm, 24_000, kugelaudio::AudioEncoding::PcmS16le),
    )
    .expect("write llm_turn.wav");
    session.close().await?;
    client.close().await;
    Ok(())
}

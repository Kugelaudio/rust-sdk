//! Two speakers on one socket with a multi-context session.
//!
//! `KUGELAUDIO_API_KEY=... cargo run --example multi_context -- 1071 1072`

use futures_util::StreamExt;
use kugelaudio::{KugelAudio, MultiContextConfig};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let mut args = std::env::args().skip(1);
    let narrator: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(1071);
    let character: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(1072);
    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;

    let mut session = client
        .tts()
        .multi_context_session(MultiContextConfig::default().language("en"));
    session.connect().await?;
    session
        .create_context("narrator", Some(narrator.into()))
        .await?;
    session
        .create_context("character", Some(character.into()))
        .await?;

    for (context, line) in [
        ("narrator", "The story begins."),
        ("character", "Hello, I'm the main character!"),
    ] {
        let mut audio = session.send(context, line, true, None);
        let mut bytes = 0;
        while let Some(chunk) = audio.next().await {
            bytes += chunk?.audio.len();
        }
        println!("[{context}] {bytes} bytes");
    }
    for context in ["narrator", "character"] {
        let mut tail = session.close_context(context, false);
        while let Some(chunk) = tail.next().await {
            chunk?;
        }
        drop(tail);
        println!("[{context}] usage {:?}", session.usage_for(context));
    }
    session.close().await;
    client.close().await;
    Ok(())
}

//! List voices, optionally filtered by language.
//!
//! `KUGELAUDIO_API_KEY=... cargo run --example list_voices -- de`

use kugelaudio::{KugelAudio, VoiceListOptions};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;
    let mut options = VoiceListOptions::default().limit(50);
    if let Some(language) = std::env::args().nth(1) {
        options = options.language(language);
    }
    let page = client.voices().list(&options).await?;
    for voice in &page.voices {
        println!(
            "{:>6}  {:<24} {:?}",
            voice.id, voice.name, voice.supported_languages
        );
    }
    println!("{} of {} catalog voices", page.voices.len(), page.total);
    for model in client.models().list().await? {
        println!("model {} ({})", model.id, model.name);
    }
    client.close().await;
    Ok(())
}

//! Opt-in tests against the production API.
//!
//! Run with `KUGELAUDIO_API_KEY=... cargo test --test live -- --ignored`.
//! `KUGELAUDIO_VOICE_ID` picks the voice (default: the first listed voice).
//! Without the key the key-dependent test returns at once. CI never sets it.

use futures_util::StreamExt;
use kugelaudio::{Error, KugelAudio, StreamConfig, TtsRequest, VoiceListOptions};

fn api_key() -> Option<String> {
    std::env::var("KUGELAUDIO_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())
}

#[tokio::test]
#[ignore = "talks to the production API; needs KUGELAUDIO_API_KEY"]
async fn live_generate_one_sentence_and_stream_one_turn() {
    let Some(key) = api_key() else {
        eprintln!("KUGELAUDIO_API_KEY is not set; skipping the live test");
        return;
    };
    let client = KugelAudio::builder(key).telemetry(false).build().unwrap();
    let voice_id: i64 = match std::env::var("KUGELAUDIO_VOICE_ID") {
        Ok(id) => id.parse().expect("KUGELAUDIO_VOICE_ID must be a number"),
        Err(_) => {
            let page = client
                .voices()
                .list(&VoiceListOptions::default().limit(1))
                .await
                .unwrap();
            page.voices
                .first()
                .expect("the account sees at least one voice")
                .id
        }
    };

    let audio = client
        .tts()
        .generate(
            &TtsRequest::new("Hello from the KugelAudio Rust SDK.")
                .voice_id(voice_id)
                .language("en"),
        )
        .await
        .unwrap();
    assert!(
        audio.duration_seconds() > 0.5,
        "{:?}s",
        audio.duration_seconds()
    );

    let mut session = client
        .tts()
        .streaming_session(StreamConfig::new(voice_id).language("en"));
    let mut bytes = 0;
    for token in ["This turn ", "was streamed ", "token by token."] {
        let mut chunks = session.send(token, false);
        while let Some(chunk) = chunks.next().await {
            bytes += chunk.unwrap().audio.len();
        }
    }
    {
        let mut tail = session.flush();
        while let Some(chunk) = tail.next().await {
            bytes += chunk.unwrap().audio.len();
        }
    }
    assert!(bytes > 24_000, "streamed {bytes} bytes");
    assert!(session.last_usage().is_some());
    session.close().await.unwrap();
    client.close().await;
}

#[tokio::test]
#[ignore = "talks to the production API (no key needed)"]
async fn live_tls_and_auth_rejection() {
    let client = KugelAudio::builder("invalid-key-for-sdk-test")
        .telemetry(false)
        .build()
        .unwrap();
    let err = client
        .voices()
        .list(&VoiceListOptions::default().limit(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Authentication(_)), "REST: {err:?}");
    let err = client
        .tts()
        .stream(&TtsRequest::new("Hello").voice_id(1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Authentication(_)),
        "WebSocket: {err:?}"
    );
}

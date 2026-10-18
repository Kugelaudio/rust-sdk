//! A collected `/ws/tts` stream reports the length of the audio it delivered.
//!
//! The server's `final` frame counts `total_samples` and `dur_ms` on the native
//! 24 kHz audio, before resampling and the speed change, so they disagree with
//! the delivered bytes whenever the stream is not 24 kHz at speed 1.0.

mod common;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use common::{serve, Conn};
use kugelaudio::{AudioEncoding, KugelAudio, OutputFormat, TtsRequest};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

fn client(url: &str) -> KugelAudio {
    KugelAudio::builder("ws-key")
        .api_url(url)
        .telemetry(false)
        .build()
        .unwrap()
}

/// An audio frame of `bytes` bytes in `enc` at `sr` Hz.
fn frame(index: u64, enc: &str, sr: u32, bytes: usize) -> Value {
    json!({
        "audio": BASE64.encode(vec![0x7f_u8; bytes]),
        "enc": enc,
        "idx": index,
        "sr": sr,
        "samples": if enc == "pcm_s16le" { bytes / 2 } else { bytes },
        "chunk_id": 0,
    })
}

/// Serve one `/ws/tts` turn per request; the request text names the shape.
/// Every `final` reports the native 24 kHz counts of 4.4 s of audio
/// (105600 samples), as the server does.
async fn native_final_counts(mut conn: Conn) {
    while let Some(Message::Text(text)) = conn.recv().await {
        let request: Value = serde_json::from_str(&text).unwrap();
        let (enc, sr, chunk_bytes) = match request["text"].as_str().unwrap() {
            // 4.4 s of mu-law at 8 kHz: 35200 bytes.
            "ulaw" => ("mulaw", 8000, 17600),
            // 4.4 s of PCM16 at 16 kHz: 140800 bytes.
            "pcm16k" => ("pcm_s16le", 16000, 70400),
            // 4.4 s at speed 1.2 is 3.667 s of PCM16 at 24 kHz: 176000 bytes.
            "fast" => ("pcm_s16le", 24000, 88000),
            other => panic!("unexpected request text {other}"),
        };
        conn.send_json(frame(0, enc, sr, chunk_bytes)).await;
        conn.send_json(frame(1, enc, sr, chunk_bytes)).await;
        conn.send_json(json!({"final": true, "chunks": 2, "total_samples": 105600,
                               "dur_ms": 4400.0, "gen_ms": 900.0, "rtf": 4.9}))
            .await;
    }
}

async fn collect(client: &KugelAudio, request: &TtsRequest) -> kugelaudio::AudioResponse {
    client
        .tts()
        .stream(request)
        .await
        .unwrap()
        .collect_response()
        .await
        .unwrap()
}

#[tokio::test]
async fn mulaw_stream_reports_the_delivered_length() {
    let server = serve(native_final_counts).await;
    let client = client(&server.url());
    let request = TtsRequest::new("ulaw")
        .voice_id(1071)
        .output_format(OutputFormat::Ulaw8000);
    let response = collect(&client, &request).await;
    assert_eq!(response.encoding, AudioEncoding::Mulaw);
    assert_eq!(response.sample_rate, 8000);
    assert_eq!(response.audio.len(), 35200);
    assert_eq!(response.samples, 35200, "one sample per mu-law byte");
    assert_eq!(response.duration_ms, Some(4400.0));
    assert!((response.duration_seconds() - 4.4).abs() < 1e-9);
    // The server's own stats stay readable as reported.
    assert_eq!(response.generation_ms, Some(900.0));
    assert_eq!(response.rtf, Some(4.9));
}

#[tokio::test]
async fn resampled_pcm_stream_reports_the_delivered_length() {
    let server = serve(native_final_counts).await;
    let client = client(&server.url());
    let request = TtsRequest::new("pcm16k").voice_id(1071).sample_rate(16000);
    let response = collect(&client, &request).await;
    assert_eq!(response.sample_rate, 16000);
    assert_eq!(response.samples, 70400, "two bytes per PCM16 sample");
    assert_eq!(response.duration_ms, Some(4400.0));
    assert!((response.duration_seconds() - 4.4).abs() < 1e-9);
}

#[tokio::test]
async fn sped_up_stream_reports_the_delivered_length() {
    let server = serve(native_final_counts).await;
    let client = client(&server.url());
    let request = TtsRequest::new("fast").voice_id(1071).speed(1.2);
    let response = collect(&client, &request).await;
    assert_eq!(response.samples, 88000);
    let expected_ms = 88000.0 * 1000.0 / 24000.0;
    assert!(
        (response.duration_ms.unwrap() - expected_ms).abs() < 1e-9,
        "{:?}",
        response.duration_ms
    );
    assert!((response.duration_seconds() - 88000.0 / 24000.0).abs() < 1e-9);
}

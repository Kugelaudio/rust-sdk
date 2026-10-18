//! WebSocket endpoints against a scripted local server.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{audio_frame, serve, serve_with_status, Conn};
use futures_util::StreamExt;
use kugelaudio::{
    AudioStream, EnhanceOptions, Error, KugelAudio, MultiContextConfig, PcmInput, SettingsUpdate,
    StreamConfig, StreamEvent, TtsRequest,
};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

fn client(url: &str) -> KugelAudio {
    KugelAudio::builder("ws-key")
        .api_url(url)
        .telemetry(false)
        .build()
        .unwrap()
}

/// Serve `/ws/tts` turns: audio, word timestamps, final with usage.
async fn tts_turns(mut conn: Conn) {
    while let Some(Message::Text(text)) = conn.recv().await {
        let request: Value = serde_json::from_str(&text).unwrap();
        if request["text"] == "fail" {
            conn.send_json(
                json!({"error": "Rate limit exceeded", "error_code": "RATE_LIMITED",
                                   "code": 429, "request_id": "ws-req-1"}),
            )
            .await;
            continue;
        }
        if request["text"] == "credits" {
            conn.close(4003, "out of credits").await;
            return;
        }
        conn.send_json(audio_frame(0, 4, 100)).await;
        conn.send_json(audio_frame(1, 2, 200)).await;
        if request["word_timestamps"] == true {
            conn.send_json(json!({"word_timestamps": [
                {"word": "Hello", "start_ms": 0, "end_ms": 100, "char_start": 0, "char_end": 5, "score": 1.0}
            ], "chunk_id": 0}))
            .await;
        }
        conn.send_json(json!({"final": true, "chunks": 2, "total_samples": 6, "dur_ms": 0.25,
                               "gen_ms": 1.0, "rtf": 4.0,
                               "usage": {"audio_seconds": 0.00025, "characters": 5, "cost_cents": 0.01,
                                         "currency": "eur", "model_id": "kugel-3.5"}}))
            .await;
    }
}

#[tokio::test]
async fn stream_collects_audio_timestamps_and_usage_and_reuses_the_socket() {
    let server = serve(tts_turns).await;
    let client = client(&server.url());
    let request = TtsRequest::new("Hello world")
        .voice_id(1071)
        .word_timestamps(true)
        .language("en");
    let response = client
        .tts()
        .stream(&request)
        .await
        .unwrap()
        .collect_response()
        .await
        .unwrap();
    assert_eq!(response.audio.len(), 12);
    assert_eq!(response.samples, 6);
    assert_eq!(response.sample_rate, 24000);
    assert_eq!(response.word_timestamps[0].word, "Hello");
    assert_eq!(response.usage.as_ref().unwrap().cost_cents, Some(0.01));
    assert!(client.tts().is_connected());

    let mut events = client
        .tts()
        .stream(&TtsRequest::new("Again").voice_id(1071))
        .await
        .unwrap();
    let mut kinds = Vec::new();
    while let Some(event) = events.next().await {
        kinds.push(match event.unwrap() {
            StreamEvent::Audio(_) => "audio",
            StreamEvent::WordTimestamps(_) => "words",
            StreamEvent::Final(_) => "final",
            _ => "other",
        });
    }
    assert_eq!(kinds, ["audio", "audio", "final"]);
    assert_eq!(
        server.connection_count(),
        1,
        "the second request reused the socket"
    );
    let uri = server.uris.lock().unwrap()[0].clone();
    assert!(uri.starts_with("/ws/tts?"), "{uri}");
    assert!(
        uri.contains("api_key=ws-key")
            && uri.contains("model_id=kugel-3.5")
            && uri.contains("sdk=rust")
    );
    client.close().await;
}

#[tokio::test]
async fn stream_error_frame_is_typed_and_keeps_the_socket() {
    let server = serve(tts_turns).await;
    let client = client(&server.url());
    let err = client
        .tts()
        .stream(&TtsRequest::new("fail").voice_id(1))
        .await
        .unwrap()
        .collect_response()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimit(_)), "{err:?}");
    assert_eq!(err.request_id(), Some("ws-req-1"));
    let ok = client
        .tts()
        .stream(&TtsRequest::new("ok").voice_id(1))
        .await
        .unwrap()
        .collect_response()
        .await;
    assert!(ok.is_ok());
    assert_eq!(server.connection_count(), 1);
}

#[tokio::test]
async fn stream_close_code_maps_to_error() {
    let server = serve(tts_turns).await;
    let client = client(&server.url());
    let err = client
        .tts()
        .stream(&TtsRequest::new("credits").voice_id(1))
        .await
        .unwrap()
        .collect_response()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InsufficientCredits(_)), "{err:?}");
    assert_eq!(err.details().ws_close_code, Some(4003));
}

#[tokio::test]
async fn refused_upgrades_are_typed() {
    let server = serve_with_status(
        Some((
            401,
            r#"{"error":"Invalid API key","error_code":"UNAUTHORIZED","code":401}"#,
        )),
        |_| async {},
    )
    .await;
    let err = client(&server.url())
        .tts()
        .stream(&TtsRequest::new("x").voice_id(1))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Authentication(_)), "{err:?}");
    assert_eq!(err.message(), "Invalid API key");
    assert_eq!(err.request_id(), Some("req-handshake"));

    let server = serve_with_status(Some((403, "Forbidden")), |_| async {}).await;
    let mut session = client(&server.url())
        .tts()
        .streaming_session(StreamConfig::new(1));
    let err = session.connect().await.unwrap_err();
    assert!(matches!(err, Error::Authentication(_)), "{err:?}");

    let server = serve_with_status(
        Some((
            429,
            r#"{"error":"Too many connections","error_code":"RATE_LIMITED"}"#,
        )),
        |_| async {},
    )
    .await;
    let mut multi = client(&server.url())
        .tts()
        .multi_context_session(MultiContextConfig::default());
    assert!(matches!(
        multi.connect().await.unwrap_err(),
        Error::RateLimit(_)
    ));
}

/// Serve `/ws/tts/stream`: buffer text, synthesize on flush, end the turn.
async fn stream_input_turns(mut conn: Conn, seen: Arc<Mutex<Vec<Value>>>) {
    while let Some(Message::Text(text)) = conn.recv().await {
        let message: Value = serde_json::from_str(&text).unwrap();
        seen.lock().unwrap().push(message.clone());
        if message.get("update_settings").is_some() {
            let mut settings = message["update_settings"].clone();
            settings["normalize"] = json!(true);
            conn.send_json(json!({"settings_updated": true, "settings": settings}))
                .await;
        } else if message["cancel"] == true {
            conn.send_json(audio_frame(9, 2, 1)).await;
            conn.send_json(json!({"interrupted": true})).await;
        } else if message["close"] == true {
            conn.send_json(json!({"session_closed": true, "total_audio_seconds": 0.0}))
                .await;
        } else if message["flush"] == true {
            conn.send_json(json!({"generation_started": true, "chunk_id": 0}))
                .await;
            conn.send_json(audio_frame(0, 3, 7)).await;
            conn.send_json(json!({"word_timestamps": [
                {"word": "Hi", "start_ms": 0, "end_ms": 50, "char_start": 0, "char_end": 2}
            ], "chunk_id": 0}))
                .await;
            conn.send_json(json!({"chunk_complete": true, "chunk_id": 0}))
                .await;
            conn.send_json(json!({"final": true, "total_audio_seconds": 0.000125}))
                .await;
            conn.send_json(
                json!({"session_closed": true, "total_audio_seconds": 0.000125,
                                   "usage": {"audio_seconds": 0.000125, "characters": 2,
                                             "cost_cents": null, "cost_unavailable": true}}),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn streaming_session_turn_lifecycle() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = serve(move |conn| stream_input_turns(conn, log.clone())).await;
    let client = client(&server.url());
    let mut session = client
        .tts()
        .streaming_session(StreamConfig::new(1071).language("en"));
    let words = Arc::new(Mutex::new(Vec::new()));
    let sink = words.clone();
    session.on_word_timestamps(move |w| {
        sink.lock()
            .unwrap()
            .extend(w.iter().map(|t| t.word.clone()))
    });
    session.connect().await.unwrap();

    // A token that does not end a sentence yields nothing after the poll.
    let early: Vec<_> = session.send("Hi", false).collect().await;
    assert!(early.is_empty());
    let audio: Vec<_> = session.send(" there.", true).collect().await;
    assert_eq!(audio.len(), 1);
    assert_eq!(audio[0].as_ref().unwrap().audio.len(), 6);
    assert_eq!(session.last_word_timestamps()[0].word, "Hi");
    assert_eq!(*words.lock().unwrap(), vec!["Hi".to_string()]);
    let usage = session.last_usage().unwrap();
    assert!(!usage.cost_available());
    assert!(session.last_final().is_some());

    // Next turn: the config is sticky, so it is not sent again.
    let audio: Vec<_> = session.send("Second turn.", true).collect().await;
    assert_eq!(audio.len(), 1);
    session.close().await.unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen[0]["voice_id"], 1071,
        "config rides with the first text"
    );
    assert_eq!(seen[0]["language"], "en");
    assert_eq!(seen[0]["text"], "Hi");
    assert!(seen[1].get("voice_id").is_none());
    assert_eq!(seen[1]["flush"], true);
    assert_eq!(server.connection_count(), 1);
}

#[tokio::test]
async fn streaming_session_cancel_and_update_settings() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = serve(move |conn| stream_input_turns(conn, log.clone())).await;
    let client = client(&server.url());
    let mut session = client.tts().streaming_session(StreamConfig::new(5));
    let _: Vec<_> = session.send("Start of a turn", false).collect().await;
    session.cancel().await.unwrap();

    let settings = session
        .update_settings(&SettingsUpdate::new().speed(1.1))
        .await
        .unwrap();
    assert_eq!(settings.speed, Some(1.1));
    assert_eq!(session.config().speed, 1.1);
    let err = session
        .update_settings(&SettingsUpdate::new())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validation(_)));

    let audio: Vec<_> = session.send("After barge-in.", true).collect().await;
    assert_eq!(
        audio.len(),
        1,
        "stale audio of the cancelled turn was discarded"
    );
    session.close().await.unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen[1], json!({"cancel": true}));
    assert_eq!(seen[2], json!({"update_settings": {"speed": 1.1}}));
    assert_eq!(seen[3]["voice_id"], 5, "config is re-sent after a cancel");
    assert_eq!(seen[3]["speed"], 1.1);
}

#[tokio::test]
async fn streaming_session_replays_a_silent_turn_after_a_restart() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = serve(move |mut conn: Conn| {
        let log = log.clone();
        async move {
            if conn.index == 0 {
                let first = conn.recv_json().await;
                log.lock().unwrap().push(first);
                conn.close(1012, "service restart").await;
                return;
            }
            stream_input_turns(conn, log).await;
        }
    })
    .await;
    let client = client(&server.url());
    let mut session = client.tts().streaming_session(StreamConfig::new(9));
    let audio: Vec<_> = session.send("Survive the deploy.", true).collect().await;
    assert_eq!(audio.len(), 1, "{audio:?}");
    assert!(audio[0].is_ok());
    assert_eq!(server.connection_count(), 2);
    let seen = seen.lock().unwrap();
    assert_eq!(seen[1]["text"], "Survive the deploy.");
    assert_eq!(seen[1]["voice_id"], 9, "the replay carries the config");
}

/// Serve `/ws/tts/multi`.
async fn multi_contexts(mut conn: Conn) {
    while let Some(Message::Text(text)) = conn.recv().await {
        let message: Value = serde_json::from_str(&text).unwrap();
        let ctx = message["context_id"].clone();
        if message["close_socket"] == true {
            conn.send_json(json!({"session_closed": true, "total_audio_seconds": 1.0}))
                .await;
            return;
        }
        if message["close_context"] == true {
            conn.send_json(json!({"final": true, "context_id": ctx}))
                .await;
            conn.send_json(json!({"context_closed": true, "context_id": ctx,
                                   "usage": {"audio_seconds": 0.5, "cost_cents": 0.04, "currency": "eur"}}))
                .await;
            continue;
        }
        if message["text"] == " " {
            conn.send_json(json!({"context_created": true, "context_id": ctx}))
                .await;
            continue;
        }
        if message["flush"] == true {
            if ctx == "a" {
                // Pre-buffered audio for context b arrives while a is read.
                let mut early = audio_frame(0, 1, 5);
                early["context_id"] = json!("b");
                conn.send_json(early).await;
            }
            let mut frame = audio_frame(0, 2, 3);
            frame["context_id"] = ctx.clone();
            conn.send_json(frame).await;
            conn.send_json(json!({"chunk_complete": true, "context_id": ctx}))
                .await;
            conn.send_json(json!({"final": true, "context_id": ctx}))
                .await;
        }
    }
}

#[tokio::test]
async fn multi_context_routes_audio_per_context() {
    let server = serve(multi_contexts).await;
    let client = client(&server.url());
    let mut session = client.tts().multi_context_session(
        MultiContextConfig::default()
            .language("en")
            .default_voice_id(1),
    );
    session.connect().await.unwrap();
    session.create_context("a", Some(11.into())).await.unwrap();
    session.create_context("b", Some(12.into())).await.unwrap();
    assert_eq!(session.active_contexts().len(), 2);

    let a: Vec<_> = session.send("a", "Narrator.", true, None).collect().await;
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].as_ref().unwrap().context_id.as_deref(), Some("a"));

    let b: Vec<_> = session.send("b", "Character.", true, None).collect().await;
    assert_eq!(
        b.len(),
        2,
        "the stashed pre-buffered chunk is delivered first"
    );
    assert_eq!(b[0].as_ref().unwrap().audio.len(), 2);

    let tail: Vec<_> = session.close_context("a", false).collect().await;
    assert!(tail.is_empty());
    assert_eq!(session.usage_for("a").unwrap().cost_cents, Some(0.04));
    assert!(!session.active_contexts().contains("a"));
    let stats = session.close().await.unwrap();
    assert_eq!(stats.total_audio_seconds, Some(1.0));
    assert!(!session.is_alive());
}

/// Serve `/v1/audio/enhance/stream`: echo audio back, `done` after `end`.
async fn enhance_echo(mut conn: Conn, received: Arc<Mutex<Vec<usize>>>) {
    loop {
        let config = match conn.recv().await {
            Some(Message::Text(text)) => serde_json::from_str::<Value>(&text).unwrap(),
            _ => return,
        };
        assert_eq!(config["type"], "config");
        assert_eq!(config["encoding"], "pcm_s16le");
        let session = conn.uri.contains("session=1");
        let mut ready = json!({"type": "ready", "sample_rate_hz": 24000, "encoding": "pcm_s16le"});
        if session {
            ready["request_id"] = json!("enh-1");
        }
        conn.send_json(ready).await;
        while let Some(message) = conn.recv().await {
            match message {
                Message::Binary(data) => {
                    received.lock().unwrap().push(data.len());
                    conn.send_binary(data.to_vec()).await;
                }
                Message::Text(_) => break,
                _ => return,
            }
        }
        conn.send_json(json!({"type": "done", "duration_s": 1.0}))
            .await;
        if !session {
            conn.close(1000, "").await;
            return;
        }
    }
}

#[tokio::test]
async fn enhance_stream_splits_input_and_yields_output() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let log = received.clone();
    let server = serve(move |conn| enhance_echo(conn, log.clone())).await;
    let client = client(&server.url());
    // 1.25 s at 8 kHz in one chunk: split into messages of at most one second.
    let input = PcmInput::from_chunks(vec![vec![0u8; 20_000]], 8_000);
    let output: Vec<Vec<u8>> = client
        .enhance()
        .stream(input, &EnhanceOptions::new("clarity-1"))
        .map(|chunk| chunk.unwrap())
        .collect()
        .await;
    assert_eq!(output.iter().map(Vec::len).sum::<usize>(), 20_000);
    assert_eq!(*received.lock().unwrap(), vec![16_000, 4_000]);
    assert!(server.uris.lock().unwrap()[0].starts_with("/v1/audio/enhance/stream?api_key=ws-key"));
}

#[tokio::test]
async fn enhance_session_reuses_one_socket() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let log = received.clone();
    let server = serve(move |conn| enhance_echo(conn, log.clone())).await;
    let client = client(&server.url());
    let mut session = client.enhance().session();
    session.connect().await.unwrap();
    for _ in 0..2 {
        let audio = AudioStream::from_pcm(vec![1u8; 3_200], 16_000, 0.05).unwrap();
        let mut stream = session.stream(audio, &EnhanceOptions::new("clarity-1"));
        let mut total = 0;
        while let Some(chunk) = stream.next().await {
            total += chunk.unwrap().len();
        }
        assert_eq!(total, 3_200);
    }
    session.close().await;
    assert_eq!(server.connection_count(), 1);
    assert!(server.uris.lock().unwrap()[0].contains("session=1"));
}

#[tokio::test]
async fn idle_socket_keepalive_does_not_break_requests() {
    let server = serve(tts_turns).await;
    let client = KugelAudio::builder("ws-key")
        .api_url(server.url())
        .keepalive_ping_interval(Some(Duration::from_millis(50)))
        .telemetry(false)
        .build()
        .unwrap();
    client.tts().connect(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        client.tts().is_connected(),
        "pongs kept the pooled socket alive"
    );
    let response = client
        .tts()
        .stream(&TtsRequest::new("After idle").voice_id(1))
        .await
        .unwrap()
        .collect_response()
        .await
        .unwrap();
    assert_eq!(response.audio.len(), 12);
    assert_eq!(server.connection_count(), 1);
}

#[tokio::test]
async fn streaming_session_reconnects_after_an_idle_drop() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = serve(move |mut conn: Conn| {
        let log = log.clone();
        async move {
            if conn.index == 0 {
                let first = conn.recv_json().await;
                log.lock().unwrap().push(first);
                conn.send_json(audio_frame(0, 1, 1)).await;
                conn.send_json(json!({"session_closed": true, "total_audio_seconds": 0.0}))
                    .await;
                // A proxy idle timeout closes the socket between turns.
                conn.close(1001, "going away").await;
                return;
            }
            stream_input_turns(conn, log).await;
        }
    })
    .await;
    let client = client(&server.url());
    let mut session = client.tts().streaming_session(StreamConfig::new(3));
    let first: Vec<_> = session.send("Turn one.", true).collect().await;
    assert_eq!(first.len(), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second: Vec<_> = session.send("Turn two.", true).collect().await;
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].is_ok());
    assert_eq!(server.connection_count(), 2);
    assert_eq!(
        seen.lock().unwrap()[1]["voice_id"],
        3,
        "the new socket gets the config"
    );
}

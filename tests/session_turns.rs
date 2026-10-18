//! Turn boundaries of `/ws/tts/stream`, against a server that behaves like
//! the production endpoint: a flush ends the turn on its own (`final`, then
//! `session_closed` with usage), and a `close` with no turn open ends an
//! empty session (`session_closed` with 0 s). Sending that `close` after a
//! flushed turn used to overwrite the turn's usage with zeros and leave the
//! empty `session_closed` in the socket, where it ended the next turn before
//! its audio arrived.

mod common;

use std::sync::{Arc, Mutex};

use common::{audio_frame, serve, Conn};
use futures_util::StreamExt;
use kugelaudio::{KugelAudio, StreamConfig};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

/// Audio frames each turn produces.
const FRAMES_PER_TURN: u64 = 5;
const SAMPLES_PER_FRAME: usize = 240;

fn client(url: &str) -> KugelAudio {
    KugelAudio::builder("ws-key")
        .api_url(url)
        .telemetry(false)
        .build()
        .unwrap()
}

/// Every sample of a turn's audio carries the turn number, so a test can tell
/// which turn a chunk belongs to.
async fn speak_turn(conn: &mut Conn, turn: i16) {
    conn.send_json(json!({"generation_started": true, "chunk_id": 0}))
        .await;
    for idx in 0..FRAMES_PER_TURN {
        conn.send_json(audio_frame(idx, SAMPLES_PER_FRAME, turn))
            .await;
    }
    let seconds = (FRAMES_PER_TURN as f64) * (SAMPLES_PER_FRAME as f64) / 24000.0;
    conn.send_json(json!({"chunk_complete": true, "chunk_id": 0}))
        .await;
    conn.send_json(json!({"final": true, "total_audio_seconds": seconds}))
        .await;
    conn.send_json(
        json!({"session_closed": true, "total_audio_seconds": seconds,
                           "usage": {"audio_seconds": seconds, "characters": 10,
                                     "cost_cents": 0.5, "currency": "eur"}}),
    )
    .await;
}

async fn production_like(mut conn: Conn, seen: Arc<Mutex<Vec<Value>>>) {
    let mut turn: i16 = 0;
    let mut buffered = false;
    while let Some(Message::Text(text)) = conn.recv().await {
        let message: Value = serde_json::from_str(&text).unwrap();
        seen.lock().unwrap().push(message.clone());
        if message.get("text").and_then(Value::as_str).is_some() {
            buffered = true;
        }
        let flush = message["flush"] == true;
        if message["close"] == true && !buffered {
            // No turn open: the close ends an empty session.
            conn.send_json(json!({"session_closed": true, "total_audio_seconds": 0.0,
                                   "usage": {"audio_seconds": 0.0, "characters": 0,
                                             "cost_cents": 0.0, "currency": "eur"}}))
                .await;
        } else if flush || message["close"] == true {
            turn += 1;
            buffered = false;
            speak_turn(&mut conn, turn).await;
        }
    }
}

fn turn_of(chunk: &kugelaudio::AudioChunk) -> i16 {
    i16::from_le_bytes([chunk.audio[0], chunk.audio[1]])
}

fn closes_sent(seen: &Mutex<Vec<Value>>) -> usize {
    seen.lock()
        .unwrap()
        .iter()
        .filter(|m| m["close"] == true)
        .count()
}

async fn session() -> (
    common::Server,
    Arc<Mutex<Vec<Value>>>,
    kugelaudio::StreamingSession,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let server = serve(move |conn| production_like(conn, log.clone())).await;
    let session = client(&server.url())
        .tts()
        .streaming_session(StreamConfig::new(7).language("en"));
    (server, seen, session)
}

#[tokio::test]
async fn drain_and_end_session_after_a_flushed_turn_keep_its_usage() {
    let (_server, seen, mut session) = session().await;
    let audio: Vec<_> = session.send("Turn one.", true).collect().await;
    assert_eq!(audio.len(), FRAMES_PER_TURN as usize);
    let usage = session.last_usage().unwrap().audio_seconds;
    assert!(usage > 0.0);

    let tail: Vec<_> = session.drain().collect().await;
    assert!(tail.is_empty(), "the flushed turn had no tail: {tail:?}");
    assert_eq!(
        session.last_usage().unwrap().audio_seconds,
        usage,
        "drain must not replace the turn's usage with an empty session's"
    );
    let stats = session.end_session().await.unwrap().unwrap();
    assert_eq!(stats.total_audio_seconds, Some(usage));

    let next: Vec<_> = session.send("Turn two.", true).collect().await;
    assert_eq!(next.len(), FRAMES_PER_TURN as usize, "{next:?}");
    assert!(next.iter().all(|c| turn_of(c.as_ref().unwrap()) == 2));
    let stats = session.close().await.unwrap().unwrap();
    assert_eq!(
        stats.total_audio_seconds,
        Some(usage),
        "close returns turn two's stats"
    );
    assert_eq!(closes_sent(&seen), 0, "no turn was left open to close");
}

#[tokio::test]
async fn dropping_a_flushed_turn_then_draining_yields_its_tail_and_no_stale_frames() {
    let (_server, seen, mut session) = session().await;
    {
        let mut audio = session.send("A long turn.", true);
        let first = audio.next().await.unwrap().unwrap();
        assert_eq!(turn_of(&first), 1);
    }
    let tail: Vec<_> = session.drain().collect().await;
    assert_eq!(tail.len(), FRAMES_PER_TURN as usize - 1);
    assert!(tail.iter().all(|c| turn_of(c.as_ref().unwrap()) == 1));
    assert!(session.last_usage().unwrap().audio_seconds > 0.0);
    assert!(session.end_session().await.unwrap().is_some());

    let next: Vec<_> = session.send("Follow-up.", true).collect().await;
    assert_eq!(next.len(), FRAMES_PER_TURN as usize, "{next:?}");
    assert!(next.iter().all(|c| turn_of(c.as_ref().unwrap()) == 2));
    assert!(session.last_usage().unwrap().audio_seconds > 0.0);
    assert_eq!(closes_sent(&seen), 0);
}

#[tokio::test]
async fn dropping_a_flushed_turn_then_end_session_leaves_no_stale_frames() {
    let (_server, seen, mut session) = session().await;
    {
        let mut audio = session.send("A long turn.", true);
        audio.next().await.unwrap().unwrap();
    }
    let stats = session.end_session().await.unwrap().unwrap();
    assert!(stats.total_audio_seconds.unwrap() > 0.0, "{stats:?}");

    let next: Vec<_> = session.send("Follow-up.", true).collect().await;
    assert_eq!(next.len(), FRAMES_PER_TURN as usize, "{next:?}");
    assert!(next.iter().all(|c| turn_of(c.as_ref().unwrap()) == 2));
    assert_eq!(closes_sent(&seen), 0);
}

#[tokio::test]
async fn drain_closes_an_unflushed_turn() {
    let (_server, seen, mut session) = session().await;
    let early: Vec<_> = session.send("Buffered text", false).collect().await;
    assert!(early.is_empty());
    let tail: Vec<_> = session.drain().collect().await;
    assert_eq!(tail.len(), FRAMES_PER_TURN as usize);
    assert!(session.last_usage().unwrap().audio_seconds > 0.0);
    assert_eq!(closes_sent(&seen), 1);
    // A second drain has nothing left to end.
    let again: Vec<_> = session.drain().collect().await;
    assert!(again.is_empty());
    assert_eq!(closes_sent(&seen), 1);

    // drain, then a new turn, then drain again: the new turn is closed too.
    let _: Vec<_> = session.send("More buffered text", false).collect().await;
    let tail: Vec<_> = session.drain().collect().await;
    assert_eq!(tail.len(), FRAMES_PER_TURN as usize);
    assert!(tail.iter().all(|c| turn_of(c.as_ref().unwrap()) == 2));
    assert_eq!(closes_sent(&seen), 2);
}

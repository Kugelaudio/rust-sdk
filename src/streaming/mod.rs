//! WebSocket sessions: token-by-token input (`/ws/tts/stream`) and
//! multi-context streaming (`/ws/tts/multi`).

mod multi;
mod session;
pub(crate) mod ws;

pub use multi::MultiContextSession;
pub use session::StreamingSession;

use std::time::Duration;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::response::WordTimestamp;

/// Default wait for a server answer.
pub(crate) const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_secs(30);
/// First-read poll of an open-ended `send` whose text may not end a sentence.
pub(crate) const POLL_TIMEOUT: Duration = Duration::from_millis(50);
/// After a `chunk_complete`, wait this long for the next sentence to start.
pub(crate) const INTER_CHUNK_IDLE: Duration = Duration::from_secs(2);
/// Keepalive ping interval (and pong deadline) of session sockets.
pub(crate) const SESSION_PING_INTERVAL: Duration = Duration::from_secs(15);
/// Reconnect delay after a rolling-deploy close without a hint.
pub(crate) const RESTART_RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// The close codes that end a session with an error the caller must see;
/// other codes (1000, 1001) end it cleanly.
pub(crate) fn is_error_close(code: Option<u16>) -> bool {
    matches!(code, Some(4001 | 4003 | 4029 | 4500 | 1012 | 1013))
}

/// The rolling-deploy close codes.
pub(crate) fn is_restart_close(code: Option<u16>) -> bool {
    matches!(code, Some(1012 | 1013))
}

/// The wait before reconnecting after a rolling-deploy close.
pub(crate) fn restart_delay(error: &Error) -> Duration {
    error
        .retry_after()
        .map(Duration::from_secs)
        .unwrap_or(RESTART_RECONNECT_DELAY)
}

/// Python truthiness of a frame field: `true`, a non-empty string, a non-zero
/// number, a non-empty array or object.
pub(crate) fn truthy(frame: &Value, key: &str) -> bool {
    match frame.get(key) {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Number(n)) => n.as_f64().is_some_and(|f| f != 0.0),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
    }
}

/// The `word_timestamps` array of a frame, when present. Every word also
/// carries the frame's `text` and `original_text`.
pub(crate) fn word_timestamps(frame: &Value) -> Result<Option<Vec<WordTimestamp>>> {
    match frame.get("word_timestamps") {
        None => Ok(None),
        Some(value) => {
            let mut words: Vec<WordTimestamp> =
                serde_json::from_value(value.clone()).map_err(|e| {
                    Error::api(format!(
                        "Malformed word_timestamps frame from KugelAudio: {e}"
                    ))
                })?;
            let text = frame.get("text").and_then(Value::as_str);
            let original_text = frame.get("original_text").and_then(Value::as_str);
            for word in &mut words {
                word.text = text.map(str::to_owned);
                word.original_text = original_text.map(str::to_owned);
            }
            Ok(Some(words))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn word_timestamps_carry_original_offsets_and_frame_texts() {
        let frame = json!({
            "word_timestamps": [{
                "word": "twelve", "start_ms": 0, "end_ms": 300, "char_start": 6, "char_end": 12,
                "original_char_start": 6, "original_char_end": 12, "score": 1.0
            }],
            "chunk_id": 0,
            "text": "About twelve thousand visitors.",
            "original_text": "About 12,000 visitors."
        });
        let words = word_timestamps(&frame).unwrap().unwrap();
        assert_eq!(words[0].original_char_start, Some(6));
        assert_eq!(words[0].original_char_end, Some(12));
        assert_eq!(
            words[0].text.as_deref(),
            Some("About twelve thousand visitors.")
        );
        assert_eq!(
            words[0].original_text.as_deref(),
            Some("About 12,000 visitors.")
        );
    }

    #[test]
    fn original_offsets_count_chars_after_an_emoji() {
        let frame = json!({
            "word_timestamps": [{
                "word": "twelve", "start_ms": 0, "end_ms": 300, "char_start": 2, "char_end": 8,
                "original_char_start": 2, "original_char_end": 8
            }],
            "text": "\u{1F600} twelve visitors.",
            "original_text": "\u{1F600} 12,000 visitors."
        });
        let word = word_timestamps(&frame).unwrap().unwrap().remove(0);
        let start = word.original_char_start.unwrap() as usize;
        let end = word.original_char_end.unwrap() as usize;
        let yours: String = word
            .original_text
            .unwrap()
            .chars()
            .skip(start)
            .take(end - start)
            .collect();
        assert_eq!(yours, "12,000");
    }

    #[test]
    fn word_timestamps_from_an_older_server_leave_the_new_fields_none() {
        let frame = json!({"word_timestamps": [
            {"word": "Hi", "start_ms": 0, "end_ms": 50, "char_start": 0, "char_end": 2}
        ]});
        let words = word_timestamps(&frame).unwrap().unwrap();
        assert_eq!(words[0].original_char_start, None);
        assert_eq!(words[0].original_char_end, None);
        assert_eq!(words[0].text, None);
        assert_eq!(words[0].original_text, None);
    }

    #[test]
    fn truthiness_matches_python() {
        let frame = json!({"a": true, "b": false, "c": "", "d": "x", "e": 0, "f": 1, "g": null});
        assert!(truthy(&frame, "a"));
        assert!(!truthy(&frame, "b"));
        assert!(!truthy(&frame, "c"));
        assert!(truthy(&frame, "d"));
        assert!(!truthy(&frame, "e"));
        assert!(truthy(&frame, "f"));
        assert!(!truthy(&frame, "g"));
        assert!(!truthy(&frame, "missing"));
    }

    #[test]
    fn close_code_sets() {
        assert!(is_error_close(Some(4001)));
        assert!(is_error_close(Some(1012)));
        assert!(!is_error_close(Some(1000)));
        assert!(is_restart_close(Some(1013)));
        assert!(!is_restart_close(Some(4500)));
    }
}

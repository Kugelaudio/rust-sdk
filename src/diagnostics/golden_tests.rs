//! Golden diagnostics batch: the reporter must reproduce it exactly.
//!
//! The ingress contract test runs the same file through the server sanitizer
//! and asserts every attribute of every record survives, so any encoder drift
//! fails CI on one side or the other.
//!
//! Regenerate after an intentional contract change, then review the diff:
//!
//! ```text
//! UPDATE_GOLDEN=1 cargo test --lib golden
//! ```

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;

use super::*;
use crate::error::ApiError;

const GOLDEN_VERSION: &str = "0.1.0";
const GOLDEN_RUNTIME: &str = "rust/1.89.0";
/// 2026-01-01T00:00:00Z, one second apart per record.
const GOLDEN_TIME_NS: u128 = 1_767_225_600_000_000_000;

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diagnostics_golden_batch.json")
}

struct NoSender;

impl Sender for NoSender {
    fn send(&self, _: String, _: Vec<(String, String)>, _: Vec<u8>) -> SendFuture {
        Box::pin(async { Ok(200) })
    }
}

fn reporter() -> Diagnostics {
    let config = DiagnosticsConfig::resolve("https://api.kugelaudio.com", Some(true), None, vec![]);
    Diagnostics::with_version(config, Arc::new(NoSender), GOLDEN_VERSION)
}

fn failure(extra: Attributes) -> Attributes {
    let mut attributes: Attributes = vec![
        ("kugel.outcome", OUTCOME_FAILED.into()),
        ("kugel.runtime", GOLDEN_RUNTIME.into()),
    ];
    attributes.extend(extra);
    attributes
}

/// One record per event type; together they cover every allowlisted key.
/// The Rust SDK has no framework integrations, so every record says `none`.
fn golden_events() -> Vec<(&'static str, Attributes)> {
    vec![
        (
            EVENT_CONNECTION_FAILED,
            failure(vec![
                ("kugel.event_id", "0a1b2c3d4e5f40718293a4b5c6d7e8f9".into()),
                (
                    "kugel.operation_id",
                    "1b2c3d4e5f60418293a4b5c6d7e8f90a".into(),
                ),
                ("kugel.operation", OP_STREAM_SESSION.into()),
                ("kugel.transport", TRANSPORT_WEBSOCKET.into()),
                ("kugel.failure_stage", STAGE_HANDSHAKE.into()),
                ("kugel.error_type", "AuthenticationError".into()),
                ("kugel.error_code", "UNAUTHORIZED".into()),
                ("kugel.http_status", 401u16.into()),
                (
                    "kugel.server_request_id",
                    "5e0c7d2a9b3f4e61a8c2d4f6b8e0a1c3".into(),
                ),
                ("kugel.elapsed_ms", 212u64.into()),
                ("kugel.audio_chunks", 0u64.into()),
                ("kugel.audio_bytes", 0u64.into()),
                ("kugel.retry_count", 0u32.into()),
            ]),
        ),
        (
            EVENT_REQUEST_FAILED,
            failure(vec![
                ("kugel.event_id", "2c3d4e5f6071429384a5b6c7d8e9f0a1".into()),
                (
                    "kugel.operation_id",
                    "3d4e5f607182439485b6c7d8e9f0a1b2".into(),
                ),
                ("kugel.operation", OP_MODELS.into()),
                ("kugel.transport", TRANSPORT_HTTP.into()),
                ("kugel.failure_stage", STAGE_SENDING_REQUEST.into()),
                ("kugel.error_type", "RateLimitError".into()),
                ("kugel.error_code", "RATE_LIMITED".into()),
                ("kugel.http_status", 429u16.into()),
                ("kugel.server_request_id", "req_7f3a9c1e2b4d".into()),
                ("kugel.elapsed_ms", 87u64.into()),
                ("kugel.audio_chunks", 0u64.into()),
                ("kugel.audio_bytes", 0u64.into()),
                ("kugel.retry_count", 0u32.into()),
            ]),
        ),
        (
            EVENT_STREAM_INTERRUPTED,
            failure(vec![
                ("kugel.event_id", "4e5f60718293440596c7d8e9f0a1b2c3".into()),
                (
                    "kugel.operation_id",
                    "5f6071829304451607d8e9f0a1b2c3d4".into(),
                ),
                ("kugel.operation", OP_MULTI_CONTEXT.into()),
                ("kugel.transport", TRANSPORT_WEBSOCKET.into()),
                ("kugel.failure_stage", STAGE_RECEIVING_AUDIO.into()),
                ("kugel.error_type", "ConnectionError".into()),
                ("kugel.http_status", 503u16.into()),
                ("kugel.ws_close_code", 1006u16.into()),
                ("kugel.elapsed_ms", 1840u64.into()),
                ("kugel.audio_chunks", 12u64.into()),
                ("kugel.audio_bytes", 46080u64.into()),
                ("kugel.retry_count", 0u32.into()),
            ]),
        ),
        (
            EVENT_RETRY_EXHAUSTED,
            failure(vec![
                ("kugel.event_id", "607182930415462718e9f0a1b2c3d4e5".into()),
                (
                    "kugel.operation_id",
                    "7182930415264728a9f0a1b2c3d4e5f6".into(),
                ),
                ("kugel.operation", OP_STREAM_SESSION.into()),
                ("kugel.transport", TRANSPORT_WEBSOCKET.into()),
                ("kugel.failure_stage", STAGE_AWAITING_FIRST_AUDIO.into()),
                ("kugel.error_type", "ServerRestartingError".into()),
                ("kugel.http_status", 503u16.into()),
                ("kugel.ws_close_code", 1012u16.into()),
                ("kugel.elapsed_ms", 2310u64.into()),
                ("kugel.audio_chunks", 0u64.into()),
                ("kugel.audio_bytes", 0u64.into()),
                ("kugel.retry_count", 1u32.into()),
            ]),
        ),
        (
            EVENT_SDK_STATS,
            vec![
                ("kugel.runtime", GOLDEN_RUNTIME.into()),
                ("kugel.event_id", "82930415263748b9a0f1b2c3d4e5f607".into()),
                (
                    "kugel.operation_id",
                    "930415263748590a1b2c3d4e5f607182".into(),
                ),
                ("kugel.success_count", 41u64.into()),
                ("kugel.failure_count", 4u64.into()),
                ("kugel.cancelled_count", 3u64.into()),
            ],
        ),
    ]
}

fn encode_golden() -> String {
    let reporter = reporter();
    let records: Vec<Value> = golden_events()
        .into_iter()
        .enumerate()
        .map(|(index, (event, attributes))| {
            reporter.record_at(
                event,
                attributes,
                GOLDEN_TIME_NS + index as u128 * 1_000_000_000,
            )
        })
        .collect();
    let payload: Value = serde_json::from_slice(&build_payload(&records, GOLDEN_VERSION)).unwrap();
    serde_json::to_string_pretty(&payload).unwrap() + "\n"
}

fn record_keys(record: &Value) -> BTreeSet<String> {
    record["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["key"].as_str().unwrap().to_string())
        .collect()
}

fn golden_records() -> Vec<Value> {
    let body: Value =
        serde_json::from_str(&std::fs::read_to_string(golden_path()).unwrap()).unwrap();
    body["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn the_reporter_reproduces_the_golden_batch_exactly() {
    let encoded = encode_golden();
    if std::env::var("UPDATE_GOLDEN").as_deref() == Ok("1") {
        std::fs::write(golden_path(), &encoded).unwrap();
    }
    let golden = std::fs::read_to_string(golden_path()).expect("the golden batch exists");
    assert_eq!(
        golden, encoded,
        "regenerate with UPDATE_GOLDEN=1 and review the diff"
    );
}

#[test]
fn the_golden_batch_covers_the_contract() {
    let records = golden_records();
    let events: Vec<&str> = records
        .iter()
        .map(|r| r["body"]["stringValue"].as_str().unwrap())
        .collect();
    assert_eq!(
        events,
        [
            EVENT_CONNECTION_FAILED,
            EVENT_REQUEST_FAILED,
            EVENT_STREAM_INTERRUPTED,
            EVENT_RETRY_EXHAUSTED,
            EVENT_SDK_STATS
        ]
    );
    let covered: BTreeSet<String> = records.iter().flat_map(record_keys).collect();
    let allowlist: BTreeSet<String> = ATTRIBUTE_TYPES.iter().map(|(k, _)| k.to_string()).collect();
    assert_eq!(covered, allowlist);
    // One batch is one POST: it must respect the per-POST cap.
    assert!(records.len() <= BATCH_SIZE);
    for record in &records {
        let runtime = record["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == "kugel.runtime")
            .and_then(|a| a["value"]["stringValue"].as_str())
            .unwrap();
        let version = runtime
            .strip_prefix("rust/")
            .expect("rust/<major>.<minor>.<patch>");
        assert_eq!(
            version
                .split('.')
                .filter(|p| p.parse::<u32>().is_ok())
                .count(),
            3
        );
    }
}

/// The failure records carry exactly the keys a real failed operation emits,
/// so the fixture cannot list attributes the reporter never sets.
#[test]
fn a_real_failure_emits_the_golden_failure_keys() {
    let reporter = reporter();
    let mut op = reporter.operation(Some(OP_STREAM), TRANSPORT_WEBSOCKET);
    op.expect_audio();
    let mut details = ApiError::new("dropped")
        .status(Some(503))
        .code(Some("MODEL_UNAVAILABLE".to_string()))
        .request_id(Some("r-1".to_string()));
    details.ws_close_code = Some(4500);
    let error = Error::Connection(details);
    let real = record_keys(&reporter.record_at(op.event_name(), op.attributes(&error), 0));
    let golden: BTreeSet<String> = golden_records()
        .iter()
        .filter(|r| r["body"]["stringValue"] != EVENT_SDK_STATS)
        .flat_map(record_keys)
        .collect();
    assert_eq!(real, golden);

    let stats = reporter.record_at(
        EVENT_SDK_STATS,
        vec![
            ("kugel.success_count", 1u64.into()),
            ("kugel.failure_count", 0u64.into()),
            ("kugel.cancelled_count", 0u64.into()),
        ],
        0,
    );
    let golden_stats = golden_records()
        .into_iter()
        .find(|r| r["body"]["stringValue"] == EVENT_SDK_STATS)
        .unwrap();
    assert_eq!(record_keys(&stats), record_keys(&golden_stats));
}

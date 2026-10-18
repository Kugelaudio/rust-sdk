//! REST endpoints against a mock HTTP server.

use std::time::Duration;

use kugelaudio::{
    wav_bytes, Audio, AudioEncoding, DictionaryEntryInput, EnhanceOptions, Error, KugelAudio,
    OutputFormat, ReferenceFile, TtsRequest, VoiceCreate, VoiceListOptions, VoiceUpdate,
};
use serde_json::{json, Value};
use wiremock::matchers::{
    body_json, body_string_contains, header, header_regex, method, path, query_param,
};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn client(server: &MockServer) -> KugelAudio {
    KugelAudio::builder("test-key")
        .api_url(server.uri())
        .telemetry(false)
        .build()
        .unwrap()
}

#[tokio::test]
async fn models_list_sends_auth_and_sdk_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .and(header("authorization", "Bearer test-key"))
        .and(header("x-api-key", "test-key"))
        .and(header("x-kugelaudio-sdk", "rust"))
        .and(header_regex("user-agent", "^kugelaudio-rust/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"models": [
            {"id": "kugel-3.5", "model_id": "kugel-3.5", "name": "Kugel 3.5", "description": "Most Natural",
             "parameters": null, "max_input_length": 10000, "sample_rate": 24000}
        ]})))
        .expect(1)
        .mount(&server)
        .await;
    let models = client(&server).models().list().await.unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].id, "kugel-3.5");
}

#[tokio::test]
async fn region_prefix_is_stripped_from_the_key() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"models": []})))
        .expect(1)
        .mount(&server)
        .await;
    let client = KugelAudio::builder("eu-secret")
        .api_url(server.uri())
        .telemetry(false)
        .build()
        .unwrap();
    assert!(client.models().list().await.unwrap().is_empty());
}

#[tokio::test]
async fn voices_list_passes_filters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/voices"))
        .and(query_param("limit", "5"))
        .and(query_param("offset", "10"))
        .and(query_param("language", "de"))
        .and(query_param("include_public", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "voices": [{"id": 1071, "name": "Emma", "category": "premade", "sex": "female",
                        "age": "middle_age", "supported_languages": ["de-DE"]}],
            "total": 83, "limit": 5, "offset": 10
        })))
        .expect(1)
        .mount(&server)
        .await;
    let page = client(&server)
        .voices()
        .list(
            &VoiceListOptions::default()
                .language("de")
                .limit(5)
                .offset(10),
        )
        .await
        .unwrap();
    assert_eq!(page.total, 83);
    assert_eq!(page.voices[0].supported_languages, vec!["de-DE"]);
}

#[tokio::test]
async fn not_found_carries_request_id() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/voices/999"))
        .respond_with(
            ResponseTemplate::new(404)
                .insert_header("x-request-id", "req-404")
                .set_body_json(
                    json!({"error": "Voice not found", "error_code": "NOT_FOUND", "code": 404}),
                ),
        )
        .mount(&server)
        .await;
    let err = client(&server).voices().get(999).await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    assert_eq!(err.request_id(), Some("req-404"));
    assert_eq!(err.to_string(), "Voice not found (request_id: req-404)");
}

#[tokio::test]
async fn rate_limit_reads_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/dictionaries"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "12")
                .set_body_json(json!({"error": "Rate limit exceeded", "error_code": "RATE_LIMITED", "code": 429})),
        )
        .mount(&server)
        .await;
    let err = client(&server).dictionaries().list(None).await.unwrap_err();
    assert!(matches!(err, Error::RateLimit(_)));
    assert_eq!(err.retry_after(), Some(12));
}

#[tokio::test]
async fn voice_create_is_multipart_with_consent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/voices"))
        .and(header_regex("content-type", "^multipart/form-data"))
        .and(body_string_contains("\"speaker_consent\":true"))
        .and(body_string_contains("filename=\"ref.wav\""))
        .and(body_string_contains("name=\"files\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 5, "name": "Mine"})))
        .expect(1)
        .mount(&server)
        .await;
    let voice = VoiceCreate::new("Mine", "female", true)
        .reference_file(ReferenceFile::new("ref.wav", vec![1, 2, 3]));
    let created = client(&server).voices().create(&voice).await.unwrap();
    assert_eq!(created.id, 5);
}

#[tokio::test]
async fn voice_update_sends_only_set_fields_and_delete_accepts_204() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/v1/voices/7"))
        .and(body_json(json!({"name": "New"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 7, "name": "New"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/v1/voices/7"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server);
    let updated = client
        .voices()
        .update(7, &VoiceUpdate::new().name("New"))
        .await
        .unwrap();
    assert_eq!(updated.name, "New");
    client.voices().delete(7).await.unwrap();
}

#[tokio::test]
async fn references_and_samples() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/voices/7/references"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "voice_id": 7, "name": "a.wav", "reference_text": "hi"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/voices/7/references"))
        .and(body_string_contains("name=\"speaker_consent\"\r\n\r\ntrue"))
        .and(body_string_contains("name=\"reference_text\"\r\n\r\nhello"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 2, "voice_id": 7})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/voices/7/generate-sample"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"sample_s3_path": "s/7.wav", "sample_url": null})),
        )
        .mount(&server)
        .await;
    let client = client(&server);
    let refs = client.voices().list_references(7).await.unwrap();
    assert_eq!(refs[0].reference_text.as_deref(), Some("hi"));
    let added = client
        .voices()
        .add_reference(
            7,
            &ReferenceFile::new("b.wav", vec![0; 4]),
            true,
            Some("hello"),
        )
        .await
        .unwrap();
    assert_eq!(added.id, 2);
    let sample = client.voices().generate_sample(7).await.unwrap();
    assert_eq!(sample.sample_url, None);
}

#[tokio::test]
async fn dictionary_replace_all_and_project_scope() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/v1/dictionaries/3/entries"))
        .and(query_param("project_id", "42"))
        .and(body_json(json!({"entries": [
            {"word": "K8s", "replacement": "kubernetes"},
            {"word": "Postgres", "replacement": "post-gres", "case_sensitive": true}
        ]})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"upserted": 2, "deleted": 1, "total": 2})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let entries = [
        DictionaryEntryInput::new("K8s", "kubernetes"),
        DictionaryEntryInput::new("Postgres", "post-gres").case_sensitive(true),
    ];
    let result = client(&server)
        .dictionaries()
        .entries()
        .replace_all(3, &entries, Some(42))
        .await
        .unwrap();
    assert_eq!((result.upserted, result.deleted, result.total), (2, 1, 2));
}

#[tokio::test]
async fn generate_uses_rest_and_reads_format_headers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/tts/generate"))
        .and(|request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            body["voice_id"] == 1071
                && body["output_format"] == "ulaw_8000"
                && body["cfg_scale"] == 2.0
                && body.get("word_timestamps").is_none()
        })
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "audio/basic")
                .insert_header("x-sample-rate", "8000")
                .insert_header("x-audio-format", "mulaw")
                .insert_header("x-request-id", "gen-1")
                .set_body_bytes(vec![0xFFu8; 800]),
        )
        .expect(1)
        .mount(&server)
        .await;
    let request = TtsRequest::new("Code 4 8 1 5")
        .voice_id(1071)
        .language("en")
        .output_format(OutputFormat::Ulaw8000);
    let audio = client(&server).tts().generate(&request).await.unwrap();
    assert_eq!(audio.encoding, AudioEncoding::Mulaw);
    assert_eq!(audio.sample_rate, 8000);
    assert_eq!(audio.samples, 800);
    assert!((audio.duration_seconds() - 0.1).abs() < 1e-9);
    assert_eq!(audio.request_id.as_deref(), Some("gen-1"));
    let wav = kugelaudio::parse_wav(&audio.to_wav()).unwrap();
    assert_eq!(wav.format_tag, 7);
}

#[tokio::test]
async fn generate_rejects_word_timestamps_locally() {
    let server = MockServer::start().await;
    let request = TtsRequest::new("Hello").voice_id(1).word_timestamps(true);
    let err = client(&server).tts().generate(&request).await.unwrap_err();
    assert!(matches!(err, Error::Validation(_)));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn timeouts_are_connection_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
        .mount(&server)
        .await;
    let client = KugelAudio::builder("k")
        .api_url(server.uri())
        .timeout(Duration::from_millis(200))
        .telemetry(false)
        .build()
        .unwrap();
    let err = client.models().list().await.unwrap_err();
    assert!(matches!(err, Error::Connection(_)), "{err:?}");
    assert!(err.message().contains("timed out"));
}

#[tokio::test]
async fn enhance_generate_with_speaker() {
    let server = MockServer::start().await;
    let enhanced = wav_bytes(&[1, 0, 2, 0], 24_000, AudioEncoding::PcmS16le);
    Mock::given(method("POST"))
        .and(path("/v1/audio/enhance"))
        .and(|request: &Request| {
            // The WAV parts are not UTF-8, so match on a lossy view of the body.
            let body = String::from_utf8_lossy(&request.body);
            body.contains("name=\"task\"\r\n\r\ntarget_speaker_extraction")
                && body.contains("name=\"model\"\r\n\r\nclarity-1")
                && body.contains("name=\"speaker\"; filename=\"speaker.wav\"")
        })
        .respond_with(ResponseTemplate::new(200).set_body_bytes(enhanced))
        .expect(1)
        .mount(&server)
        .await;
    let input = Audio::from_wav_bytes(
        wav_bytes(&[0; 8], 16_000, AudioEncoding::PcmS16le),
        "meeting.wav",
    )
    .unwrap();
    let speaker = Audio::from_wav_bytes(
        wav_bytes(&[0; 8], 16_000, AudioEncoding::PcmS16le),
        "speaker.wav",
    )
    .unwrap();
    let result = client(&server)
        .enhance()
        .generate(&input, &EnhanceOptions::new("clarity-1").speaker(speaker))
        .await
        .unwrap();
    assert_eq!(result.audio, vec![1, 0, 2, 0]);
    assert_eq!(result.sample_rate, 24_000);
}

#[tokio::test]
async fn enhance_rejects_empty_model_and_prewarm_never_fails() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/audio/enhance/warmup"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let client = client(&server);
    let input = Audio::from_wav_bytes(vec![1], "a.wav").unwrap();
    let err = client
        .enhance()
        .generate(&input, &EnhanceOptions::new(" "))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validation(_)));
    client.enhance().prewarm().await;
}

#[tokio::test]
async fn diagnostics_report_failures_without_secrets() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/voices/1"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "error": "secret detail", "error_code": "MODEL_UNAVAILABLE", "code": 503
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/sdk-diagnostics"))
        .and(header("authorization", "Bearer diag-key"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let client = KugelAudio::builder("diag-key")
        .api_url(server.uri())
        .telemetry(true)
        .build()
        .unwrap();
    assert!(client.voices().get(1).await.is_err());
    client.close().await;
    let requests = server.received_requests().await.unwrap();
    let report = requests
        .iter()
        .find(|r| r.url.path() == "/v1/sdk-diagnostics")
        .expect("a diagnostics batch");
    let body = String::from_utf8_lossy(&report.body);
    assert!(body.contains("request_failed"), "{body}");
    assert!(body.contains("\"ConnectionError\""));
    assert!(body.contains("\"voices\""));
    assert!(body.contains("sdk_stats"));
    assert!(!body.contains("secret detail"));
    assert!(!body.contains("diag-key"));
}

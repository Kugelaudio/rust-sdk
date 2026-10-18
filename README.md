# KugelAudio Rust SDK

The official Rust client for the [KugelAudio](https://kugelaudio.com) API:
text to speech (REST, WebSocket streaming, token-by-token input, multi-context
sessions), voices, pronunciation dictionaries and speech enhancement.

- Async on [Tokio](https://tokio.rs), TLS through rustls (no OpenSSL).
- Streams are [`futures::Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html)s.
- Typed requests, responses and errors.

Documentation: [docs.rs/kugelaudio](https://docs.rs/kugelaudio) and
[docs.kugelaudio.com](https://docs.kugelaudio.com).

## Install

```toml
[dependencies]
kugelaudio = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures-util = "0.3" # for StreamExt in the streaming examples
```

Minimum supported Rust version: 1.89.

## Quickstart

Create an API key in the [dashboard](https://app.kugelaudio.com/settings/api-keys)
and export it as `KUGELAUDIO_API_KEY`.

```rust
use kugelaudio::{KugelAudio, TtsRequest};

#[tokio::main]
async fn main() -> kugelaudio::Result<()> {
    let client = KugelAudio::new(std::env::var("KUGELAUDIO_API_KEY").unwrap_or_default())?;

    let audio = client
        .tts()
        .generate(&TtsRequest::new("Hello from KugelAudio!").voice_id(1071).language("en"))
        .await?;
    audio.save_wav("hello.wav")?;

    client.close().await;
    Ok(())
}
```

`generate` is one REST request (`POST /v1/tts/generate`) that returns the whole
audio: mono PCM16 at 24 kHz by default. Set `sample_rate` (8000, 16000, 22050,
24000, 44100) or an `OutputFormat` such as `OutputFormat::Ulaw8000` for
telephony; `AudioResponse::to_wav` and `save_wav` write a matching WAV file.

Every request parameter of the API is a `TtsRequest` setter: `model_id`
(default `kugel-3.5`), `voice_id` (numeric id or handle), `cfg_scale`,
`temperature`, `max_new_tokens`, `sample_rate`, `output_format`, `normalize`,
`language`, `word_timestamps`, `speed`, `dictionary_ids` and `project_id`.

The snippets below run inside an async function that returns
`kugelaudio::Result`, with `client` built as in the quickstart.

## Streaming

`stream` sends one request over a pooled WebSocket (`/ws/tts`) and yields audio
as it is generated, then word timestamps (when requested) and the final stats
with usage. The socket is kept for the next request; call
`client.tts().connect(None)` at startup to open it before the first one.

```rust
use futures_util::StreamExt;
use kugelaudio::{StreamEvent, TtsRequest};

let request = TtsRequest::new("Streaming starts before the sentence ends.")
    .voice_id(1071)
    .word_timestamps(true);
let mut stream = client.tts().stream(&request).await?;
while let Some(event) = stream.next().await {
    match event? {
        StreamEvent::Audio(chunk) => { /* play chunk.audio (PCM16) */ }
        StreamEvent::WordTimestamps(words) => println!("{words:?}"),
        StreamEvent::Final(stats) => println!("usage: {:?}", stats.usage),
        _ => {}
    }
}
```

`TtsStream::collect_response()` gathers a stream into one `AudioResponse`
(audio, word timestamps, usage). Word timestamps exist only on the WebSocket
endpoints; `generate` rejects a request that asks for them.

### Text from an LLM

A `StreamingSession` (`/ws/tts/stream`) takes text token by token and returns
audio as soon as sentences are complete. One turn is one assistant reply: send
the tokens as they arrive, then flush once.

```rust
use futures_util::StreamExt;
use kugelaudio::StreamConfig;

let mut session = client.tts().streaming_session(StreamConfig::new(1071).language("en"));
session.connect().await?;

for token in &llm_tokens {
    let mut audio = session.send(token, false);
    while let Some(chunk) = audio.next().await {
        let chunk = chunk?; // play chunk.audio
    }
}
let mut tail = session.flush(); // ends the turn
while let Some(chunk) = tail.next().await {
    let chunk = chunk?;
}
drop(tail);
println!("turn usage: {:?}", session.last_usage());
session.close().await?;
```

- `cancel()` is barge-in: it stops the current turn and drops its text; the
  socket stays open for the next turn.
- `update_settings(&SettingsUpdate::new().speed(1.1))` changes `cfg_scale`,
  `temperature`, `speed`, `max_new_tokens`, `language` or `normalize` from the
  next turn on; `update_config` replaces the voice or format between turns.
- `drain()` yields the remaining audio of a turn closed with `close`;
  `end_session()` ends the turn and discards it.
- A rolling deploy (close code 1012/1013) is absorbed while the turn is still
  silent: the session reconnects and replays it. Once audio went out you get
  `Error::ServerRestarting` and decide what to resend.

### Several voices on one socket

```rust
use futures_util::StreamExt;
use kugelaudio::MultiContextConfig;

let mut session = client.tts().multi_context_session(MultiContextConfig::default().language("en"));
session.connect().await?;
session.create_context("narrator", Some(1071.into())).await?;
session.create_context("character", Some(1072.into())).await?;

let mut audio = session.send("narrator", "The story begins.", true, None);
while let Some(chunk) = audio.next().await {
    let chunk = chunk?;
}
drop(audio);
let mut tail = session.close_context("narrator", false); // `true` = barge-in
while let Some(chunk) = tail.next().await {
    chunk?;
}
drop(tail);
println!("{:?}", session.usage_for("narrator"));
session.close().await;
```

Up to 20 contexts per socket; a context closes after 20 s without text unless
you call `keep_alive`.

## Voices, models and dictionaries

```rust
use kugelaudio::{DictionaryCreate, DictionaryEntryInput, NewDictionaryEntry, VoiceListOptions};

let page = client.voices().list(&VoiceListOptions::default().language("de").limit(10)).await?;
for voice in &page.voices {
    println!("{} {}", voice.id, voice.name);
}
let models = client.models().list().await?;

let dictionary = client.dictionaries().create(&DictionaryCreate::new("Brand names"), None).await?;
client
    .dictionaries()
    .entries()
    .add(dictionary.id, &NewDictionaryEntry::new("K8s", "kubernetes"), None)
    .await?;
client
    .dictionaries()
    .entries()
    .replace_all(dictionary.id, &[DictionaryEntryInput::new("Postgres", "post-gres")], None)
    .await?;
```

Voice management covers `get`, `create` (multipart, with
`VoiceCreate::new(name, sex, speaker_consent)`), `update`, `delete`,
`list_references`, `add_reference`, `delete_reference`, `publish` and
`generate_sample`. Creating a voice or uploading a reference requires
`speaker_consent = true`: your confirmation that you are the speaker or have
the speaker's permission.

## Speech enhancement

```rust
use futures_util::StreamExt;
use kugelaudio::{load_audio, load_audio_stream, EnhanceOptions};

let options = EnhanceOptions::new("clarity-1");

// A whole recording (any supported WAV, at most 300 s).
let clean = client.enhance().generate(&load_audio("call.wav")?, &options).await?;
clean.save("clean.wav")?;

// Real time: 16-bit PCM in, enhanced 24 kHz PCM16 out.
let mut out = client.enhance().stream(load_audio_stream("call.wav", 0.1)?, &options);
while let Some(chunk) = out.next().await {
    let pcm = chunk?;
}
```

`EnhanceOptions::speaker(load_audio("speaker.wav")?)` keeps only that voice.
`PcmInput::new(stream, sample_rate)` streams live microphone chunks;
`client.enhance().session()` keeps one warm socket for many streams, and
`prewarm()` readies capacity before the first request.

## Regions

The default endpoint `api.kugelaudio.com` is geo-routed. To pin traffic to the
EU, prefix the key with `eu-` (the SDK strips it) or set the region; an
explicit API URL wins over both:

```rust
use kugelaudio::{KugelAudio, Region};

let client = KugelAudio::builder("YOUR_API_KEY").region(Region::Eu).build()?;
let client = KugelAudio::new("eu-YOUR_API_KEY")?;
let client = KugelAudio::builder("YOUR_API_KEY").api_url("https://tts.example.internal").build()?;
```

The builder also sets the request `timeout` (default 60 s), a separate
`tts_url` for the WebSockets, and the `keepalive_ping_interval` of the pooled
socket (default 20 s).

## Errors

Every call returns `kugelaudio::Result<T>`. `Error` has one variant per
category, each carrying the message, HTTP status, `error_code`, the server's
`request_id` and `retry_after` where known:

| Variant | When |
|---|---|
| `Authentication` | Missing or rejected key (401, refused upgrade 403, close 4001) |
| `InsufficientCredits` | Out of credits (402, close 4003) |
| `NotFound` | Unknown voice, dictionary, entry (404) |
| `RateLimit` | Rate or concurrency limit (429, close 4029, `TOO_MANY_CONTEXTS`) |
| `Validation` | Invalid request (400, `MISSING_VOICE_ID`) or a failed local check |
| `ServerRestarting` | Rolling deploy (close 1012/1013); reconnect and resend |
| `Connection` | Network failure, timeout, model unavailable (503, close 4500) |
| `Api` | Anything else |

```rust
use kugelaudio::{Error, TtsRequest};

match client.tts().generate(&TtsRequest::new("Hallo").voice_id(1071)).await {
    Ok(audio) => println!("{:.1}s", audio.duration_seconds()),
    Err(Error::RateLimit(e)) => println!("retry after {:?}s", e.retry_after),
    Err(e) => println!("failed: {e} (request id {:?})", e.request_id()),
}
```

Quote the request id to support; it also finds the call on your project's
Logs page.

## Diagnostics

The SDK reports its own failures (error category, failure stage, timings,
audio chunk counts and the server request id) to the API host it already
calls, so we can see problems that never reach our servers. Reports never
contain your text, audio, API keys, URLs or error messages, and a failed report
never affects your call. Diagnostics are on for the hosted API and off for a
custom `api_url`. Turn them off in code or with an environment variable, which
wins in both directions:

```rust
let client = kugelaudio::KugelAudio::builder("YOUR_API_KEY").telemetry(false).build()?;
```

```bash
export KUGELAUDIO_TELEMETRY=0
```

Call `client.close().await` before your program exits: it delivers pending
reports, waiting at most one second.

## Logging

The SDK logs through [`tracing`](https://docs.rs/tracing): a warning when
normalization runs without a `language`, server warnings (for example an idle
turn the server ended), and an info line when a session replays a turn after
a rolling deploy.

## Examples

`examples/` has runnable programs; each reads `KUGELAUDIO_API_KEY`:

```bash
cargo run --example generate_wav -- 1071 "Hello there!"
cargo run --example stream_turn -- 1071
cargo run --example llm_tokens -- 1071
cargo run --example multi_context -- 1071 1072
cargo run --example list_voices -- de
cargo run --example enhance_file -- noisy.wav
```

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test                               # unit and mock-server tests
KUGELAUDIO_API_KEY=... cargo test --test live -- --ignored   # against the API
```

## License

MIT, see [LICENSE](LICENSE).

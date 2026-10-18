//! SDK metadata sent to the API for observability: headers on HTTP requests
//! and WebSocket handshakes, query parameters on WebSocket URLs.

/// `X-KugelAudio-SDK` header and `sdk` query value.
pub(crate) const SDK_NAME: &str = "rust";

/// This crate's version.
pub(crate) const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// `rustc` version this crate was built with (`1.85.0`), empty when unknown.
const RUSTC_VERSION: &str = env!("KUGELAUDIO_RUSTC_VERSION");

/// `User-Agent` header value.
pub(crate) fn user_agent() -> String {
    format!("kugelaudio-rust/{SDK_VERSION}")
}

/// The SDK identification headers (name, version, user agent).
pub(crate) fn sdk_headers() -> [(&'static str, String); 3] {
    [
        ("X-KugelAudio-SDK", SDK_NAME.to_string()),
        ("X-KugelAudio-SDK-Version", SDK_VERSION.to_string()),
        ("User-Agent", user_agent()),
    ]
}

/// The SDK identification query pairs appended to WebSocket URLs.
pub(crate) fn sdk_query() -> [(&'static str, &'static str); 2] {
    [("sdk", SDK_NAME), ("sdk_version", SDK_VERSION)]
}

/// `rust/1.85.0`: major.minor.patch only, or `None` when the build could not
/// read the compiler version.
pub(crate) fn runtime_string() -> Option<String> {
    if RUSTC_VERSION.is_empty() {
        None
    } else {
        Some(format!("rust/{RUSTC_VERSION}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_name_the_sdk() {
        let headers = sdk_headers();
        assert_eq!(headers[0], ("X-KugelAudio-SDK", "rust".to_string()));
        assert_eq!(headers[1].1, SDK_VERSION);
        assert_eq!(headers[2].1, format!("kugelaudio-rust/{SDK_VERSION}"));
    }

    #[test]
    fn runtime_is_major_minor_patch() {
        if let Some(runtime) = runtime_string() {
            let version = runtime.strip_prefix("rust/").unwrap();
            assert_eq!(version.split('.').count(), 3);
        }
    }
}

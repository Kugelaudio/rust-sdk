//! Diagnostics enablement and target resolution. Nothing here touches the network.

/// Appended to the effective API base URL. Diagnostics go to the same
/// authenticated host the SDK already calls; there is no override.
pub(crate) const DIAGNOSTICS_PATH: &str = "/v1/sdk-diagnostics";

/// Environment variable that overrides the constructor option in both directions.
pub const ENV_TELEMETRY: &str = "KUGELAUDIO_TELEMETRY";

const HOSTED_HOST_SUFFIX: &str = ".kugelaudio.com";
pub(crate) const ENDPOINT_KIND_HOSTED: &str = "hosted";
pub(crate) const ENDPOINT_KIND_CUSTOM: &str = "custom";

/// Resolved telemetry configuration.
#[derive(Debug, Clone)]
pub(crate) struct DiagnosticsConfig {
    pub enabled: bool,
    pub url: String,
    pub auth_headers: Vec<(String, String)>,
    pub endpoint_kind: &'static str,
}

/// `hosted` when the effective API host ends in `.kugelaudio.com`, else `custom`.
/// An unparseable URL fails closed (`custom`: telemetry off by default).
pub(crate) fn endpoint_kind_for(api_url: &str) -> &'static str {
    match reqwest::Url::parse(api_url) {
        Ok(url) => match url.host_str() {
            Some(host) if host.to_ascii_lowercase().ends_with(HOSTED_HOST_SUFFIX) => {
                ENDPOINT_KIND_HOSTED
            }
            _ => ENDPOINT_KIND_CUSTOM,
        },
        Err(_) => ENDPOINT_KIND_CUSTOM,
    }
}

impl DiagnosticsConfig {
    /// A configuration that never sends anything.
    #[cfg(test)]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            auth_headers: Vec::new(),
            endpoint_kind: ENDPOINT_KIND_CUSTOM,
        }
    }

    /// Resolution order, first match wins (identical in every SDK):
    /// 1. the `KUGELAUDIO_TELEMETRY` value (`env_value`),
    /// 2. the explicit `telemetry` option,
    /// 3. enabled only when the API host ends in `.kugelaudio.com`.
    pub fn resolve(
        api_url: &str,
        telemetry: Option<bool>,
        env_value: Option<&str>,
        auth_headers: Vec<(String, String)>,
    ) -> Self {
        let endpoint_kind = endpoint_kind_for(api_url);
        let raw = env_value.unwrap_or("").trim().to_ascii_lowercase();
        let enabled = match raw.as_str() {
            "0" | "false" | "off" | "no" => false,
            "1" | "true" | "on" | "yes" => true,
            _ => telemetry.unwrap_or(endpoint_kind == ENDPOINT_KIND_HOSTED),
        };
        let url = if api_url.is_empty() {
            String::new()
        } else {
            format!("{}{DIAGNOSTICS_PATH}", api_url.trim_end_matches('/'))
        };
        Self {
            enabled,
            url,
            auth_headers,
            endpoint_kind,
        }
    }

    /// True when the reporter must make no network call at all.
    pub fn inert(&self) -> bool {
        !self.enabled || self.url.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_is_on_by_default_custom_is_off() {
        let hosted = DiagnosticsConfig::resolve("https://api.kugelaudio.com", None, None, vec![]);
        assert!(hosted.enabled);
        assert_eq!(hosted.url, "https://api.kugelaudio.com/v1/sdk-diagnostics");
        assert_eq!(hosted.endpoint_kind, "hosted");
        let custom = DiagnosticsConfig::resolve("http://localhost:8080/", None, None, vec![]);
        assert!(!custom.enabled);
        assert_eq!(custom.endpoint_kind, "custom");
        assert_eq!(custom.url, "http://localhost:8080/v1/sdk-diagnostics");
    }

    #[test]
    fn env_overrides_option_both_ways() {
        let off =
            DiagnosticsConfig::resolve("https://api.kugelaudio.com", Some(true), Some("0"), vec![]);
        assert!(!off.enabled);
        let on = DiagnosticsConfig::resolve("http://localhost", Some(false), Some(" YES "), vec![]);
        assert!(on.enabled);
        let option = DiagnosticsConfig::resolve(
            "https://api.kugelaudio.com",
            Some(false),
            Some("maybe"),
            vec![],
        );
        assert!(!option.enabled);
    }

    #[test]
    fn lookalike_host_is_custom() {
        assert_eq!(
            endpoint_kind_for("https://kugelaudio.com.evil.example"),
            "custom"
        );
        assert_eq!(endpoint_kind_for("not a url"), "custom");
        assert_eq!(endpoint_kind_for("https://api.eu.kugelaudio.com"), "hosted");
    }
}

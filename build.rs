//! Records the compiler version for the diagnostics `kugel.runtime` attribute.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    // `rustc 1.85.0 (4d91de4e4 2025-02-17)` -> `1.85.0`. An unreadable version
    // leaves the attribute out of diagnostics reports; it never fails the build.
    let version = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| text.split_whitespace().nth(1).map(str::to_string))
        .map(|v| v.split('-').next().unwrap_or_default().to_string())
        .filter(|v| v.split('.').count() == 3 && v.split('.').all(|p| p.parse::<u32>().is_ok()))
        .unwrap_or_default();
    println!("cargo:rustc-env=KUGELAUDIO_RUSTC_VERSION={version}");
}

use std::process::Command;

fn main() {
    // Get current date
    let date = if let Ok(output) = Command::new("date").arg("+%Y-%m-%d").output() {
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    } else {
        "unknown".to_string()
    };

    println!("cargo:rustc-env=BUILD_DATE={}", date);

    // Get Rust compiler version
    let rustc_version = if let Ok(output) = Command::new("rustc").arg("--version").output() {
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    } else {
        "unknown".to_string()
    };

    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);

    // Get target triple
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TARGET={}", target);
}

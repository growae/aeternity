#![forbid(unsafe_code)]
use ae_node::configuration::Source;
use std::{ffi::OsStr, path::PathBuf, process::ExitCode};

// Like the native check_config command, this validates the file only; it
// neither applies AE__ overrides nor starts node services.
fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let Some(file) = args.next() else {
        return usage(false);
    };
    if args.next().is_some() {
        return usage(false);
    }
    if file == OsStr::new("--help") || file == OsStr::new("-h") {
        return usage(true);
    }
    let source = Source {
        path: PathBuf::from(file),
        deprecated: false,
    };
    match source.read() {
        Ok(_) => {
            println!("Configuration is valid.");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
fn usage(success: bool) -> ExitCode {
    eprintln!(
        "Usage: ae-check-config FILE.json|FILE.yaml\nValidate a node configuration file without starting services."
    );
    if success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}

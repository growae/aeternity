#![forbid(unsafe_code)]
use ae_node::application::{
    self,
    command::{Command, USAGE},
};
use std::{
    io::{self, Write},
    process::ExitCode,
    time::Duration,
};

fn emit(value: serde_json::Value) -> io::Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &value)?;
    writeln!(out)?;
    out.flush()
}
fn main() -> ExitCode {
    let command = match Command::parse(std::env::args_os().skip(1)) {
        Ok(Command::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(Command::Run(args)) => args,
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Cannot start async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(*command)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
async fn run(arguments: application::command::Arguments) -> Result<(), Box<dyn std::error::Error>> {
    // Install handlers before opening storage or launching children.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let shutdown = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => (), _ = interrupt.recv() => () }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(shutdown);
    let prepared = arguments.prepare(
        &std::env::current_dir()?,
        &std::env::vars_os().collect(),
        &std::env::current_exe()?,
    )?;
    let identity = prepared.configured().chain().identity().clone();
    let mut running = prepared.launch()?;
    let mut handle = running.handle();
    let started = tokio::select! {
        biased;
        _ = &mut shutdown => None,
        result = running.start_http() => Some(result),
    };
    let mut failure: Option<Box<dyn std::error::Error>> = None;
    match started {
        None => (),
        Some(Err(error)) => failure = Some(Box::new(error)),
        Some(Ok(listeners)) => {
            let sync = handle
                .connections()
                .and_then(|c| c.current())
                .and_then(|g| g.network.status().listening)
                .map(|a| a.to_string());
            let ready = emit(
                serde_json::json!({"event":"ready","network_id":String::from_utf8_lossy(&identity.network),"genesis":ae_codec::api::encode(ae_codec::api::ApiType::KeyBlockHash,identity.genesis.as_ref()),"family":format!("{:?}",handle.status().family),"sync":sync,"stratum":handle.pow().and_then(|h|h.stratum()).map(|s|s.transport.address().to_string()),"http":listeners.iter().map(|h|h.status().address.to_string()).collect::<Vec<_>>()}),
            );
            if let Err(error) = ready {
                failure = Some(Box::new(error));
            } else {
                loop {
                    // Terminal receipts free bounded production output capacity.
                    while handle.try_output().is_some() {}
                    let status = handle.status();
                    if !status.running || status.failure.is_some() {
                        break;
                    }
                    tokio::select! {
                        _ = &mut shutdown => break,
                        result = handle.changed() => { if result.is_err() { break; } },
                        _ = tokio::time::sleep(Duration::from_millis(20)) => (),
                    }
                }
            }
        }
    }
    let exit = running.shutdown().await?;
    let status = exit.status();
    emit(
        serde_json::json!({"event":"stopped","joined":status.joined,"failed":status.failure.is_some() || failure.is_some()}),
    )?;
    if let Some(error) = failure {
        return Err(error);
    }
    if !status.joined || status.failure.is_some() {
        return Err("node stopped with a runtime failure".into());
    }
    Ok(())
}

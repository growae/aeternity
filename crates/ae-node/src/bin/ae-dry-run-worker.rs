#![forbid(unsafe_code)]
#[global_allocator]
static HEAP: ae_worker_heap::Guard = ae_worker_heap::Guard::new();
fn main() -> std::process::ExitCode {
    match ae_node::dry_run_worker::serve(std::io::stdin(), std::io::stdout(), &HEAP) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(ae_node::dry_run_worker::ServeError::Encoding(_)) => std::process::ExitCode::from(94),
        Err(ae_node::dry_run_worker::ServeError::Transport(_)) => std::process::ExitCode::from(95),
        Err(ae_node::dry_run_worker::ServeError::Poisoned) => std::process::ExitCode::from(96),
    }
}

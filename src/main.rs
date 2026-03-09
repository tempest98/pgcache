use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use pgcache_lib::metrics::prometheus_install;
use pgcache_lib::proxy::{ConnectionError, proxy_run};
use pgcache_lib::settings::Settings;
use rootcause::Report;
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "console"))]
use pgcache_lib::tracing_utils::SimpeFormatter;
use tokio::io;
use tracing::info;
#[cfg(not(feature = "console"))]
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Report> {
    // Install rustls crypto provider for TLS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| io::Error::other("crypto provider already installed"))?;

    #[cfg(feature = "hotpath")]
    let _guard = hotpath::HotpathGuardBuilder::new("pgcache")
        .percentiles(&[50, 95, 99])
        .format(hotpath::Format::Table)
        .build();

    let settings = Settings::from_args()?;
    let cancel = CancellationToken::new();

    let metrics_socket = settings.metrics.as_ref().map(|m| m.socket);
    prometheus_install(metrics_socket, cancel.child_token())?;

    if let Some(socket) = metrics_socket {
        info!("Prometheus metrics available at http://{}/metrics", socket);
    }

    #[cfg(feature = "console")]
    console_subscriber::init();

    #[cfg(not(feature = "console"))]
    {
        // Log level precedence: CLI arg > config file > RUST_LOG env var > default (info)
        let filter = settings
            .log_level
            .as_deref()
            .map(EnvFilter::new)
            .or_else(|| EnvFilter::try_from_default_env().ok())
            .unwrap_or_else(|| EnvFilter::new("info"));

        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .event_format(SimpeFormatter)
            .finish();
        tracing::subscriber::set_global_default(subscriber)?;
    }

    let sigint = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&sigint))?;

    thread::scope(|scope| {
        let cancel_proxy = cancel.child_token();
        let proxy_handle = thread::Builder::new()
            .name("proxy".to_owned())
            .spawn_scoped(scope, || proxy_run(&settings, cancel_proxy))?;

        let sleep_duration = Duration::from_millis(500);

        // Wait for either SIGINT or proxy thread to finish
        while !sigint.load(Ordering::Relaxed) && !proxy_handle.is_finished() {
            sleep(sleep_duration);
        }

        // Signal all threads to shut down
        info!("shutting down");
        cancel.cancel();

        // Wait for proxy thread to finish
        let res = proxy_handle
            .join()
            .unwrap_or_else(|_panic| {
                Err(
                    ConnectionError::IoError(io::Error::other("proxy thread panicked"))
                        .into(),
                )
            })
            .map_err(|e| e.into_dynamic());

        info!("process terminated {res:?}");

        // print the report.
        #[cfg(feature = "hotpath")]
        drop(_guard);

        res
    })
}

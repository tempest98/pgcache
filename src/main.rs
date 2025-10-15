use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
use std::{error::Error, thread};

use pgcache_lib::proxy::{ConnectionError, proxy_run};
use pgcache_lib::settings::Settings;
use pgcache_lib::tracing_utils::SimpeFormatter;

use tokio::io;
use tracing::{Level, info};

fn main() -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "hotpath")]
    let _guard = hotpath::GuardBuilder::new("pgcache")
        .percentiles(&[50, 95, 99])
        .format(hotpath::Format::Table)
        .build();

    let settings = Settings::from_args()?;

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .event_format(SimpeFormatter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let sigint = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&sigint))?;

    thread::scope(|scope| {
        let proxy_handle = thread::Builder::new()
            .name("proxy".to_owned())
            .spawn_scoped(scope, || proxy_run(&settings))?;

        let sleep_duration = Duration::from_millis(500);

        let mut res = Ok(());
        while !sigint.load(Ordering::Relaxed) {
            if proxy_handle.is_finished() {
                res = proxy_handle
                    .join()
                    .unwrap_or_else(|_panic| {
                        Err(ConnectionError::IoError(io::Error::other(
                            "proxy thread panicked",
                        )))
                    })
                    .map_err(|e| Box::new(e) as Box<dyn Error>);
                break;
            }
            sleep(sleep_duration);
        }
        info!("process terminating {res:?}");

        // print the report.
        #[cfg(feature = "hotpath")]
        drop(_guard);

        //kill the process
        exit(if res.is_ok() { 0 } else { 1 });
    })
}

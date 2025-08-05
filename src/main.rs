use std::process::exit;
use std::thread::sleep;
use std::time::Duration;
use std::{error::Error, thread};

use pgcache_lib::cache::cache_run;
use pgcache_lib::proxy::{ConnectionError, proxy_run};
use pgcache_lib::settings::Settings;
use pgcache_lib::tracing_utils::SimpeFormatter;

use tokio::io;
use tokio::sync::mpsc;
use tracing::{Level, error};

fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::from_args()?;

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .event_format(SimpeFormatter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    thread::scope(|scope| {
        const DEFAULT_CHANNEL_SIZE: usize = 100;
        let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let listen_handle = thread::Builder::new()
            .name("listen".to_owned())
            .spawn_scoped(scope, || proxy_run(&settings, tx))?;

        let cache_handle = thread::Builder::new()
            .name("cache".to_owned())
            .spawn_scoped(scope, || cache_run(&settings, rx))?;

        let res = loop {
            if cache_handle.is_finished() {
                match cache_handle.join() {
                    Ok(res) => break res.map_err(|e| Box::new(e) as Box<dyn Error>),
                    Err(_panic) => {
                        break Err(Box::new(ConnectionError::IoError(io::Error::other(
                            "cache thread panicked",
                        ))));
                    }
                }
            }

            if listen_handle.is_finished() {
                match listen_handle.join() {
                    Ok(res) => break res.map_err(|e| Box::new(e) as Box<dyn Error>),
                    Err(_panic) => {
                        break Err(Box::new(ConnectionError::IoError(io::Error::other(
                            "listen thread panicked",
                        ))));
                    }
                }
            }

            sleep(Duration::from_secs(1));
        };

        error!("process terminating {res:?}");
        exit(if res.is_ok() { 0 } else { 1 });
    })
}

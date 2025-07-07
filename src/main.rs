use std::{error::Error, thread};

use pgcache_lib::cache::cache_run;
use pgcache_lib::proxy::{ConnectionError, proxy_run};
use pgcache_lib::settings::Settings;
use pgcache_lib::tracing_utils::SimpeFormatter;

use tokio::io;
use tokio::sync::mpsc;
use tracing::{Level, error};

fn main() -> Result<(), Box<dyn Error>> {
    // let settings = Settings::new()?;
    let settings = Settings::from_args()?;

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .event_format(SimpeFormatter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let res = thread::scope(|scope| {
        const DEFAULT_CHANNEL_SIZE: usize = 100;
        let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let listen_handle = thread::Builder::new()
            .name("listen".to_owned())
            .spawn_scoped(scope, || proxy_run(&settings, tx))?;

        let _cache_handle = thread::Builder::new()
            .name("cache".to_owned())
            .spawn_scoped(scope, || cache_run(&settings, rx))?;

        match listen_handle.join() {
            Ok(res) => res,
            Err(_panic) => {
                error!("listen thread panicked");
                Err(ConnectionError::IoError(io::Error::other(
                    "listen thread panicked",
                )))
            }
        }
    });

    res.map_err(|e| Box::new(e) as Box<dyn Error>)
}

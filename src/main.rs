use std::process::exit;
use std::{error::Error, thread};

use pgcache_lib::proxy::{ConnectionError, proxy_run};
use pgcache_lib::settings::Settings;
use pgcache_lib::tracing_utils::SimpeFormatter;

use tokio::io;
use tracing::{Level, error};

fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::from_args()?;

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .event_format(SimpeFormatter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    thread::scope(|scope| {
        let proxy_handle = thread::Builder::new()
            .name("proxy".to_owned())
            .spawn_scoped(scope, || proxy_run(&settings))?;

        let res = proxy_handle
            .join()
            .unwrap_or_else(|_panic| {
                Err(ConnectionError::IoError(io::Error::other(
                    "proxy thread panicked",
                )))
            })
            .map_err(|e| Box::new(e) as Box<dyn Error>);

        error!("process terminating {res:?}");
        exit(if res.is_ok() { 0 } else { 1 });
    })
}

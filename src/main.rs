use std::{error::Error, thread};

use pgcache_lib::proxy::{ConnectionError, handle_listen};
use pgcache_lib::settings::Settings;
use pgcache_lib::tracing_utils::SimpeFormatter;

use lexopt::prelude::*;
use tokio::io;
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
        let handle = thread::Builder::new()
            .name("listen".to_owned())
            .spawn_scoped(scope, || handle_listen(&settings))?;

        match handle.join() {
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

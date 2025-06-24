mod pg;
mod proxy;
mod settings;
mod tracing_utils;

use std::{error::Error, thread};

use proxy::handle_listen;
use settings::Settings;
use tracing::Level;
use tracing_utils::SimpeFormatter;

fn main() -> Result<(), Box<dyn Error>> {
    let settings = Settings::new()?;

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .event_format(SimpeFormatter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    thread::scope(|scope| {
        let _ = thread::Builder::new()
            .name("listen".to_owned())
            .spawn_scoped(scope, || handle_listen(&settings));
    });

    Ok(())
}

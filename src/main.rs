mod settings;
mod proxy;

use std::{io::Error, thread};

use settings::Settings;
use proxy::handle_listen;


fn main() -> Result<(), Error> {
    let settings = Settings::new()?;
    println!("{settings:?}");

    thread::scope(|scope| {
        let _ = thread::Builder::new()
            .name("listen".to_owned())
            .spawn_scoped(scope, || handle_listen(&settings));
    });

    Ok(())
}

//! PostgreSQL connection utilities.
//!
//! Provides functions for building connection configs and establishing
//! connections to PostgreSQL databases with optional TLS support.

use tokio_postgres::{Client, Config, Error, NoTls};
use tracing::{debug, error, warn};

use crate::result::error_chain_format;
use crate::settings::{PgSettings, SslMode};
use crate::tls;

/// Build a tokio_postgres Config from PgSettings.
///
/// The returned config can be modified before connecting (e.g., to add replication mode).
pub fn config_build(settings: &PgSettings) -> Config {
    let mut config = Config::new();
    config
        .host(&settings.host)
        .port(settings.port)
        .user(&settings.user)
        .dbname(&settings.database);
    if let Some(ref password) = settings.password {
        config.password(password);
    }
    config
}

/// Connect to a PostgreSQL database using the provided settings.
///
/// Handles TLS negotiation based on `ssl_mode` and spawns a background task
/// to drive the connection. The `context` parameter is used in error messages
/// to identify the connection source.
pub async fn connect(settings: &PgSettings, context: &str) -> Result<Client, Error> {
    let config = config_build(settings);
    debug!(
        "[{context}] connecting to postgres {}:{} db={} user={} ssl={:?}",
        settings.host, settings.port, settings.database, settings.user, settings.ssl_mode
    );
    let result = config_connect(config, settings.ssl_mode, context).await;
    if let Err(ref e) = result {
        warn!(
            "[{context}] connection failed to {}:{} db={} user={} ssl={:?}: {}",
            settings.host,
            settings.port,
            settings.database,
            settings.user,
            settings.ssl_mode,
            error_chain_format(e),
        );
    }
    result
}

/// Connect using an existing Config with the specified SSL mode.
///
/// This is useful when the config has been modified (e.g., replication mode added).
pub async fn config_connect(
    config: Config,
    ssl_mode: SslMode,
    context: &str,
) -> Result<Client, Error> {
    let tls_config = match ssl_mode {
        SslMode::Disable => None,
        SslMode::Require => Some(tls::tls_config_no_verify_build()),
        SslMode::VerifyFull => Some(tls::tls_config_verify_build()),
    };

    match tls_config {
        None => {
            let (client, connection) = config.connect(NoTls).await?;
            let context = context.to_owned();
            tokio::spawn(async move {
                match connection.await {
                    Ok(()) => debug!("[{context}] connection task ended"),
                    Err(e) => error!(
                        "[{context}] connection task error: {}",
                        error_chain_format(&e)
                    ),
                }
            });
            Ok(client)
        }
        Some(config_tls) => {
            let tls_connector = tls::MakeRustlsConnect::new(config_tls);
            let (client, connection) = config.connect(tls_connector).await?;
            let context = context.to_owned();
            tokio::spawn(async move {
                match connection.await {
                    Ok(()) => debug!("[{context}] connection task ended"),
                    Err(e) => error!(
                        "[{context}] connection task error: {}",
                        error_chain_format(&e)
                    ),
                }
            });
            Ok(client)
        }
    }
}

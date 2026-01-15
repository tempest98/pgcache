use error_set::error_set;
use std::io;
use tokio_postgres::{Config, Error, NoTls};
use tracing::{debug, error};

use crate::{
    settings::{Settings, SslMode},
    tls,
};

error_set! {
    PgCdcError := {
        IoError(io::Error),
        PgError(Error),
    }
}

/// Ensures publication and replication slot exist on origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub async fn replication_provision(settings: &Settings) -> Result<(), PgCdcError> {
    let publication_name = &settings.cdc.publication_name;
    let slot_name = &settings.cdc.slot_name;

    debug!(
        "Provisioning replication: publication={}, slot={}",
        publication_name, slot_name
    );

    // Build regular (non-replication) connection config
    let mut config = Config::new();
    config
        .host(&settings.origin.host)
        .port(settings.origin.port)
        .user(&settings.origin.user)
        .dbname(&settings.origin.database);
    if let Some(ref password) = settings.origin.password {
        config.password(password);
    }

    debug!("cdc provisioning config {:?}", config);

    // Connect with appropriate TLS mode
    let client = match settings.origin.ssl_mode {
        SslMode::Disable => {
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Provisioning connection error: {e}");
                }
            });
            client
        }
        SslMode::Require => {
            let tls_connector = tls::MakeRustlsConnect::new(tls::tls_config_build());
            let (client, connection) = config.connect(tls_connector).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Provisioning connection error: {e}");
                }
            });
            client
        }
    };

    debug!("cdc provisioning connected");

    // Check if publication exists
    let pub_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?
        .is_some();

    if !pub_exists {
        debug!("Creating publication: {}", publication_name);
        // Note: publication names can't be parameterized, must use format!
        let create_pub = format!("CREATE PUBLICATION {} FOR ALL TABLES", publication_name);
        client.execute(&create_pub, &[]).await?;
        debug!("Publication created successfully");
    } else {
        debug!("Publication already exists: {}", publication_name);
    }

    // Check if replication slot exists
    let slot_exists = client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?
        .is_some();

    if !slot_exists {
        debug!("Creating replication slot: {}", slot_name);
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot_name],
            )
            .await?;
        debug!("Replication slot created successfully");
    } else {
        debug!("Replication slot already exists: {}", slot_name);
    }

    Ok(())
}

/// Cleanup publication and replication slot from origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub async fn replication_cleanup(settings: &Settings) -> Result<(), PgCdcError> {
    let publication_name = &settings.cdc.publication_name;
    let slot_name = &settings.cdc.slot_name;

    debug!(
        "Cleaning up replication: publication={}, slot={}",
        publication_name, slot_name
    );

    // Build regular (non-replication) connection config
    let mut config = Config::new();
    config
        .host(&settings.origin.host)
        .port(settings.origin.port)
        .user(&settings.origin.user)
        .dbname(&settings.origin.database);
    if let Some(ref password) = settings.origin.password {
        config.password(password);
    }

    // Connect with appropriate TLS mode
    let client = match settings.origin.ssl_mode {
        SslMode::Disable => {
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Cleanup connection error: {e}");
                }
            });
            client
        }
        SslMode::Require => {
            let tls_connector = tls::MakeRustlsConnect::new(tls::tls_config_build());
            let (client, connection) = config.connect(tls_connector).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Cleanup connection error: {e}");
                }
            });
            client
        }
    };

    // Drop replication slot if it exists
    debug!("Dropping replication slot: {}", slot_name);
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;

    // Drop publication if it exists
    debug!("Dropping publication: {}", publication_name);
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", publication_name);
    client.execute(&drop_pub, &[]).await?;

    debug!("Replication cleanup complete");
    Ok(())
}

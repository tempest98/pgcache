use error_set::error_set;
use rootcause::Report;
use std::io;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::{Client, Error};
use tracing::debug;

use crate::result::MapIntoReport;
use crate::settings::{PgSettings, Settings};

use super::connect::{config_build, config_connect, connect};

error_set! {
    PgCdcError := {
        IoError(io::Error),
        PgError(Error),
    }
}

/// Result type with location-tracking error reports for CDC operations.
pub type PgCdcResult<T> = Result<T, Report<PgCdcError>>;

/// Ensures publication and replication slot exist on origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub async fn replication_provision(settings: &Settings) -> PgCdcResult<()> {
    let publication_name = &settings.cdc.publication_name;
    let slot_name = &settings.cdc.slot_name;

    debug!(
        "Provisioning replication: publication={}, slot={}",
        publication_name, slot_name
    );

    let client = connect(&settings.origin, "replication provisioning")
        .await
        .map_into_report::<PgCdcError>()?;

    debug!("cdc provisioning connected");

    // Check if publication exists
    let pub_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .map_into_report::<PgCdcError>()?
        .is_some();

    if !pub_exists {
        debug!("Creating publication: {}", publication_name);
        // Note: publication names can't be parameterized, must use format!
        let create_pub = format!("CREATE PUBLICATION {} FOR ALL TABLES", publication_name);
        client
            .execute(&create_pub, &[])
            .await
            .map_into_report::<PgCdcError>()?;
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
        .await
        .map_into_report::<PgCdcError>()?
        .is_some();

    if !slot_exists {
        debug!("Creating replication slot: {}", slot_name);
        client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot_name],
            )
            .await
            .map_into_report::<PgCdcError>()?;
        debug!("Replication slot created successfully");
    } else {
        debug!("Replication slot already exists: {}", slot_name);
    }

    Ok(())
}

/// Cleanup publication and replication slot from origin.
/// Uses a regular (non-replication) connection since replication connections can't run SQL.
pub async fn replication_cleanup(settings: &Settings) -> PgCdcResult<()> {
    let publication_name = &settings.cdc.publication_name;
    let slot_name = &settings.cdc.slot_name;

    debug!(
        "Cleaning up replication: publication={}, slot={}",
        publication_name, slot_name
    );

    let client = connect(&settings.origin, "replication cleanup")
        .await
        .map_into_report::<PgCdcError>()?;

    // Drop replication slot if it exists
    debug!("Dropping replication slot: {}", slot_name);
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_into_report::<PgCdcError>()?;

    // Drop publication if it exists
    debug!("Dropping publication: {}", publication_name);
    let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", publication_name);
    client
        .execute(&drop_pub, &[])
        .await
        .map_into_report::<PgCdcError>()?;

    debug!("Replication cleanup complete");
    Ok(())
}

/// Connect to a PostgreSQL database in logical replication mode.
///
/// This is used for CDC streaming connections that receive logical replication events.
pub async fn connect_replication(settings: &PgSettings, context: &str) -> Result<Client, Error> {
    let mut config = config_build(settings);
    config.replication_mode(ReplicationMode::Logical);
    config_connect(config, settings.ssl_mode, context).await
}

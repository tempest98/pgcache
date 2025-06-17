use std::{
    error::Error,
    fmt, io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};

use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ConfigError;

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "there was an error with the config")
    }
}
impl Error for ConfigError {}

impl From<ConfigError> for io::Error {
    fn from(error: ConfigError) -> Self {
        io::Error::other(error)
    }
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct PgSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct ListenSettings {
    pub socket: SocketAddr,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub origin: PgSettings,
    pub cache: PgSettings,
    pub listen: ListenSettings,
    pub num_workers: usize,
}

impl Settings {
    pub(crate) fn new() -> Result<Settings, ConfigError> {
        let settings = Settings {
            origin: PgSettings {
                host: "localhost".to_owned(),
                port: 5432,
                user: "posgres".to_owned(),
                database: "origin".to_owned(),
            },
            cache: PgSettings {
                host: "localhost".to_owned(),
                port: 7654,
                user: "posgres".to_owned(),
                database: "cache".to_owned(),
            },
            listen: ListenSettings {
                socket: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6432)),
            },
            num_workers: 2,
        };

        Ok(settings)
    }
}

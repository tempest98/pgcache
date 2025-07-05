use std::{
    error::Error,
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};

use error_set::error_set;
use lexopt::prelude::*;
use serde::Deserialize;

error_set! {
    ConfigError = {
        ArgumentError(Box<dyn Error + Send + Sync + 'static>),

        #[display("Missing argument: {name}")]
        ArgumentMissing{ name: &'static str},
        IoError(io::Error),
    };
}

impl From<lexopt::Error> for ConfigError {
    fn from(error: lexopt::Error) -> Self {
        Self::ArgumentError(Box::new(error))
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
    pub fn new() -> Result<Settings, ConfigError> {
        let settings = Settings {
            origin: PgSettings {
                host: "localhost".to_owned(),
                port: 5432,
                user: "postgres".to_owned(),
                database: "origin".to_owned(),
            },
            cache: PgSettings {
                host: "localhost".to_owned(),
                port: 7654,
                user: "postgres".to_owned(),
                database: "cache".to_owned(),
            },
            listen: ListenSettings {
                socket: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6432)),
            },
            num_workers: 2,
        };

        Ok(settings)
    }

    pub fn from_args() -> Result<Settings, ConfigError> {
        let mut origin_host: Option<String> = None;
        let mut origin_port: Option<u16> = None;
        let mut origin_user: Option<String> = None;
        let mut origin_database: Option<String> = None;
        let mut cache_host: Option<String> = None;
        let mut cache_port: Option<u16> = None;
        let mut cache_user: Option<String> = None;
        let mut cache_database: Option<String> = None;

        let mut parser = lexopt::Parser::from_env();
        while let Some(arg) = parser.next()? {
            match arg {
                Long("origin_host") => origin_host = Some(parser.value()?.string()?),
                Long("origin_port") => origin_port = Some(parser.value()?.parse()?),
                Long("origin_user") => origin_user = Some(parser.value()?.parse()?),
                Long("origin_database") => origin_database = Some(parser.value()?.parse()?),
                Long("cache_host") => cache_host = Some(parser.value()?.string()?),
                Long("cache_port") => cache_port = Some(parser.value()?.parse()?),
                Long("cache_user") => cache_user = Some(parser.value()?.parse()?),
                Long("cache_database") => cache_database = Some(parser.value()?.parse()?),
                Long("help") => {
                    Self::print_usage_and_exit(parser.bin_name().unwrap_or_default());
                }
                _ => return Err(ConfigError::ArgumentError(Box::new(arg.unexpected()))),
            }
        }

        let settings = Settings {
            origin: PgSettings {
                host: origin_host.ok_or_else(|| ConfigError::ArgumentMissing {
                    name: "origin_host",
                })?,
                port: origin_port.ok_or_else(|| ConfigError::ArgumentMissing {
                    name: "origin_port",
                })?,
                user: origin_user.ok_or_else(|| ConfigError::ArgumentMissing {
                    name: "origin_user",
                })?,
                database: origin_database.ok_or_else(|| ConfigError::ArgumentMissing {
                    name: "origin_database",
                })?,
            },
            cache: PgSettings {
                host: cache_host
                    .ok_or_else(|| ConfigError::ArgumentMissing { name: "cache_host" })?,
                port: cache_port
                    .ok_or_else(|| ConfigError::ArgumentMissing { name: "cache_port" })?,
                user: cache_user
                    .ok_or_else(|| ConfigError::ArgumentMissing { name: "cache_user" })?,
                database: cache_database.ok_or_else(|| ConfigError::ArgumentMissing {
                    name: "cache_database",
                })?,
            },
            listen: ListenSettings {
                socket: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6432)),
            },
            num_workers: 2,
        };

        Ok(settings)
    }

    fn print_usage_and_exit(name: &str) -> ! {
        println!(
            "Usage: {name} --origin_host HOST --origin_port PORT --origin_user USER --origin_database DB \n \
            --cache_host HOST --cache_port PORT --cache_user USER --cache_database DB"
        );
        std::process::exit(1);
    }
}

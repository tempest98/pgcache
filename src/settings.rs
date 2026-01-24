use std::{
    error::Error, fmt, fs::read_to_string, io, net::SocketAddr, path::PathBuf, str::FromStr,
};

use error_set::error_set;
use lexopt::prelude::*;
use rootcause::Report;
use serde::Deserialize;

use crate::result::MapIntoReport;

error_set! {
    ConfigError := {
        ArgumentError(Box<dyn Error + Send + Sync + 'static>),
        TomlError(Box<dyn Error + Send + Sync + 'static>),

        #[display("Missing argument: {name}")]
        ArgumentMissing{ name: &'static str},
        IoError(io::Error),
    }
}

/// Result type with location-tracking error reports for configuration operations.
pub type ConfigResult<T> = Result<T, Report<ConfigError>>;

impl From<lexopt::Error> for ConfigError {
    fn from(error: lexopt::Error) -> Self {
        Self::ArgumentError(Box::new(error))
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(error: toml::de::Error) -> Self {
        Self::TomlError(Box::new(error))
    }
}

/// SSL/TLS connection mode for PostgreSQL connections
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    /// No TLS encryption (default for backwards compatibility)
    #[default]
    Disable,
    /// Require TLS encryption, fail if not supported
    Require,
}

/// Error returned when parsing an invalid SSL mode string
#[derive(Debug, Clone)]
pub struct ParseSslModeError(String);

impl fmt::Display for ParseSslModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid SSL mode: '{}', expected 'disable' or 'require'",
            self.0
        )
    }
}

impl Error for ParseSslModeError {}

impl FromStr for SslMode {
    type Err = ParseSslModeError;

    /// Parse SSL mode from string (case-insensitive)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "require" => Ok(SslMode::Require),
            _ => Err(ParseSslModeError(s.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
    #[serde(default)]
    pub ssl_mode: SslMode,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CdcSettings {
    pub publication_name: String,
    pub slot_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenSettings {
    pub socket: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsSettings {
    pub socket: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub origin: PgSettings,
    pub cache: PgSettings,
    pub cdc: CdcSettings,
    pub listen: ListenSettings,
    pub num_workers: usize,
    pub cache_size: Option<usize>,
    /// TLS certificate file path (PEM format) for client connections
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    /// TLS private key file path (PEM format) for client connections
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    /// Prometheus metrics endpoint configuration
    #[serde(default)]
    pub metrics: Option<MetricsSettings>,
    /// Log level filter (supports tracing EnvFilter syntax)
    /// Examples: "debug", "info", "pgcache_lib::cache=debug,info"
    #[serde(default)]
    pub log_level: Option<String>,
}

impl Settings {
    pub fn from_args() -> ConfigResult<Settings> {
        let mut origin_host: Option<String> = None;
        let mut origin_port: Option<u16> = None;
        let mut origin_user: Option<String> = None;
        let mut origin_database: Option<String> = None;
        let mut origin_ssl_mode: Option<SslMode> = None;
        let mut origin_password: Option<String> = None;
        let mut cache_host: Option<String> = None;
        let mut cache_port: Option<u16> = None;
        let mut cache_user: Option<String> = None;
        let mut cache_database: Option<String> = None;
        let mut cdc_publication_name: Option<String> = None;
        let mut cdc_slot_name: Option<String> = None;
        let mut listen_socket: Option<SocketAddr> = None;
        let mut num_workers: Option<usize> = None;
        let mut cache_size: Option<usize> = None;
        let mut tls_cert: Option<PathBuf> = None;
        let mut tls_key: Option<PathBuf> = None;
        let mut metrics_socket: Option<SocketAddr> = None;
        let mut log_level: Option<String> = None;

        let mut config_settings: Option<Settings> = None;
        let mut parser = lexopt::Parser::from_env();
        while let Some(arg) = parser.next().map_into_report::<ConfigError>()? {
            match arg {
                Short('c') | Long("config") => {
                    let path = parser
                        .value()
                        .map_into_report::<ConfigError>()?
                        .string()
                        .map_into_report::<ConfigError>()?;
                    let file = read_to_string(path).map_into_report::<ConfigError>()?;
                    config_settings = Some(toml::from_str(&file).map_into_report::<ConfigError>()?);
                }
                Long("origin_host") => {
                    origin_host = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("origin_port") => {
                    origin_port = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("origin_user") => {
                    origin_user = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("origin_database") => {
                    origin_database = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("origin_ssl_mode") => {
                    let mode_str = parser
                        .value()
                        .map_into_report::<ConfigError>()?
                        .string()
                        .map_into_report::<ConfigError>()?;
                    origin_ssl_mode = Some(mode_str.parse().map_err(|e: ParseSslModeError| {
                        Report::from(ConfigError::ArgumentError(e.to_string().into()))
                    })?);
                }
                Long("origin_password") => {
                    origin_password = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cache_host") => {
                    cache_host = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cache_port") => {
                    cache_port = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cache_user") => {
                    cache_user = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cache_database") => {
                    cache_database = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cdc_publication_name") => {
                    cdc_publication_name = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cdc_slot_name") => {
                    cdc_slot_name = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("listen_socket") => {
                    listen_socket = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("num_workers") => {
                    num_workers = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("cache_size") => {
                    cache_size = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("tls_cert") => {
                    tls_cert = Some(PathBuf::from(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    ))
                }
                Long("tls_key") => {
                    tls_key = Some(PathBuf::from(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    ))
                }
                Long("metrics_socket") => {
                    metrics_socket = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("log_level") => {
                    log_level = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("help") => {
                    Self::print_usage_and_exit(parser.bin_name().unwrap_or_default());
                }
                Short(_) | Long(_) | Value(_) => {
                    return Err(ConfigError::ArgumentError(Box::new(arg.unexpected())).into());
                }
            }
        }

        let mut settings = if let Some(mut config) = config_settings {
            //command line arguments can override values loaded from a config file
            config.origin.host = origin_host.unwrap_or(config.origin.host);
            config.origin.port = origin_port.unwrap_or(config.origin.port);
            config.origin.user = origin_user.unwrap_or(config.origin.user);
            config.origin.database = origin_database.unwrap_or(config.origin.database);
            config.origin.ssl_mode = origin_ssl_mode.unwrap_or(config.origin.ssl_mode);
            config.origin.password = origin_password.or(config.origin.password);

            config.cache.host = cache_host.unwrap_or(config.cache.host);
            config.cache.port = cache_port.unwrap_or(config.cache.port);
            config.cache.user = cache_user.unwrap_or(config.cache.user);
            config.cache.database = cache_database.unwrap_or(config.cache.database);

            config.cdc.publication_name =
                cdc_publication_name.unwrap_or(config.cdc.publication_name);
            config.cdc.slot_name = cdc_slot_name.unwrap_or(config.cdc.slot_name);
            config.listen.socket = listen_socket.unwrap_or(config.listen.socket);
            config.num_workers = num_workers.unwrap_or(config.num_workers);
            config.cache_size = cache_size.or(config.cache_size);
            config.tls_cert = tls_cert.or(config.tls_cert);
            config.tls_key = tls_key.or(config.tls_key);
            config.metrics = metrics_socket
                .map(|socket| MetricsSettings { socket })
                .or(config.metrics);
            config.log_level = log_level.or(config.log_level);

            config
        } else {
            Settings {
                origin: PgSettings {
                    host: origin_host.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "origin_host",
                        })
                    })?,
                    port: origin_port.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "origin_port",
                        })
                    })?,
                    user: origin_user.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "origin_user",
                        })
                    })?,
                    password: origin_password,
                    database: origin_database.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "origin_database",
                        })
                    })?,
                    ssl_mode: origin_ssl_mode.unwrap_or_default(),
                },
                cache: PgSettings {
                    host: cache_host.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing { name: "cache_host" })
                    })?,
                    port: cache_port.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing { name: "cache_port" })
                    })?,
                    user: cache_user.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing { name: "cache_user" })
                    })?,
                    password: None, // Cache is localhost, uses trust auth
                    database: cache_database.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "cache_database",
                        })
                    })?,
                    ssl_mode: SslMode::Disable, // Cache is always localhost, no TLS needed
                },
                cdc: CdcSettings {
                    publication_name: cdc_publication_name.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "cdc_publication_name",
                        })
                    })?,
                    slot_name: cdc_slot_name.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "cdc_slot_name",
                        })
                    })?,
                },
                listen: ListenSettings {
                    socket: listen_socket.ok_or_else(|| {
                        Report::from(ConfigError::ArgumentMissing {
                            name: "listen_socket",
                        })
                    })?,
                },
                num_workers: num_workers.ok_or_else(|| {
                    Report::from(ConfigError::ArgumentMissing {
                        name: "num_workers",
                    })
                })?,
                cache_size,
                tls_cert,
                tls_key,
                metrics: metrics_socket.map(|socket| MetricsSettings { socket }),
                log_level,
            }
        };

        //make cdc settings lowercase to avoid having to quote them in postgres
        settings.cdc.publication_name = settings.cdc.publication_name.to_ascii_lowercase();
        settings.cdc.slot_name = settings.cdc.slot_name.to_ascii_lowercase();

        Ok(settings)
    }

    fn print_usage_and_exit(name: &str) -> ! {
        println!(
            "Usage: {name} -c|--config TOML_FILE --origin_host HOST --origin_port PORT --origin_user USER --origin_database DB \n \
            [--origin_password PASSWORD] [--origin_ssl_mode disable|require] \n \
            --cache_host HOST --cache_port PORT --cache_user USER --cache_database DB \n \
            --cdc_publication_name NAME --cdc_slot_name SLOT_NAME \n \
            --listen_socket IP_AND_PORT \n \
            --num_workers NUMBER \n \
            [--cache_size BYTES] \n \
            [--tls_cert CERT_FILE --tls_key KEY_FILE] \n \
            [--metrics_socket IP_AND_PORT] \n \
            [--log_level LEVEL] (e.g., debug, info, pgcache_lib::cache=debug)"
        );
        std::process::exit(1);
    }
}

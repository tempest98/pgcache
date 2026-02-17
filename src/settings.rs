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

/// Cache eviction policy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CachePolicy {
    /// FIFO eviction: oldest-registered query evicted first, no admission gating
    Fifo,
    /// CLOCK eviction: second-chance algorithm with frequency-based admission
    #[default]
    Clock,
}

/// Error returned when parsing an invalid cache policy string
#[derive(Debug, Clone)]
pub struct ParseCachePolicyError(String);

impl fmt::Display for ParseCachePolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid cache policy: '{}', expected 'fifo' or 'clock'",
            self.0
        )
    }
}

impl Error for ParseCachePolicyError {}

impl FromStr for CachePolicy {
    type Err = ParseCachePolicyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fifo" => Ok(CachePolicy::Fifo),
            "clock" => Ok(CachePolicy::Clock),
            _ => Err(ParseCachePolicyError(s.to_owned())),
        }
    }
}

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

/// Partial PostgreSQL settings where all fields are optional.
/// Used for replication settings that cascade defaults from origin.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PgSettingsPartial {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub database: Option<String>,
    #[serde(default)]
    pub ssl_mode: Option<SslMode>,
}

impl PgSettingsPartial {
    /// Merge with a base PgSettings, using base values for any unspecified fields.
    pub fn merge_with(&self, base: &PgSettings) -> PgSettings {
        PgSettings {
            host: self.host.clone().unwrap_or_else(|| base.host.clone()),
            port: self.port.unwrap_or(base.port),
            user: self.user.clone().unwrap_or_else(|| base.user.clone()),
            password: self.password.clone().or_else(|| base.password.clone()),
            database: self
                .database
                .clone()
                .unwrap_or_else(|| base.database.clone()),
            ssl_mode: self.ssl_mode.unwrap_or(base.ssl_mode),
        }
    }
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

/// Internal struct for TOML deserialization with optional replication settings.
#[derive(Debug, Clone, Deserialize)]
struct SettingsToml {
    origin: PgSettings,
    #[serde(default)]
    replication: Option<PgSettingsPartial>,
    cache: PgSettings,
    cdc: CdcSettings,
    listen: ListenSettings,
    num_workers: usize,
    cache_size: Option<usize>,
    #[serde(default)]
    tls_cert: Option<PathBuf>,
    #[serde(default)]
    tls_key: Option<PathBuf>,
    #[serde(default)]
    metrics: Option<MetricsSettings>,
    #[serde(default)]
    log_level: Option<String>,
    #[serde(default)]
    cache_policy: Option<CachePolicy>,
    #[serde(default)]
    admission_threshold: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub origin: PgSettings,
    /// Replication connection settings. Defaults to origin if not specified.
    /// Each field cascades from origin if not explicitly set.
    pub replication: PgSettings,
    pub cache: PgSettings,
    pub cdc: CdcSettings,
    pub listen: ListenSettings,
    pub num_workers: usize,
    pub cache_size: Option<usize>,
    /// TLS certificate file path (PEM format) for client connections
    pub tls_cert: Option<PathBuf>,
    /// TLS private key file path (PEM format) for client connections
    pub tls_key: Option<PathBuf>,
    /// Prometheus metrics endpoint configuration
    pub metrics: Option<MetricsSettings>,
    /// Log level filter (supports tracing EnvFilter syntax)
    /// Examples: "debug", "info", "pgcache_lib::cache=debug,info"
    pub log_level: Option<String>,
    /// Cache eviction policy: fifo or clock (default: clock)
    pub cache_policy: CachePolicy,
    /// Number of times a query must be seen before admission to cache (default: 2)
    /// Only used with clock policy; fifo always admits immediately.
    pub admission_threshold: u32,
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
        let mut replication_host: Option<String> = None;
        let mut replication_port: Option<u16> = None;
        let mut replication_user: Option<String> = None;
        let mut replication_database: Option<String> = None;
        let mut replication_ssl_mode: Option<SslMode> = None;
        let mut replication_password: Option<String> = None;
        let mut cache_policy: Option<CachePolicy> = None;
        let mut admission_threshold: Option<u32> = None;

        let mut config_settings: Option<SettingsToml> = None;
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
                Long("replication_host") => {
                    replication_host = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("replication_port") => {
                    replication_port = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("replication_user") => {
                    replication_user = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("replication_database") => {
                    replication_database = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .string()
                            .map_into_report::<ConfigError>()?,
                    )
                }
                Long("replication_ssl_mode") => {
                    let mode_str = parser
                        .value()
                        .map_into_report::<ConfigError>()?
                        .string()
                        .map_into_report::<ConfigError>()?;
                    replication_ssl_mode =
                        Some(mode_str.parse().map_err(|e: ParseSslModeError| {
                            Report::from(ConfigError::ArgumentError(e.to_string().into()))
                        })?);
                }
                Long("replication_password") => {
                    replication_password = Some(
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
                Long("cache_policy") => {
                    let policy_str = parser
                        .value()
                        .map_into_report::<ConfigError>()?
                        .string()
                        .map_into_report::<ConfigError>()?;
                    cache_policy =
                        Some(policy_str.parse().map_err(|e: ParseCachePolicyError| {
                            Report::from(ConfigError::ArgumentError(e.to_string().into()))
                        })?);
                }
                Long("admission_threshold") => {
                    admission_threshold = Some(
                        parser
                            .value()
                            .map_into_report::<ConfigError>()?
                            .parse()
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

            // Compute replication: TOML partial -> merge with origin -> CLI overrides
            let mut replication = match config.replication {
                Some(partial) => partial.merge_with(&config.origin),
                None => config.origin.clone(),
            };
            // Apply CLI overrides on top
            if let Some(host) = replication_host {
                replication.host = host;
            }
            if let Some(port) = replication_port {
                replication.port = port;
            }
            if let Some(user) = replication_user {
                replication.user = user;
            }
            if let Some(database) = replication_database {
                replication.database = database;
            }
            if let Some(ssl_mode) = replication_ssl_mode {
                replication.ssl_mode = ssl_mode;
            }
            if replication_password.is_some() {
                replication.password = replication_password;
            }

            config.cache.host = cache_host.unwrap_or(config.cache.host);
            config.cache.port = cache_port.unwrap_or(config.cache.port);
            config.cache.user = cache_user.unwrap_or(config.cache.user);
            config.cache.database = cache_database.unwrap_or(config.cache.database);

            let cdc_publication_name = cdc_publication_name.unwrap_or(config.cdc.publication_name);
            let cdc_slot_name = cdc_slot_name.unwrap_or(config.cdc.slot_name);
            let listen_socket = listen_socket.unwrap_or(config.listen.socket);
            let num_workers = num_workers.unwrap_or(config.num_workers);
            let cache_size = cache_size.or(config.cache_size);
            let tls_cert = tls_cert.or(config.tls_cert);
            let tls_key = tls_key.or(config.tls_key);
            let metrics = metrics_socket
                .map(|socket| MetricsSettings { socket })
                .or(config.metrics);
            let log_level = log_level.or(config.log_level);
            let cache_policy = cache_policy
                .or(config.cache_policy)
                .unwrap_or_default();
            let admission_threshold = admission_threshold
                .or(config.admission_threshold)
                .unwrap_or(2);

            Settings {
                origin: config.origin,
                replication,
                cache: config.cache,
                cdc: CdcSettings {
                    publication_name: cdc_publication_name,
                    slot_name: cdc_slot_name,
                },
                listen: ListenSettings {
                    socket: listen_socket,
                },
                num_workers,
                cache_size,
                tls_cert,
                tls_key,
                metrics,
                log_level,
                cache_policy,
                admission_threshold,
            }
        } else {
            let origin = PgSettings {
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
            };
            // CLI-only mode: replication defaults to origin, with CLI overrides
            let mut replication = origin.clone();
            if let Some(host) = replication_host {
                replication.host = host;
            }
            if let Some(port) = replication_port {
                replication.port = port;
            }
            if let Some(user) = replication_user {
                replication.user = user;
            }
            if let Some(database) = replication_database {
                replication.database = database;
            }
            if let Some(ssl_mode) = replication_ssl_mode {
                replication.ssl_mode = ssl_mode;
            }
            if replication_password.is_some() {
                replication.password = replication_password;
            }
            Settings {
                origin,
                replication,
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
                cache_policy: cache_policy.unwrap_or_default(),
                admission_threshold: admission_threshold.unwrap_or(2),
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
            [--replication_host HOST] [--replication_port PORT] [--replication_user USER] [--replication_database DB] \n \
            [--replication_password PASSWORD] [--replication_ssl_mode disable|require] \n \
            --cache_host HOST --cache_port PORT --cache_user USER --cache_database DB \n \
            --cdc_publication_name NAME --cdc_slot_name SLOT_NAME \n \
            --listen_socket IP_AND_PORT \n \
            --num_workers NUMBER \n \
            [--cache_size BYTES] \n \
            [--cache_policy fifo|clock] (default: clock) \n \
            [--admission_threshold N] (default: 2, clock policy only) \n \
            [--tls_cert CERT_FILE --tls_key KEY_FILE] \n \
            [--metrics_socket IP_AND_PORT] \n \
            [--log_level LEVEL] (e.g., debug, info, pgcache_lib::cache=debug)"
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_settings() -> PgSettings {
        PgSettings {
            host: "base.example.com".to_owned(),
            port: 5432,
            user: "base_user".to_owned(),
            password: Some("base_password".to_owned()),
            database: "base_db".to_owned(),
            ssl_mode: SslMode::Disable,
        }
    }

    #[test]
    fn partial_merge_empty_uses_all_base_values() {
        let base = base_settings();
        let partial = PgSettingsPartial::default();

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "base.example.com");
        assert_eq!(result.port, 5432);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn partial_merge_host_only_override() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "override.example.com");
        assert_eq!(result.port, 5432);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn partial_merge_multiple_fields_override() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            port: Some(6432),
            ssl_mode: Some(SslMode::Require),
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "override.example.com");
        assert_eq!(result.port, 6432);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn partial_merge_all_fields_override() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            port: Some(6432),
            user: Some("override_user".to_owned()),
            password: Some("override_password".to_owned()),
            database: Some("override_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "override.example.com");
        assert_eq!(result.port, 6432);
        assert_eq!(result.user, "override_user");
        assert_eq!(result.password, Some("override_password".to_owned()));
        assert_eq!(result.database, "override_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn partial_merge_password_override_when_base_has_none() {
        let mut base = base_settings();
        base.password = None;

        let partial = PgSettingsPartial {
            password: Some("new_password".to_owned()),
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.password, Some("new_password".to_owned()));
    }

    #[test]
    fn partial_merge_password_inherited_when_partial_has_none() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            password: None, // Not specified, should inherit from base
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.password, Some("base_password".to_owned()));
    }

    #[test]
    fn partial_merge_both_passwords_none() {
        let mut base = base_settings();
        base.password = None;

        let partial = PgSettingsPartial::default();

        let result = partial.merge_with(&base);

        assert_eq!(result.password, None);
    }

    #[test]
    fn toml_parse_no_replication_section() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
password = "origin_password"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

        assert!(settings.replication.is_none());

        // When replication is None, it should default to origin
        let replication = match settings.replication {
            Some(partial) => partial.merge_with(&settings.origin),
            None => settings.origin,
        };

        assert_eq!(replication.host, "origin.example.com");
        assert_eq!(replication.port, 5432);
        assert_eq!(replication.user, "origin_user");
        assert_eq!(replication.password, Some("origin_password".to_owned()));
        assert_eq!(replication.database, "origin_db");
    }

    #[test]
    fn toml_parse_partial_replication_section() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
password = "secret"
database = "mydb"
ssl_mode = "require"

[replication]
host = "postgres.example.com"
port = 5432

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

        assert!(settings.replication.is_some());
        let partial = settings.replication.as_ref().expect("replication section");

        assert_eq!(partial.host, Some("postgres.example.com".to_owned()));
        assert_eq!(partial.port, Some(5432));
        assert_eq!(partial.user, None);
        assert_eq!(partial.password, None);
        assert_eq!(partial.database, None);
        assert_eq!(partial.ssl_mode, None);

        // After merging, unspecified fields should come from origin
        let replication = partial.merge_with(&settings.origin);

        assert_eq!(replication.host, "postgres.example.com");
        assert_eq!(replication.port, 5432);
        assert_eq!(replication.user, "app_user");
        assert_eq!(replication.password, Some("secret".to_owned()));
        assert_eq!(replication.database, "mydb");
        assert_eq!(replication.ssl_mode, SslMode::Require);
    }

    #[test]
    fn toml_parse_full_replication_section() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
password = "secret"
database = "mydb"

[replication]
host = "postgres.example.com"
port = 5432
user = "replication_user"
password = "replication_secret"
database = "mydb"
ssl_mode = "require"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");
        let partial = settings.replication.as_ref().expect("replication section");

        let replication = partial.merge_with(&settings.origin);

        assert_eq!(replication.host, "postgres.example.com");
        assert_eq!(replication.port, 5432);
        assert_eq!(replication.user, "replication_user");
        assert_eq!(replication.password, Some("replication_secret".to_owned()));
        assert_eq!(replication.database, "mydb");
        assert_eq!(replication.ssl_mode, SslMode::Require);
    }

    #[test]
    fn toml_parse_replication_host_only() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
database = "mydb"

[replication]
host = "postgres.example.com"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");
        let partial = settings.replication.as_ref().expect("replication section");

        let replication = partial.merge_with(&settings.origin);

        // Only host is overridden
        assert_eq!(replication.host, "postgres.example.com");
        // All other fields come from origin
        assert_eq!(replication.port, 6432);
        assert_eq!(replication.user, "app_user");
        assert_eq!(replication.password, None);
        assert_eq!(replication.database, "mydb");
        assert_eq!(replication.ssl_mode, SslMode::Disable);
    }
}

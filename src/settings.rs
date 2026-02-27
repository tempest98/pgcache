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

/// Resolve replication settings from the three-tier cascade:
/// 1. Origin defaults (base)
/// 2. TOML `[replication]` partial (if present)
/// 3. CLI `--replication_*` overrides (if present)
pub fn replication_settings_resolve(
    origin: &PgSettings,
    toml_replication: Option<PgSettingsPartial>,
    cli_overrides: PgSettingsPartial,
) -> PgSettings {
    let base = match toml_replication {
        Some(partial) => partial.merge_with(origin),
        None => origin.clone(),
    };
    cli_overrides.merge_with(&base)
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

/// Parse the next CLI argument as a string.
fn arg_string(parser: &mut lexopt::Parser) -> ConfigResult<String> {
    parser
        .value()
        .map_into_report::<ConfigError>()?
        .string()
        .map_into_report::<ConfigError>()
}

/// Parse the next CLI argument via `FromStr`.
fn arg_parse<T: FromStr>(parser: &mut lexopt::Parser) -> ConfigResult<T>
where
    T::Err: Error + Send + Sync + 'static,
{
    parser
        .value()
        .map_into_report::<ConfigError>()?
        .parse()
        .map_into_report::<ConfigError>()
}

/// Parse the next CLI argument as a custom enum type, mapping parse errors to `ArgumentError`.
fn arg_enum<T: FromStr>(parser: &mut lexopt::Parser) -> ConfigResult<T>
where
    T::Err: fmt::Display,
{
    let s = arg_string(parser)?;
    s.parse()
        .map_err(|e: T::Err| Report::from(ConfigError::ArgumentError(e.to_string().into())))
}

/// Require an `Option<T>` to be `Some`, or return `ArgumentMissing`.
fn require<T>(value: Option<T>, name: &'static str) -> ConfigResult<T> {
    value.ok_or_else(|| Report::from(ConfigError::ArgumentMissing { name }))
}

/// Raw CLI argument values before merging with config file.
#[derive(Default)]
struct CliArgs {
    origin_host: Option<String>,
    origin_port: Option<u16>,
    origin_user: Option<String>,
    origin_database: Option<String>,
    origin_ssl_mode: Option<SslMode>,
    origin_password: Option<String>,
    replication_host: Option<String>,
    replication_port: Option<u16>,
    replication_user: Option<String>,
    replication_database: Option<String>,
    replication_ssl_mode: Option<SslMode>,
    replication_password: Option<String>,
    cache_host: Option<String>,
    cache_port: Option<u16>,
    cache_user: Option<String>,
    cache_database: Option<String>,
    cdc_publication_name: Option<String>,
    cdc_slot_name: Option<String>,
    listen_socket: Option<SocketAddr>,
    num_workers: Option<usize>,
    cache_size: Option<usize>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    metrics_socket: Option<SocketAddr>,
    log_level: Option<String>,
    cache_policy: Option<CachePolicy>,
    admission_threshold: Option<u32>,
}

fn cli_args_parse() -> ConfigResult<(CliArgs, Option<SettingsToml>)> {
    let mut args = CliArgs::default();
    let mut config = None;
    let mut parser = lexopt::Parser::from_env();

    while let Some(arg) = parser.next().map_into_report::<ConfigError>()? {
        match arg {
            Short('c') | Long("config") => {
                let path = arg_string(&mut parser)?;
                let file = read_to_string(path).map_into_report::<ConfigError>()?;
                config = Some(toml::from_str(&file).map_into_report::<ConfigError>()?);
            }
            Long("origin_host") => args.origin_host = Some(arg_string(&mut parser)?),
            Long("origin_port") => args.origin_port = Some(arg_parse(&mut parser)?),
            Long("origin_user") => args.origin_user = Some(arg_string(&mut parser)?),
            Long("origin_database") => args.origin_database = Some(arg_string(&mut parser)?),
            Long("origin_ssl_mode") => args.origin_ssl_mode = Some(arg_enum(&mut parser)?),
            Long("origin_password") => args.origin_password = Some(arg_string(&mut parser)?),
            Long("replication_host") => args.replication_host = Some(arg_string(&mut parser)?),
            Long("replication_port") => args.replication_port = Some(arg_parse(&mut parser)?),
            Long("replication_user") => args.replication_user = Some(arg_string(&mut parser)?),
            Long("replication_database") => {
                args.replication_database = Some(arg_string(&mut parser)?)
            }
            Long("replication_ssl_mode") => {
                args.replication_ssl_mode = Some(arg_enum(&mut parser)?)
            }
            Long("replication_password") => {
                args.replication_password = Some(arg_string(&mut parser)?)
            }
            Long("cache_host") => args.cache_host = Some(arg_string(&mut parser)?),
            Long("cache_port") => args.cache_port = Some(arg_parse(&mut parser)?),
            Long("cache_user") => args.cache_user = Some(arg_string(&mut parser)?),
            Long("cache_database") => args.cache_database = Some(arg_string(&mut parser)?),
            Long("cdc_publication_name") => {
                args.cdc_publication_name = Some(arg_string(&mut parser)?)
            }
            Long("cdc_slot_name") => args.cdc_slot_name = Some(arg_string(&mut parser)?),
            Long("listen_socket") => args.listen_socket = Some(arg_parse(&mut parser)?),
            Long("num_workers") => args.num_workers = Some(arg_parse(&mut parser)?),
            Long("cache_size") => args.cache_size = Some(arg_parse(&mut parser)?),
            Long("tls_cert") => args.tls_cert = Some(PathBuf::from(arg_string(&mut parser)?)),
            Long("tls_key") => args.tls_key = Some(PathBuf::from(arg_string(&mut parser)?)),
            Long("metrics_socket") => args.metrics_socket = Some(arg_parse(&mut parser)?),
            Long("log_level") => args.log_level = Some(arg_string(&mut parser)?),
            Long("cache_policy") => args.cache_policy = Some(arg_enum(&mut parser)?),
            Long("admission_threshold") => args.admission_threshold = Some(arg_parse(&mut parser)?),
            Long("help") => {
                Settings::print_usage_and_exit(parser.bin_name().unwrap_or_default());
            }
            Short(_) | Long(_) | Value(_) => {
                return Err(ConfigError::ArgumentError(Box::new(arg.unexpected())).into());
            }
        }
    }

    Ok((args, config))
}

fn settings_build(args: CliArgs, config: Option<SettingsToml>) -> ConfigResult<Settings> {
    let mut settings = if let Some(mut config) = config {
        settings_build_with_config(args, &mut config)?
    } else {
        settings_build_cli_only(args)?
    };

    // Lowercase CDC names to avoid quoting in postgres
    settings.cdc.publication_name = settings.cdc.publication_name.to_ascii_lowercase();
    settings.cdc.slot_name = settings.cdc.slot_name.to_ascii_lowercase();

    Ok(settings)
}

/// Build settings by merging CLI args over a TOML config file.
fn settings_build_with_config(args: CliArgs, config: &mut SettingsToml) -> ConfigResult<Settings> {
    let origin_overrides = PgSettingsPartial {
        host: args.origin_host,
        port: args.origin_port,
        user: args.origin_user,
        password: args.origin_password,
        database: args.origin_database,
        ssl_mode: args.origin_ssl_mode,
    };
    let origin = origin_overrides.merge_with(&config.origin);

    let replication = replication_settings_resolve(
        &origin,
        config.replication.take(),
        PgSettingsPartial {
            host: args.replication_host,
            port: args.replication_port,
            user: args.replication_user,
            password: args.replication_password,
            database: args.replication_database,
            ssl_mode: args.replication_ssl_mode,
        },
    );

    let cache_overrides = PgSettingsPartial {
        host: args.cache_host,
        port: args.cache_port,
        user: args.cache_user,
        password: None,
        database: args.cache_database,
        ssl_mode: None,
    };
    let cache = cache_overrides.merge_with(&config.cache);

    Ok(Settings {
        origin,
        replication,
        cache,
        cdc: CdcSettings {
            publication_name: args
                .cdc_publication_name
                .unwrap_or_else(|| config.cdc.publication_name.clone()),
            slot_name: args
                .cdc_slot_name
                .unwrap_or_else(|| config.cdc.slot_name.clone()),
        },
        listen: ListenSettings {
            socket: args.listen_socket.unwrap_or(config.listen.socket),
        },
        num_workers: args.num_workers.unwrap_or(config.num_workers),
        cache_size: args.cache_size.or(config.cache_size),
        tls_cert: args.tls_cert.or_else(|| config.tls_cert.clone()),
        tls_key: args.tls_key.or_else(|| config.tls_key.clone()),
        metrics: args
            .metrics_socket
            .map(|socket| MetricsSettings { socket })
            .or_else(|| config.metrics.clone()),
        log_level: args.log_level.or_else(|| config.log_level.clone()),
        cache_policy: args
            .cache_policy
            .or(config.cache_policy)
            .unwrap_or_default(),
        admission_threshold: args
            .admission_threshold
            .or(config.admission_threshold)
            .unwrap_or(2),
    })
}

/// Build settings from CLI args alone (no config file). Required fields must be present.
fn settings_build_cli_only(args: CliArgs) -> ConfigResult<Settings> {
    let origin = PgSettings {
        host: require(args.origin_host, "origin_host")?,
        port: require(args.origin_port, "origin_port")?,
        user: require(args.origin_user, "origin_user")?,
        password: args.origin_password,
        database: require(args.origin_database, "origin_database")?,
        ssl_mode: args.origin_ssl_mode.unwrap_or_default(),
    };

    // CLI-only mode: replication defaults to origin, with CLI overrides
    let replication = replication_settings_resolve(
        &origin,
        None,
        PgSettingsPartial {
            host: args.replication_host,
            port: args.replication_port,
            user: args.replication_user,
            password: args.replication_password,
            database: args.replication_database,
            ssl_mode: args.replication_ssl_mode,
        },
    );

    Ok(Settings {
        origin,
        replication,
        cache: PgSettings {
            host: require(args.cache_host, "cache_host")?,
            port: require(args.cache_port, "cache_port")?,
            user: require(args.cache_user, "cache_user")?,
            password: None, // Cache is localhost, uses trust auth
            database: require(args.cache_database, "cache_database")?,
            ssl_mode: SslMode::Disable, // Cache is always localhost, no TLS needed
        },
        cdc: CdcSettings {
            publication_name: require(args.cdc_publication_name, "cdc_publication_name")?,
            slot_name: require(args.cdc_slot_name, "cdc_slot_name")?,
        },
        listen: ListenSettings {
            socket: require(args.listen_socket, "listen_socket")?,
        },
        num_workers: require(args.num_workers, "num_workers")?,
        cache_size: args.cache_size,
        tls_cert: args.tls_cert,
        tls_key: args.tls_key,
        metrics: args.metrics_socket.map(|socket| MetricsSettings { socket }),
        log_level: args.log_level,
        cache_policy: args.cache_policy.unwrap_or_default(),
        admission_threshold: args.admission_threshold.unwrap_or(2),
    })
}

impl Settings {
    pub fn from_args() -> ConfigResult<Settings> {
        let (args, config) = cli_args_parse()?;
        settings_build(args, config)
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

    // ==================== replication_settings_resolve Tests ====================

    #[test]
    fn replication_resolve_no_toml_no_cli_defaults_to_origin() {
        let origin = base_settings();

        let result = replication_settings_resolve(&origin, None, PgSettingsPartial::default());

        assert_eq!(result.host, origin.host);
        assert_eq!(result.port, origin.port);
        assert_eq!(result.user, origin.user);
        assert_eq!(result.password, origin.password);
        assert_eq!(result.database, origin.database);
        assert_eq!(result.ssl_mode, origin.ssl_mode);
    }

    #[test]
    fn replication_resolve_toml_partial_merges_with_origin() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            port: Some(5433),
            ..Default::default()
        };

        let result =
            replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

        assert_eq!(result.host, "replica.example.com");
        assert_eq!(result.port, 5433);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn replication_resolve_cli_overrides_origin_when_no_toml() {
        let origin = base_settings();
        let cli = PgSettingsPartial {
            host: Some("cli-host.example.com".to_owned()),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, None, cli);

        assert_eq!(result.host, "cli-host.example.com");
        assert_eq!(result.port, origin.port);
        assert_eq!(result.user, origin.user);
        assert_eq!(result.password, origin.password);
        assert_eq!(result.database, origin.database);
    }

    #[test]
    fn replication_resolve_cli_overrides_toml_partial() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("toml-replica.example.com".to_owned()),
            port: Some(5433),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            host: Some("cli-replica.example.com".to_owned()),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        // CLI host wins over TOML host
        assert_eq!(result.host, "cli-replica.example.com");
        // TOML port wins over origin port (CLI didn't specify)
        assert_eq!(result.port, 5433);
        // Remaining fields from origin
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
    }

    #[test]
    fn replication_resolve_cli_overrides_all_fields() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("toml-replica.example.com".to_owned()),
            user: Some("toml_user".to_owned()),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            host: Some("cli-host.example.com".to_owned()),
            port: Some(6432),
            user: Some("cli_user".to_owned()),
            password: Some("cli_password".to_owned()),
            database: Some("cli_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        assert_eq!(result.host, "cli-host.example.com");
        assert_eq!(result.port, 6432);
        assert_eq!(result.user, "cli_user");
        assert_eq!(result.password, Some("cli_password".to_owned()));
        assert_eq!(result.database, "cli_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn replication_resolve_cli_password_overrides_toml_password() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            password: Some("toml_password".to_owned()),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            password: Some("cli_password".to_owned()),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        assert_eq!(result.password, Some("cli_password".to_owned()));
    }

    #[test]
    fn replication_resolve_cli_password_not_set_preserves_toml_password() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            password: Some("toml_password".to_owned()),
            ..Default::default()
        };

        let result =
            replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

        assert_eq!(result.password, Some("toml_password".to_owned()));
    }

    #[test]
    fn replication_resolve_full_toml_with_no_cli_uses_toml() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            port: Some(5433),
            user: Some("repl_user".to_owned()),
            password: Some("repl_pass".to_owned()),
            database: Some("repl_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        };

        let result =
            replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

        assert_eq!(result.host, "replica.example.com");
        assert_eq!(result.port, 5433);
        assert_eq!(result.user, "repl_user");
        assert_eq!(result.password, Some("repl_pass".to_owned()));
        assert_eq!(result.database, "repl_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn replication_resolve_cli_port_only_with_toml_host() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("toml-host.example.com".to_owned()),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            port: Some(6432),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        // TOML host preserved
        assert_eq!(result.host, "toml-host.example.com");
        // CLI port applied
        assert_eq!(result.port, 6432);
        // Origin fills the rest
        assert_eq!(result.user, "base_user");
        assert_eq!(result.database, "base_db");
    }

    // ==================== settings_build Tests ====================

    fn base_toml_config() -> SettingsToml {
        SettingsToml {
            origin: PgSettings {
                host: "origin.example.com".to_owned(),
                port: 5432,
                user: "origin_user".to_owned(),
                password: Some("origin_pass".to_owned()),
                database: "origin_db".to_owned(),
                ssl_mode: SslMode::Disable,
            },
            replication: None,
            cache: PgSettings {
                host: "localhost".to_owned(),
                port: 5433,
                user: "cache_user".to_owned(),
                password: None,
                database: "cache_db".to_owned(),
                ssl_mode: SslMode::Disable,
            },
            cdc: CdcSettings {
                publication_name: "pub".to_owned(),
                slot_name: "slot".to_owned(),
            },
            listen: ListenSettings {
                socket: "127.0.0.1:6432".parse().expect("valid socket addr"),
            },
            num_workers: 4,
            cache_size: None,
            tls_cert: None,
            tls_key: None,
            metrics: None,
            log_level: None,
            cache_policy: None,
            admission_threshold: None,
        }
    }

    #[test]
    fn settings_build_no_replication_defaults_to_origin() {
        let config = base_toml_config();
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config)).expect("build settings");

        assert_eq!(settings.replication.host, "origin.example.com");
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
    }

    #[test]
    fn settings_build_toml_replication_partial_merges_with_origin() {
        let mut config = base_toml_config();
        config.replication = Some(PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            ..Default::default()
        });
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config)).expect("build settings");

        assert_eq!(settings.replication.host, "replica.example.com");
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
    }

    #[test]
    fn settings_build_cli_replication_overrides_no_toml_section() {
        let config = base_toml_config();
        let args = CliArgs {
            replication_host: Some("cli-replica.example.com".to_owned()),
            replication_port: Some(6432),
            ..Default::default()
        };

        let settings = settings_build(args, Some(config)).expect("build settings");

        assert_eq!(settings.replication.host, "cli-replica.example.com");
        assert_eq!(settings.replication.port, 6432);
        // Remaining fields cascade from origin
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
    }

    #[test]
    fn settings_build_cli_replication_overrides_toml_replication() {
        let mut config = base_toml_config();
        config.replication = Some(PgSettingsPartial {
            host: Some("toml-replica.example.com".to_owned()),
            port: Some(5433),
            ..Default::default()
        });
        let args = CliArgs {
            replication_host: Some("cli-replica.example.com".to_owned()),
            ..Default::default()
        };

        let settings = settings_build(args, Some(config)).expect("build settings");

        // CLI host wins over TOML host
        assert_eq!(settings.replication.host, "cli-replica.example.com");
        // TOML port preserved (CLI didn't specify)
        assert_eq!(settings.replication.port, 5433);
        // Origin fills unspecified fields
        assert_eq!(settings.replication.user, "origin_user");
    }

    #[test]
    fn settings_build_cli_origin_override_cascades_to_replication() {
        let config = base_toml_config();
        let args = CliArgs {
            origin_host: Some("cli-origin.example.com".to_owned()),
            ..Default::default()
        };

        let settings = settings_build(args, Some(config)).expect("build settings");

        // Origin was overridden by CLI
        assert_eq!(settings.origin.host, "cli-origin.example.com");
        // Replication inherits the CLI-overridden origin (no TOML replication section)
        assert_eq!(settings.replication.host, "cli-origin.example.com");
    }

    #[test]
    fn settings_build_cdc_names_lowercased() {
        let mut config = base_toml_config();
        config.cdc.publication_name = "MY_PUB".to_owned();
        config.cdc.slot_name = "MY_SLOT".to_owned();
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config)).expect("build settings");

        assert_eq!(settings.cdc.publication_name, "my_pub");
        assert_eq!(settings.cdc.slot_name, "my_slot");
    }

    // ==================== settings_build CLI-only Tests ====================

    /// All required CLI fields populated, no config file.
    fn base_cli_args() -> CliArgs {
        CliArgs {
            origin_host: Some("origin.example.com".to_owned()),
            origin_port: Some(5432),
            origin_user: Some("origin_user".to_owned()),
            origin_password: Some("origin_pass".to_owned()),
            origin_database: Some("origin_db".to_owned()),
            cache_host: Some("localhost".to_owned()),
            cache_port: Some(5433),
            cache_user: Some("cache_user".to_owned()),
            cache_database: Some("cache_db".to_owned()),
            cdc_publication_name: Some("pub".to_owned()),
            cdc_slot_name: Some("slot".to_owned()),
            listen_socket: Some("127.0.0.1:6432".parse().expect("valid socket addr")),
            num_workers: Some(4),
            ..Default::default()
        }
    }

    #[test]
    fn settings_build_cli_only_replication_defaults_to_origin() {
        let args = base_cli_args();

        let settings = settings_build(args, None).expect("build settings");

        assert_eq!(settings.replication.host, "origin.example.com");
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
        assert_eq!(settings.replication.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn settings_build_cli_only_replication_host_override() {
        let args = CliArgs {
            replication_host: Some("replica.example.com".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None).expect("build settings");

        assert_eq!(settings.replication.host, "replica.example.com");
        // Remaining fields inherited from origin
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
    }

    #[test]
    fn settings_build_cli_only_replication_all_fields_override() {
        let args = CliArgs {
            replication_host: Some("replica.example.com".to_owned()),
            replication_port: Some(6432),
            replication_user: Some("repl_user".to_owned()),
            replication_password: Some("repl_pass".to_owned()),
            replication_database: Some("repl_db".to_owned()),
            replication_ssl_mode: Some(SslMode::Require),
            ..base_cli_args()
        };

        let settings = settings_build(args, None).expect("build settings");

        assert_eq!(settings.replication.host, "replica.example.com");
        assert_eq!(settings.replication.port, 6432);
        assert_eq!(settings.replication.user, "repl_user");
        assert_eq!(settings.replication.password, Some("repl_pass".to_owned()));
        assert_eq!(settings.replication.database, "repl_db");
        assert_eq!(settings.replication.ssl_mode, SslMode::Require);
        // Origin unchanged
        assert_eq!(settings.origin.host, "origin.example.com");
        assert_eq!(settings.origin.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn settings_build_cli_only_missing_origin_host_errors() {
        let mut args = base_cli_args();
        args.origin_host = None;

        let err = settings_build(args, None).expect_err("missing origin_host");
        assert!(err.to_string().contains("origin_host"));
    }

    #[test]
    fn settings_build_cli_only_defaults() {
        let args = base_cli_args();

        let settings = settings_build(args, None).expect("build settings");

        assert_eq!(settings.origin.ssl_mode, SslMode::Disable);
        assert_eq!(settings.cache_policy, CachePolicy::Clock);
        assert_eq!(settings.admission_threshold, 2);
        assert_eq!(settings.cache.ssl_mode, SslMode::Disable);
        assert_eq!(settings.cache.password, None);
    }

    #[test]
    fn settings_build_cli_only_cdc_names_lowercased() {
        let args = CliArgs {
            cdc_publication_name: Some("MY_PUB".to_owned()),
            cdc_slot_name: Some("MY_SLOT".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None).expect("build settings");

        assert_eq!(settings.cdc.publication_name, "my_pub");
        assert_eq!(settings.cdc.slot_name, "my_slot");
    }
}

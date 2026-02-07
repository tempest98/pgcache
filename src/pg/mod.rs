pub(crate) mod cache_connection;
pub(crate) mod cdc;
pub(crate) mod connect;
pub(crate) mod protocol;

pub use connect::{config_build, config_connect, connect};

pub fn identifier_needs_quotes(id: &str) -> bool {
    match id.as_bytes() {
        [] => true,
        [first, rest @ ..] => {
            (!first.is_ascii_lowercase() && *first != b'_')
                || !rest
                    .iter()
                    .all(|&b| b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit())
        }
    }
}

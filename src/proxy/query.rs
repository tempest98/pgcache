use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use tokio_util::bytes::{Buf, BytesMut};
use tracing::trace;

use crate::{cache::query::CacheableQuery, query::ast::{AstError, sql_query_convert}};

use super::ParseError;

#[derive(Debug, Clone, Copy)]
pub(super) enum ForwardReason {
    UnsupportedStatement,
    UncacheableSelect,
    Invalid,
}

pub(super) enum Action {
    Forward(ForwardReason),
    CacheCheck(Box<CacheableQuery>),
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn handle_query(
    data: &BytesMut,
    fp_cache: &mut HashMap<u64, Result<Box<CacheableQuery>, ForwardReason>>,
) -> Result<Action, ParseError> {
    let msg_len = (&data[1..5]).get_u32() as usize;
    let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;

    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    let fingerprint = hasher.finish();

    match fp_cache.get(&fingerprint) {
        Some(Ok(cacheable_query)) => {
            trace!("cache hit: cacheable true");
            Ok(Action::CacheCheck(cacheable_query.clone()))
        }
        Some(Err(reason)) => {
            trace!("cache hit: cacheable false");
            Ok(Action::Forward(*reason))
        }
        None => {
            let ast = pg_query::parse(query)?;

            match sql_query_convert(&ast) {
                Ok(query) => {
                    // Successfully parsed as SELECT
                    if let Ok(cacheable_query) = CacheableQuery::try_from(&query) {
                        fp_cache.insert(fingerprint, Ok(Box::new(cacheable_query.clone())));
                        Ok(Action::CacheCheck(Box::new(cacheable_query)))
                    } else {
                        let reason = ForwardReason::UncacheableSelect;
                        fp_cache.insert(fingerprint, Err(reason));
                        Ok(Action::Forward(reason))
                    }
                }
                Err(ast_error) => {
                    let reason = match ast_error {
                        AstError::UnsupportedStatement { .. } => {
                            // Not a SELECT statement (INSERT, UPDATE, DELETE, DDL, etc.)
                            ForwardReason::UnsupportedStatement
                        }
                        AstError::UnsupportedSelectFeature { .. }
                        | AstError::UnsupportedFeature { .. }
                        | AstError::UnsupportedJoinType
                        | AstError::WhereParseError(_) => ForwardReason::UncacheableSelect,
                        AstError::MultipleStatements
                        | AstError::MissingStatement
                        | AstError::InvalidTableRef => ForwardReason::Invalid,
                    };

                    fp_cache.insert(fingerprint, Err(reason));

                    Ok(Action::Forward(reason))
                }
            }
        }
    }
}

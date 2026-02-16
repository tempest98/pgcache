use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use tokio_util::bytes::BytesMut;
use tracing::trace;

use crate::{
    cache::query::CacheableQuery,
    catalog::FunctionVolatility,
    query::ast::{AstError, query_expr_convert},
};

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
    func_volatility: &HashMap<String, FunctionVolatility>,
) -> Result<Action, ParseError> {
    let len_bytes: [u8; 4] = data
        .get(1..5)
        .and_then(|s| s.try_into().ok())
        .ok_or(ParseError::InvalidUtf8)?;
    let msg_len = u32::from_be_bytes(len_bytes) as usize;
    let query = data
        .get(5..msg_len)
        .and_then(|b| str::from_utf8(b).ok())
        .ok_or(ParseError::InvalidUtf8)?;

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

            match query_expr_convert(&ast) {
                Ok(query) => {
                    // Successfully parsed as SELECT
                    if let Ok(cacheable_query) = CacheableQuery::try_new(&query, func_volatility) {
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
                        | AstError::UnsupportedSubLinkType { .. }
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

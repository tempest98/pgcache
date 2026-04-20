use pg_query::ParseResult;
use pg_query::protobuf::{
    DiscardMode, TransactionStmtKind, VariableSetKind, node::Node as NodeEnum,
};

/// Classification of a parsed statement with respect to search_path state.
///
/// On pre-PG18, the proxy maintains a cached view of the session's search_path
/// that is only refreshed via `SHOW search_path`. Detecting these statement
/// kinds lets the proxy mark its view stale before the next cacheable query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    /// `SET search_path`, `RESET search_path`, or `RESET ALL`. Safe to
    /// piggyback (PG runs these without a txn restriction).
    SearchPathSet,
    /// `DISCARD ALL`. Also invalidates the cached search_path, but must be
    /// issued alone because some sub-operations it performs (DEALLOCATE ALL,
    /// DISCARD TEMP, DISCARD SEQUENCES) cannot run inside a transaction —
    /// including the implicit transaction PG wraps multi-statement
    /// simple-query strings in. So the piggyback path must skip it and rely
    /// on the lazy SHOW-on-RFQ fallback.
    DiscardAll,
    /// `COMMIT` or `ROLLBACK`. `SET LOCAL` and any `SET` inside an aborted
    /// transaction revert at this point, so the cached search_path is
    /// conservatively marked stale on every txn end.
    TxnEnd,
}

impl MutationKind {
    /// Whether it's safe to rewrite a single-statement Query message to
    /// `<stmt>; SHOW search_path` and strip the SHOW response — i.e. the
    /// statement tolerates running inside PG's implicit multi-statement
    /// transaction.
    fn piggybackable(self) -> bool {
        match self {
            Self::SearchPathSet | Self::TxnEnd => true,
            Self::DiscardAll => false,
        }
    }
}

/// Classify a single parsed statement node.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "NodeEnum has hundreds of variants; listing non-mutating ones is impractical"
)]
fn stmt_classify(node: &NodeEnum) -> Option<MutationKind> {
    match node {
        NodeEnum::VariableSetStmt(stmt) => match VariableSetKind::try_from(stmt.kind).ok()? {
            VariableSetKind::VarResetAll => Some(MutationKind::SearchPathSet),
            _ if stmt.name.eq_ignore_ascii_case("search_path") => {
                Some(MutationKind::SearchPathSet)
            }
            _ => None,
        },
        NodeEnum::DiscardStmt(stmt) => match DiscardMode::try_from(stmt.target).ok()? {
            DiscardMode::DiscardAll => Some(MutationKind::DiscardAll),
            _ => None,
        },
        NodeEnum::TransactionStmt(stmt) => match TransactionStmtKind::try_from(stmt.kind).ok()? {
            TransactionStmtKind::TransStmtCommit
            | TransactionStmtKind::TransStmtRollback
            | TransactionStmtKind::TransStmtCommitPrepared
            | TransactionStmtKind::TransStmtRollbackPrepared => Some(MutationKind::TxnEnd),
            _ => None,
        },
        _ => None,
    }
}

fn stmt_node(raw: &pg_query::protobuf::RawStmt) -> Option<&NodeEnum> {
    raw.stmt.as_ref()?.node.as_ref()
}

/// Returns true if any statement in the parse invalidates the cached
/// search_path — used to mark state stale regardless of piggyback eligibility
/// or multi-statement structure.
pub fn search_path_mutates_any(ast: &ParseResult) -> bool {
    ast.protobuf
        .stmts
        .iter()
        .filter_map(stmt_node)
        .any(|n| stmt_classify(n).is_some())
}

/// If the parse contains exactly one statement AND that statement is a
/// mutation that can safely ride in PG's implicit multi-statement transaction,
/// returns its kind. Used to gate the piggyback rewrite.
pub fn search_path_mutates_single_piggybackable(ast: &ParseResult) -> Option<MutationKind> {
    let [raw] = ast.protobuf.stmts.as_slice() else {
        return None;
    };
    stmt_classify(stmt_node(raw)?).filter(|k| k.piggybackable())
}

/// Represents a single entry in the PostgreSQL search_path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPathEntry {
    /// Literal schema name (includes quoted "$user" which is a literal schema name)
    Schema(String),
    /// Unquoted $user - resolves to session_user at query time
    SessionUser,
}

/// Parsed search_path from PostgreSQL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchPath(pub Vec<SearchPathEntry>);

impl SearchPath {
    /// Parse a search_path string from PostgreSQL.
    ///
    /// Handles:
    /// - Unquoted `$user` -> `SessionUser`
    /// - Quoted `"$user"` -> `Schema("$user")` (literal schema name)
    /// - Regular identifiers -> `Schema(name)` (lowercased)
    /// - Double-quoted identifiers -> `Schema(name)` (preserves case)
    pub fn parse(s: &str) -> Self {
        let entries = s
            .split(',')
            .map(|entry| entry.trim())
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                if let Some(quoted) = entry.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
                    // Quoted identifier: preserve case, unescape double quotes
                    let unescaped = quoted.replace("\"\"", "\"");
                    SearchPathEntry::Schema(unescaped)
                } else if entry == "$user" {
                    // Unquoted $user is the special session user token
                    SearchPathEntry::SessionUser
                } else {
                    // Unquoted identifier: lowercase
                    SearchPathEntry::Schema(entry.to_lowercase())
                }
            })
            .collect();

        Self(entries)
    }

    /// Resolve the search path to a list of schema names.
    ///
    /// Expands `SessionUser` to the provided session_user value.
    /// Skips `SessionUser` entries if session_user is `None`.
    pub fn resolve<'a>(&'a self, session_user: Option<&'a str>) -> Vec<&'a str> {
        self.0
            .iter()
            .filter_map(|entry| match entry {
                SearchPathEntry::Schema(name) => Some(name.as_str()),
                SearchPathEntry::SessionUser => session_user,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let path = SearchPath::parse("");
        assert_eq!(path.0, vec![]);
    }

    #[test]
    fn test_parse_single_schema() {
        let path = SearchPath::parse("public");
        assert_eq!(path.0, vec![SearchPathEntry::Schema("public".to_owned())]);
    }

    #[test]
    fn test_parse_multiple_schemas() {
        let path = SearchPath::parse("myschema, public");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("myschema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_user_unquoted() {
        let path = SearchPath::parse("$user, public");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::SessionUser,
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_user_quoted() {
        // Quoted "$user" is a literal schema name, not the special token
        let path = SearchPath::parse(r#""$user", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("$user".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_user_mixed() {
        // First $user is unquoted (SessionUser), second is quoted (literal)
        let path = SearchPath::parse(r#"$user, "$user", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::SessionUser,
                SearchPathEntry::Schema("$user".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_quoted_preserves_case() {
        let path = SearchPath::parse(r#""MySchema", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("MySchema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_unquoted_lowercases() {
        let path = SearchPath::parse("MySchema, PUBLIC");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("myschema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_extra_whitespace() {
        let path = SearchPath::parse("  $user  ,  public  ");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::SessionUser,
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_quoted_with_spaces() {
        let path = SearchPath::parse(r#""my schema", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("my schema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_quoted_with_escaped_quote() {
        let path = SearchPath::parse(r#""my""schema", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema(r#"my"schema"#.to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_resolve_with_session_user() {
        let path = SearchPath::parse("$user, public");
        let resolved = path.resolve(Some("alice"));
        assert_eq!(resolved, vec!["alice", "public"]);
    }

    #[test]
    fn test_resolve_without_session_user() {
        let path = SearchPath::parse("$user, public");
        let resolved = path.resolve(None);
        assert_eq!(resolved, vec!["public"]);
    }

    #[test]
    fn test_resolve_no_session_user_entry() {
        let path = SearchPath::parse("myschema, public");
        let resolved = path.resolve(Some("alice"));
        assert_eq!(resolved, vec!["myschema", "public"]);
    }

    #[test]
    fn test_resolve_quoted_user_not_expanded() {
        // Quoted "$user" should stay as literal "$user", not expand to session user
        let path = SearchPath::parse(r#""$user", public"#);
        let resolved = path.resolve(Some("alice"));
        assert_eq!(resolved, vec!["$user", "public"]);
    }

    fn parse(sql: &str) -> ParseResult {
        pg_query::parse(sql).expect("parse SQL")
    }

    #[test]
    fn test_mutates_any_set_search_path() {
        assert!(search_path_mutates_any(&parse(
            "SET search_path = myapp, public"
        )));
        assert!(search_path_mutates_any(&parse("SET search_path TO myapp")));
        assert!(search_path_mutates_any(&parse(
            "SET LOCAL search_path = myapp"
        )));
        assert!(search_path_mutates_any(&parse(
            "SET SESSION search_path = myapp"
        )));
        assert!(search_path_mutates_any(&parse(
            "SET search_path TO DEFAULT"
        )));
        // Name comparison is case-insensitive.
        assert!(search_path_mutates_any(&parse("SET Search_Path = myapp")));
    }

    #[test]
    fn test_mutates_any_reset() {
        assert!(search_path_mutates_any(&parse("RESET search_path")));
        assert!(search_path_mutates_any(&parse("RESET ALL")));
    }

    #[test]
    fn test_mutates_any_discard() {
        assert!(search_path_mutates_any(&parse("DISCARD ALL")));
        assert!(!search_path_mutates_any(&parse("DISCARD TEMP")));
        assert!(!search_path_mutates_any(&parse("DISCARD PLANS")));
    }

    #[test]
    fn test_mutates_any_txn_end() {
        assert!(search_path_mutates_any(&parse("COMMIT")));
        assert!(search_path_mutates_any(&parse("ROLLBACK")));
        assert!(search_path_mutates_any(&parse("END")));
        assert!(search_path_mutates_any(&parse("ABORT")));
    }

    #[test]
    fn test_mutates_any_ignored_statements() {
        assert!(!search_path_mutates_any(&parse("SELECT 1")));
        assert!(!search_path_mutates_any(&parse("SET work_mem = 1")));
        assert!(!search_path_mutates_any(&parse("RESET work_mem")));
        assert!(!search_path_mutates_any(&parse("BEGIN")));
        assert!(!search_path_mutates_any(&parse("SAVEPOINT sp")));
        assert!(!search_path_mutates_any(&parse(
            "ROLLBACK TO SAVEPOINT sp"
        )));
    }

    #[test]
    fn test_mutates_any_multi_statement() {
        assert!(search_path_mutates_any(&parse("SELECT 1; COMMIT")));
        assert!(search_path_mutates_any(&parse(
            "COMMIT; SET search_path = x"
        )));
    }

    #[test]
    fn test_mutates_single_piggybackable_only_single_statement() {
        assert_eq!(
            search_path_mutates_single_piggybackable(&parse("COMMIT")),
            Some(MutationKind::TxnEnd)
        );
        assert_eq!(
            search_path_mutates_single_piggybackable(&parse("SET search_path = x")),
            Some(MutationKind::SearchPathSet)
        );
        // DISCARD ALL can't run in an implicit multi-statement txn, so it is
        // detected by mutates_any (see test above) but never piggybackable.
        assert_eq!(
            search_path_mutates_single_piggybackable(&parse("DISCARD ALL")),
            None
        );
        // Multi-statement: ineligible even if it contains a mutation.
        assert_eq!(
            search_path_mutates_single_piggybackable(&parse("SELECT 1; COMMIT")),
            None
        );
        assert_eq!(
            search_path_mutates_single_piggybackable(&parse(
                "COMMIT; SET search_path = x"
            )),
            None
        );
    }
}

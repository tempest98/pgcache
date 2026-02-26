mod convert;
pub mod expr_parse;
mod types;

use ecow::EcoString;
use error_set::error_set;

use crate::pg::identifier_needs_quotes;

use self::expr_parse::WhereParseError;

error_set! {
    AstError := {
        #[display("Unsupported statement type: {statement_type}")]
        UnsupportedStatement { statement_type: String },
        #[display("Multiple statements not supported")]
        MultipleStatements,
        #[display("Missing statement")]
        MissingStatement,
        #[display("Unsupported SELECT feature: {feature}")]
        UnsupportedSelectFeature { feature: String },
        #[display("Unsupported feature: {feature}")]
        UnsupportedFeature { feature: String },
        #[display("Invalid table reference")]
        InvalidTableRef,
        UnsupportedJoinType,
        #[display("Unsupported SubLink type: {sublink_type}")]
        UnsupportedSubLinkType { sublink_type: String },
        WhereParseError(WhereParseError),
    }
}

pub trait Deparse {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String;
}

impl Deparse for String {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => {
                buf.push('"');
                buf.push_str(self);
                buf.push('"');
            }
            false => buf.push_str(self),
        };

        buf
    }
}

impl Deparse for &str {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => {
                buf.push('"');
                buf.push_str(self);
                buf.push('"');
            }
            false => buf.push_str(self),
        };

        buf
    }
}

impl Deparse for EcoString {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.as_str().deparse(buf)
    }
}

// Re-export everything public from submodules
pub use convert::*;
pub use types::*;

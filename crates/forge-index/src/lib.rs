//! forge-index: full-text search over `.fts` sidecars (M4): the analyzer shared with the memtable scan, the
//! file format, the builder and the reader.

pub mod analyzer;
pub mod build;
pub mod format;
pub mod matcher;
pub mod query;
pub mod read;
#[cfg(test)]
mod tests_fts;

pub use analyzer::Analyzer;
pub use build::{BuildStats, FtsBuilder};
pub use matcher::{Matcher, PrefixSeen};
pub use read::FtsFile;

use async_trait::async_trait;
use forge_model::catalog::SegmentMeta;
use forge_model::error::{Classify, ErrorKind, Unimplemented};
use forge_model::schema::{FTS_COLUMNS, JSON_COLUMNS};

#[derive(Clone, Debug, thiserror::Error)]
pub enum IndexError {
    #[error("corrupt .fts {path}: {msg}")]
    Corrupt { path: String, msg: String },
    /// Fetching the file failed (a store error other than "not found": throttling, a timeout, a network error).
    #[error("reading .fts {path}: {msg}")]
    Io { path: String, msg: String },
    #[error("invalid search query: {0}")]
    Query(String),
    #[error("building .fts: {0}")]
    Build(String),
    #[error(transparent)]
    Unimplemented(#[from] Unimplemented),
}
pub type IndexResult<T> = Result<T, IndexError>;

impl Classify for IndexError {
    fn kind(&self) -> ErrorKind {
        match self {
            Self::Query(_) => ErrorKind::InvalidInput,
            Self::Unimplemented(e) => e.kind(),
            Self::Io { .. } => ErrorKind::Transient,
            Self::Corrupt { .. } | Self::Build(_) => ErrorKind::Internal,
        }
    }
}

/// FTS column id = position in forge_model::schema::FTS_COLUMNS (stored as u8 in .fts keys).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum FtsColumn {
    Name = 0,
    Error = 1,
    Inputs = 2,
    Outputs = 3,
    Metadata = 4,
}
impl FtsColumn {
    /// In id order, parallel to `FTS_COLUMNS`.
    pub const ALL: [Self; 5] = [Self::Name, Self::Error, Self::Inputs, Self::Outputs, Self::Metadata];
    pub fn name(self) -> &'static str {
        FTS_COLUMNS[self as usize]
    }
    pub fn from_name(name: &str) -> Option<Self> {
        FTS_COLUMNS.iter().position(|c| *c == name).map(|i| Self::ALL[i])
    }
    pub fn is_json(self) -> bool {
        JSON_COLUMNS.contains(&self.name())
    }
}

/// One UDF predicate, as written in SQL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FtsPredicate {
    /// search(col, 'q')
    Search { column: FtsColumn, query: String },
    /// json_key_search(col, '$.path', 'text'); empty text = path exists
    JsonKeySearch {
        column: FtsColumn,
        path: String,
        text: String,
    },
}

impl FtsPredicate {
    pub fn column(&self) -> FtsColumn {
        match self {
            Self::Search { column, .. } | Self::JsonKeySearch { column, .. } => *column,
        }
    }
}

/// Boolean combination of predicates pushed into one scan (AND of filters, OR inside a filter).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FtsExpr {
    Pred(FtsPredicate),
    And(Vec<FtsExpr>),
    Or(Vec<FtsExpr>),
}

impl FtsExpr {
    /// `leaf` on every predicate in depth-first order (all of them: no short-circuit), combined by `and` / `or`
    /// over the children's values.
    pub fn fold<'a, T>(
        &'a self,
        leaf: &mut dyn FnMut(&'a FtsPredicate) -> T,
        and: &dyn Fn(Vec<T>) -> T,
        or: &dyn Fn(Vec<T>) -> T,
    ) -> T {
        match self {
            Self::Pred(p) => leaf(p),
            Self::And(v) => and(v.iter().map(|c| c.fold(leaf, and, or)).collect()),
            Self::Or(v) => or(v.iter().map(|c| c.fold(leaf, and, or)).collect()),
        }
    }

    /// The predicates in depth-first order (`fold`'s leaf order).
    pub fn predicates(&self) -> Vec<&FtsPredicate> {
        let mut out = Vec::new();
        self.fold(&mut |p| out.push(p), &|_| (), &|_| ());
        out
    }

    /// The columns the expression reads.
    pub fn columns(&self) -> std::collections::BTreeSet<FtsColumn> {
        self.predicates().into_iter().map(FtsPredicate::column).collect()
    }
}

#[derive(Clone, Debug)]
pub enum FtsHits {
    /// The segment has no usable index for some column in the expression: caller must scan + brute-force.
    NoIndex,
    /// Candidate rows (file-relative), one set per expression. May be a superset (phrases, concrete array
    /// indexes, see `Query::needs_recheck`): the caller re-checks with `matches()`.
    Rows {
        rows: Vec<roaring::RoaringTreemap>,
        /// Some hit row has a null start_time (the footer's null-start bitmap): the segment's `max_start_ns` does not
        /// bound the merged start of its hits (plans/phase-5 D8).
        start_less: bool,
    },
}

#[async_trait]
pub trait IndexReader: Send + Sync + std::fmt::Debug + 'static {
    /// Every expression of `exprs` on `seg`, at once: ≤ 2 round trips with a cached footer.
    async fn search(&self, seg: &SegmentMeta, exprs: &[FtsExpr]) -> IndexResult<FtsHits>;
}

/// A reader that never has an index: callers fall back to scan + brute force (tests).
#[derive(Debug, Default)]
pub struct NoopIndexReader;
#[async_trait]
impl IndexReader for NoopIndexReader {
    async fn search(&self, _: &SegmentMeta, _: &[FtsExpr]) -> IndexResult<FtsHits> {
        Ok(FtsHits::NoIndex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::catalog::{HourBucket, Level};
    use forge_model::testing::segment_meta;

    #[test]
    fn column_ids_match_schema_positions() {
        assert_eq!(FtsColumn::ALL.len(), FTS_COLUMNS.len());
        for (i, c) in FtsColumn::ALL.into_iter().enumerate() {
            assert_eq!(c as u8 as usize, i);
            assert_eq!(FtsColumn::from_name(c.name()), Some(c));
        }
        let json: Vec<_> = FtsColumn::ALL.into_iter().filter(|c| c.is_json()).collect();
        assert_eq!(json, [FtsColumn::Inputs, FtsColumn::Outputs, FtsColumn::Metadata]);
        assert_eq!(FtsColumn::from_name("model"), None);
    }

    #[tokio::test]
    async fn noop_reader_reports_no_index() {
        let seg = segment_meta("p", Level::L0, HourBucket(0));
        let expr = FtsExpr::Pred(FtsPredicate::Search {
            column: FtsColumn::Name,
            query: "x".into(),
        });
        assert!(matches!(
            NoopIndexReader.search(&seg, &[expr]).await,
            Ok(FtsHits::NoIndex)
        ));
    }
}

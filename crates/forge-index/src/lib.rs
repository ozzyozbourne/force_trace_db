//! forge-index: full-text search over `.fts` sidecars (M4). M0 defines the read-side types only.

use async_trait::async_trait;
use forge_model::catalog::SegmentMeta;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("corrupt .fts {path}: {msg}")]
    Corrupt { path: String, msg: String },
    #[error("invalid search query: {0}")]
    Query(String),
    #[error("not implemented until {0}")]
    Unimplemented(&'static str),
}
pub type IndexResult<T> = Result<T, IndexError>;

/// FTS column id = position in forge_model::schema::FTS_COLUMNS (stored as u8 in .fts keys).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FtsColumn {
    Name = 0,
    Error = 1,
    Inputs = 2,
    Outputs = 3,
    Metadata = 4,
}
impl FtsColumn {
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "name" => Self::Name,
            "error" => Self::Error,
            "inputs" => Self::Inputs,
            "outputs" => Self::Outputs,
            "metadata" => Self::Metadata,
            _ => return None,
        })
    }
    pub fn is_json(self) -> bool {
        matches!(self, Self::Inputs | Self::Outputs | Self::Metadata)
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

/// Boolean combination of predicates pushed into one scan (AND of filters, OR inside a filter).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FtsExpr {
    Pred(FtsPredicate),
    And(Vec<FtsExpr>),
    Or(Vec<FtsExpr>),
}

#[derive(Clone, Debug)]
pub enum FtsHits {
    /// The segment has no usable index for some column in the expression: caller must scan + brute-force.
    NoIndex,
    /// Candidate rows (file-relative). May be a superset (phrases, concrete array indexes): the
    /// caller re-checks with `matches()` when `needs_recheck`.
    Rows {
        rows: roaring::RoaringTreemap,
        needs_recheck: bool,
    },
}

#[async_trait]
pub trait IndexReader: Send + Sync + std::fmt::Debug + 'static {
    /// ≤ 2 round trips with a cached footer. `limit_hint` lets the reader stop decoding row groups early.
    async fn search(&self, seg: &SegmentMeta, expr: &FtsExpr, limit_hint: Option<u64>) -> IndexResult<FtsHits>;
}

/// M0–M3: every segment reports "no index", so callers fall back to scan + brute force.
#[derive(Debug, Default)]
pub struct NoopIndexReader;
#[async_trait]
impl IndexReader for NoopIndexReader {
    async fn search(&self, _: &SegmentMeta, _: &FtsExpr, _: Option<u64>) -> IndexResult<FtsHits> {
        Ok(FtsHits::NoIndex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::catalog::{HourBucket, Level, SegmentId, SegmentState, paths};
    use forge_model::ids::ProjectId;

    #[test]
    fn column_ids_match_schema_positions() {
        let all = [
            FtsColumn::Name,
            FtsColumn::Error,
            FtsColumn::Inputs,
            FtsColumn::Outputs,
            FtsColumn::Metadata,
        ];
        assert_eq!(all.len(), forge_model::schema::FTS_COLUMNS.len());
        for (i, n) in forge_model::schema::FTS_COLUMNS.iter().enumerate() {
            let c = FtsColumn::from_name(n).unwrap();
            assert_eq!(c as u8 as usize, i);
            assert_eq!(c, all[i]);
        }
        assert_eq!(FtsColumn::from_name("model"), None);
    }

    #[tokio::test]
    async fn noop_reader_reports_no_index() {
        let p = ProjectId::new("p").unwrap();
        let id = SegmentId::new();
        let seg = SegmentMeta {
            id,
            path: paths::segment_path(&p, Level::L0, HourBucket(0), id),
            project_id: p,
            level: Level::L0,
            partition_hour: HourBucket(0),
            row_count: 1,
            byte_size: 1,
            fts_byte_size: None,
            min_start_ns: None,
            max_start_ns: None,
            start_time_nulls: 1,
            min_trace_id: Default::default(),
            max_trace_id: Default::default(),
            min_version: 1,
            max_version: 1,
            schema_version: 1,
            created_at_ms: 0,
            state: SegmentState::Live,
            deleted_at_ms: None,
        };
        let expr = FtsExpr::Pred(FtsPredicate::Search {
            column: FtsColumn::Name,
            query: "x".into(),
        });
        assert!(matches!(
            NoopIndexReader.search(&seg, &expr, None).await,
            Ok(FtsHits::NoIndex)
        ));
    }
}

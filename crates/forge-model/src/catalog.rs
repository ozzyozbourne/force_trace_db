//! Catalog types shared by meta, storage, index, ingest and query.
use crate::enums::str_enum;
use crate::error::ModelError;
use crate::ids::{ProjectId, TraceId};
use chrono::format::{Item, StrftimeItems};
use std::sync::LazyLock;
use std::{fmt, str::FromStr};

/// Segment identity = ULID (time-sortable; Crockford base32, 26 chars, used in object paths).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub ulid::Ulid);
impl SegmentId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}
impl Default for SegmentId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SegmentId({})", self.0)
    }
}
impl FromStr for SegmentId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ulid::Ulid::from_string(s).map(Self)
    }
}

str_enum!(
    #[derive(PartialOrd, Ord)]
    #[repr(u8)]
    Level, "level", { L0 => "L0", L1 => "L1" }
);
impl Level {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> Option<Self> {
        Self::ALL.get(usize::from(v)).copied()
    }
}

/// Hours since the Unix epoch (UTC). L1: the (project, hour) partition. L0: hour of the flush
/// (path placement only; pruning uses min/max_start_ns).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HourBucket(pub i64);
impl HourBucket {
    pub const NANOS: i64 = 3_600_000_000_000;
    /// Total: every i64 nanosecond lands in a bucket that `path_component` can format.
    pub fn from_nanos(ns: i64) -> Self {
        Self(ns.div_euclid(Self::NANOS))
    }
    /// First nanosecond of the hour; `HourOutOfRange` when it does not fit in i64 nanoseconds.
    pub fn start_nanos(self) -> Result<i64, ModelError> {
        self.0
            .checked_mul(Self::NANOS)
            .ok_or(ModelError::HourOutOfRange(self.0))
    }
    /// `yyyy-mm-dd/hh` (object layout).
    pub fn path_component(self) -> Result<String, ModelError> {
        Ok(self.path_display()?.to_string())
    }
    fn path_display(self) -> Result<impl fmt::Display, ModelError> {
        static ITEMS: LazyLock<Vec<Item<'static>>> = LazyLock::new(|| StrftimeItems::new("%Y-%m-%d/%H").collect());
        // Formatted from seconds, not `start_nanos`: the lowest bucket holds valid i64-ns timestamps although
        // its first nanosecond is below i64::MIN. chrono's range covers every `from_nanos` bucket.
        let t = self
            .0
            .checked_mul(3600)
            .and_then(|s| chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0))
            .ok_or(ModelError::HourOutOfRange(self.0))?;
        Ok(t.format_with_items(ITEMS.iter()))
    }
}

str_enum!(SegmentState, "segment_state", {
    Live => "live",
    Compacting => "compacting",
    Deleted => "deleted",
});
impl SegmentState {
    pub fn parse(s: &str) -> Option<Self> {
        s.parse().ok()
    }
    /// Live and compacting segments are both visible to queries.
    pub fn is_visible(self) -> bool {
        !matches!(self, Self::Deleted)
    }
}

/// One row of `segments`. Times: `*_ns` = data time (unix ns), `*_ms` = wall-clock bookkeeping (unix ms).
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentMeta {
    pub id: SegmentId,
    pub project_id: ProjectId,
    pub level: Level,
    pub partition_hour: HourBucket,
    /// Object key of the .vortex file relative to the store root (see paths::segment_path).
    pub path: String,
    pub row_count: u64,
    pub byte_size: u64,
    /// Size of the .fts sidecar (`paths::fts_path(&path)`); None = no sidecar (search falls back to scan).
    pub fts_byte_size: Option<u64>,
    /// None when every row has a null start_time.
    pub min_start_ns: Option<i64>,
    pub max_start_ns: Option<i64>,
    /// Rows with null start_time. Segment-level time pruning is allowed only when 0 (decision 5).
    pub start_time_nulls: u64,
    pub min_trace_id: TraceId,
    pub max_trace_id: TraceId,
    pub min_version: u64,
    pub max_version: u64,
    pub schema_version: u32,
    pub created_at_ms: i64,
    pub state: SegmentState,
    pub deleted_at_ms: Option<i64>,
}

impl SegmentMeta {
    /// Conservative: true unless the segment provably has no row with start_time in [start, end).
    pub fn may_overlap_time(&self, start_ns: Option<i64>, end_ns: Option<i64>) -> bool {
        if self.start_time_nulls > 0 {
            return true;
        }
        let (Some(lo), Some(hi)) = (self.min_start_ns, self.max_start_ns) else {
            return true;
        };
        start_ns.is_none_or(|s| hi >= s) && end_ns.is_none_or(|e| lo < e)
    }
}

/// A trace's contiguous rows inside one segment (rows are sorted by trace).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceRange {
    pub trace_id: TraceId,
    pub row_start: u64,
    pub row_count: u64,
}

/// trace_index lookup result: where a trace lives.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceLocation {
    pub segment: SegmentMeta,
    pub row_start: u64,
    pub row_count: u64,
}

impl TraceLocation {
    pub fn rows(&self) -> std::ops::Range<u64> {
        self.row_start..self.row_start + self.row_count
    }
}

pub mod paths {
    use super::{HourBucket, Level, SegmentId};
    use crate::error::ModelError;
    use crate::ids::ProjectId;
    /// `HourOutOfRange` for a bucket outside the i64-nanosecond time range.
    pub fn segment_path(p: &ProjectId, level: Level, hour: HourBucket, id: SegmentId) -> Result<String, ModelError> {
        Ok(format!(
            "segments/{p}/{}/{}/{id}.vortex",
            level.as_str(),
            hour.path_display()?
        ))
    }
    pub fn fts_path(segment_path: &str) -> String {
        format!("{}.fts", segment_path.strip_suffix(".vortex").unwrap_or(segment_path))
    }
    pub fn blob_path(p: &ProjectId, sha256_hex: &str) -> String {
        format!("blobs/{p}/{sha256_hex}")
    }
    pub fn wal_object_path(node_id: &str, first_seq: u64) -> String {
        format!("wal/{node_id}/{first_seq:020}.wal")
    }
    pub const SEGMENTS_PREFIX: &str = "segments/";
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn meta(min: Option<i64>, max: Option<i64>, nulls: u64) -> SegmentMeta {
        SegmentMeta {
            min_start_ns: min,
            max_start_ns: max,
            start_time_nulls: nulls,
            ..crate::testing::segment_meta("p", Level::L0, HourBucket(0))
        }
    }

    #[test]
    fn hour_bucket() {
        assert_eq!(HourBucket::from_nanos(-1), HourBucket(-1));
        assert_eq!(HourBucket::from_nanos(HourBucket::NANOS - 1), HourBucket(0));
        assert_eq!(HourBucket::from_nanos(HourBucket::NANOS), HourBucket(1));
        let ns = chrono::Utc
            .with_ymd_and_hms(2026, 10, 22, 10, 30, 0)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let h = HourBucket::from_nanos(ns);
        assert_eq!(h.path_component().unwrap(), "2026-10-22/10");
        assert_eq!(HourBucket::from_nanos(h.start_nanos().unwrap()), h);
    }

    #[test]
    fn out_of_range_hours_are_errors_not_panics() {
        let p = crate::testing::pid("p");
        // Every bucket `from_nanos` can produce formats, including the lowest one, whose first nanosecond is
        // below i64::MIN.
        let lowest = HourBucket::from_nanos(i64::MIN);
        assert_eq!(lowest.path_component().unwrap(), "1677-09-21/00");
        assert!(paths::segment_path(&p, Level::L0, lowest, SegmentId::new()).is_ok());
        assert!(matches!(lowest.start_nanos(), Err(ModelError::HourOutOfRange(_))));
        let lo = HourBucket::from_nanos(i64::MIN + HourBucket::NANOS);
        let hi = HourBucket::from_nanos(i64::MAX);
        assert_eq!(hi.path_component().unwrap(), "2262-04-11/23");
        for h in [lo, hi] {
            assert!(h.start_nanos().is_ok(), "{h:?}");
        }
        assert!(matches!(
            HourBucket(hi.0 + 1).start_nanos(),
            Err(ModelError::HourOutOfRange(_))
        ));
        // Hand-built buckets outside chrono's range are errors, not panics.
        for h in [HourBucket(i64::MAX / 3600), HourBucket(i64::MIN), HourBucket(i64::MAX)] {
            assert!(
                matches!(h.path_component(), Err(ModelError::HourOutOfRange(_))),
                "{h:?}"
            );
            assert!(matches!(h.start_nanos(), Err(ModelError::HourOutOfRange(_))), "{h:?}");
            assert!(paths::segment_path(&p, Level::L0, h, SegmentId::new()).is_err());
        }
    }

    #[test]
    fn paths_and_ids() {
        let id: SegmentId = "01JAAAAAAAAAAAAAAAAAAAAAAA".parse().unwrap();
        assert_eq!(id.to_string().parse::<SegmentId>().unwrap(), id);
        assert!("nope".parse::<SegmentId>().is_err());
        let p = crate::testing::pid("p");
        let path = paths::segment_path(&p, Level::L1, HourBucket(0), id).unwrap();
        assert_eq!(path, "segments/p/L1/1970-01-01/00/01JAAAAAAAAAAAAAAAAAAAAAAA.vortex");
        assert_eq!(
            paths::fts_path(&path),
            "segments/p/L1/1970-01-01/00/01JAAAAAAAAAAAAAAAAAAAAAAA.fts"
        );
        for l in [Level::L0, Level::L1] {
            assert_eq!(Level::from_u8(l.as_u8()), Some(l));
        }
        assert_eq!(Level::from_u8(2), None);
        for &s in SegmentState::ALL {
            assert_eq!(SegmentState::parse(s.as_str()), Some(s));
        }
        assert_eq!(SegmentState::parse("x"), None);
        assert_ne!(SegmentId::default(), SegmentId::default());
    }

    #[test]
    fn may_overlap_time() {
        assert!(meta(None, None, 1).may_overlap_time(Some(0), Some(1)));
        assert!(meta(Some(100), Some(200), 1).may_overlap_time(Some(500), Some(600)));
        let m = meta(Some(100), Some(200), 0);
        assert!(!m.may_overlap_time(Some(300), Some(400)));
        assert!(!m.may_overlap_time(Some(0), Some(50)));
        assert!(
            m.may_overlap_time(Some(200), Some(300)),
            "max == start is inside (inclusive)"
        );
        assert!(
            !m.may_overlap_time(Some(0), Some(100)),
            "min == end is outside (exclusive)"
        );
        assert!(m.may_overlap_time(None, None));
        assert!(m.may_overlap_time(Some(150), None));
        assert!(m.may_overlap_time(None, Some(150)));
    }
}

//! Fixtures shared by tests in other crates (feature `test-util`).

use crate::catalog::{HourBucket, Level, SegmentId, SegmentMeta, SegmentState, paths};
use crate::ids::{ProjectId, TraceId};

pub fn pid(s: &str) -> ProjectId {
    ProjectId::new(s).expect("valid test project id")
}

/// A one-row live segment of `project` whose start time is null; override fields with `..segment_meta(..)`.
pub fn segment_meta(project: &str, level: Level, hour: HourBucket) -> SegmentMeta {
    let id = SegmentId::new();
    let project_id = pid(project);
    SegmentMeta {
        id,
        path: paths::segment_path(&project_id, level, hour, id).expect("test hour in range"),
        project_id,
        level,
        partition_hour: hour,
        row_count: 1,
        byte_size: 1,
        fts_byte_size: None,
        min_start_ns: None,
        max_start_ns: None,
        start_time_nulls: 1,
        min_trace_id: TraceId::default(),
        max_trace_id: TraceId::default(),
        min_version: 1,
        max_version: 1,
        schema_version: 1,
        created_at_ms: 0,
        state: SegmentState::Live,
        deleted_at_ms: None,
    }
}

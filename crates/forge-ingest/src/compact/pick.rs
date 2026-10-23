//! Pure picker: one project's visible segments -> a lease request (plan §2.8 picker rules).
use super::{L1_MAX_RUNS, MAX_INPUTS};
use forge_model::catalog::{HourBucket, JobKind, Level, SegmentId, SegmentMeta, SegmentState};
use forge_model::config::CompactionConfig;
use std::collections::BTreeMap;

pub struct PickInput<'a> {
    /// Visible segments of ONE project (list_segments).
    pub segs: &'a [SegmentMeta],
    pub wall_now_ms: i64,
    pub data_now_ns: i64,
    /// Retention cutoff on the data clock; None = keep forever.
    pub expired_before_ns: Option<i64>,
    pub force: bool,
}

fn hour_end_ns(h: HourBucket) -> i64 {
    h.0.saturating_add(1).saturating_mul(HourBucket::NANOS)
}

/// Kind-B trigger (replaced in step 5).
pub(crate) fn needs_rewrite(runs: &[&SegmentMeta], closed: bool, force: bool) -> bool {
    // Fixed rule: rewrite only when two segments of the same class (main / orphan / mixed) may share a key, i.e.
    // their trace ranges intersect, or any segment mixes null and non-null starts.
    let class = |s: &SegmentMeta| match s.start_time_nulls {
        0 => 0u8,
        n if n == s.row_count => 1,
        _ => 2,
    };
    if runs.iter().any(|s| class(s) == 2) {
        return runs.len() > 1;
    }
    let overlapping = |c: u8| {
        let mut v: Vec<&&SegmentMeta> = runs.iter().filter(|s| class(s) == c).collect();
        v.sort_by_key(|s| s.min_trace_id);
        v.windows(2).any(|w| w[1].min_trace_id <= w[0].max_trace_id)
    };
    let n = (0..2).filter(|&c| overlapping(c)).count();
    n > 0 && (closed || force || runs.len() > L1_MAX_RUNS)
}

pub fn pick(i: &PickInput<'_>, cfg: &CompactionConfig) -> Option<(JobKind, Vec<SegmentId>)> {
    let segs: Vec<&SegmentMeta> = i.segs.iter().filter(|s| s.state.is_visible()).collect();
    let expired = |h: HourBucket| i.expired_before_ns.is_some_and(|c| hour_end_ns(h) <= c);
    let mut hours: BTreeMap<HourBucket, Vec<&SegmentMeta>> = BTreeMap::new();
    for s in segs.iter().filter(|s| s.level == Level::L1) {
        hours.entry(s.partition_hour).or_default().push(s);
    }
    let all_live = |runs: &[&SegmentMeta]| runs.iter().all(|s| s.state == SegmentState::Live);

    // Kind A.
    let mut l0: Vec<&SegmentMeta> = segs.iter().filter(|s| s.level == Level::L0).copied().collect();
    l0.sort_by_key(|s| s.min_version);
    if !l0.is_empty() && l0.iter().all(|s| s.state == SegmentState::Live) {
        let oldest = l0.iter().map(|s| s.created_at_ms).min().unwrap_or(i64::MAX);
        let max_age = i64::try_from(cfg.l0_max_age.as_millis()).unwrap_or(i64::MAX);
        if i.force || l0.len() >= cfg.l0_trigger.max(1) || oldest < i.wall_now_ms.saturating_sub(max_age) {
            let mut inputs: Vec<SegmentId> = Vec::new();
            let (mut bytes, mut lo, mut hi) = (0u64, None::<i64>, None::<i64>);
            for s in &l0 {
                if !inputs.is_empty()
                    && (inputs.len() >= MAX_INPUTS || bytes + s.byte_size > cfg.max_job_bytes.as_u64())
                {
                    break;
                }
                inputs.push(s.id);
                bytes += s.byte_size;
                if let (Some(a), Some(b)) = (s.min_start_ns, s.max_start_ns) {
                    lo = Some(lo.map_or(a, |x| x.min(a)));
                    hi = Some(hi.map_or(b, |x| x.max(b)));
                }
            }
            if let (Some(lo), Some(hi)) = (lo, hi) {
                let range = HourBucket::from_nanos(lo)..=HourBucket::from_nanos(hi);
                for (h, runs) in hours.range(range) {
                    let sum: u64 = runs.iter().map(|s| s.byte_size).sum();
                    if !expired(*h)
                        && all_live(runs)
                        && sum < cfg.target_segment.as_u64() / 2
                        && inputs.len() + runs.len() <= MAX_INPUTS
                    {
                        inputs.extend(runs.iter().map(|s| s.id));
                    }
                }
            }
            return Some((JobKind::L0, inputs));
        }
    }
    // Kind B.
    for (h, runs) in &hours {
        if expired(*h) || !all_live(runs) {
            continue;
        }
        let closed = hour_end_ns(*h) < i.data_now_ns.saturating_sub(HourBucket::NANOS);
        if needs_rewrite(runs, closed, i.force) {
            return Some((JobKind::L1, runs.iter().map(|s| s.id).collect()));
        }
    }
    None
}

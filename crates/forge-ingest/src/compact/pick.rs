//! Pure picker: one project's visible segments -> a job request (plans/phase-4 §2.7).
use super::{L1_MAX_RUNS, MAX_INPUTS};
use forge_model::catalog::{HourBucket, JobKind, Level, SegmentId, SegmentMeta, SegmentState, trace_ranges_overlap};
use forge_model::config::CompactionConfig;
use std::collections::BTreeMap;

pub struct PickInput<'a> {
    /// Visible segments of ONE project (list_segments).
    pub segs: &'a [SegmentMeta],
    pub wall_now_ms: i64,
    pub data_now_ns: i64,
    /// Retention cutoff on the data clock; None = keep forever.
    pub expired_before_ns: Option<i64>,
    /// `forge compact`: the age / count / closed gates are off.
    pub force: bool,
}

/// First nanosecond after the hour.
fn hour_end_ns(h: HourBucket) -> i64 {
    h.0.saturating_add(1).saturating_mul(HourBucket::NANOS)
}

/// One hour's L1 segments grouped by overlapping `[min_version, max_version]`, oldest group first. Different jobs are
/// version-disjoint (§3), so a group never splits a job's file from an overlapping one. One job's files usually
/// overlap in version, but rotated files of time-ordered traces can be version-disjoint: one job's output can then
/// be several groups (review 2 R2-3).
pub fn groups<'a>(segs: &[&'a SegmentMeta]) -> Vec<Vec<&'a SegmentMeta>> {
    let mut v = segs.to_vec();
    v.sort_by_key(|s| s.min_version);
    let mut out: Vec<(u64, Vec<&SegmentMeta>)> = Vec::new();
    for s in v {
        match out.last_mut() {
            Some((hi, g)) if s.min_version <= *hi => {
                *hi = (*hi).max(s.max_version);
                g.push(s);
            }
            _ => out.push((s.max_version, vec![s])),
        }
    }
    out.into_iter().map(|x| x.1).collect()
}

/// Kind B for one hour: a newest suffix of whole groups within `max_bytes` and `MAX_INPUTS`. Either
/// - the longest such suffix (at least 2 groups), if some class (main / orphan) has two segments with intersecting
///   trace ranges in it and the hour is closed, forced, or that class has more than `L1_MAX_RUNS` segments there; or
/// - (review 1 #12: time-ordered traces give disjoint runs) the newest `L1_MAX_RUNS` or more groups made only of
///   segments below `small` bytes. It cannot retrigger on its own output: the output rotates at about the target
///   size, so only the last file of each run (main, orphan) can be below `small` (target / 2), which adds at most 2
///   small groups (< `L1_MAX_RUNS`). Rewriting stops once the merged runs reach target / 2 (review 2 R2-3).
///
/// Never because the hour merely has several segments, nor for one group (one job's files).
fn rewrite(segs: &[&SegmentMeta], closed: bool, force: bool, max_bytes: u64, small: u64) -> Option<Vec<SegmentId>> {
    let g = groups(segs);
    let (mut take, mut total, mut count) = (0, 0u64, 0usize);
    for grp in g.iter().rev() {
        let b: u64 = grp.iter().map(|s| s.byte_size).sum();
        if total.saturating_add(b) > max_bytes || count + grp.len() > MAX_INPUTS {
            break;
        }
        (total, count, take) = (total + b, count + grp.len(), take + 1);
    }
    let suffix = |n: usize| -> Vec<&SegmentMeta> { g[g.len() - n..].iter().flatten().copied().collect() };
    let ids = |v: Vec<&SegmentMeta>| v.iter().map(|s| s.id).collect();
    if take >= 2 {
        let suffix = suffix(take);
        debug_assert!(
            suffix
                .iter()
                .all(|s| s.start_time_nulls == 0 || s.start_time_nulls == s.row_count),
            "mixed L1 segment"
        );
        let trigger = [false, true].into_iter().any(|orphan| {
            let mut v: Vec<&SegmentMeta> = suffix.iter().copied().filter(|s| s.is_orphan_run() == orphan).collect();
            trace_ranges_overlap(&mut v) && (closed || force || v.len() > L1_MAX_RUNS)
        });
        if trigger {
            return Some(ids(suffix));
        }
    }
    let small_groups = g[g.len() - take..]
        .iter()
        .rev()
        .take_while(|grp| grp.iter().all(|s| s.byte_size < small))
        .count();
    (small_groups >= L1_MAX_RUNS).then(|| ids(suffix(small_groups)))
}

/// Segments split into the L0s, oldest first, and the L1s by hour.
#[allow(clippy::type_complexity)]
pub fn layout<'a>(segs: &[&'a SegmentMeta]) -> (Vec<&'a SegmentMeta>, BTreeMap<HourBucket, Vec<&'a SegmentMeta>>) {
    let mut l0: Vec<&SegmentMeta> = segs.iter().filter(|s| s.level == Level::L0).copied().collect();
    l0.sort_by_key(|s| s.min_version);
    let mut hours: BTreeMap<HourBucket, Vec<&SegmentMeta>> = BTreeMap::new();
    for s in segs.iter().filter(|s| s.level == Level::L1) {
        hours.entry(s.partition_hour).or_default().push(s);
    }
    (l0, hours)
}

pub fn pick(i: &PickInput<'_>, cfg: &CompactionConfig) -> Option<(JobKind, Vec<SegmentId>)> {
    pick_excluding(i, cfg, |_| false)
}

/// `pick`, passing over candidates whose input set is `excluded` (backing off after a failure, review 2 R2-4): kind A
/// then goes on to kind B, and kind B to the next hour.
pub fn pick_excluding(
    i: &PickInput<'_>,
    cfg: &CompactionConfig,
    excluded: impl Fn(&[SegmentId]) -> bool,
) -> Option<(JobKind, Vec<SegmentId>)> {
    let segs: Vec<&SegmentMeta> = i.segs.iter().filter(|s| s.state.is_visible()).collect();
    let expired = |h: HourBucket| i.expired_before_ns.is_some_and(|c| h.expired_by(c));
    let (l0, hours) = layout(&segs);
    let all_live = |runs: &[&SegmentMeta]| runs.iter().all(|s| s.state == SegmentState::Live);
    let max_bytes = cfg.max_job_bytes.as_u64();

    // Kind A: the oldest-first L0 prefix, plus whole small hours it touches.
    if !l0.is_empty() && all_live(&l0) {
        let oldest = l0.iter().map(|s| s.created_at_ms).min().unwrap_or(i64::MAX);
        let max_age = i64::try_from(cfg.l0_max_age.as_millis()).unwrap_or(i64::MAX);
        if i.force || l0.len() >= cfg.l0_trigger.max(1) || oldest < i.wall_now_ms.saturating_sub(max_age) {
            let mut inputs: Vec<SegmentId> = Vec::new();
            let (mut bytes, mut lo, mut hi) = (0u64, None::<i64>, None::<i64>);
            for s in &l0 {
                if !inputs.is_empty() && (inputs.len() >= MAX_INPUTS || bytes.saturating_add(s.byte_size) > max_bytes) {
                    break;
                }
                inputs.push(s.id);
                bytes = bytes.saturating_add(s.byte_size);
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
                        && bytes.saturating_add(sum) <= max_bytes
                        && inputs.len() + runs.len() <= MAX_INPUTS
                    {
                        inputs.extend(runs.iter().map(|s| s.id));
                        bytes += sum;
                    }
                }
            }
            if !excluded(&inputs) {
                return Some((JobKind::L0, inputs));
            }
        }
    }
    // Kind B: a newest version suffix of one hour.
    for (h, runs) in &hours {
        if expired(*h) || !all_live(runs) {
            continue;
        }
        let closed = hour_end_ns(*h) < i.data_now_ns.saturating_sub(HourBucket::NANOS);
        if let Some(ids) = rewrite(runs, closed, i.force, max_bytes, cfg.target_segment.as_u64() / 2)
            && !excluded(&ids)
        {
            return Some((JobKind::L1, ids));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::ids::TraceId;
    use forge_model::testing::segment_meta;

    const H0: i64 = 470_000;

    fn t(n: u8) -> TraceId {
        TraceId([n; 16])
    }

    /// An L1 segment of hour H0 (or `level`): versions, trace range, bytes, orphan?
    fn seg(level: Level, v: (u64, u64), tr: (u8, u8), bytes: u64, orphan: bool) -> SegmentMeta {
        SegmentMeta {
            min_version: v.0,
            max_version: v.1,
            min_trace_id: t(tr.0),
            max_trace_id: t(tr.1),
            byte_size: bytes,
            row_count: 4,
            start_time_nulls: if orphan { 4 } else { 0 },
            min_start_ns: (!orphan).then_some(H0 * HourBucket::NANOS),
            max_start_ns: (!orphan).then_some(H0 * HourBucket::NANOS + 1),
            created_at_ms: i64::MAX / 2,
            ..segment_meta("p", level, HourBucket(H0))
        }
    }

    fn input(segs: &[SegmentMeta], force: bool, closed: bool) -> PickInput<'_> {
        PickInput {
            segs,
            wall_now_ms: 0,
            data_now_ns: if closed {
                (H0 + 5) * HourBucket::NANOS
            } else {
                H0 * HourBucket::NANOS
            },
            expired_before_ns: None,
            force,
        }
    }

    fn cfg(max: u64) -> CompactionConfig {
        CompactionConfig {
            max_job_bytes: bytesize::ByteSize::b(max),
            l0_trigger: 100,
            ..Default::default()
        }
    }

    #[test]
    fn one_jobs_files_never_trigger() {
        // One job: main + orphan + two rotated mains (disjoint ranges), overlapping versions = one group.
        let s = vec![
            seg(Level::L1, (1, 9), (1, 4), 10, false),
            seg(Level::L1, (2, 8), (5, 9), 10, false),
            seg(Level::L1, (1, 9), (1, 9), 10, true),
        ];
        assert_eq!(
            groups(&s.iter().collect::<Vec<_>>()).len(),
            1,
            "groups never split a job"
        );
        assert!(pick(&input(&s, true, true), &cfg(u64::MAX)).is_none());
    }

    #[test]
    fn trigger_needs_same_class_overlap_and_a_gate() {
        let a = seg(Level::L1, (1, 5), (1, 5), 10, false);
        let b_orphan = seg(Level::L1, (6, 9), (1, 5), 10, true);
        // Two groups, but main vs orphan: no same-class overlap.
        assert!(pick(&input(&[a.clone(), b_orphan], true, true), &cfg(u64::MAX)).is_none());
        let b = seg(Level::L1, (6, 9), (5, 7), 10, false); // touching range: <=
        let s = [a.clone(), b.clone()];
        assert!(
            pick(&input(&s, false, false), &cfg(u64::MAX)).is_none(),
            "open hour, 2 runs"
        );
        assert_eq!(
            pick(&input(&s, true, false), &cfg(u64::MAX)).unwrap().0,
            JobKind::L1,
            "force"
        );
        assert_eq!(
            pick(&input(&s, false, true), &cfg(u64::MAX)).unwrap().0,
            JobKind::L1,
            "closed"
        );
        // More than L1_MAX_RUNS overlapping runs in an open hour.
        let many: Vec<SegmentMeta> = (0..=L1_MAX_RUNS as u64)
            .map(|k| seg(Level::L1, (10 * k + 1, 10 * k + 5), (1, 5), 10, false))
            .collect();
        assert_eq!(
            pick(&input(&many, false, false), &cfg(u64::MAX)).unwrap().1.len(),
            many.len()
        );
        // Disjoint ranges: no rewrite even when forced.
        let c = seg(Level::L1, (6, 9), (6, 7), 10, false);
        assert!(pick(&input(&[a, c], true, true), &cfg(u64::MAX)).is_none());
    }

    #[test]
    fn suffix_stops_at_the_cap() {
        let g1 = seg(Level::L1, (1, 5), (1, 5), 100, false);
        let g2 = seg(Level::L1, (6, 9), (1, 5), 10, false);
        let g3 = seg(Level::L1, (10, 12), (1, 5), 10, false);
        let s = [g1.clone(), g2.clone(), g3.clone()];
        let (_, ids) = pick(&input(&s, true, true), &cfg(25)).unwrap();
        assert_eq!(ids, vec![g2.id, g3.id], "newest suffix within the cap");
        assert_eq!(pick(&input(&s, true, true), &cfg(u64::MAX)).unwrap().1.len(), 3);
        assert!(
            pick(&input(&s, true, true), &cfg(15)).is_none(),
            "fewer than 2 groups fit"
        );
    }

    /// Review 1 #3: the suffix is capped by count too, in whole groups, and stays the newest.
    #[test]
    fn suffix_stops_at_the_input_count() {
        let s: Vec<SegmentMeta> = (0..20u64)
            .map(|k| seg(Level::L1, (10 * k + 1, 10 * k + 5), (1, 5), 10, false))
            .collect();
        let (_, ids) = pick(&input(&s, true, true), &cfg(u64::MAX)).unwrap();
        let newest: Vec<SegmentId> = s[20 - MAX_INPUTS..].iter().map(|s| s.id).collect();
        assert_eq!(ids, newest);
        // Two-file groups: whole groups only.
        let s: Vec<SegmentMeta> = (0..20u64)
            .flat_map(|k| {
                let v = (10 * k + 1, 10 * k + 5);
                [
                    seg(Level::L1, v, (1, 5), 10, false),
                    seg(Level::L1, v, (1, 5), 10, true),
                ]
            })
            .collect();
        let (_, ids) = pick(&input(&s, true, true), &cfg(u64::MAX)).unwrap();
        assert_eq!(ids.len(), MAX_INPUTS);
        let s: Vec<SegmentMeta> = (0..20u64)
            .flat_map(|k| {
                let v = (10 * k + 1, 10 * k + 5);
                let orphan = seg(Level::L1, v, (1, 5), 10, true);
                [seg(Level::L1, v, (1, 5), 10, false), orphan.clone(), orphan]
            })
            .collect();
        let (_, ids) = pick(&input(&s, true, true), &cfg(u64::MAX)).unwrap();
        assert_eq!(ids.len(), MAX_INPUTS / 3 * 3, "whole groups of 3");
    }

    /// Review 1 #12: disjoint small groups trigger once there are `L1_MAX_RUNS` of them (newest, small only); one
    /// job's files (one group) never do.
    #[test]
    fn many_small_disjoint_groups_trigger() {
        let target = CompactionConfig::default().target_segment.as_u64();
        let disjoint = |k: u64, bytes: u64| {
            seg(
                Level::L1,
                (10 * k + 1, 10 * k + 5),
                (k as u8 * 2, k as u8 * 2 + 1),
                bytes,
                false,
            )
        };
        let s: Vec<SegmentMeta> = (0..L1_MAX_RUNS as u64 - 1).map(|k| disjoint(k, 10)).collect();
        assert!(pick(&input(&s, false, false), &cfg(u64::MAX)).is_none(), "too few");
        let mut s: Vec<SegmentMeta> = (0..6).map(|k| disjoint(k, 10)).collect();
        s[0].byte_size = target; // old big run: not in the suffix
        let (kind, ids) = pick(&input(&s, false, false), &cfg(u64::MAX)).unwrap();
        assert_eq!(kind, JobKind::L1);
        assert_eq!(ids, s[1..].iter().map(|s| s.id).collect::<Vec<_>>());
        s[5].byte_size = target / 2; // the newest group is not small: nothing
        assert!(pick(&input(&s, false, false), &cfg(u64::MAX)).is_none());
        let one_job: Vec<SegmentMeta> = (0..6)
            .map(|k| seg(Level::L1, (1, 9), (k * 2, k * 2 + 1), 10, false))
            .collect();
        assert!(
            pick(&input(&one_job, true, true), &cfg(u64::MAX)).is_none(),
            "one group"
        );
    }

    #[test]
    fn folded_bytes_count_and_expired_hours_are_skipped() {
        let l0 = seg(Level::L0, (20, 21), (1, 5), 10, false);
        let r = seg(Level::L1, (1, 5), (1, 5), 10, false);
        let s = [l0.clone(), r.clone()];
        assert_eq!(
            pick(&input(&s, true, false), &cfg(u64::MAX)).unwrap().1,
            vec![l0.id, r.id]
        );
        assert_eq!(
            pick(&input(&s, true, false), &cfg(15)).unwrap().1,
            vec![l0.id],
            "fold over the cap"
        );
        let mut i = input(&s, true, false);
        i.expired_before_ns = Some((H0 + 1) * HourBucket::NANOS);
        assert_eq!(
            pick(&i, &cfg(u64::MAX)).unwrap().1,
            vec![l0.id],
            "expired hour not folded"
        );
        let two = [r.clone(), seg(Level::L1, (6, 9), (1, 5), 10, false)];
        let mut i = input(&two, true, true);
        i.expired_before_ns = Some((H0 + 1) * HourBucket::NANOS);
        assert!(pick(&i, &cfg(u64::MAX)).is_none(), "expired hour not rewritten");
    }

    #[test]
    fn force_bypasses_only_the_gates() {
        let l0 = seg(Level::L0, (20, 21), (1, 5), 10, false);
        assert!(pick(&input(std::slice::from_ref(&l0), false, false), &cfg(u64::MAX)).is_none());
        assert!(pick(&input(std::slice::from_ref(&l0), true, false), &cfg(u64::MAX)).is_some());
        // A compacting L0 blocks kind A even when forced.
        let busy = SegmentMeta {
            state: SegmentState::Compacting,
            ..l0
        };
        assert!(pick(&input(&[busy], true, false), &cfg(u64::MAX)).is_none());
    }
}

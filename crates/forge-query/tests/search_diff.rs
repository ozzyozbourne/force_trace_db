//! E2 sketch (plans/phase-5 §5.3): every `/v1/search` page over random layouts (L0 flushes with sink-built
//! sidecars, memtable, upserts, tombstones, start-less patches) equals a brute-force reference on merged rows.
mod common;
use common::{Fixture, RANDOM_PROJECTS, random_layout};
use forge_index::{Analyzer, FtsColumn, FtsPredicate, Matcher};
use forge_model::config::QueryConfig;
use forge_model::ids::{SpanId, TraceId};
use forge_model::row::SpanRow;
use forge_model::testing::pid;
use forge_query::search::{SearchBody, SearchRequest};
use forge_query::testing::{positions, search_pages};

fn cell(r: &SpanRow, c: FtsColumn) -> Option<&str> {
    match c {
        FtsColumn::Name => r.name.as_deref(),
        FtsColumn::Error => r.error.as_deref(),
        FtsColumn::Inputs => r.inputs.as_deref(),
        FtsColumn::Outputs => r.outputs.as_deref(),
        FtsColumn::Metadata => r.metadata.as_deref(),
    }
}

fn ref_search(reference: &[SpanRow], project: &str, q: &str) -> Vec<(i64, TraceId, SpanId)> {
    let mut an = Analyzer::default();
    let ms: Vec<Matcher> = FtsColumn::ALL
        .iter()
        .map(|c| {
            Matcher::new(
                &mut an,
                &FtsPredicate::Search {
                    column: *c,
                    query: q.into(),
                },
            )
            .unwrap()
        })
        .collect();
    let mut out: Vec<_> = reference
        .iter()
        .filter(|r| r.project_id.as_ref().is_some_and(|p| p.as_str() == project))
        .filter(|r| {
            FtsColumn::ALL
                .iter()
                .zip(&ms)
                .any(|(c, m)| cell(r, *c).is_some_and(|v| m.matches(&mut an, v)))
        })
        .filter_map(|r| Some((r.start_time_ns?, r.trace_id, r.span_id)))
        .collect();
    out.sort_by(|a, b| b.cmp(a));
    out
}

async fn pages(f: &Fixture, project: &str, q: &str, limit: usize) -> Vec<(i64, TraceId, SpanId)> {
    let req = |cursor| {
        let body = SearchBody {
            q: q.into(),
            limit: Some(limit),
            cursor,
            ..Default::default()
        };
        SearchRequest::parse(body, pid(project)).unwrap()
    };
    let mut out = Vec::new();
    for p in search_pages(&f.engine, req, usize::MAX).await {
        out.extend(positions(&p.rows));
        assert!(p.rows.num_rows() <= limit);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn search_pages_match_reference() {
    for seed in 1..=6u64 {
        let (layout, traps) = random_layout(seed);
        let f = Fixture::build(layout, traps, QueryConfig::default()).await;
        let reference = f.reference();
        for project in RANDOM_PROJECTS {
            for q in [
                "hi",
                "out",
                "boom",
                "span",
                "sess",
                "hi OR boom",
                "nothingatall",
                "span-1*",
            ] {
                let want = ref_search(&reference, project, q);
                for limit in [1, 3, 50] {
                    let got = pages(&f, project, q, limit).await;
                    assert_eq!(got, want, "seed {seed} project {project} q {q:?} limit {limit}");
                }
            }
        }
    }
}

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
    use arrow::array::{Array, AsArray};
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let body = SearchBody {
            q: q.into(),
            limit: Some(limit),
            cursor: cursor.take(),
            ..Default::default()
        };
        let r = SearchRequest::parse(body, pid(project)).unwrap();
        let page = f.engine.search(&r).await.unwrap();
        let b = &page.rows;
        let s = b.schema();
        let st = b
            .column(s.index_of("start_time").unwrap())
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        let t = b.column(s.index_of("trace_id").unwrap()).as_binary::<i32>();
        let sp = b.column(s.index_of("span_id").unwrap()).as_binary::<i32>();
        for i in 0..b.num_rows() {
            assert!(st.is_valid(i));
            out.push((
                st.value(i),
                TraceId::from_slice(t.value(i)).unwrap(),
                SpanId::from_slice(sp.value(i)).unwrap(),
            ));
        }
        assert!(b.num_rows() <= limit);
        match page.next_cursor {
            Some(c) => cursor = Some(r.encode_cursor(&c)),
            None => break,
        }
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

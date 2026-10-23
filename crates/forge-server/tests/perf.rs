//! Exit test 1 (plan §6.5): a 2,000-span trace spread over 3 L0 segments plus a memtable of many small batches
//! loads in under 100 ms p50 from local disk, timed through the API router to the last body byte.
//!
//! cargo test -p forge-server --profile quick-release --test perf -- --ignored --nocapture
#![allow(clippy::print_stdout)]
use axum::Router;
use axum::body::{Body, to_bytes};
use forge_ingest::Ingest;
use forge_meta::{FlushCommit, Metastore, NewSegment};
use forge_model::catalog::{HourBucket, Level, SegmentId};
use forge_model::config::ForgeConfig;
use forge_model::enums::{RunType, SpanStatus};
use forge_model::merge::{Tombstones, merge_versions_chunked, split_chunks_by_project};
use forge_model::row::SpanRow;
use forge_model::testing::{Rng, batch_from_rows, pid, sid, tid};
use forge_server::{Forge, api_router};
use forge_storage::{SegmentSpec, SegmentStore, VortexSegmentStore, build_object_store};
use futures::StreamExt;
use http::Request;
use std::time::{Duration, Instant};
use tower::ServiceExt;

const BASE: i64 = 1_700_000_000_000_000_000;
const SPANS: u64 = 2_000;
const TARGET: u64 = 1_000_000;

/// One trace's start posts (`kind` 0), end posts (1) or patches of every other span (2). Agent-shaped: parent
/// among the previous 8 spans, depth <= 10; one llm span in three carries ~1.6 KB of input and of output.
fn posts(trace: u64, kind: u8, payload: usize) -> Vec<SpanRow> {
    let mut r = Rng(trace + 1);
    let mut depth = vec![0u32; SPANS as usize];
    let text = "lorem ipsum ".repeat(payload / 12);
    let mut out = Vec::new();
    for i in 0..SPANS {
        let parent = (i > 0).then(|| {
            let mut p = i - 1 - r.below(i.min(8));
            if depth[p as usize] >= 9 {
                p = 0;
            }
            depth[i as usize] = depth[p as usize] + 1;
            p
        });
        let llm = i % 3 == 1;
        let start = BASE + (trace as i64 % 1000) * 1_000_000_000 + i as i64 * 1_000_017;
        let base = SpanRow {
            project_id: Some(pid("perf")),
            trace_id: tid(trace),
            span_id: sid(i),
            ..Default::default()
        };
        out.push(match kind {
            0 => SpanRow {
                parent_span_id: parent.map(sid),
                name: Some(format!("step-{i}")),
                run_type: Some(if llm { RunType::Llm } else { RunType::Chain }),
                start_time_ns: Some(start),
                status: Some(SpanStatus::Unset),
                session_id: Some(format!("thread-{}", trace % 50)),
                tags: Some(vec!["prod".into(), "agent".into()]),
                metadata: Some(format!("{{\"user\":\"u{}\",\"step\":{i}}}", i % 17)),
                attributes: Some(format!(
                    "{{\"http.status_code\":200,\"gen_ai.system\":\"openai\",\"i\":{i}}}"
                )),
                inputs: llm.then(|| serde_json::json!({"messages": [{"role": "user", "content": text}]}).to_string()),
                ..base
            },
            1 => SpanRow {
                end_time_ns: Some(start + 1_000_000 + r.below(1_000_000_000) as i64),
                status: Some(if i % 97 == 0 { SpanStatus::Error } else { SpanStatus::Ok }),
                error: (i % 97 == 0).then(|| "Timeout".into()),
                model: Some("gpt-4o".into()),
                provider: Some("openai".into()),
                prompt_tokens: Some(100 + i as i64),
                completion_tokens: Some(20 + i as i64 % 13),
                total_tokens: llm.then_some(150),
                cost_usd: Some(0.000_25 * i as f64),
                events: Some("[{\"name\":\"tick\"}]".into()),
                outputs: llm.then(|| serde_json::json!({"text": text}).to_string()),
                ..base
            },
            _ => SpanRow {
                name: Some(format!("step-{i}-patched")),
                ..base
            },
        });
    }
    if kind == 2 {
        out.retain(|r| r.span_id.as_bytes()[7] % 2 == 0);
    }
    out
}

async fn write_flush(meta: &dyn Metastore, store: &dyn SegmentStore, rows: Vec<SpanRow>, seq: u64, max_version: u64) {
    let rows: Vec<SpanRow> = rows
        .into_iter()
        .enumerate()
        .map(|(i, r)| SpanRow {
            version: max_version - 100_000 + i as u64,
            ..r
        })
        .collect();
    let merged = merge_versions_chunked(&[batch_from_rows(&rows)], Tombstones::Keep, 64 << 20).unwrap();
    let mut segments = Vec::new();
    for (p, batches) in split_chunks_by_project(&merged).unwrap() {
        let spec = SegmentSpec {
            id: SegmentId::new(),
            project_id: p,
            level: Level::L0,
            partition_hour: HourBucket::from_nanos(BASE),
        };
        let w = store
            .write_segment(spec, futures::stream::iter(batches.into_iter().map(Ok)).boxed())
            .await
            .unwrap();
        segments.push(NewSegment {
            meta: w.meta,
            traces: w.traces,
        });
    }
    meta.commit_flush(FlushCommit {
        node_id: "perf".into(),
        segments,
        flushed_wal_seq: seq,
        max_version,
    })
    .await
    .unwrap();
}

async fn timed(api: &Router, uri: &str, gzip: bool) -> (Duration, usize) {
    let mut req = Request::get(uri);
    if gzip {
        req = req.header("accept-encoding", "gzip");
    }
    let t = Instant::now();
    let resp = api.clone().oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    assert!(resp.status().is_success(), "{uri}: {}", resp.status());
    if gzip {
        assert_eq!(resp.headers()["content-encoding"], "gzip");
    }
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (t.elapsed(), body.len())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// Times `n` calls and prints p50 / p95 / p99 / max; returns p50.
async fn measure(api: &Router, label: &str, uri: &str, gzip: bool, n: usize) -> Duration {
    for _ in 0..5 {
        timed(api, uri, gzip).await;
    }
    let mut v = Vec::with_capacity(n);
    let mut bytes = 0;
    for _ in 0..n {
        let (d, b) = timed(api, uri, gzip).await;
        v.push(d);
        bytes = b;
    }
    v.sort();
    let q = |p: f64| v[((n as f64 * p) as usize).min(n - 1)];
    println!(
        "{label:<52} p50 {:7.2} ms  p95 {:7.2} ms  p99 {:7.2} ms  max {:7.2} ms  body {:>9} B",
        ms(q(0.50)),
        ms(q(0.95)),
        ms(q(0.99)),
        ms(v[n - 1]),
        bytes
    );
    q(0.50)
}

#[ignore = "perf: run in quick-release"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn trace_2000_spans_p50_under_100ms() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ForgeConfig::default();
    cfg.metastore.url = format!("sqlite://{}", dir.path().join("meta.db").display());
    cfg.storage.url = format!("file://{}", dir.path().join("data").display());
    cfg.wal.dir = dir.path().join("wal");
    cfg.memtable.flush_bytes = bytesize::ByteSize::gib(2);
    cfg.memtable.memory_budget = bytesize::ByteSize::gib(4);
    cfg.memtable.flush_interval = Duration::from_secs(3600);
    cfg.ingest.default_project = "perf".into();

    // 1. Three L0 flushes, written directly: the start / end / patch posts of 16 filler traces and the target.
    let t0 = Instant::now();
    {
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let meta = forge_meta::SqliteMetastore::open_path(&dir.path().join("meta.db")).unwrap();
        let store = VortexSegmentStore::new(build_object_store(&cfg.storage).unwrap());
        for f in 0..3u64 {
            let mut rows = posts(TARGET, f as u8, 1600);
            for filler in 0..16 {
                rows.extend(posts(filler, f as u8, 1600));
            }
            write_flush(&meta, &store, rows, f + 1, (f + 1) * 1_000_000).await;
        }
    }
    println!("setup: 3 flushes written in {:.1} s", t0.elapsed().as_secs_f64());

    // 2. Forge on that directory; 3. the memtable: 50k filler rows + 200 target patches in 5k submits of ~10 rows.
    let forge = Forge::open(cfg).await.unwrap();
    let target_patches: Vec<SpanRow> = (0..200)
        .map(|i| SpanRow {
            project_id: Some(pid("perf")),
            trace_id: tid(TARGET),
            span_id: sid(i * 10),
            status: Some(SpanStatus::Ok),
            name: Some(format!("mem-patch-{i}")),
            ..Default::default()
        })
        .collect();
    let mut submits: Vec<Vec<SpanRow>> = Vec::new();
    for t in 0..25u64 {
        let rows = posts(100 + t, 0, 200);
        submits.extend(rows.chunks(10).map(|c| c.to_vec()));
    }
    for (i, p) in target_patches.into_iter().enumerate() {
        submits[i * 25].push(p);
    }
    let t1 = Instant::now();
    futures::stream::iter(submits)
        .map(|rows| {
            let f = forge.clone();
            async move { f.ingest.submit(rows).await.unwrap() }
        })
        .buffer_unordered(64)
        .collect::<Vec<()>>()
        .await;
    println!("setup: 5,000 submits in {:.1} s", t1.elapsed().as_secs_f64());

    // Preconditions.
    let locs = forge.meta.locate_trace(&pid("perf"), tid(TARGET)).await.unwrap();
    assert_eq!(locs.len(), 3, "the target is in 3 segments");
    let snap = forge.ingest.memtable_view().snapshot();
    let in_mem: usize = snap
        .batches()
        .map(|b| {
            let t = arrow::array::AsArray::as_binary::<i32>(b.column(1));
            (0..b.num_rows())
                .filter(|&i| t.value(i) == tid(TARGET).as_bytes())
                .count()
        })
        .sum();
    assert_eq!(in_mem, 200, "200 target rows in the memtable");
    assert!(snap.batches().count() >= 5_000, "many small batches");
    drop(snap);

    let api = api_router(forge.clone());
    let uri = format!("/v1/traces/{}?project=perf", tid(TARGET));
    let (first, _) = timed(&api, &uri, false).await;
    let body = to_bytes(
        api.clone()
            .oneshot(Request::get(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["span_count"], 2000);
    assert!(body.len() > 2 << 20, "payload body {} B", body.len());
    let (_, small) = timed(&api, &format!("{uri}&payloads=false"), false).await;
    assert!(small < 1 << 20, "payloads=false body {small} B");

    println!("first call (footer-cold, page-cache-warm): {:.2} ms", ms(first));
    let p50 = measure(
        &api,
        "trace 2,000 spans, payloads, 3 seg + mem, no gzip",
        &uri,
        false,
        200,
    )
    .await;
    measure(&api, "trace 2,000 spans, payloads, gzip", &uri, true, 200).await;
    measure(
        &api,
        "trace 2,000 spans, payloads=false",
        &format!("{uri}&payloads=false"),
        false,
        200,
    )
    .await;
    measure(
        &api,
        "runs list limit 50 (17 roots in 3 segments + 25 in mem)",
        "/v1/runs?project=perf&limit=50",
        false,
        200,
    )
    .await;
    if !cfg!(debug_assertions) {
        assert!(p50 < Duration::from_millis(100), "p50 {:.2} ms", ms(p50));
    }
}

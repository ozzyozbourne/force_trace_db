//! `.fts` round trips: hand-written expectations, then the encode/decode proptest (spec "Tests").
use crate::analyzer::{Analyzer, key_path_token, key_token};
use crate::format::{BlockMeta, Footer, decode_postings, encode_postings};
use crate::query::{Leaf, Query, json_key_search_query, search_query};
use crate::read::{FtsFile, Io};
use crate::{FtsBuilder, FtsColumn, IndexResult};
use bytes::Bytes;
use proptest::prelude::*;
use roaring::RoaringBitmap;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

fn fetcher(file: Bytes) -> impl Fn(Vec<Range<u64>>) -> std::future::Ready<IndexResult<Vec<Bytes>>> {
    move |rs| {
        std::future::ready(Ok(rs
            .into_iter()
            .map(|r| file.slice(r.start as usize..r.end as usize))
            .collect()))
    }
}

async fn run(file: &Bytes, q: &Query) -> (RoaringBitmap, Io) {
    let fetch = fetcher(file.clone());
    let (f, _) = FtsFile::open("t.fts", file.len() as u64, &fetch).await.unwrap();
    f.execute(q, &fetch).await.unwrap()
}

fn set(v: &[u32]) -> RoaringBitmap {
    v.iter().copied().collect()
}

#[tokio::test]
async fn small_corpus() {
    use FtsColumn::*;
    let mut b = FtsBuilder::default();
    let rows: Vec<(u32, &str, Option<&str>, Value, Value, Value)> = vec![
        (
            0,
            "ChatOpenAI",
            None,
            json!({"messages":[{"role":"user","content":"I want a refund for order #123"}]}),
            json!({"content":"Your refund is processed"}),
            json!({}),
        ),
        (
            1,
            "search_tool",
            None,
            json!({"query":"refund policy"}),
            json!({"result":"30 days"}),
            json!({}),
        ),
        (
            2,
            "ChatOpenAI",
            Some("Timeout calling café API"),
            json!({"messages":[{"role":"system","content":"Be nice at the Café"}]}),
            json!(null),
            json!({}),
        ),
        (
            3,
            "agent",
            None,
            json!("refunded twice"),
            json!(["a", 1, true]),
            json!({"user.id":"alice","env":"prod"}),
        ),
        (
            70_000,
            "refund_tool",
            None,
            json!({"query":"policy for refund"}),
            json!({}),
            json!({}),
        ),
    ];
    for (row, name, err, i, o, m) in &rows {
        b.add(*row, Name, name).unwrap();
        if let Some(e) = err {
            b.add(*row, Error, e).unwrap();
        }
        b.add(*row, Inputs, &i.to_string()).unwrap();
        b.add(*row, Outputs, &o.to_string()).unwrap();
        b.add(*row, Metadata, &m.to_string()).unwrap();
    }
    let (file, st) = b.finish().unwrap();
    assert_eq!(st.rows, 70_001);
    let file = Bytes::from(file);
    let mut an = Analyzer::default();
    let cases: Vec<(&str, Query, Vec<u32>)> = vec![
        ("refund", search_query(&mut an, Inputs, "refund"), vec![0, 1, 70_000]),
        (
            "Refund*",
            search_query(&mut an, Inputs, "Refund*"),
            vec![0, 1, 3, 70_000],
        ),
        ("outputs refund", search_query(&mut an, Outputs, "refund"), vec![0]),
        ("CAFE", search_query(&mut an, Inputs, "CAFE"), vec![2]),
        (
            "OR",
            search_query(&mut an, Inputs, "refund OR café"),
            vec![0, 1, 2, 70_000],
        ),
        (
            "phrase",
            search_query(&mut an, Inputs, "\"refund policy\""),
            vec![1, 70_000],
        ),
        ("AND", search_query(&mut an, Inputs, "refund order"), vec![0]),
        ("error", search_query(&mut an, Error, "timeout"), vec![2]),
        ("name split", search_query(&mut an, Name, "refund"), vec![70_000]),
        ("miss", search_query(&mut an, Inputs, "zzz"), vec![]),
        ("AND miss", search_query(&mut an, Inputs, "refund zzz"), vec![]),
        (
            "jks",
            json_key_search_query(&mut an, Inputs, "$.messages[*].content", "refund").unwrap(),
            vec![0],
        ),
        (
            "jks idx",
            json_key_search_query(&mut an, Inputs, "$.messages[0].content", "REFUND").unwrap(),
            vec![0],
        ),
        (
            "jks quoted",
            json_key_search_query(&mut an, Metadata, "$['user.id']", "alice").unwrap(),
            vec![3],
        ),
        (
            "jks root",
            json_key_search_query(&mut an, Inputs, "$", "refunded").unwrap(),
            vec![3],
        ),
        (
            "exists",
            json_key_search_query(&mut an, Inputs, "$.messages", "").unwrap(),
            vec![0, 2],
        ),
        (
            "array",
            json_key_search_query(&mut an, Outputs, "$[*]", "true").unwrap(),
            vec![3],
        ),
    ];
    for (name, q, want) in cases {
        let (got, io) = run(&file, &q).await;
        assert_eq!(got, set(&want), "{name}");
        assert!(io.waves <= 1, "{name}: small file, dict came with the tail: {io:?}");
    }
    assert!(json_key_search_query(&mut an, Name, "$.a", "x").is_err());
}

#[tokio::test]
async fn rows_must_not_decrease_per_column() {
    let mut b = FtsBuilder::default();
    b.add(5, FtsColumn::Name, "a").unwrap();
    b.add(3, FtsColumn::Error, "a").unwrap(); // another column: fine
    assert!(b.add(4, FtsColumn::Name, "a").is_err());
}

#[tokio::test]
async fn corrupt_files_are_errors() {
    let mut b = FtsBuilder::default();
    b.add(0, FtsColumn::Name, "hello world").unwrap();
    let (file, _) = b.finish().unwrap();
    for i in [0, 4, file.len() / 2, file.len() - 13, file.len() - 9, file.len() - 1] {
        let mut bad = file.clone();
        bad[i] ^= 0x55;
        let bad = Bytes::from(bad);
        let fetch = fetcher(bad.clone());
        let q = Query {
            groups: vec![vec![Leaf::Exact(key_token(FtsColumn::Name, "hello"))]],
            needs_recheck: false,
        };
        // Never a panic; a flipped bit in the dict or footer is caught by its checksum.
        if let Ok((f, _)) = FtsFile::open("t.fts", bad.len() as u64, &fetch).await {
            let _ = f.execute(&q, &fetch).await;
        }
    }
    assert!(
        FtsFile::open("t.fts", 3, &fetcher(Bytes::from_static(b"FTS")))
            .await
            .is_err()
    );
}

// ---------------------------------------------------------------- proptest

const WORDS: [&str; 12] = [
    "refund", "Order", "café", "the", "a", "42", "x9", "TIMEOUT", "Ørsted", "user_id", "é", "zz",
];

fn text() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(&WORDS[..]), 0..6).prop_map(|w| w.join(" "))
}

fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        text().prop_map(Value::String),
        (0i64..1000).prop_map(|n| json!(n)),
        any::<bool>().prop_map(Value::Bool),
        Just(Value::Null),
    ];
    let key = prop::sample::select(&["messages", "content", "role", "user.id", "a b", "", "x[0]", "k_1"][..]);
    leaf.prop_recursive(4, 24, 4, move |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(key.clone(), inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().map(|(k, v)| (k.to_string(), v)).collect())),
        ]
    })
}

/// (row gap, column, value). Gaps of 70k put rows in several 64K row groups.
fn cells() -> impl Strategy<Value = Vec<(u32, usize, String)>> {
    let gap = prop_oneof![8 => 0u32..3, 1 => Just(70_000u32)];
    let value = prop_oneof![
        text(),
        json_value().prop_map(|v| v.to_string()),
        Just(forge_model::blob::blob_ref("blobs/p/abc", 9)),
    ];
    prop::collection::vec((gap, 0usize..5, value), 1..120)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Every key decodes to exactly the rows the analyzer emitted it for, with a cold reader in at most 2 waves
    /// after the footer; absent keys, prefixes and ANDs agree with the same oracle.
    #[test]
    fn fts_encode_decode(cells in cells(), budget in 1usize..4096, probe in text()) {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let mut oracle: BTreeMap<Vec<u8>, BTreeSet<u32>> = BTreeMap::new();
            let (mut b, mut an, mut row) = (FtsBuilder::default().with_block_budget(budget), Analyzer::default(), 0u32);
            for (gap, c, v) in &cells {
                row += gap;
                let col = FtsColumn::ALL[*c];
                b.add(row, col, v).unwrap();
                an.for_each_key(col, v, &mut |k| { oracle.entry(k.to_vec()).or_default().insert(row); });
            }
            let (file, st) = b.finish().unwrap();
            prop_assert_eq!(st.rows, row + 1);
            prop_assert_eq!(st.terms, oracle.len() as u64);
            let file = Bytes::from(file);
            let fetch = fetcher(file.clone());
            let exact = |k: &[u8]| Query { groups: vec![vec![Leaf::Exact(k.to_vec())]], needs_recheck: false };
            for (k, rows) in &oracle {
                // Cold for every key: the footer read, then <= 2 waves.
                let (f, open_io) = FtsFile::open("t.fts", file.len() as u64, &fetch).await.unwrap();
                prop_assert!(open_io.waves <= 2);
                let (got, io) = f.execute(&exact(k), &fetch).await.unwrap();
                prop_assert_eq!(got, rows.iter().copied().collect::<RoaringBitmap>());
                prop_assert!(io.waves <= 2);
            }
            let (f, _) = FtsFile::open("t.fts", file.len() as u64, &fetch).await.unwrap();
            prop_assert_eq!(f.footer().num_terms, oracle.len() as u64);
            // Absent keys: empty and no postings wave.
            for w in an.tokens(&probe) {
                for k in [key_token(FtsColumn::Error, &format!("{w}qq")), key_path_token(FtsColumn::Inputs, "$.nope", &w)] {
                    let (got, io) = f.execute(&exact(&k), &fetch).await.unwrap();
                    prop_assert!(got.is_empty());
                    prop_assert!(io.waves <= 1);
                }
            }
            // Prefix of every key's first 3 bytes, and AND of neighbours.
            let keys: Vec<&Vec<u8>> = oracle.keys().collect();
            for (i, k) in keys.iter().enumerate().step_by(7) {
                let p = &k[..k.len().min(3)];
                let want: RoaringBitmap = oracle.range(p.to_vec()..).take_while(|(k, _)| k.starts_with(p))
                    .flat_map(|(_, r)| r.iter().copied()).collect();
                let q = Query { groups: vec![vec![Leaf::Prefix(p.to_vec())]], needs_recheck: false };
                prop_assert_eq!(f.execute(&q, &fetch).await.unwrap().0, want);
                let j = (i * 31 + 1) % keys.len();
                let want: RoaringBitmap = oracle[keys[i]].intersection(&oracle[keys[j]]).copied().collect();
                let q = Query { groups: vec![vec![Leaf::Exact(keys[i].clone()), Leaf::Exact(keys[j].clone())]], needs_recheck: false };
                let (got, io) = f.execute(&q, &fetch).await.unwrap();
                prop_assert_eq!(got, want);
                prop_assert!(io.waves <= 2);
            }
            Ok(())
        })?;
    }

    #[test]
    fn postings_round_trip(rows in prop::collection::btree_set(any::<u32>(), 2..300), dense in 0u32..40_000) {
        let mut rows: Vec<u32> = rows.into_iter().collect();
        rows.extend((0..dense).map(|i| 1_000_000 + i)); // a run container next to sparse rows
        rows.sort_unstable();
        rows.dedup();
        let mut buf = Vec::new();
        encode_postings(&rows, &mut buf);
        let all = decode_postings(&buf, &|_| true).unwrap();
        prop_assert_eq!(all.iter().collect::<Vec<_>>(), rows.clone());
        let even = decode_postings(&buf, &|rg| rg % 2 == 0).unwrap();
        prop_assert_eq!(even.iter().collect::<Vec<_>>(), rows.iter().copied().filter(|r| (r >> 16) % 2 == 0).collect::<Vec<_>>());
        for cut in [0, 1, buf.len() / 2, buf.len() - 1] {
            prop_assert!(decode_postings(&buf[..cut], &|_| true).is_err());
        }
    }

    #[test]
    fn footer_round_trip(blocks in prop::collection::vec((prop::collection::vec(any::<u8>(), 0..40), any::<u32>(), any::<u32>()), 0..20), rows: u32) {
        let blocks: Vec<BlockMeta> = blocks.into_iter().enumerate().map(|(i, (k, n, crc))| BlockMeta {
            first_key: k.clone(), last_key: k, offset: 100 + i as u64 * 10, len: 10, num_terms: n, crc,
        }).collect();
        let f = Footer { num_rows: rows, cols: vec![0, 1, 2, 3, 4], postings: 4..100, dict: 100..100 + blocks.len() as u64 * 10, num_terms: 7, blocks };
        let mut buf = Vec::new();
        f.encode(&mut buf);
        prop_assert_eq!(Footer::decode(&buf, f.dict.end).unwrap(), f.clone());
        prop_assert!(Footer::decode(&buf[..buf.len() - 1], f.dict.end).is_err());
        prop_assert!(f.dict.end == 100 || Footer::decode(&buf, f.dict.end - 1).is_err());
    }
}

//! Brute-force evaluation of one FTS predicate on one cell, with exactly the index's semantics: the cell's keys
//! come from `Analyzer::for_each_key` (the builder's function), and a group matches when every leaf key is among
//! them. Phrases and concrete array indexes (which the index over-matches) are re-checked on the cell's nodes,
//! walked by the same `for_each_node` (same 1 MiB budget cut). Used for memtable rows, segments without a
//! sidecar, the `search` / `json_key_search` UDFs and the post-merge re-check.
use crate::analyzer::{Analyzer, Cell, Node, key_path, key_path_token, key_token, normalize_path};
use crate::query::{GroupSpec, Leaf, Query, parse};
use crate::{FtsColumn, FtsPredicate, IndexError, IndexResult};
use std::collections::HashMap;

/// Leaves per AND group (a u64 mask per group).
pub const MAX_GROUP_TERMS: usize = 64;

#[derive(Clone, Debug)]
pub struct Matcher {
    col: FtsColumn,
    /// What the index looks up (`FtsFile::execute`).
    query: Query,
    groups: Vec<GroupSpec>,
    /// Exact leaf key -> (group, leaf) bits.
    exact: HashMap<Vec<u8>, Vec<(usize, u64)>>,
    prefixes: Vec<(Vec<u8>, usize, u64)>,
    /// json_key_search: the `[*]` path and one step per `[*]` (Some(n) = concrete index). Empty text: `exists`.
    path: Option<(String, Vec<Option<u32>>)>,
    exists: bool,
    /// Per group: tokens that must occur in an ASCII cell lowercased (sound pre-filter, see `may_match`).
    must: Vec<Vec<String>>,
}

impl Matcher {
    pub fn new(an: &mut Analyzer, p: &FtsPredicate) -> IndexResult<Self> {
        let (col, groups, path, exists) = match p {
            FtsPredicate::Search { column, query } => (*column, parse(an, query), None, false),
            FtsPredicate::JsonKeySearch { column, path, text } => {
                if !column.is_json() {
                    return Err(IndexError::Query(format!(
                        "json_key_search on non-JSON column {}",
                        column.name()
                    )));
                }
                let (p, steps) = normalize_path(path).map_err(IndexError::Query)?;
                let exists = text.trim().is_empty();
                let groups = if exists {
                    vec![GroupSpec::default()]
                } else {
                    parse(an, text)
                };
                (*column, groups, Some((p, steps)), exists)
            }
        };
        if groups.iter().any(|g| g.terms.len() > MAX_GROUP_TERMS) {
            return Err(IndexError::Query(format!(
                "more than {MAX_GROUP_TERMS} terms in one AND group"
            )));
        }
        let key = |t: &str| match &path {
            None => key_token(col, t),
            Some((p, _)) => key_path_token(col, p, t),
        };
        let mut query = Query {
            groups: Vec::with_capacity(groups.len()),
            needs_recheck: groups.iter().any(|g| !g.phrases.is_empty())
                || path.as_ref().is_some_and(|(_, s)| s.iter().any(Option::is_some)),
        };
        let (mut exact, mut prefixes) = (HashMap::<Vec<u8>, Vec<(usize, u64)>>::new(), Vec::new());
        for (gi, g) in groups.iter().enumerate() {
            let leaves: Vec<Leaf> = if exists {
                vec![Leaf::Exact(key_path(col, &path.as_ref().expect("json_key_search").0))]
            } else {
                g.terms
                    .iter()
                    .map(|(t, prefix)| {
                        if *prefix {
                            Leaf::Prefix(key(t))
                        } else {
                            Leaf::Exact(key(t))
                        }
                    })
                    .collect()
            };
            for (li, l) in leaves.iter().enumerate() {
                match l {
                    Leaf::Exact(k) => exact.entry(k.clone()).or_default().push((gi, 1 << li)),
                    Leaf::Prefix(k) => prefixes.push((k.clone(), gi, 1 << li)),
                }
            }
            query.groups.push(leaves);
        }
        let must = groups
            .iter()
            .map(|g| {
                g.terms
                    .iter()
                    .filter(|(t, _)| t.is_ascii() && t.bytes().any(|b| !b.is_ascii_digit() && b != b'e'))
                    .map(|(t, _)| t.clone())
                    .collect()
            })
            .collect();
        Ok(Self {
            col,
            query,
            groups,
            exact,
            prefixes,
            path,
            exists,
            must,
        })
    }

    pub fn column(&self) -> FtsColumn {
        self.col
    }

    /// The index lookup for this predicate. `needs_recheck`: index hits are a superset of `matches`.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// The predicate on one non-null cell, exactly as the index answers it (plus the phrase / concrete-index
    /// re-check the index cannot do).
    pub fn matches(&self, an: &mut Analyzer, value: &str) -> bool {
        self.matches_inner(an, value, None).expect("no prefix counting")
    }

    /// `matches`, adding every distinct key a prefix leaf expands to into `seen`: more than `MAX_PREFIX_TERMS` in
    /// one source is the index's "prefix too broad" error (plans/phase-5 D13).
    pub fn matches_counting(&self, an: &mut Analyzer, value: &str, seen: &mut PrefixSeen) -> IndexResult<bool> {
        self.matches_inner(an, value, Some(seen))
    }

    fn matches_inner(&self, an: &mut Analyzer, value: &str, mut seen: Option<&mut PrefixSeen>) -> IndexResult<bool> {
        if self.query.groups.is_empty() || !self.may_match(value) {
            return Ok(false);
        }
        let full: Vec<u64> = self.query.groups.iter().map(|g| mask(g.len())).collect();
        let mut sat = vec![0u64; full.len()];
        an.for_each_key(self.col, value, &mut |k| {
            if let Some(hits) = self.exact.get(k) {
                for &(g, bit) in hits {
                    sat[g] |= bit;
                }
            }
            for (p, g, bit) in &self.prefixes {
                if k.starts_with(p) {
                    sat[*g] |= bit;
                    if let Some(s) = seen.as_deref_mut() {
                        s.add(k);
                    }
                }
            }
        });
        if seen.is_some_and(|s| s.too_broad()) {
            return Err(prefix_too_broad());
        }
        let live: Vec<usize> = (0..full.len()).filter(|&g| sat[g] == full[g]).collect();
        if live.is_empty() {
            return Ok(false);
        }
        if !self.query.needs_recheck {
            return Ok(true);
        }
        Ok(self.recheck(an, value, &live))
    }

    /// Sound pre-filter: in an ASCII cell without `\u` escapes, every token the analyzer emits is a substring of the
    /// lowercased cell, except tokens of re-formatted JSON numbers (`1E+21` -> `1e21`), which use only `0-9` and `e`
    /// (escapes decode to non-alphanumerics, lowercasing and folding are ASCII-only). So a group with a term holding
    /// any other character, missing from the cell, cannot match.
    fn may_match(&self, value: &str) -> bool {
        if !value.is_ascii() || value.contains("\\u") || self.must.iter().any(Vec::is_empty) {
            return true;
        }
        let lower = value.to_ascii_lowercase();
        self.must
            .iter()
            .any(|terms| terms.iter().all(|t| lower.contains(t.as_str())))
    }

    /// Re-checks the key-level survivors on the cell's token runs (one per leaf, or the text).
    fn recheck(&self, an: &mut Analyzer, value: &str, live: &[usize]) -> bool {
        let mut texts: Vec<String> = Vec::new();
        let mut exists = false;
        let wanted = |n: &Node<'_>| match &self.path {
            None => true,
            Some((p, steps)) => {
                n.path == p
                    && n.indices.len() == steps.len()
                    && steps.iter().zip(n.indices).all(|(s, i)| s.is_none_or(|s| s == *i))
            }
        };
        let cell = an.for_each_node(self.col, value, &mut |n, _| {
            if wanted(&n) {
                exists = true;
                if let Some(t) = n.text {
                    texts.push(t.to_string());
                }
            }
        });
        let runs: Vec<Vec<String>> = match cell {
            Cell::Skipped => return false,
            // A text cell has no paths: json_key_search never matches it (the index has no path keys for it).
            Cell::Text(_) if self.path.is_some() => return false,
            Cell::Text(t) => vec![an.tokens(t)],
            Cell::Json => texts.iter().map(|t| an.tokens(t)).collect(),
        };
        if self.exists {
            return exists;
        }
        live.iter().any(|&g| {
            let g = &self.groups[g];
            g.terms.iter().all(|(t, prefix)| {
                runs.iter()
                    .flatten()
                    .any(|x| if *prefix { x.starts_with(t.as_str()) } else { x == t })
            }) && g
                .phrases
                .iter()
                .all(|ph| runs.iter().any(|r| r.windows(ph.len()).any(|w| w == ph.as_slice())))
        })
    }
}

/// Distinct keys that prefix leaves expanded to in one source (a brute-forced segment, the memtable), capped.
#[derive(Debug, Default)]
pub struct PrefixSeen(std::collections::HashSet<Vec<u8>>);

impl PrefixSeen {
    fn add(&mut self, k: &[u8]) {
        if !self.too_broad() && !self.0.contains(k) {
            self.0.insert(k.to_vec());
        }
    }
    fn too_broad(&self) -> bool {
        self.0.len() > crate::read::MAX_PREFIX_TERMS
    }
}

/// The same error for an indexed segment and a brute-forced source.
pub fn prefix_too_broad() -> IndexError {
    IndexError::Query(format!(
        "prefix too broad: it matches more than {} terms",
        crate::read::MAX_PREFIX_TERMS
    ))
}

fn mask(n: usize) -> u64 {
    if n >= 64 { u64::MAX } else { (1u64 << n) - 1 }
}

/// `json_key(cell, '$.path')`: the value at a concrete path (`[*]` is not allowed) — a string as itself, any other
/// JSON value as JSON text, null / missing / not JSON as None.
pub fn json_key(value: &str, path: &str) -> IndexResult<Option<String>> {
    let steps = parse_concrete(path)?;
    let Ok(v) = serde_json::from_str::<serde_json::Value>(value) else {
        return Ok(None);
    };
    let mut cur = &v;
    for s in &steps {
        let next = match s {
            Step::Key(k) => cur.get(k.as_str()),
            Step::Index(i) => cur.get(*i),
        };
        let Some(n) = next else { return Ok(None) };
        cur = n;
    }
    Ok(match cur {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    })
}

enum Step {
    Key(String),
    Index(usize),
}

/// `$.a["b.c"][0]` -> steps. Shares `normalize_path`'s grammar: it validates, then the keys are read back from
/// the normalized form (identifier keys as `.k`, the rest as `["json"]`).
fn parse_concrete(path: &str) -> IndexResult<Vec<Step>> {
    let (norm, steps) = normalize_path(path).map_err(IndexError::Query)?;
    if steps.iter().any(Option::is_none) {
        return Err(IndexError::Query(format!("json_key needs a concrete path, got {path}")));
    }
    let (b, mut i, mut idx, mut out) = (norm.as_bytes(), 1, steps.into_iter().flatten(), Vec::new());
    while i < b.len() {
        if b[i] == b'.' {
            let st = i + 1;
            i = st;
            while i < b.len() && b[i] != b'.' && b[i] != b'[' {
                i += 1;
            }
            out.push(Step::Key(norm[st..i].to_string()));
        } else if norm[i..].starts_with("[*]") {
            out.push(Step::Index(idx.next().expect("one step per [*]") as usize));
            i += 3;
        } else {
            // `["json"]`: the string runs to the first `"]` that is not escaped.
            let mut de = serde_json::Deserializer::from_str(&norm[i + 1..]).into_iter::<String>();
            let key = de
                .next()
                .and_then(Result::ok)
                .ok_or_else(|| IndexError::Query(format!("bad path {path}")))?;
            i += 1 + de.byte_offset() + 1;
            out.push(Step::Key(key));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use FtsColumn::*;

    fn m(p: FtsPredicate) -> Matcher {
        Matcher::new(&mut Analyzer::default(), &p).unwrap()
    }
    fn s(col: FtsColumn, q: &str) -> Matcher {
        m(FtsPredicate::Search {
            column: col,
            query: q.into(),
        })
    }
    fn j(col: FtsColumn, path: &str, text: &str) -> Matcher {
        m(FtsPredicate::JsonKeySearch {
            column: col,
            path: path.into(),
            text: text.into(),
        })
    }

    #[test]
    fn search_semantics() {
        let an = &mut Analyzer::default();
        let doc = r#"{"messages":[{"role":"user","content":"Refund my CAFÉ order"},{"role":"assistant","content":"policy: refunds take 5 days"}]}"#;
        assert!(s(Inputs, "refund").matches(an, doc));
        assert!(s(Inputs, "cafe ORDER").matches(an, doc));
        assert!(s(Inputs, "refun*").matches(an, doc));
        assert!(!s(Inputs, "refund zzz").matches(an, doc));
        assert!(s(Inputs, "refund zzz OR policy").matches(an, doc));
        assert!(!s(Inputs, "messages").matches(an, doc), "object keys are not tokens");
        assert!(s(Inputs, "\"cafe order\"").matches(an, doc));
        assert!(
            !s(Inputs, "\"order policy\"").matches(an, doc),
            "a phrase never spans two leaves"
        );
        assert!(!s(Inputs, "\"order cafe\"").matches(an, doc));
        assert!(s(Inputs, "5").matches(an, doc));
        assert!(s(Name, "chat*").matches(an, "ChatOpenAI"));
        assert!(!s(Name, "").matches(an, "anything"), "no tokens: matches nothing");
        let blob = forge_model::blob::blob_ref("blobs/p/abc", 9);
        assert!(!s(Inputs, "blobs").matches(an, &blob));
        // \u escapes decode to letters: the pre-filter must not reject them.
        assert!(s(Inputs, "cafe").matches(an, r#"{"a":"caf\u00e9"}"#));
        assert!(s(Inputs, "abc").matches(an, r#"{"a":"\u0041bc"}"#));
        // Numbers are normalized by serde_json (1e2 -> 100.0).
        assert!(s(Inputs, "100").matches(an, r#"{"n":1e2}"#));
        // A re-formatted number's tokens need not be substrings of the cell (the pre-filter must let them through).
        for t in an.tokens(&serde_json::json!(1e21).to_string()) {
            assert!(s(Inputs, &t).matches(an, r#"{"n":1E21}"#), "{t}");
        }
        assert!(s(Inputs, "refund0").matches(an, r#"{"q":"REFUND0"}"#));
        assert!(!s(Inputs, "refund0").matches(an, r#"{"q":"refund1"}"#));
    }

    #[test]
    fn json_key_search_semantics() {
        let an = &mut Analyzer::default();
        let doc = r#"{"messages":[{"role":"system","content":"be nice"},{"role":"user","content":"refund please"}],"user.id":"U1"}"#;
        assert!(j(Inputs, "$.messages[*].content", "refund").matches(an, doc));
        assert!(j(Inputs, "$.messages[1].content", "refund").matches(an, doc));
        assert!(
            !j(Inputs, "$.messages[0].content", "refund").matches(an, doc),
            "concrete index re-check"
        );
        assert!(j(Inputs, "$.messages[0].role", "system").matches(an, doc));
        assert!(!j(Inputs, "$.messages[*].role", "refund").matches(an, doc));
        assert!(j(Inputs, "$['user.id']", "u1").matches(an, doc));
        assert!(j(Inputs, "$.messages", "").matches(an, doc), "empty text = path exists");
        assert!(j(Inputs, "$.messages[1]", "").matches(an, doc));
        assert!(!j(Inputs, "$.messages[2]", "").matches(an, doc));
        assert!(!j(Inputs, "$.nope", "").matches(an, doc));
        assert!(!j(Inputs, "$", "refund").matches(an, "refund (not json)"));
        assert!(
            Matcher::new(
                an,
                &FtsPredicate::JsonKeySearch {
                    column: Name,
                    path: "$".into(),
                    text: "x".into()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn json_key_paths() {
        let doc = r#"{"a":{"b.c":[1,{"d":"x"}]},"s":"str","n":null}"#;
        assert_eq!(json_key(doc, "$.a[\"b.c\"][1].d").unwrap().as_deref(), Some("x"));
        assert_eq!(json_key(doc, "$.a['b.c'][0]").unwrap().as_deref(), Some("1"));
        assert_eq!(
            json_key(doc, "$.a").unwrap().as_deref(),
            Some(r#"{"b.c":[1,{"d":"x"}]}"#)
        );
        assert_eq!(json_key(doc, "$.s").unwrap().as_deref(), Some("str"));
        assert_eq!(json_key(doc, "$.n").unwrap(), None);
        assert_eq!(json_key(doc, "$.zz").unwrap(), None);
        assert_eq!(json_key("not json", "$.a").unwrap(), None);
        assert!(json_key(doc, "$.a[*]").is_err());
        assert!(json_key(doc, "a").is_err());
    }

    /// E1: the index and the brute-force matcher agree. The sidecar is built through `FtsBuilder::add_batch` on
    /// spans_schema batches with several FTS columns per row (the path the segment sink uses).
    mod agreement {
        use super::*;
        use crate::FtsBuilder;
        use crate::analyzer::MAX_FIELD_BYTES;
        use crate::read::FtsFile;
        use bytes::Bytes;
        use forge_model::ids::{SpanId, TraceId};
        use forge_model::row::SpanRow;
        use forge_model::testing::{batch_from_rows, pid};
        use proptest::prelude::*;
        use roaring::RoaringBitmap;
        use serde_json::{Value, json};

        fn words() -> Vec<String> {
            let mut w: Vec<String> = [
                "refund",
                "Order",
                "café",
                "the",
                "42",
                "x9",
                "TIMEOUT",
                "user_id",
                "é",
                "zz",
                "日本語テキスト",
                "ß",
                "Æ",
                "CAFÉ",
                "Straße",
            ]
            .map(str::to_string)
            .to_vec();
            // RemoveLong keeps tokens of < 256 bytes.
            w.push("a".repeat(255));
            w.push("b".repeat(256));
            w
        }
        fn text() -> impl Strategy<Value = String> {
            prop::collection::vec(prop::sample::select(words()), 0..6).prop_map(|w| w.join(" "))
        }
        fn key() -> impl Strategy<Value = String> {
            let mut keys: Vec<String> = [
                "messages", "content", "role", "user.id", "a\"b", "x[0]", "", "0", "ключ", "a.b", "\0",
            ]
            .map(str::to_string)
            .to_vec();
            keys.push("k".repeat(600)); // a path over MAX_PATH_BYTES
            prop::sample::select(keys)
        }
        fn json_value() -> impl Strategy<Value = Value> {
            let leaf = prop_oneof![
                text().prop_map(Value::String),
                (0i64..100).prop_map(|n| json!(n)),
                Just(json!(1e21)),
                any::<bool>().prop_map(Value::Bool),
                Just(Value::Null),
            ];
            leaf.prop_recursive(3, 16, 3, move |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..3).prop_map(Value::Array),
                    prop::collection::btree_map(key(), inner, 0..3)
                        .prop_map(|m| Value::Object(m.into_iter().collect())),
                ]
            })
        }
        fn cell() -> impl Strategy<Value = Option<String>> {
            prop::option::weighted(
                0.7,
                prop_oneof![
                    4 => text(),
                    4 => json_value().prop_map(|v| v.to_string()),
                    1 => Just(forge_model::blob::blob_ref("blobs/p/abc", 9)),
                    // `\u` escapes (the `may_match` guard) and an escaped key.
                    1 => Just(r#"{"content":"caf\u00e9 REFUND","a\"b":"\u65e5\u672c"}"#.to_string()),
                ],
            )
        }
        /// A cell of MAX_FIELD_BYTES - 4 + d bytes plus `marker`: the 1 MiB cut lands in or around the marker, which
        /// starts with a multi-byte character.
        fn big_cell(d: usize, json: bool) -> String {
            let len = MAX_FIELD_BYTES - 4 + d;
            let mut s = "the ".repeat(len / 4);
            s.push_str(&" ".repeat(len - s.len()));
            s.push_str("écaférefund order");
            if json { json!({ "content": s }).to_string() } else { s }
        }
        fn query() -> impl Strategy<Value = String> {
            let word = prop_oneof![
                prop::sample::select(words()),
                prop::sample::select(
                    &[
                        "ref*",
                        "or*",
                        "c*",
                        "\"refund order\"",
                        "\"the zz\"",
                        "OR",
                        "4*",
                        "CAFÉ*",
                        "caf*",
                        "日本*",
                        "ecaferefund",
                        "ecaf*",
                        "ss",
                        "ae",
                    ][..]
                )
                .prop_map(str::to_string),
            ];
            prop_oneof![
                prop::collection::vec(word, 1..4).prop_map(|w| w.join(" ")),
                Just("OR".to_string()),
            ]
        }
        fn path() -> impl Strategy<Value = String> {
            prop::sample::select(
                &[
                    "$",
                    "$.content",
                    "$.messages",
                    "$.messages[*]",
                    "$.messages[0]",
                    "$.messages[1].content",
                    "$.messages[*].content",
                    "$['user.id']",
                    "$.role[0]",
                    "$.messages[*][*]",
                    r#"$["a\"b"]"#,
                    "$['x[0]']",
                    "$['']",
                    "$['0']",
                    "$['ключ']",
                    "$['a.b']",
                    r#"$["\u0000"]"#,
                ][..],
            )
            .prop_map(str::to_string)
        }
        fn pred() -> impl Strategy<Value = FtsPredicate> {
            let col = prop::sample::select(&FtsColumn::ALL[..]);
            let json_col = prop::sample::select(&[Inputs, Outputs, Metadata][..]);
            prop_oneof![
                (col, query()).prop_map(|(column, query)| FtsPredicate::Search { column, query }),
                (json_col, path(), prop_oneof![query(), Just(String::new())])
                    .prop_map(|(column, path, text)| FtsPredicate::JsonKeySearch { column, path, text }),
            ]
        }
        /// Per row: the 5 FTS cells and a null start.
        type Row = ([Option<String>; 5], bool);
        fn rows() -> impl Strategy<Value = Vec<Row>> {
            let row = ([cell(), cell(), cell(), cell(), cell()], prop::bool::weighted(0.2));
            // ~5% of cases: one cell at the 1 MiB cut (row, column, offset, json).
            let big = prop::option::weighted(0.05, (0usize..40, 0usize..5, 0usize..8, any::<bool>()));
            (prop::collection::vec(row, 1..40), big).prop_map(|(mut rows, big)| {
                if let Some((r, c, d, json)) = big {
                    let n = rows.len();
                    rows[r % n].0[c] = Some(big_cell(d, json));
                }
                rows
            })
        }
        fn batches(rows: &[Row]) -> Vec<arrow::record_batch::RecordBatch> {
            let span_rows: Vec<SpanRow> = rows
                .iter()
                .enumerate()
                .map(|(i, (c, null_start))| SpanRow {
                    project_id: Some(pid("p")),
                    trace_id: TraceId([1; 16]),
                    span_id: SpanId((i as u64 + 1).to_be_bytes()),
                    name: c[0].clone(),
                    error: c[1].clone(),
                    inputs: c[2].clone(),
                    outputs: c[3].clone(),
                    metadata: c[4].clone(),
                    start_time_ns: (!null_start).then_some(i as i64),
                    version: 1,
                    ..Default::default()
                })
                .collect();
            // Two batches when possible: row ids continue across `add_batch` calls.
            let cut = span_rows.len() / 2;
            [&span_rows[..cut], &span_rows[cut..]]
                .into_iter()
                .filter(|r| !r.is_empty())
                .map(batch_from_rows)
                .collect()
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// The index and the brute-force matcher agree on every row: equal when the query needs no re-check,
            /// index ⊇ matcher otherwise (phrases, concrete indexes) — the property the query path relies on.
            #[test]
            fn index_agrees_with_matcher(rows in rows(), preds in prop::collection::vec(pred(), 1..8)) {
                let mut b = FtsBuilder::default().with_block_budget(64);
                for batch in batches(&rows) {
                    b.add_batch(&batch).unwrap();
                }
                let file = Bytes::from(b.finish().unwrap().0);
                let fetch = |rs: Vec<std::ops::Range<u64>>| {
                    std::future::ready(Ok::<_, IndexError>(rs.into_iter().map(|r| file.slice(r.start as usize..r.end as usize)).collect()))
                };
                let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
                let f = rt.block_on(FtsFile::open("t.fts", file.len() as u64, &fetch)).unwrap().0;
                let nulls: RoaringBitmap = rows.iter().enumerate().filter(|(_, r)| r.1).map(|(i, _)| i as u32).collect();
                prop_assert_eq!(f.null_starts(), &nulls);
                prop_assert_eq!(f.footer().num_rows as usize, rows.len());
                let an = &mut Analyzer::default();
                for p in &preds {
                    let m = Matcher::new(an, p).unwrap();
                    let col = m.column() as usize;
                    let brute: RoaringBitmap = rows
                        .iter()
                        .enumerate()
                        .filter(|(_, (c, _))| c[col].as_ref().is_some_and(|v| m.matches(an, v)))
                        .map(|(r, _)| r as u32)
                        .collect();
                    let (hits, _) = rt.block_on(f.execute(m.query(), &fetch)).unwrap();
                    if m.query().needs_recheck {
                        prop_assert!(brute.is_subset(&hits), "{p:?}: brute {brute:?} not in index {hits:?}");
                    } else {
                        prop_assert_eq!(&hits, &brute, "{:?}", p);
                    }
                }
            }
        }
    }
}

//! What gets indexed (research/fts.md §3 "Keys"): the tokenizer, JSON flattening and the FST keys. The `.fts`
//! builder and the memtable brute-force search both go through `Analyzer::for_each_key`, so they agree on every
//! term.
use crate::FtsColumn;
use crate::format::{KIND_PATH, KIND_PATH_TOKEN, KIND_TOKEN, PATH_SEP};
use serde_json::Value;
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, TokenStream,
};

/// Indexed text per field per span (spec). JSON: shared by all leaves of the document.
pub const MAX_FIELD_BYTES: usize = 1 << 20;
/// Tokens of this many bytes or more are dropped (tantivy keeps `len < limit`).
pub const MAX_TOKEN_BYTES: usize = 256;
/// Leaves under a longer path are indexed as plain tokens only.
pub const MAX_PATH_BYTES: usize = 512;

/// Runs of `char::is_alphanumeric` -> drop long tokens -> lowercase -> ASCII folding (master decision 6).
pub fn text_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build()
}

/// Reusable tokenizer state plus scratch buffers (one per builder / per scan thread).
#[derive(Clone)]
pub struct Analyzer {
    an: TextAnalyzer,
    path: String,
    key: Vec<u8>,
}

impl Default for Analyzer {
    fn default() -> Self {
        Self {
            an: text_analyzer(),
            path: String::new(),
            key: Vec::with_capacity(128),
        }
    }
}

impl Analyzer {
    /// The analyzed tokens of `text` (query words, phrase checks).
    pub fn tokens(&mut self, text: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut s = self.an.token_stream(text);
        while s.advance() {
            out.push(s.token().text.clone());
        }
        out
    }

    /// Calls `f` with every FST key of one cell (duplicates included; the caller dedupes per row). JSON columns
    /// get plain tokens, `path\0token` keys and path-existence keys; a JSON column whose value does not parse is
    /// indexed as text. Blob references are skipped (Phase 2 L8: blob-backed payloads are not searchable in v1).
    pub fn for_each_key(&mut self, col: FtsColumn, value: &str, f: &mut dyn FnMut(&[u8])) {
        let c = col as u8;
        let mut key = std::mem::take(&mut self.key);
        let cell = self.for_each_node(col, value, &mut |n, an| {
            let qualify = n.path.len() <= MAX_PATH_BYTES;
            if qualify {
                set_key(&mut key, c, KIND_PATH, &[n.path.as_bytes()]);
                f(&key);
            }
            let Some(text) = n.text else { return };
            let mut s = an.token_stream(text);
            while s.advance() {
                let t = s.token().text.as_bytes();
                set_key(&mut key, c, KIND_TOKEN, &[t]);
                f(&key);
                if qualify {
                    set_key(&mut key, c, KIND_PATH_TOKEN, &[n.path.as_bytes(), &[PATH_SEP], t]);
                    f(&key);
                }
            }
        });
        if let Cell::Text(text) = cell {
            let mut s = self.an.token_stream(text);
            while s.advance() {
                set_key(&mut key, c, KIND_TOKEN, &[s.token().text.as_bytes()]);
                f(&key);
            }
        }
        self.key = key;
    }

    /// How one cell is indexed. For a JSON cell, `f` gets every node in index order with the leaf text already
    /// cut by the shared 1 MiB budget (the builder, `for_each_key` and the brute-force re-check all walk cells
    /// through here, so they cut at the same byte).
    pub fn for_each_node<'v>(
        &mut self,
        col: FtsColumn,
        value: &'v str,
        f: &mut dyn FnMut(Node<'_>, &mut TextAnalyzer),
    ) -> Cell<'v> {
        if forge_model::blob::parse_blob_ref(value).is_some() {
            return Cell::Skipped;
        }
        if col.is_json()
            && let Ok(v) = serde_json::from_str::<Value>(value)
        {
            let Self { an, path, .. } = self;
            path.clear();
            path.push('$');
            let mut idx = Vec::new();
            let mut budget = MAX_FIELD_BYTES;
            walk(&v, path, &mut idx, &mut |p, indices, text| {
                let text = text.map(|t| {
                    let t = truncate(t, budget);
                    budget -= t.len();
                    t
                });
                f(Node { path: p, indices, text }, an);
            });
            return Cell::Json;
        }
        Cell::Text(truncate(value, MAX_FIELD_BYTES))
    }
}

/// One JSON node as indexed: the `[*]` path, the concrete array index of each `[*]` (outermost first), and for
/// scalar leaves the text after the field budget (None for containers below the root and nulls).
#[derive(Clone, Copy, Debug)]
pub struct Node<'a> {
    pub path: &'a str,
    pub indices: &'a [u32],
    pub text: Option<&'a str>,
}

/// What `for_each_node` did with a cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cell<'v> {
    /// A blob reference: nothing is indexed.
    Skipped,
    /// Walked as JSON (the nodes went to `f`).
    Json,
    /// Indexed as plain text: this prefix of the value.
    Text(&'v str),
}

fn set_key(key: &mut Vec<u8>, col: u8, kind: u8, parts: &[&[u8]]) {
    key.clear();
    key.extend_from_slice(&[col, kind]);
    for p in parts {
        key.extend_from_slice(p);
    }
}

pub fn key_token(col: FtsColumn, token: &str) -> Vec<u8> {
    let mut k = Vec::new();
    set_key(&mut k, col as u8, KIND_TOKEN, &[token.as_bytes()]);
    k
}
pub fn key_path_token(col: FtsColumn, path: &str, token: &str) -> Vec<u8> {
    let mut k = Vec::new();
    set_key(
        &mut k,
        col as u8,
        KIND_PATH_TOKEN,
        &[path.as_bytes(), &[PATH_SEP], token.as_bytes()],
    );
    k
}
pub fn key_path(col: FtsColumn, path: &str) -> Vec<u8> {
    let mut k = Vec::new();
    set_key(&mut k, col as u8, KIND_PATH, &[path.as_bytes()]);
    k
}

/// The longest prefix of `s` of at most `max` bytes, cut at a char boundary.
pub fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut i = max;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    &s[..i]
}

/// `.key` for identifier keys (`[A-Za-z_][A-Za-z0-9_]*`), `["json-escaped"]` otherwise, so `.`, `[`, spaces and
/// NUL stay unambiguous and a path never contains the 0x00 separator.
fn push_key(path: &mut String, k: &str) {
    let ident = !k.is_empty()
        && k.bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()));
    if ident {
        path.push('.');
        path.push_str(k);
    } else {
        path.push('[');
        path.push_str(&serde_json::to_string(k).expect("string"));
        path.push(']');
    }
}

/// `f(path, indices, Some(text))` for scalar leaves, `f(path, indices, None)` for containers below the root and
/// nulls. Arrays are `[*]` at every level; `indices` holds the concrete position of each. Object keys are not text
/// tokens (path existence covers them).
fn walk(v: &Value, path: &mut String, idx: &mut Vec<u32>, f: &mut dyn FnMut(&str, &[u32], Option<&str>)) {
    match v {
        Value::Object(m) => {
            if path.len() > 1 {
                f(path, idx, None)
            }
            for (k, c) in m {
                let l = path.len();
                push_key(path, k);
                walk(c, path, idx, f);
                path.truncate(l);
            }
        }
        Value::Array(a) => {
            if path.len() > 1 {
                f(path, idx, None)
            }
            let l = path.len();
            path.push_str("[*]");
            for (i, c) in a.iter().enumerate() {
                idx.push(u32::try_from(i).unwrap_or(u32::MAX));
                walk(c, path, idx, f);
                idx.pop();
            }
            path.truncate(l);
        }
        Value::String(s) => f(path, idx, Some(s)),
        Value::Number(n) => f(path, idx, Some(&n.to_string())),
        Value::Bool(b) => f(path, idx, Some(if *b { "true" } else { "false" })),
        Value::Null => f(path, idx, None),
    }
}

/// A user path -> the indexed path: `$.a[3].b` -> `$.a[*].b`, `$['x.y']` / `$["x.y"]` -> `$["x.y"]`.
/// The second value has one entry per array step: `None` for `[*]`, `Some(n)` for a concrete `[n]` (the index
/// then over-matches: re-check the rows).
pub fn normalize_path(p: &str) -> Result<(String, Vec<Option<u32>>), String> {
    let b = p.as_bytes();
    if b.first() != Some(&b'$') {
        return Err(format!("path must start with $: {p}"));
    }
    let (mut out, mut i, mut steps) = (String::from("$"), 1, Vec::new());
    while i < b.len() {
        match b[i] {
            b'.' => {
                i += 1;
                let st = i;
                while i < b.len() && b[i] != b'.' && b[i] != b'[' {
                    i += 1;
                }
                if st == i {
                    return Err(format!("empty key in {p}"));
                }
                push_key(&mut out, &p[st..i]);
            }
            b'[' => {
                i += 1;
                let q = *b.get(i).ok_or("unterminated [")?;
                if q == b'"' || q == b'\'' {
                    let st = i;
                    i += 1;
                    while i < b.len() && b[i] != q {
                        i += if b[i] == b'\\' { 2 } else { 1 };
                    }
                    if i >= b.len() {
                        return Err("unterminated quote".into());
                    }
                    let key = if q == b'"' {
                        serde_json::from_str::<String>(&p[st..=i]).map_err(|e| e.to_string())?
                    } else {
                        p[st + 1..i].to_string()
                    };
                    push_key(&mut out, &key);
                    i += 1;
                } else {
                    let st = i;
                    while i < b.len() && b[i] != b']' {
                        i += 1;
                    }
                    let inner = &p[st..i];
                    if inner == "*" {
                        steps.push(None);
                    } else {
                        let n = inner
                            .bytes()
                            .all(|c| c.is_ascii_digit())
                            .then(|| inner.parse::<u32>().ok())
                            .flatten()
                            .ok_or_else(|| format!("bad index [{inner}]"))?;
                        steps.push(Some(n));
                    }
                    out.push_str("[*]");
                }
                if b.get(i) != Some(&b']') {
                    return Err("expected ]".into());
                }
                i += 1;
            }
            _ => return Err(format!("unexpected {:?} in {p}", b[i] as char)),
        }
    }
    Ok((out, steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FtsColumn::{Inputs, Metadata, Name};

    fn keys(col: FtsColumn, v: &str) -> Vec<String> {
        let mut out = Vec::new();
        Analyzer::default().for_each_key(col, v, &mut |k| {
            let body = String::from_utf8_lossy(&k[2..]).replace('\0', ":");
            out.push(format!("{}{body}", ["", "t ", "pt ", "p "][k[1] as usize]));
        });
        out
    }

    #[test]
    fn tokenizer() {
        let mut a = Analyzer::default();
        assert_eq!(a.tokens("Héllo naïve CAFÉ"), ["hello", "naive", "cafe"]);
        assert_eq!(
            a.tokens("user_id=42; https://example.com/a"),
            ["user", "id", "42", "https", "example", "com", "a"]
        );
        assert_eq!(
            a.tokens("can't 3.14 Straße ÆON"),
            ["can", "t", "3", "14", "strasse", "aeon"]
        );
        assert_eq!(a.tokens("日本語のテキスト 中文"), ["日本語のテキスト", "中文"]); // known gap: CJK runs are one token
        assert_eq!(a.tokens(&format!("a {} b", "x".repeat(256))), ["a", "b"]);
        assert_eq!(a.tokens(&format!("{} b", "x".repeat(255))).len(), 2);
    }

    #[test]
    fn json_flattening() {
        let v = r#"{"messages":[{"role":"user","content":"Refund please"}],"user.id":"u1","n":5,"ok":true,"nested":[[1]],"z":null}"#;
        let mut got = keys(Inputs, v);
        got.sort();
        let mut want = vec![
            "p $.messages",
            "p $.messages[*]",
            "p $.messages[*].role",
            "t user",
            "pt $.messages[*].role:user",
            "p $.messages[*].content",
            "t refund",
            "pt $.messages[*].content:refund",
            "t please",
            "pt $.messages[*].content:please",
            "p $[\"user.id\"]",
            "t u1",
            "pt $[\"user.id\"]:u1",
            "p $.n",
            "t 5",
            "pt $.n:5",
            "p $.ok",
            "t true",
            "pt $.ok:true",
            "p $.nested",
            "p $.nested[*]",
            "p $.nested[*][*]",
            "t 1",
            "pt $.nested[*][*]:1",
            "p $.z",
        ];
        want.sort();
        assert_eq!(got, want); // order is irrelevant (serde_json maps iterate sorted)
        // A root scalar is the path `$`; a non-JSON value in a JSON column is plain text; text columns never parse.
        assert_eq!(
            keys(Inputs, r#""Hi there""#),
            ["p $", "t hi", "pt $:hi", "t there", "pt $:there"]
        );
        assert_eq!(keys(Metadata, "not json {"), ["t not", "t json"]);
        assert_eq!(keys(Name, r#"{"a":"b"}"#), ["t a", "t b"]);
    }

    #[test]
    fn blob_refs_are_skipped() {
        let r = forge_model::blob::blob_ref("blobs/p/abc", 5_000_000);
        assert!(keys(Inputs, &r).is_empty());
        assert!(keys(Name, &r).is_empty());
    }

    #[test]
    fn field_cap_is_shared_by_json_leaves() {
        let big = "ab ".repeat(MAX_FIELD_BYTES / 3 + 10);
        let v = serde_json::json!({"a": big, "b": "tail"}).to_string();
        let ks = keys(Inputs, &v);
        assert!(
            ks.contains(&"p $.b".to_string()),
            "paths are still recorded past the cap"
        );
        assert!(
            !ks.contains(&"t tail".to_string()),
            "leaf text past 1 MiB is not indexed"
        );
        let text = format!("{}zz", "a ".repeat(MAX_FIELD_BYTES / 2));
        assert!(!keys(Name, &text).contains(&"t zz".to_string()));
    }

    #[test]
    fn paths() {
        for (inp, want, concrete) in [
            ("$.messages[0].content", "$.messages[*].content", true),
            ("$.messages[*].content", "$.messages[*].content", false),
            ("$['user.id']", "$[\"user.id\"]", false),
            ("$[\"user.id\"]", "$[\"user.id\"]", false),
            ("$.a.b2._c", "$.a.b2._c", false),
            ("$", "$", false),
        ] {
            let (got, steps) = normalize_path(inp).unwrap();
            assert_eq!(
                (got.as_str(), steps.iter().any(Option::is_some)),
                (want, concrete),
                "{inp}"
            );
        }
        for bad in ["messages", "$.a[x]", "$..a", "$.a[", "$['a"] {
            assert!(normalize_path(bad).is_err(), "{bad}");
        }
    }
}

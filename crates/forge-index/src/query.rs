//! Query strings -> index leaves (research/fts.md §3 "Query string semantics"). Whitespace-separated words are
//! AND; an uppercase `OR` separates AND groups; `word*` is a prefix on the word's last token; `"a b"` is AND(a, b)
//! plus a re-check of the fetched rows (no positions in v1). A word the tokenizer splits (`foo-bar`, `user_id`)
//! becomes AND of its tokens.
use crate::analyzer::{Analyzer, key_path, key_path_token, key_token, normalize_path};
use crate::{FtsColumn, IndexError, IndexResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Leaf {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
}

/// OR of AND groups of leaves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Query {
    pub groups: Vec<Vec<Leaf>>,
    /// The rows are a superset (phrases, a concrete array index): re-check them.
    pub needs_recheck: bool,
}

enum Item {
    Word(String),
    Phrase(String),
    Or,
}

fn lex(q: &str) -> Vec<Item> {
    let (mut out, mut cur, mut chars) = (Vec::new(), String::new(), q.chars());
    let flush = |cur: &mut String, out: &mut Vec<Item>| {
        if !cur.is_empty() {
            let w = std::mem::take(cur);
            out.push(if w == "OR" { Item::Or } else { Item::Word(w) });
        }
    };
    while let Some(ch) = chars.next() {
        if ch == '"' {
            flush(&mut cur, &mut out);
            out.push(Item::Phrase(chars.by_ref().take_while(|&c| c != '"').collect()));
        } else if ch.is_whitespace() {
            flush(&mut cur, &mut out);
        } else {
            cur.push(ch);
        }
    }
    flush(&mut cur, &mut out);
    out
}

/// One AND group as written: its analyzed terms (`true` = prefix) and its phrases of 2+ tokens (re-checked).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GroupSpec {
    pub terms: Vec<(String, bool)>,
    pub phrases: Vec<Vec<String>>,
}

/// The OR of AND groups of `q`; groups without terms are dropped (a query of no tokens has no groups).
pub fn parse(an: &mut Analyzer, q: &str) -> Vec<GroupSpec> {
    let mut groups = Vec::new();
    let mut g = GroupSpec::default();
    for it in lex(q) {
        match it {
            Item::Or => {
                if !g.terms.is_empty() {
                    groups.push(std::mem::take(&mut g))
                }
            }
            Item::Word(w) => {
                let (w, prefix) = match w.strip_suffix('*') {
                    Some(s) => (s, true),
                    None => (w.as_str(), false),
                };
                let ts = an.tokens(w);
                let n = ts.len();
                g.terms
                    .extend(ts.into_iter().enumerate().map(|(i, t)| (t, prefix && i + 1 == n)));
            }
            Item::Phrase(p) => {
                let ts = an.tokens(&p);
                if ts.len() > 1 {
                    g.phrases.push(ts.clone());
                }
                g.terms.extend(ts.into_iter().map(|t| (t, false)));
            }
        }
    }
    if !g.terms.is_empty() {
        groups.push(g);
    }
    groups
}

fn compile(an: &mut Analyzer, q: &str, mk: &dyn Fn(&str) -> Vec<u8>) -> Query {
    let groups = parse(an, q);
    Query {
        needs_recheck: groups.iter().any(|g| !g.phrases.is_empty()),
        groups: groups
            .iter()
            .map(|g| {
                g.terms
                    .iter()
                    .map(|(t, prefix)| {
                        if *prefix {
                            Leaf::Prefix(mk(t))
                        } else {
                            Leaf::Exact(mk(t))
                        }
                    })
                    .collect()
            })
            .collect(),
    }
}

/// `search(col, q)`. A query with no tokens has no groups (matches nothing).
pub fn search_query(an: &mut Analyzer, col: FtsColumn, q: &str) -> Query {
    compile(an, q, &|t| key_token(col, t))
}

/// `json_key_search(col, '$.path', text)`; empty text = the path exists.
pub fn json_key_search_query(an: &mut Analyzer, col: FtsColumn, path: &str, text: &str) -> IndexResult<Query> {
    if !col.is_json() {
        return Err(IndexError::Query(format!(
            "json_key_search on non-JSON column {}",
            col.name()
        )));
    }
    let (p, steps) = normalize_path(path).map_err(IndexError::Query)?;
    let concrete = steps.iter().any(Option::is_some);
    let mut q = if text.trim().is_empty() {
        Query {
            groups: vec![vec![Leaf::Exact(key_path(col, &p))]],
            needs_recheck: false,
        }
    } else {
        compile(an, text, &|t| key_path_token(col, &p, t))
    };
    q.needs_recheck |= concrete;
    Ok(q)
}

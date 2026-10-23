//! OTLP attribute access: a key -> AnyValue map with typed, coercing getters and "take" (consume) helpers.
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use serde_json::{Map, Value as J};
use std::collections::HashMap;

/// AnyValue -> JSON, by value (moves strings; no copy of large payloads).
pub fn any_to_json(v: AnyValue) -> J {
    match v.value {
        None | Some(Value::StringValueStrindex(_)) => J::Null,
        Some(Value::StringValue(s)) => J::String(s),
        Some(Value::BoolValue(b)) => J::Bool(b),
        Some(Value::IntValue(i)) => J::from(i),
        // NaN / ±inf have no JSON form.
        Some(Value::DoubleValue(d)) => serde_json::Number::from_f64(d).map_or(J::Null, J::Number),
        Some(Value::BytesValue(b)) => J::String(hex::encode(b)),
        Some(Value::ArrayValue(a)) => J::Array(a.values.into_iter().map(any_to_json).collect()),
        Some(Value::KvlistValue(kv)) => J::Object(kvs_to_json(kv.values)),
    }
}

pub fn kvs_to_json(kvs: Vec<KeyValue>) -> Map<String, J> {
    kvs.into_iter()
        .map(|kv| (kv.key, kv.value.map_or(J::Null, any_to_json)))
        .collect()
}

/// A string attribute that holds JSON (`gen_ai.input.messages`, `traceloop.entity.input`, ...):
/// parse it when it looks like a JSON object/array/string, else keep the text. Non-string AnyValues
/// (arrayValue / kvlistValue senders) convert structurally.
pub fn json_payload(v: AnyValue) -> J {
    match v.value {
        Some(Value::StringValue(s)) => parse_json_text(s),
        _ => any_to_json(v),
    }
}

pub fn parse_json_text(s: String) -> J {
    let t = s.trim_start();
    if t.starts_with(['{', '[', '"'])
        && let Ok(v) = serde_json::from_str(t)
    {
        return v;
    }
    J::String(s)
}

fn is_indexed(k: &str, p: &str) -> bool {
    k.strip_prefix(p)
        .is_some_and(|r| r.starts_with(|c: char| c.is_ascii_digit()))
}

#[derive(Debug, Default)]
pub struct Attrs(HashMap<String, AnyValue>);

impl Attrs {
    /// Duplicate keys: the last one wins (OTLP says keys MUST be unique; be lenient).
    pub fn new(kvs: Vec<KeyValue>) -> Self {
        let mut m = HashMap::with_capacity(kvs.len());
        for kv in kvs {
            m.insert(kv.key, kv.value.unwrap_or_default());
        }
        Self(m)
    }
    pub fn has(&self, k: &str) -> bool {
        self.0.get(k).is_some_and(|v| v.value.is_some())
    }
    pub fn has_prefix(&self, p: &str) -> bool {
        self.0.keys().any(|k| k.starts_with(p))
    }
    /// `prefix` followed by a numeric index: `gen_ai.prompt.0.role` matches `gen_ai.prompt.`.
    pub fn has_indexed(&self, p: &str) -> bool {
        self.0.keys().any(|k| is_indexed(k, p))
    }
    /// Non-empty string value.
    pub fn str(&self, k: &str) -> Option<&str> {
        match &self.0.get(k)?.value {
            Some(Value::StringValue(s)) if !s.trim().is_empty() => Some(s),
            _ => None,
        }
    }
    pub fn first_str(&self, keys: &[&str]) -> Option<&str> {
        keys.iter().find_map(|k| self.str(k))
    }
    /// Tokens arrive as int, double or string: coerce to a non-negative i64.
    pub fn int(&self, k: &str) -> Option<i64> {
        let v = match &self.0.get(k)?.value {
            Some(Value::IntValue(i)) => Some(*i),
            Some(Value::DoubleValue(d)) if d.is_finite() => Some(d.round() as i64),
            Some(Value::StringValue(s)) => {
                let s = s.trim();
                s.parse::<i64>().ok().or_else(|| {
                    s.parse::<f64>()
                        .ok()
                        .filter(|d| d.is_finite())
                        .map(|d| d.round() as i64)
                })
            }
            _ => None,
        }?;
        (v >= 0).then_some(v)
    }
    pub fn first_int(&self, keys: &[&str]) -> Option<i64> {
        keys.iter().find_map(|k| self.int(k))
    }
    pub fn f64(&self, k: &str) -> Option<f64> {
        let v = match &self.0.get(k)?.value {
            Some(Value::IntValue(i)) => Some(*i as f64),
            Some(Value::DoubleValue(d)) => Some(*d),
            Some(Value::StringValue(s)) => s.trim().parse().ok(),
            _ => None,
        }?;
        (v.is_finite() && v >= 0.0).then_some(v)
    }
    pub fn first_f64(&self, keys: &[&str]) -> Option<f64> {
        keys.iter().find_map(|k| self.f64(k))
    }
    pub fn take(&mut self, k: &str) -> Option<AnyValue> {
        self.0.remove(k).filter(|v| v.value.is_some())
    }
    /// Removes every `prefix*` key; returns (key without prefix, value), unsorted.
    pub fn take_prefix(&mut self, p: &str) -> Vec<(String, AnyValue)> {
        self.take_where(p, |k| k.starts_with(p))
    }
    /// Like `take_prefix`, only for `prefix<digit>...` keys.
    pub fn take_indexed(&mut self, p: &str) -> Vec<(String, AnyValue)> {
        self.take_where(p, |k| is_indexed(k, p))
    }
    /// Removes the keys matching `keep` (all start with `p`); returns (key without `p`, value).
    fn take_where(&mut self, p: &str, keep: impl Fn(&str) -> bool) -> Vec<(String, AnyValue)> {
        let keys: Vec<String> = self.0.keys().filter(|k| keep(k)).cloned().collect();
        keys.into_iter()
            .filter_map(|k| {
                let v = self.0.remove(&k)?;
                Some((k[p.len()..].to_string(), v))
            })
            .collect()
    }
    pub fn into_json(self) -> Map<String, J> {
        self.0.into_iter().map(|(k, v)| (k, any_to_json(v))).collect()
    }
}

/// `{"0.message.role": "user", "0.message.contents.0.message_content.text": "hi"}` ->
/// `[{"role": "user", "contents": [{"text": "hi"}]}]`.
/// Objects whose keys are all integers become arrays (sorted numerically, so 10 comes after 9).
/// One-key wrappers used by OpenInference (`message`, `document`, `tool_call`, `message_content`,
/// `prompt`, `completion`) are unwrapped. `metadata` string leaves holding JSON objects are parsed.
pub fn unflatten(items: Vec<(String, AnyValue)>) -> J {
    let mut root = Map::new();
    for (k, v) in items {
        let segs: Vec<&str> = k.split('.').collect();
        insert(&mut root, &segs, any_to_json(v));
    }
    arrayify(J::Object(root))
}

fn insert(obj: &mut Map<String, J>, segs: &[&str], v: J) {
    let (head, rest) = (segs[0], &segs[1..]);
    if rest.is_empty() {
        let v = match v {
            J::String(s) if head == "metadata" => parse_json_text(s),
            v => v,
        };
        // A leaf that collides with an existing subtree keeps the subtree (and the leaf under "").
        match obj.get_mut(head) {
            Some(J::Object(m)) => {
                m.insert(String::new(), v);
            }
            _ => {
                obj.insert(head.to_string(), v);
            }
        }
        return;
    }
    let e = obj.entry(head.to_string()).or_insert_with(|| J::Object(Map::new()));
    if !e.is_object() {
        let leaf = std::mem::take(e);
        let mut m = Map::new();
        m.insert(String::new(), leaf);
        *e = J::Object(m);
    }
    if let J::Object(m) = e {
        insert(m, rest, v);
    }
}

const WRAPPERS: [&str; 6] = [
    "message",
    "document",
    "tool_call",
    "message_content",
    "prompt",
    "completion",
];

fn arrayify(v: J) -> J {
    match v {
        J::Object(m) if !m.is_empty() && m.keys().all(|k| k.parse::<u32>().is_ok()) => {
            let mut items: Vec<(u32, J)> = m.into_iter().map(|(k, v)| (k.parse().expect("checked"), v)).collect();
            items.sort_by_key(|(i, _)| *i);
            J::Array(items.into_iter().map(|(_, v)| unwrap_wrapper(arrayify(v))).collect())
        }
        J::Object(m) => J::Object(m.into_iter().map(|(k, v)| (k, arrayify(v))).collect()),
        v => v,
    }
}

fn unwrap_wrapper(v: J) -> J {
    match v {
        J::Object(mut m) if m.len() == 1 && WRAPPERS.contains(&m.keys().next().expect("len 1").as_str()) => {
            let k = m.keys().next().expect("len 1").clone();
            m.remove(&k).expect("present")
        }
        v => v,
    }
}

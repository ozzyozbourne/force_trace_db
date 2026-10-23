//! Fast-path JSON (D5): RFC3339 UTC with 9 fractional digits, lowercase hex ids, JSON columns embedded raw after
//! validation, `latency_ms` as a float, an explicit-stack nested writer (no recursion).
use crate::QueryError;
use crate::tree::TraceTree;
use arrow::array::{
    Array, AsArray, BinaryArray, Float64Array, Int64Array, ListArray, PrimitiveArray, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimestampNanosecondType};
use forge_model::merge::Cancel;
use forge_model::schema::{JSON_COLUMNS, col};
use std::cell::Cell;
use std::io::Write;

type Ts = PrimitiveArray<TimestampNanosecondType>;

/// Typed column refs of the projected fields, in output order.
pub(crate) struct SpanCols<'a> {
    /// (name, `"name":` with its leading comma except on the first field, column).
    fields: Vec<(&'static str, Vec<u8>, Col<'a>)>,
    /// One debug log per response for invalid JSON text.
    logged: Cell<bool>,
}

enum Col<'a> {
    Id(&'a BinaryArray),
    Str(&'a StringArray),
    Json(&'a StringArray),
    Ts(&'a Ts),
    I64(&'a Int64Array),
    F64(&'a Float64Array),
    List(&'a ListArray),
    /// end_time - start_time (both projected, else null).
    Latency(Option<&'a Ts>, Option<&'a Ts>),
}

const LATENCY: &str = "latency_ms";
/// Output order (FP §1.5). Absent columns are skipped (e.g. payloads=false), not written as null.
const ORDER: [&str; 24] = [
    col::TRACE_ID,
    col::SPAN_ID,
    col::PARENT_SPAN_ID,
    col::NAME,
    col::RUN_TYPE,
    col::STATUS,
    col::ERROR,
    col::START_TIME,
    col::END_TIME,
    LATENCY,
    col::MODEL,
    col::PROVIDER,
    col::PROMPT_TOKENS,
    col::COMPLETION_TOKENS,
    col::TOTAL_TOKENS,
    col::COST_USD,
    col::SESSION_ID,
    col::TAGS,
    col::METADATA,
    col::ATTRIBUTES,
    col::EVENTS,
    col::INPUTS,
    col::OUTPUTS,
    col::VERSION,
];

impl<'a> SpanCols<'a> {
    /// `trace_id`: write it per span (runs) or not (traces: once in the envelope).
    pub fn new(b: &'a RecordBatch, trace_id: bool) -> Self {
        let s = b.schema_ref();
        let get = |n: &str| s.index_of(n).ok().map(|i| b.column(i));
        let ts = |n: &str| get(n).map(|a| a.as_primitive::<TimestampNanosecondType>());
        let mut fields = Vec::new();
        for n in ORDER {
            if n == LATENCY {
                fields.push((n, Col::Latency(ts(col::START_TIME), ts(col::END_TIME))));
                continue;
            }
            if n == col::TRACE_ID && !trace_id {
                continue;
            }
            let Some(a) = get(n) else { continue };
            let c = match a.data_type() {
                DataType::Binary => Col::Id(a.as_binary::<i32>()),
                DataType::Utf8 if JSON_COLUMNS.contains(&n) => Col::Json(a.as_string::<i32>()),
                DataType::Utf8 => Col::Str(a.as_string::<i32>()),
                DataType::Timestamp(..) => Col::Ts(a.as_primitive::<TimestampNanosecondType>()),
                DataType::Int64 => Col::I64(a.as_primitive::<Int64Type>()),
                DataType::Float64 => Col::F64(a.as_primitive::<Float64Type>()),
                DataType::List(_) => Col::List(a.as_list::<i32>()),
                _ => continue,
            };
            fields.push((n, c));
        }
        let fields = fields
            .into_iter()
            .enumerate()
            .map(|(i, (n, c))| (n, format!("{}\"{n}\":", if i > 0 { "," } else { "" }).into_bytes(), c))
            .collect();
        Self {
            fields,
            logged: Cell::new(false),
        }
    }

    /// Writes the fields of one span (no braces), then `"orphan":true` / `"cycle":true` when set.
    pub fn write_span(&self, out: &mut Vec<u8>, row: usize, orphan: bool, cycle: bool) {
        for (name, key, c) in &self.fields {
            out.extend_from_slice(key);
            match c {
                Col::Id(a) if a.is_valid(row) => hex_str(out, a.value(row)),
                Col::Str(a) if a.is_valid(row) => json_str(out, a.value(row)),
                Col::Json(a) if a.is_valid(row) => {
                    let v = a.value(row);
                    if serde_json::from_str::<serde::de::IgnoredAny>(v).is_ok() {
                        out.extend_from_slice(v.as_bytes());
                    } else {
                        if !self.logged.replace(true) {
                            tracing::debug!(column = name, "invalid JSON text written as a string");
                        }
                        json_str(out, v);
                    }
                }
                Col::Ts(a) if a.is_valid(row) => ts_str(out, a.value(row)),
                Col::I64(a) if a.is_valid(row) => {
                    let _ = write!(out, "{}", a.value(row));
                }
                Col::F64(a) if a.is_valid(row) => float(out, a.value(row)),
                Col::List(a) if a.is_valid(row) => {
                    let v = a.value(row);
                    out.push(b'[');
                    for (i, x) in v.as_string::<i32>().iter().enumerate() {
                        if i > 0 {
                            out.push(b',');
                        }
                        match x {
                            Some(x) => json_str(out, x),
                            None => out.extend_from_slice(b"null"),
                        }
                    }
                    out.push(b']');
                }
                Col::Latency(start, end) => match (start, end) {
                    // An i64 difference that overflows is written as null.
                    (Some(s), Some(e)) if s.is_valid(row) && e.is_valid(row) => {
                        match e.value(row).checked_sub(s.value(row)) {
                            Some(d) => float(out, d as f64 / 1e6),
                            None => out.extend_from_slice(b"null"),
                        }
                    }
                    _ => out.extend_from_slice(b"null"),
                },
                _ => out.extend_from_slice(b"null"),
            }
        }
        if orphan {
            out.extend_from_slice(b",\"orphan\":true");
        }
        if cycle {
            out.extend_from_slice(b",\"cycle\":true");
        }
    }
}

fn json_str(out: &mut Vec<u8>, s: &str) {
    serde_json::to_writer(&mut *out, s).expect("strings serialize");
}

/// serde_json's float form (`3.0`, not `3`); null when not finite.
fn float(out: &mut Vec<u8>, f: f64) {
    if f.is_finite() {
        serde_json::to_writer(&mut *out, &f).expect("finite floats serialize");
    } else {
        out.extend_from_slice(b"null");
    }
}

/// `"<lowercase hex>"`, without allocating for ids.
fn hex_str(out: &mut Vec<u8>, b: &[u8]) {
    let mut buf = [0u8; 64];
    out.push(b'"');
    match buf.get_mut(..b.len() * 2) {
        Some(dst) => {
            hex::encode_to_slice(b, dst).expect("sized buffer");
            out.extend_from_slice(dst);
        }
        None => out.extend_from_slice(hex::encode(b).as_bytes()),
    }
    out.push(b'"');
}

/// `"YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"` (RFC3339 UTC, 9 fractional digits).
fn ts_str(out: &mut Vec<u8>, ns: i64) {
    let t = chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    out.push(b'"');
    out.extend_from_slice(t.as_bytes());
    out.push(b'"');
}

/// `{"project","trace_id","span_count","roots":[…]}` (nested, every span with `"children":[…]`) or
/// `{…,"spans":[{…,"depth":d}]}` (flat, preorder). Checks `cancel` every 1k spans.
pub fn write_trace(
    out: &mut Vec<u8>,
    project: &str,
    trace: &str,
    rows: &RecordBatch,
    tree: &TraceTree,
    flat: bool,
    cancel: &Cancel,
) -> Result<(), QueryError> {
    let cols = SpanCols::new(rows, false);
    out.extend_from_slice(b"{\"project\":");
    json_str(out, project);
    let _ = write!(out, ",\"trace_id\":\"{trace}\",\"span_count\":{},", rows.num_rows());
    out.extend_from_slice(if flat { b"\"spans\":[" } else { b"\"roots\":[" });
    let mut prev = 0u32;
    for (i, &(r, d)) in tree.preorder.iter().enumerate() {
        if i % 1024 == 0 {
            cancel.check()?;
        }
        let r = r as usize;
        if flat {
            if i > 0 {
                out.push(b',');
            }
        } else if i > 0 && prev >= d {
            // Close the previous span and every level down to d's parent, then a sibling comma.
            for _ in 0..=(prev - d) {
                out.extend_from_slice(b"]}");
            }
            out.push(b',');
        }
        out.push(b'{');
        cols.write_span(out, r, tree.orphan[r], tree.cycle[r]);
        if flat {
            let _ = write!(out, ",\"depth\":{d}}}");
        } else {
            out.extend_from_slice(b",\"children\":[");
        }
        prev = d;
    }
    if !flat && !tree.preorder.is_empty() {
        for _ in 0..=prev {
            out.extend_from_slice(b"]}");
        }
    }
    out.extend_from_slice(b"]}");
    Ok(())
}

/// `{"runs":[…],"next_cursor":hex|null}`: each run like a trace span plus `trace_id`, no children. Checks `cancel`
/// every 1k runs.
pub fn write_runs(
    out: &mut Vec<u8>,
    rows: &RecordBatch,
    cursor: Option<&str>,
    cancel: &Cancel,
) -> Result<(), QueryError> {
    let cols = SpanCols::new(rows, true);
    out.extend_from_slice(b"{\"runs\":[");
    for i in 0..rows.num_rows() {
        if i % 1024 == 0 {
            cancel.check()?;
        }
        if i > 0 {
            out.push(b',');
        }
        out.push(b'{');
        cols.write_span(out, i, false, false);
        out.push(b'}');
    }
    out.extend_from_slice(b"],\"next_cursor\":");
    match cursor {
        Some(c) => json_str(out, c),
        None => out.extend_from_slice(b"null"),
    }
    out.push(b'}');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_model::row::SpanRow;
    use forge_model::schema::{project_to, projected_schema};
    use forge_model::testing::batch_from_rows;

    /// A latency whose i64 difference overflows is null, not a panic or a wrapped value.
    #[test]
    fn latency_overflow_is_null() {
        let r = SpanRow {
            project_id: Some(forge_model::testing::pid("p")),
            start_time_ns: Some(-10),
            end_time_ns: Some(i64::MAX),
            ..Default::default()
        };
        let s = projected_schema(&[col::SPAN_ID, col::START_TIME, col::END_TIME]);
        let b = project_to(&batch_from_rows(&[r]), &s).unwrap();
        let mut out = Vec::new();
        SpanCols::new(&b, false).write_span(&mut out, 0, false, false);
        let v: serde_json::Value = serde_json::from_slice(&[b"{", &out[..], b"}"].concat()).unwrap();
        assert_eq!(v["latency_ms"], serde_json::Value::Null, "{v}");
    }

    /// Both writers check `cancel` every 1k spans: over 1,500 spans the second check (span 1024) fails.
    #[test]
    fn writers_check_cancel() {
        use forge_model::testing::{pid, sid, tid};
        let rows: Vec<SpanRow> = (0..1500)
            .map(|i| SpanRow {
                project_id: Some(pid("p")),
                trace_id: tid(1),
                span_id: sid(i),
                start_time_ns: Some(i as i64),
                ..Default::default()
            })
            .collect();
        let b = batch_from_rows(&rows);
        let tree = TraceTree::build(&b, &Cancel::never()).unwrap();
        for (k, ok) in [(1, false), (2, true)] {
            let r = write_trace(&mut Vec::new(), "p", "t", &b, &tree, false, &Cancel::after(k));
            assert_eq!(r.is_ok(), ok, "write_trace {r:?}");
            let r = write_runs(&mut Vec::new(), &b, None, &Cancel::after(k));
            assert_eq!(r.is_ok(), ok, "write_runs {r:?}");
        }
    }
}

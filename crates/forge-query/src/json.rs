//! Fast-path JSON (D5): RFC3339 UTC with 9 fractional digits, lowercase hex ids, JSON columns embedded raw after
//! validation, `latency_ms` as a float, an explicit-stack nested writer (no recursion).
use crate::QueryError;
use crate::tree::TraceTree;
use arrow::array::{
    Array, AsArray, BinaryArray, Float64Array, Int64Array, ListArray, PrimitiveArray, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimestampNanosecondType};
use chrono::SecondsFormat;
use forge_model::merge::Cancel;
use forge_model::schema::{JSON_COLUMNS, col};
use std::cell::Cell;
use std::io::Write;

type Ts = PrimitiveArray<TimestampNanosecondType>;

/// Typed column refs of the projected fields, in output order.
pub(crate) struct SpanCols<'a> {
    fields: Vec<(&'static str, Col<'a>)>,
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
        Self {
            fields,
            logged: Cell::new(false),
        }
    }

    /// Writes the fields of one span (no braces), then `"orphan":true` / `"cycle":true` when set.
    pub fn write_span(&self, out: &mut Vec<u8>, row: usize, orphan: bool, cycle: bool) {
        for (i, (name, c)) in self.fields.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            let _ = write!(out, "\"{name}\":");
            match c {
                Col::Id(a) if a.is_valid(row) => {
                    out.push(b'"');
                    out.extend_from_slice(hex::encode(a.value(row)).as_bytes());
                    out.push(b'"');
                }
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
                Col::Ts(a) if a.is_valid(row) => {
                    let _ = write!(out, "\"{}\"", rfc3339(a.value(row)));
                }
                Col::I64(a) if a.is_valid(row) => {
                    let _ = write!(out, "{}", a.value(row));
                }
                Col::F64(a) if a.is_valid(row) => float(out, a.value(row)),
                Col::List(a) if a.is_valid(row) => {
                    let v = a.value(row);
                    let v = v.as_string::<i32>();
                    let items: Vec<Option<&str>> = v.iter().collect();
                    serde_json::to_writer(&mut *out, &items).expect("strings serialize");
                }
                Col::Latency(start, end) => match (start, end) {
                    (Some(s), Some(e)) if s.is_valid(row) && e.is_valid(row) => {
                        float(out, (e.value(row) - s.value(row)) as f64 / 1e6)
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

pub fn rfc3339(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(SecondsFormat::Nanos, true)
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

//! Fast-path JSON (D5): RFC3339 UTC ns, lowercase hex, raw JSON columns, explicit-stack nested writer.
use crate::tree::{CYCLE, ORPHAN, TraceTree};
use arrow::array::{Array, AsArray, BinaryArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow::datatypes::{Float64Type, Int64Type, TimestampNanosecondType};
use arrow::array::PrimitiveArray;
use chrono::SecondsFormat;
use forge_model::schema::{JSON_COLUMNS, col};
use std::io::Write;

/// Typed column refs (None when not projected).
pub struct SpanCols<'a> {
    fields: Vec<(&'static str, Col<'a>)>,
    start: Option<&'a PrimitiveArray<TimestampNanosecondType>>,
    end: Option<&'a PrimitiveArray<TimestampNanosecondType>>,
}
enum Col<'a> {
    Id(&'a BinaryArray),
    Str(&'a StringArray),
    Json(&'a StringArray),
    Ts(&'a PrimitiveArray<TimestampNanosecondType>),
    I64(&'a Int64Array),
    F64(&'a Float64Array),
    List(&'a ListArray),
    Latency,
}

const ORDER: [&str; 23] = [
    col::SPAN_ID, col::PARENT_SPAN_ID, col::NAME, col::RUN_TYPE, col::STATUS, col::ERROR, col::START_TIME, col::END_TIME,
    "latency_ms", col::MODEL, col::PROVIDER, col::PROMPT_TOKENS, col::COMPLETION_TOKENS, col::TOTAL_TOKENS, col::COST_USD,
    col::SESSION_ID, col::TAGS, col::METADATA, col::ATTRIBUTES, col::EVENTS, col::INPUTS, col::OUTPUTS, col::VERSION,
];

impl<'a> SpanCols<'a> {
    pub fn new(b: &'a RecordBatch) -> Self {
        let s = b.schema();
        let get = |n: &str| s.index_of(n).ok().map(|i| b.column(i));
        let ts = |n: &str| get(n).map(|a| a.as_primitive::<TimestampNanosecondType>());
        let mut fields = Vec::new();
        for n in ORDER {
            if n == "latency_ms" {
                fields.push((n, Col::Latency));
                continue;
            }
            let Some(a) = get(n) else { continue };
            let c = match a.data_type() {
                arrow::datatypes::DataType::Binary => Col::Id(a.as_binary::<i32>()),
                arrow::datatypes::DataType::Utf8 if JSON_COLUMNS.contains(&n) => Col::Json(a.as_string::<i32>()),
                arrow::datatypes::DataType::Utf8 => Col::Str(a.as_string::<i32>()),
                arrow::datatypes::DataType::Timestamp(..) => Col::Ts(a.as_primitive::<TimestampNanosecondType>()),
                arrow::datatypes::DataType::Int64 => Col::I64(a.as_primitive::<Int64Type>()),
                arrow::datatypes::DataType::Float64 => Col::F64(a.as_primitive::<Float64Type>()),
                arrow::datatypes::DataType::List(_) => Col::List(a.as_list::<i32>()),
                _ => continue,
            };
            fields.push((n, c));
        }
        Self { fields, start: ts(col::START_TIME), end: ts(col::END_TIME) }
    }
}

pub fn rfc3339(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn raw_ok(s: &str) -> bool {
    matches!(s.trim_start().as_bytes().first(), Some(b'{' | b'[' | b'"' | b'-' | b'0'..=b'9' | b't' | b'f' | b'n'))
}

/// Writes the fields of one span (no braces).
pub fn write_span(out: &mut Vec<u8>, cols: &SpanCols<'_>, row: usize, flags: u8) {
    let mut first = true;
    for (name, c) in &cols.fields {
        if !first {
            out.push(b',');
        }
        first = false;
        let _ = write!(out, "\"{name}\":");
        match c {
            Col::Id(a) if a.is_valid(row) => {
                out.push(b'"');
                out.extend_from_slice(hex::encode(a.value(row)).as_bytes());
                out.push(b'"');
            }
            Col::Str(a) if a.is_valid(row) => serde_json::to_writer(&mut *out, a.value(row)).unwrap(),
            Col::Json(a) if a.is_valid(row) => {
                let v = a.value(row);
                if raw_ok(v) { out.extend_from_slice(v.as_bytes()) } else { serde_json::to_writer(&mut *out, v).unwrap() }
            }
            Col::Ts(a) if a.is_valid(row) => {
                let _ = write!(out, "\"{}\"", rfc3339(a.value(row)));
            }
            Col::I64(a) if a.is_valid(row) => {
                let _ = write!(out, "{}", a.value(row));
            }
            Col::F64(a) if a.is_valid(row) && a.value(row).is_finite() => {
                let _ = write!(out, "{}", a.value(row));
            }
            Col::List(a) if a.is_valid(row) => {
                let v = a.value(row);
                let v = v.as_string::<i32>();
                serde_json::to_writer(&mut *out, &v.iter().collect::<Vec<_>>()).unwrap();
            }
            Col::Latency => match (cols.start, cols.end) {
                (Some(s), Some(e)) if s.is_valid(row) && e.is_valid(row) => {
                    let _ = write!(out, "{}", (e.value(row) - s.value(row)) as f64 / 1e6);
                }
                _ => out.extend_from_slice(b"null"),
            },
            _ => out.extend_from_slice(b"null"),
        }
    }
    if flags & ORPHAN != 0 {
        out.extend_from_slice(b",\"orphan\":true");
    }
    if flags & CYCLE != 0 {
        out.extend_from_slice(b",\"cycle\":true");
    }
}

/// `{"project","trace_id","span_count","roots":[…]}` (nested) or `…"spans":[…]` (flat), no recursion.
pub fn write_trace(out: &mut Vec<u8>, project: &str, trace: &str, rows: &RecordBatch, tree: &TraceTree, flat: bool) {
    let cols = SpanCols::new(rows);
    out.extend_from_slice(b"{\"project\":");
    serde_json::to_writer(&mut *out, project).unwrap();
    let _ = write!(out, ",\"trace_id\":\"{trace}\",\"span_count\":{},", rows.num_rows());
    if flat {
        out.extend_from_slice(b"\"spans\":[");
        for (i, &(r, d)) in tree.preorder.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.push(b'{');
            write_span(out, &cols, r as usize, tree.flags[r as usize]);
            let _ = write!(out, ",\"depth\":{d}}}");
        }
        out.extend_from_slice(b"]}");
        return;
    }
    out.extend_from_slice(b"\"roots\":[");
    let mut open = 0u32; // number of nodes whose children array is open
    for (i, &(r, d)) in tree.preorder.iter().enumerate() {
        // close nodes deeper than or equal to d
        while open > d {
            out.extend_from_slice(b"]}");
            open -= 1;
        }
        // first child of its parent (or first root) needs no comma
        let first_child = i == 0 || tree.preorder[i - 1].1 < d;
        if !first_child {
            out.push(b',');
        }
        out.push(b'{');
        write_span(out, &cols, r as usize, tree.flags[r as usize]);
        out.extend_from_slice(b",\"children\":[");
        open = d + 1;
    }
    while open > 0 {
        out.extend_from_slice(b"]}");
        open -= 1;
    }
    out.extend_from_slice(b"]}");
}

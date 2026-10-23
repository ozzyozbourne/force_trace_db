//! `forge inspect segment <path>`.
//! Footer-only facts first (no data I/O), then a key-column scan that re-checks the segment
//! contract (invariant 3) and summarizes traces/versions.
use crate::segment::{nulls_last, widen};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int64Type, TimestampNanosecondType};
use forge_model::schema::{col, projected_schema};
use futures::TryStreamExt;
use std::collections::HashSet;
use std::fmt::Write;
use vortex::VortexSessionDefault;
use vortex::dtype::{DType, FieldPath, Nullability, PType};
use vortex::expr::stats::Stat;
use vortex::expr::{root, select};
use vortex::file::OpenOptionsSessionExt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

/// Metadata segment key Forge writes into every segment (spec + provenance; JSON).
pub const FORGE_META_KEY: &str = "forge.segment";

pub struct InspectOptions {
    pub layout: bool,
    /// Scan key columns and verify sort order / one row per span.
    pub verify: bool,
}

pub async fn inspect_segment(path: &std::path::Path, o: &InspectOptions) -> crate::StorageResult<String> {
    let session = VortexSession::default().with_tokio();
    let mut out = String::new();
    let size = std::fs::metadata(path)?.len();
    let file = session.open_options().include_metadata().open_path(path).await?;
    let footer = file.footer();
    let fields = file
        .dtype()
        .as_struct_fields_opt()
        .filter(|f| f.names().iter().any(|n| n == col::PROJECT_ID))
        .ok_or_else(|| crate::StorageError::InvalidInput("not a spans segment".into()))?
        .clone();
    writeln!(out, "file:      {} ({size} bytes)", path.display()).unwrap();
    writeln!(out, "rows:      {}", file.row_count()).unwrap();
    writeln!(
        out,
        "footer:    ~{} bytes, {} segments",
        footer.approx_byte_size().unwrap_or(0),
        footer.segment_map().len()
    )
    .unwrap();
    if let Some(m) = file.metadata_segment(FORGE_META_KEY) {
        writeln!(out, "forge:     {}", String::from_utf8_lossy(m.as_slice())).unwrap();
    }
    let sizes = footer.compressed_field_sizes()?;
    writeln!(
        out,
        "{:<18} {:<28} {:>10} {:>6}  min .. max",
        "column", "dtype", "bytes", "nulls"
    )
    .unwrap();
    let stats = file.file_stats();
    let u64_dt = DType::Primitive(PType::U64, Nullability::NonNullable);
    for (i, (name, dt)) in fields.names().iter().zip(fields.fields()).enumerate() {
        let bytes = sizes.get(&FieldPath::from_name(name.clone())).unwrap_or_default();
        let (nulls, range) = match stats {
            Some(st) => {
                let (set, sdt) = st.get(i);
                let show = |s: Stat| {
                    let p = set.get(s).into_scalar(sdt.clone());
                    let inexact = matches!(p, vortex::expr::stats::Precision::Inexact(_));
                    let Some(v) = p.into_inner() else {
                        return "-".to_string();
                    };
                    // ids are Binary: print hex, not Vortex's space-separated bytes
                    let t = match v.as_binary_opt().and_then(|b| b.value().cloned()) {
                        Some(bytes) => hex::encode(bytes.as_slice()),
                        None => format!("{v}"),
                    };
                    let t = if t.len() > 40 { format!("{}…", &t[..40]) } else { t };
                    if inexact { format!("~{t}") } else { t }
                };
                let nulls = set
                    .get(Stat::NullCount)
                    .into_scalar(u64_dt.clone())
                    .into_inner()
                    .and_then(|n| n.as_primitive_opt().and_then(|p| p.as_::<u64>()));
                (
                    nulls.map_or("-".into(), |n| n.to_string()),
                    format!("{} .. {}", show(Stat::Min), show(Stat::Max)),
                )
            }
            None => ("-".into(), "-".into()),
        };
        let dts = format!("{dt}");
        let name = name.to_string();
        writeln!(
            out,
            "{name:<18} {:<28} {bytes:>10} {nulls:>6}  {range}",
            &dts[..dts.len().min(28)]
        )
        .unwrap();
    }
    if o.layout {
        writeln!(out, "layout:\n{}", footer.layout().display_tree()).unwrap();
    }
    if o.verify {
        let cols = vec![
            col::PROJECT_ID,
            col::TRACE_ID,
            col::DOTTED_ORDER,
            col::SPAN_ID,
            col::VERSION,
            col::START_TIME,
            col::DELETED,
        ];
        let schema = projected_schema(&cols);
        let batches: Vec<_> = file
            .scan()?
            .with_projection(select(cols, root()).bind(file.dtype())?)
            .into_record_batch_stream(schema)?
            .try_collect()
            .await?;
        writeln!(out, "{}", verify_rows(&batches)).unwrap();
    }
    Ok(out)
}

/// Same contract as forge-storage's write-time check, re-derived from the file.
fn verify_rows(batches: &[arrow::record_batch::RecordBatch]) -> String {
    type Key = (String, Vec<u8>, (bool, String), Vec<u8>);
    let mut prev: Option<Key> = None;
    let (mut rows, mut traces, mut deleted, mut start_nulls) = (0u64, 0u64, 0u64, 0u64);
    let (mut vmin, mut vmax) = (None, None);
    let (mut tmin, mut tmax) = (None, None);
    let (mut per_trace_max, mut cur_trace_rows) = (0u64, 0u64);
    let mut projects = HashSet::new();
    let mut spans_in_trace: HashSet<Vec<u8>> = HashSet::new();
    let mut problems = Vec::new();
    for b in batches {
        let p = b.column(0).as_string::<i32>();
        let t = b.column(1).as_binary::<i32>();
        let d = b.column(2).as_string::<i32>();
        let s = b.column(3).as_binary::<i32>();
        let v = b.column(4).as_primitive::<Int64Type>();
        let st = b.column(5).as_primitive::<TimestampNanosecondType>();
        let del = b.column(6).as_boolean();
        for i in 0..b.num_rows() {
            let (null, dotted) = nulls_last(d, i);
            let dotted = (null, dotted.to_string());
            let k: Key = (p.value(i).to_string(), t.value(i).to_vec(), dotted, s.value(i).to_vec());
            let new_trace = prev.as_ref().is_none_or(|q| q.0 != k.0 || q.1 != k.1);
            if new_trace {
                traces += 1;
                per_trace_max = per_trace_max.max(cur_trace_rows);
                cur_trace_rows = 0;
                spans_in_trace.clear();
            }
            cur_trace_rows += 1;
            if prev.as_ref().is_some_and(|q| *q >= k) && problems.len() < 5 {
                problems.push(format!("row {rows}: not strictly after the previous row"));
            }
            if !spans_in_trace.insert(k.3.clone()) && problems.len() < 5 {
                problems.push(format!("row {rows}: span appears twice in its trace"));
            }
            projects.insert(k.0.clone());
            widen(&mut vmin, &mut vmax, v.value(i));
            if st.is_null(i) {
                start_nulls += 1;
            } else {
                widen(&mut tmin, &mut tmax, st.value(i));
            }
            deleted += u64::from(del.value(i));
            rows += 1;
            prev = Some(k);
        }
    }
    per_trace_max = per_trace_max.max(cur_trace_rows);
    // An empty range prints as the i64 MAX..MIN sentinels.
    let (vmin, vmax) = (vmin.unwrap_or(i64::MAX), vmax.unwrap_or(i64::MIN));
    let (tmin, tmax) = (tmin.unwrap_or(i64::MAX), tmax.unwrap_or(i64::MIN));
    let mut out = String::new();
    writeln!(
        out,
        "verify:    {rows} rows, {} project(s) {:?}, {traces} traces (max {per_trace_max} rows/trace), versions {vmin}..{vmax}, \
         start_time {tmin}..{tmax} ({start_nulls} null), {deleted} tombstones",
        projects.len(),
        projects
    )
    .unwrap();
    if projects.len() > 1 {
        problems.push("more than one project".into());
    }
    if problems.is_empty() {
        write!(
            out,
            "contract:  OK (sorted by project, trace, dotted_order NULLS LAST, span; one row per span)"
        )
        .unwrap();
    } else {
        write!(out, "contract:  VIOLATED: {}", problems.join("; ")).unwrap();
    }
    out
}

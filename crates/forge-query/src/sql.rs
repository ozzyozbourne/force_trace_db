//! `/v1/sql` (D7): `{"columns":[{name,type}],"rows":[…],"row_count","truncated","elapsed_ms"}`.
use crate::session::QueryPermit;
use crate::{QueryEngine, QueryError};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SqlRequest {
    pub query: String,
    /// Default 10,000; at most 100,000.
    pub max_rows: Option<usize>,
    /// Capped by `query.timeout`.
    pub timeout_ms: Option<u64>,
}

pub const DEFAULT_MAX_ROWS: usize = 10_000;
pub const MAX_ROWS: usize = 100_000;
/// Rows per JSON write: the body cap is checked after each slice.
const SLICE: usize = 64;

/// Timestamp(_, "UTC") -> "+00:00" (metadata only), because arrow_json without chrono-tz rejects "UTC".
fn utc_fix(b: &RecordBatch) -> Result<RecordBatch, QueryError> {
    let s = b.schema();
    if !s
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Timestamp(_, Some(z)) if z.as_ref() == "UTC"))
    {
        return Ok(b.clone());
    }
    let mut fields = Vec::new();
    let mut cols = Vec::new();
    for (f, c) in s.fields().iter().zip(b.columns()) {
        match f.data_type() {
            DataType::Timestamp(u, Some(z)) if z.as_ref() == "UTC" => {
                let t = DataType::Timestamp(*u, Some("+00:00".into()));
                cols.push(cast(c, &t)?);
                fields.push(Field::new(f.name(), t, f.is_nullable()));
            }
            _ => {
                cols.push(c.clone());
                fields.push(f.as_ref().clone());
            }
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

impl QueryEngine {
    /// Runs a read-only query under the concurrency limit (`query.sql_concurrency`; waiting past the deadline is
    /// Transient), the per-query memory pool and `min(timeout_ms, query.timeout)`.
    pub async fn sql_json(&self, r: &SqlRequest) -> Result<Vec<u8>, QueryError> {
        let t0 = Instant::now();
        let max_rows = r.max_rows.unwrap_or(DEFAULT_MAX_ROWS);
        if max_rows > MAX_ROWS {
            return Err(QueryError::InvalidInput(format!("max_rows {max_rows} > {MAX_ROWS}")));
        }
        let timeout = r
            .timeout_ms
            .map_or(self.cfg.timeout, |ms| Duration::from_millis(ms).min(self.cfg.timeout));
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::time::timeout_at(deadline, self.sql_slots.clone().acquire_owned())
            .await
            .map_err(|_| QueryError::Transient("too many concurrent queries".into()))?
            .map_err(|_| QueryError::Transient("query engine closed".into()))?;
        let ctx = self.session_with(Some(Arc::new(QueryPermit(permit))))?;
        let cap = self.cfg.max_response_bytes.as_u64() as usize;
        let fut = async {
            let df = ctx.sql_with_options(&r.query, crate::session::read_only()).await?;
            let mut stream = df.execute_stream().await?;
            let mut buf: Vec<u8> = b"{\"columns\":".to_vec();
            let cols: Vec<serde_json::Value> = stream
                .schema()
                .fields()
                .iter()
                .map(|f| serde_json::json!({"name": f.name(), "type": f.data_type().to_string()}))
                .collect();
            serde_json::to_writer(&mut buf, &cols).expect("json values serialize");
            buf.extend_from_slice(b",\"rows\":");
            let (mut n, mut truncated) = (0usize, false);
            {
                let mut w = arrow::json::WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, arrow::json::writer::JsonArray>(&mut buf);
                'batches: while let Some(b) = stream.next().await {
                    let b = utc_fix(&b?)?;
                    let mut off = 0;
                    while off < b.num_rows() {
                        // More rows remain: stop here if a limit is reached (dropping the stream cancels DF's tasks).
                        if n == max_rows || w.get_ref().len() > cap {
                            truncated = true;
                            break 'batches;
                        }
                        let len = SLICE.min(b.num_rows() - off).min(max_rows - n);
                        w.write(&b.slice(off, len))?;
                        (off, n) = (off + len, n + len);
                    }
                }
                w.finish()?;
            }
            let _ = write!(
                buf,
                ",\"row_count\":{n},\"truncated\":{truncated},\"elapsed_ms\":{}}}",
                t0.elapsed().as_secs_f64() * 1e3
            );
            Ok(buf)
        };
        // One deadline for the slot wait, planning, execution and writing.
        tokio::time::timeout_at(deadline, fut)
            .await
            .map_err(|_| QueryError::Timeout(timeout))?
    }
}

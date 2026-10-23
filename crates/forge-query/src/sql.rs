//! /v1/sql envelope (D7).
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
    pub max_rows: Option<usize>,
    pub timeout_ms: Option<u64>,
}

const MAX_BODY: usize = 64 << 20;

/// Timestamp(_, "UTC") -> "+00:00" (metadata only), because arrow_json without chrono-tz rejects "UTC".
fn utc_fix(b: &RecordBatch) -> Result<RecordBatch, QueryError> {
    let s = b.schema();
    if !s.fields().iter().any(|f| matches!(f.data_type(), DataType::Timestamp(_, Some(z)) if z.as_ref() == "UTC")) {
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
    pub async fn sql_json(&self, r: &SqlRequest) -> Result<Vec<u8>, QueryError> {
        let t0 = Instant::now();
        let max_rows = r.max_rows.unwrap_or(10_000);
        if max_rows > 100_000 {
            return Err(QueryError::InvalidInput(format!("max_rows {max_rows} > 100000")));
        }
        let timeout = r.timeout_ms.map_or(self.cfg.timeout, |ms| Duration::from_millis(ms).min(self.cfg.timeout));
        let deadline = tokio::time::Instant::now() + timeout;
        let _permit = tokio::time::timeout_at(deadline, self.sql_slots.clone().acquire_owned())
            .await
            .map_err(|_| QueryError::Transient("too many concurrent queries".into()))?
            .map_err(|_| QueryError::Transient("closed".into()))?;
        let ctx = self.session()?;
        let fut = async {
            let df = ctx.sql_with_options(&r.query, crate::session::read_only()).await?;
            let mut stream = df.execute_stream().await?;
            let schema = stream.schema();
            let mut buf: Vec<u8> = Vec::new();
            buf.extend_from_slice(b"{\"columns\":");
            let cols: Vec<serde_json::Value> = schema
                .fields()
                .iter()
                .map(|f| serde_json::json!({"name": f.name(), "type": f.data_type().to_string()}))
                .collect();
            serde_json::to_writer(&mut buf, &cols).map_err(|e| QueryError::InvalidInput(e.to_string()))?;
            buf.extend_from_slice(b",\"rows\":");
            let (mut n, mut truncated) = (0usize, false);
            {
                let mut w = arrow::json::WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, arrow::json::writer::JsonArray>(&mut buf);
                while let Some(b) = stream.next().await {
                    let b = b?;
                    let take = (max_rows - n).min(b.num_rows());
                    if take < b.num_rows() {
                        truncated = true;
                    }
                    w.write(&utc_fix(&b.slice(0, take))?)?;
                    n += take;
                    if truncated || n >= max_rows || w.get_ref().len() > MAX_BODY {
                        truncated = truncated || stream.next().await.is_some() || w.get_ref().len() > MAX_BODY;
                        break;
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
        self.deadline(timeout, fut).await
    }
}

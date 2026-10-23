//! Blob references stored in payload columns (pure).

/// Exact prefix of every reference: `{"$forge_blob":"blobs/<project>/<sha256hex>","size":<bytes>}`.
pub const BLOB_REF_PREFIX: &str = "{\"$forge_blob\":\"";

pub fn blob_ref(path: &str, size: usize) -> String {
    format!("{BLOB_REF_PREFIX}{path}\",\"size\":{size}}}")
}

/// `Some((path, size))` iff `s` is exactly a reference produced by `blob_ref` (cheap prefix check first).
pub fn parse_blob_ref(s: &str) -> Option<(&str, u64)> {
    let rest = s.strip_prefix(BLOB_REF_PREFIX)?;
    let (path, rest) = rest.split_once("\",\"size\":")?;
    let size = rest.strip_suffix('}')?.parse().ok()?;
    path.starts_with("blobs/").then_some((path, size))
}

/// Distinct blob paths referenced by the payload columns of `batches` (spans_schema). Prefix check per value.
pub fn blob_refs_in(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    use arrow::array::AsArray;
    let mut out = std::collections::BTreeSet::new();
    for b in batches {
        for name in crate::schema::PAYLOAD_COLUMNS {
            let a = b.column(crate::schema::col_idx(name)).as_string::<i32>();
            for v in a.iter().flatten() {
                if let Some((path, _)) = parse_blob_ref(v) {
                    out.insert(path.to_string());
                }
            }
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip() {
        let r = blob_ref("blobs/p/abc", 12);
        assert_eq!(parse_blob_ref(&r), Some(("blobs/p/abc", 12)));
        assert_eq!(parse_blob_ref("{\"$forge_blob\":\"x/p\",\"size\":1}"), None);
        assert_eq!(parse_blob_ref(&format!("{r} ")), None);
    }
}

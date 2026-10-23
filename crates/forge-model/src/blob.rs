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

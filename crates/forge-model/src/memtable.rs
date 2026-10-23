use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Implemented by forge-ingest's memtable set (M1); consumed by forge-query. Sync and cheap.
pub trait MemtableView: Send + Sync + std::fmt::Debug + 'static {
    fn snapshot(&self) -> MemtableSnapshot;
}

#[derive(Clone, Debug, Default)]
pub struct MemtableSnapshot {
    /// spans_schema batches, unsorted, may hold several versions per key.
    pub batches: Arc<[RecordBatch]>,
}

/// No unflushed rows (M0, offline tools).
#[derive(Debug, Default)]
pub struct EmptyMemtable;

impl MemtableView for EmptyMemtable {
    fn snapshot(&self) -> MemtableSnapshot {
        MemtableSnapshot::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_memtable_has_no_batches() {
        assert!(EmptyMemtable.snapshot().batches.is_empty());
    }
}

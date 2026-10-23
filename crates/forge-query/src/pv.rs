#![allow(unused_imports)]
pub(crate) use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array};
pub(crate) use arrow::compute::{filter_record_batch, take_record_batch};
pub(crate) use arrow::datatypes::{Schema, SchemaRef};
pub(crate) use async_trait::async_trait;
pub(crate) use datafusion::catalog::{Session, TableProvider};
pub(crate) use datafusion::common::tree_node::TreeNodeRecursion;
pub(crate) use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, exec_err, plan_err};
pub(crate) use datafusion::datasource::memory::MemorySourceConfig;
pub(crate) use datafusion::execution::memory_pool::MemoryConsumer;
pub(crate) use datafusion::execution::{SendableRecordBatchStream, TaskContext};
pub(crate) use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType};
pub(crate) use datafusion::logical_expr::expr::InList;
pub(crate) use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
pub(crate) use datafusion::physical_plan::empty::EmptyExec;
pub(crate) use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
pub(crate) use datafusion::physical_plan::expressions::Column;
pub(crate) use datafusion::physical_plan::filter::FilterExec;
pub(crate) use datafusion::physical_plan::projection::ProjectionExec;
pub(crate) use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
pub(crate) use datafusion::physical_plan::union::UnionExec;
pub(crate) use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
pub(crate) use datafusion::physical_expr::PartitioningSatisfaction;
pub(crate) use datafusion::prelude::{col, not, when};
pub(crate) use forge_meta::{Metastore, SegmentFilter};
pub(crate) use forge_model::MemtableView;
pub(crate) use forge_model::catalog::{SegmentId, SegmentMeta};
pub(crate) use forge_model::ids::{ProjectId, TraceId};
pub(crate) use forge_model::merge::{Cancel, CancelOnDrop, MERGE_COLUMNS, Tombstones, merge_versions_projected};
pub(crate) use forge_model::schema::ALL_COLUMNS;
pub(crate) use forge_model::schema::{col as c, col_idx, spans_schema};
pub(crate) use forge_storage::{RowSelection, ScanRequest, SegmentStore};
pub(crate) use futures::{StreamExt, TryStreamExt};
pub(crate) use std::collections::{BTreeMap, BTreeSet};
pub(crate) use std::fmt;
pub(crate) use std::sync::Arc;

/// Columns the merge exec hash-partitions on: every version of a key lands in one partition.
pub(crate) const PARTITION_COLUMNS: [&str; 2] = [c::PROJECT_ID, c::TRACE_ID];

pub(crate) fn ext<E: std::error::Error + Send + Sync + 'static>(e: E) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
pub(crate) use crate::filters::*;
pub(crate) use crate::provider::*;
pub(crate) use crate::scan::*;
pub(crate) use crate::merge_exec::*;
pub(crate) use crate::derived::*;

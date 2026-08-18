//! Hash aggregate operator: group-by + COUNT/SUM/MIN/MAX.
//!
//! M3 first cut — non-streaming. All input is buffered, then on
//! `no_more_input()` a single output batch with one row per group is
//! emitted. Streaming emit + spilling arrives in M4+.

use crate::op::PipelineOp;
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use pylon_types::{PylonError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};

/// Per-aggregate spec resolved at op-construction time.
#[derive(Debug, Clone)]
pub struct AggSpec {
    pub func: String, // lowercased: "count" | "sum" | "min" | "max"
    /// `None` for `COUNT(*)`; otherwise the column name to fold.
    pub arg_col: Option<String>,
    /// `None` for `COUNT(*)`; otherwise the type of the input column.
    pub input_type: Option<DataType>,
    /// Type of the result column. For COUNT this is always Int64.
    pub output_type: DataType,
    /// Output field name (alias if supplied, else `func` / `func_col`).
    pub out_name: String,
}

impl AggSpec {
    pub fn is_count_star(&self) -> bool {
        self.func == "count" && self.arg_col.is_none()
    }
}

/// Typed hash key covering the input column types M3 first cut
/// supports. We use a bits representation for `f64` to side-step
/// `NaN != NaN` issues; the bit pattern is stable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum GroupKey {
    Int64(i64),
    Float64Bits(u64),
    Utf8(String),
}

impl GroupKey {
    fn from_value(v: &dyn Array, row: usize) -> Result<Self> {
        match v.data_type() {
            DataType::Int64 => {
                let arr = v.as_any().downcast_ref::<Int64Array>().unwrap();
                if arr.is_null(row) {
                    Err(PylonError::InvalidPlan(
                        "null group_by key not supported in M3 first cut".into(),
                    ))
                } else {
                    Ok(GroupKey::Int64(arr.value(row)))
                }
            }
            DataType::Float64 => {
                let arr = v.as_any().downcast_ref::<Float64Array>().unwrap();
                if arr.is_null(row) {
                    Err(PylonError::InvalidPlan(
                        "null group_by key not supported in M3 first cut".into(),
                    ))
                } else {
                    Ok(GroupKey::Float64Bits(arr.value(row).to_bits()))
                }
            }
            DataType::Utf8 => {
                let arr = v.as_any().downcast_ref::<StringArray>().unwrap();
                if arr.is_null(row) {
                    Err(PylonError::InvalidPlan(
                        "null group_by key not supported in M3 first cut".into(),
                    ))
                } else {
                    Ok(GroupKey::Utf8(arr.value(row).to_string()))
                }
            }
            other => Err(PylonError::InvalidPlan(format!(
                "group_by column type {other:?} not supported in M3 first cut"
            ))),
        }
    }
}

/// Per-aggregate accumulator state. We keep all variants in one enum so
/// a single `HashMap<Vec<GroupKey>, Vec<AggState>>` can hold the whole
/// running result.
#[derive(Debug, Clone)]
enum AggState {
    Count(i64),
    SumI64(i64),
    SumF64(f64),
    /// `None` when no rows have hit this group yet (used for MIN/MAX).
    MinI64(Option<i64>),
    MaxI64(Option<i64>),
    MinF64(Option<f64>),
    MaxF64(Option<f64>),
    MinUtf8(Option<String>),
    MaxUtf8(Option<String>),
}

pub struct HashAggregateOp {
    pub group_by_cols: Vec<String>,
    pub aggregates: Vec<AggSpec>,
    pub output_schema: SchemaRef,
    /// Per-group state: (group_key_tuple) → (per-aggregate state).
    state: HashMap<Vec<GroupKey>, Vec<AggState>>,
    /// Buffered output batches. M3 first cut emits exactly one batch on
    /// EOS, so the buffer holds 0 or 1 entries.
    output_buf: Vec<RecordBatch>,
    pub upstream_done: bool,
    pub emitted: bool,
}

impl HashAggregateOp {
    /// Build the op from already-resolved `group_by_cols` (column names
    /// in the input schema) and a list of `AggSpec`. The output schema
    /// is supplied by the caller (typically derived from the SQL
    /// `PhysicalPlan::Aggregate.schema`).
    pub fn new(
        group_by_cols: Vec<String>,
        aggregates: Vec<AggSpec>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            group_by_cols,
            aggregates,
            output_schema,
            state: HashMap::new(),
            output_buf: Vec::new(),
            upstream_done: false,
            emitted: false,
        }
    }

    /// Resolve the indices of the group_by columns in the input schema.
    fn group_by_indices(&self, batch: &RecordBatch) -> Result<Vec<usize>> {
        let in_schema = batch.schema();
        self.group_by_cols
            .iter()
            .map(|name| {
                in_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| {
                        PylonError::InvalidPlan(format!(
                            "aggregate: group_by column {name} not found in input"
                        ))
                    })
            })
            .collect()
    }

    /// Resolve the index of an aggregate's argument column, if any.
    fn agg_arg_index(agg: &AggSpec, batch: &RecordBatch) -> Result<Option<usize>> {
        match &agg.arg_col {
            None => Ok(None),
            Some(name) => {
                let idx = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| {
                        PylonError::InvalidPlan(format!(
                            "aggregate: arg column {name} not found in input"
                        ))
                    })?;
                Ok(Some(idx))
            }
        }
    }

    /// Initialize the per-group state vector for a freshly-seen group.
    fn initial_states(&self) -> Vec<AggState> {
        self.aggregates
            .iter()
            .map(|agg| match agg.func.as_str() {
                "count" => AggState::Count(0),
                "sum" => match agg.input_type.as_ref().unwrap() {
                    DataType::Int64 => AggState::SumI64(0),
                    DataType::Float64 => AggState::SumF64(0.0),
                    other => panic!("sum unsupported type {other:?} in AggSpec"),
                },
                "min" => match agg.input_type.as_ref().unwrap() {
                    DataType::Int64 => AggState::MinI64(None),
                    DataType::Float64 => AggState::MinF64(None),
                    DataType::Utf8 => AggState::MinUtf8(None),
                    other => panic!("min unsupported type {other:?} in AggSpec"),
                },
                "max" => match agg.input_type.as_ref().unwrap() {
                    DataType::Int64 => AggState::MaxI64(None),
                    DataType::Float64 => AggState::MaxF64(None),
                    DataType::Utf8 => AggState::MaxUtf8(None),
                    other => panic!("max unsupported type {other:?} in AggSpec"),
                },
                other => panic!("unknown aggregate func {other}"),
            })
            .collect()
    }

    /// Fold one row's aggregate argument value into the per-group state.
    fn fold_value(
        state: &mut AggState,
        func: &str,
        input_type: &DataType,
        arg_value: Option<&dyn Array>,
        row: usize,
    ) -> Result<()> {
        // For COUNT(arg), null input doesn't count.
        if func == "count" && arg_value.is_some() {
            let arr = arg_value.unwrap();
            if arr.is_null(row) {
                return Ok(());
            }
            if let AggState::Count(c) = state {
                *c += 1;
            }
            return Ok(());
        }
        // COUNT(*): every row counts.
        if func == "count" {
            if let AggState::Count(c) = state {
                *c += 1;
            }
            return Ok(());
        }
        // SUM / MIN / MAX: read the value at this row, fold.
        let arr = arg_value.expect("non-count aggregate must have an arg column");
        if arr.is_null(row) {
            // M3 first cut: nulls in non-count aggregates are ignored.
            return Ok(());
        }
        match (func, input_type) {
            ("sum", DataType::Int64) => {
                let v = arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
                if let AggState::SumI64(s) = state {
                    *s += v;
                }
            }
            ("sum", DataType::Float64) => {
                let v = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
                if let AggState::SumF64(s) = state {
                    *s += v;
                }
            }
            ("min", DataType::Int64) => {
                let v = arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
                if let AggState::MinI64(slot) = state {
                    *slot = Some(match *slot {
                        None => v,
                        Some(cur) => cur.min(v),
                    });
                }
            }
            ("min", DataType::Float64) => {
                let v = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
                if let AggState::MinF64(slot) = state {
                    *slot = Some(match *slot {
                        None => v,
                        Some(cur) => cur.min(v),
                    });
                }
            }
            ("min", DataType::Utf8) => {
                let v = arr.as_any().downcast_ref::<StringArray>().unwrap().value(row);
                if let AggState::MinUtf8(slot) = state {
                    *slot = Some(match slot.as_deref() {
                        None => v.to_string(),
                        Some(cur) => {
                            if v < cur {
                                v.to_string()
                            } else {
                                cur.to_string()
                            }
                        }
                    });
                }
            }
            ("max", DataType::Int64) => {
                let v = arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
                if let AggState::MaxI64(slot) = state {
                    *slot = Some(match *slot {
                        None => v,
                        Some(cur) => cur.max(v),
                    });
                }
            }
            ("max", DataType::Float64) => {
                let v = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
                if let AggState::MaxF64(slot) = state {
                    *slot = Some(match *slot {
                        None => v,
                        Some(cur) => cur.max(v),
                    });
                }
            }
            ("max", DataType::Utf8) => {
                let v = arr.as_any().downcast_ref::<StringArray>().unwrap().value(row);
                if let AggState::MaxUtf8(slot) = state {
                    *slot = Some(match slot.as_deref() {
                        None => v.to_string(),
                        Some(cur) => {
                            if v > cur {
                                v.to_string()
                            } else {
                                cur.to_string()
                            }
                        }
                    });
                }
            }
            (f, t) => {
                return Err(PylonError::InvalidPlan(format!(
                    "aggregate {f} on {t:?} not implemented"
                )))
            }
        }
        Ok(())
    }

    /// Build the final RecordBatch with one row per group.
    fn build_output(&self) -> Result<RecordBatch> {
        // Empty input → emit zero-row batch with the correct schema so
        // downstream consumers see a well-formed result. We must hand
        // back one column-array per schema field, just with zero rows.
        if self.state.is_empty() {
            let empty_cols: Vec<Arc<dyn Array>> = self
                .output_schema
                .fields()
                .iter()
                .map(|f| arrow_array::new_empty_array(f.data_type()))
                .collect();
            return RecordBatch::try_new(self.output_schema.clone(), empty_cols)
                .map_err(Into::into);
        }

        // Stable order: sort by group key (Vec<GroupKey> impls Ord via
        // derived enum ordering — Int64 < Float64Bits < Utf8 in source
        // order, then by inner value). Determinism matters for tests and
        // for E2E result comparison.
        let mut groups: Vec<&Vec<GroupKey>> = self.state.keys().collect();
        groups.sort_by(|a, b| {
            for (x, y) in a.iter().zip(b.iter()) {
                match x.cmp(y) {
                    std::cmp::Ordering::Equal => continue,
                    non_eq => return non_eq,
                }
            }
            a.len().cmp(&b.len())
        });

        let n_groups = groups.len();
        let n_aggs = self.aggregates.len();

        // One column per output field: group_by cols + agg cols.
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(self.group_by_cols.len() + n_aggs);

        for (g_idx, _) in self.group_by_cols.iter().enumerate() {
            // Build a single column by iterating groups and pulling the
            // g_idx-th key out. We need to be type-aware: switch on the
            // first non-None key to discover the type.
            let first_key = &groups[0][g_idx];
            match first_key {
                GroupKey::Int64(_) => {
                    let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        if let GroupKey::Int64(v) = &k[g_idx] {
                            buf.push(Some(*v));
                        } else {
                            return Err(PylonError::Internal(format!(
                                "mixed types in group_by col {g_idx}"
                            )));
                        }
                    }
                    columns.push(Arc::new(Int64Array::from(buf)));
                }
                GroupKey::Float64Bits(_) => {
                    let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        if let GroupKey::Float64Bits(b) = &k[g_idx] {
                            buf.push(Some(f64::from_bits(*b)));
                        } else {
                            return Err(PylonError::Internal(format!(
                                "mixed types in group_by col {g_idx}"
                            )));
                        }
                    }
                    columns.push(Arc::new(Float64Array::from(buf)));
                }
                GroupKey::Utf8(_) => {
                    let mut buf: Vec<Option<String>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        if let GroupKey::Utf8(s) = &k[g_idx] {
                            buf.push(Some(s.clone()));
                        } else {
                            return Err(PylonError::Internal(format!(
                                "mixed types in group_by col {g_idx}"
                            )));
                        }
                    }
                    columns.push(Arc::new(StringArray::from(buf)));
                }
            }
        }

        // Aggregate columns.
        for (a_idx, agg) in self.aggregates.iter().enumerate() {
            match agg.func.as_str() {
                "count" => {
                    let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::Count(c) = &states[a_idx] {
                            buf.push(Some(*c));
                        } else {
                            return Err(PylonError::Internal("count state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Int64Array::from(buf)));
                }
                "sum" => match agg.input_type.as_ref().unwrap() {
                    DataType::Int64 => {
                        let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::SumI64(s) = &states[a_idx] {
                                buf.push(Some(*s));
                            } else {
                                return Err(PylonError::Internal("sum state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(Int64Array::from(buf)));
                    }
                    DataType::Float64 => {
                        let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::SumF64(s) = &states[a_idx] {
                                buf.push(Some(*s));
                            } else {
                                return Err(PylonError::Internal("sum state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(Float64Array::from(buf)));
                    }
                    other => {
                        return Err(PylonError::Internal(format!(
                            "sum output type {other:?} not handled"
                        )))
                    }
                },
                "min" => match agg.input_type.as_ref().unwrap() {
                    DataType::Int64 => {
                        let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::MinI64(s) = &states[a_idx] {
                                buf.push(*s);
                            } else {
                                return Err(PylonError::Internal("min state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(Int64Array::from(buf)));
                    }
                    DataType::Float64 => {
                        let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::MinF64(s) = &states[a_idx] {
                                buf.push(*s);
                            } else {
                                return Err(PylonError::Internal("min state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(Float64Array::from(buf)));
                    }
                    DataType::Utf8 => {
                        let mut buf: Vec<Option<String>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::MinUtf8(s) = &states[a_idx] {
                                buf.push(s.clone());
                            } else {
                                return Err(PylonError::Internal("min state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(StringArray::from(buf)));
                    }
                    other => {
                        return Err(PylonError::Internal(format!(
                            "min output type {other:?} not handled"
                        )))
                    }
                },
                "max" => match agg.input_type.as_ref().unwrap() {
                    DataType::Int64 => {
                        let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::MaxI64(s) = &states[a_idx] {
                                buf.push(*s);
                            } else {
                                return Err(PylonError::Internal("max state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(Int64Array::from(buf)));
                    }
                    DataType::Float64 => {
                        let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::MaxF64(s) = &states[a_idx] {
                                buf.push(*s);
                            } else {
                                return Err(PylonError::Internal("max state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(Float64Array::from(buf)));
                    }
                    DataType::Utf8 => {
                        let mut buf: Vec<Option<String>> = Vec::with_capacity(n_groups);
                        for k in &groups {
                            let states = self.state.get(*k).unwrap();
                            if let AggState::MaxUtf8(s) = &states[a_idx] {
                                buf.push(s.clone());
                            } else {
                                return Err(PylonError::Internal("max state mismatch".into()));
                            }
                        }
                        columns.push(Arc::new(StringArray::from(buf)));
                    }
                    other => {
                        return Err(PylonError::Internal(format!(
                            "max output type {other:?} not handled"
                        )))
                    }
                },
                other => {
                    return Err(PylonError::Internal(format!(
                        "aggregate {other} not implemented in build_output"
                    )))
                }
            }
        }

        // Sanity: the BooleanArray trick above isn't used; this silences
        // the unused-import warning for `BooleanArray` (kept available
        // for future COUNT(DISTINCT) etc.).
        let _ = std::any::type_name::<BooleanArray>();

        let batch = RecordBatch::try_new(self.output_schema.clone(), columns)?;
        Ok(batch)
    }
}

#[async_trait]
impl PipelineOp for HashAggregateOp {
    fn name(&self) -> &'static str {
        "HashAggregate"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.emitted {
            return Err(PylonError::InvalidPlan(
                "HashAggregate received input after emitting final batch".into(),
            ));
        }
        let group_by_indices = self.group_by_indices(&batch)?;
        let arg_indices: Vec<Option<usize>> = self
            .aggregates
            .iter()
            .map(|a| Self::agg_arg_index(a, &batch))
            .collect::<Result<Vec<_>>>()?;

        // Per row: extract group key, fold each aggregate.
        let n_rows = batch.num_rows();
        let group_by_arrays: Vec<&dyn Array> = group_by_indices
            .iter()
            .map(|&i| batch.column(i).as_ref())
            .collect();
        let arg_arrays: Vec<Option<&dyn Array>> = arg_indices
            .iter()
            .map(|opt| opt.map(|i| batch.column(i).as_ref()))
            .collect();

        for row in 0..n_rows {
            let key: Vec<GroupKey> = group_by_arrays
                .iter()
                .map(|a| GroupKey::from_value(*a, row))
                .collect::<Result<Vec<_>>>()?;
            let initial = self.initial_states();
            let states = self.state.entry(key).or_insert(initial);
            for (a_idx, agg) in self.aggregates.iter().enumerate() {
                Self::fold_value(
                    &mut states[a_idx],
                    &agg.func,
                    agg.input_type.as_ref().unwrap_or(&DataType::Int64),
                    arg_arrays[a_idx],
                    row,
                )?;
            }
        }
        trace!(
            rows = n_rows,
            groups = self.state.len(),
            "HashAggregate absorbed batch"
        );
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.output_buf.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.output_buf.remove(0)))
    }

    async fn no_more_input(&mut self) -> Result<()> {
        if !self.emitted {
            let final_batch = self.build_output()?;
            debug!(
                groups = final_batch.num_rows(),
                "HashAggregate emitting final batch"
            );
            self.output_buf.push(final_batch);
            self.emitted = true;
        }
        self.upstream_done = true;
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        self.upstream_done && self.output_buf.is_empty() && self.emitted
    }
}

/// Helper for tests / wiring: build an `output_schema` from a list of
/// group_by column fields + aggregate result fields. Kept here so
/// `pylon-worker` doesn't have to duplicate the schema-construction
/// logic when wiring op specs.
pub fn build_aggregate_output_schema(
    group_by_fields: Vec<Field>,
    aggregate_fields: Vec<Field>,
) -> SchemaRef {
    let mut fields = group_by_fields;
    fields.extend(aggregate_fields);
    Arc::new(Schema::new(fields))
}

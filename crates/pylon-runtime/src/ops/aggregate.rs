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
use crate::memory_pool::NoopMemoryPool;
use pylon_types::{MemoryPool, PylonError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};

/// Per-aggregate spec passed in at op-construction time. The
/// function name and the optional input column name (none for
/// `COUNT(*)`) are the only required fields. The op resolves the
/// column's data type lazily on the first `add_input` (we look at
/// the actual batch) and the result type is derived from the
/// `output_schema` field at construction time.
#[derive(Debug, Clone)]
pub struct AggSpec {
    pub func: String, // lowercased: "count" | "sum" | "min" | "max"
    /// `None` for `COUNT(*)`; otherwise the column name to fold.
    pub arg_col: Option<String>,
    /// Output field name (alias if supplied, else `func` / `func_col`).
    /// The op matches this to a field in `output_schema` to discover
    /// the result type.
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

/// Memory accounting hook (RFC 0007 §3.1 conformance):
/// `pool.try_grow(N)` is called at `add_input` time for the bytes we
/// estimate we'll retain; the same `N` is `release`d in `Drop`.
///
/// Default construction (`::new`) wires a [`NoopMemoryPool`], so
/// existing call sites and tests don't have to pass a budget.
/// Production code (e.g. `pylon-worker`) should call
/// `HashAggregateOp::with_pool(...)` to thread a real
/// `PerTaskPool`.
pub struct HashAggregateOp {
    pub group_by_cols: Vec<String>,
    pub aggregates: Vec<AggSpec>,
    pub output_schema: SchemaRef,
    /// Per-group state: (group_key_tuple) → (per-aggregate state).
    state: HashMap<Vec<GroupKey>, Vec<AggState>>,
    /// Buffered output batches. M3 first cut emits exactly one batch on
    /// EOS, so the buffer holds 0 or 1 entries.
    output_buf: Vec<RecordBatch>,
    /// Per-aggregate input type, resolved on first `add_input`. Outer
    /// `None` = unresolved. Inner `None` per aggregate = `COUNT(*)`
    /// (no input column).
    input_types: Option<Vec<Option<DataType>>>,
    pub upstream_done: bool,
    pub emitted: bool,
    /// Per-task byte budget. Scaled at `add_input`, balanced at `Drop`.
    pool: Arc<dyn MemoryPool>,
    /// Bytes currently claimed from `pool`. Released in `Drop`.
    pool_allocated: usize,
}

impl HashAggregateOp {
    /// Build the op from already-resolved `group_by_cols` (column names
    /// in the input schema) and a list of `AggSpec`. The output schema
    /// is supplied by the caller (typically derived from the SQL
    /// `PhysicalPlan::Aggregate.schema`).
    /// Construct an op with a no-op memory pool. Use this when
    /// you don't care about budget enforcement (most tests, default
    /// builds). Production code paths should prefer [`Self::with_pool`].
    pub fn new(
        group_by_cols: Vec<String>,
        aggregates: Vec<AggSpec>,
        output_schema: SchemaRef,
    ) -> Self {
        Self::with_pool(
            group_by_cols,
            aggregates,
            output_schema,
            Arc::new(NoopMemoryPool),
        )
    }

    /// Construct an op with an explicit per-task memory budget.
    /// Every row claimed in `add_input` is released in `Drop`.
    pub fn with_pool(
        group_by_cols: Vec<String>,
        aggregates: Vec<AggSpec>,
        output_schema: SchemaRef,
        pool: Arc<dyn MemoryPool>,
    ) -> Self {
        Self {
            group_by_cols,
            aggregates,
            output_schema,
            state: HashMap::new(),
            output_buf: Vec::new(),
            input_types: None,
            upstream_done: false,
            emitted: false,
            pool,
            pool_allocated: 0,
        }
    }

    /// Resolve each aggregate's input column type from the input
    /// batch. Idempotent: only runs once, on the first non-empty
    /// batch. After this returns, `input_types` is `Some(...)` and
    /// the fold / state-init code can use it.
    ///
    /// If `output_schema` was passed in as `Schema::empty()` (the
    /// worker uses this when the fragmenter doesn't carry the
    /// post-aggregate schema through the OpSpec), we also build the
    /// schema from the input column types here.
    fn resolve_input_types(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.input_types.is_some() {
            return Ok(());
        }
        let in_schema = batch.schema();

        // Resolve each aggregate's input type.
        let mut types = Vec::with_capacity(self.aggregates.len());
        for agg in &self.aggregates {
            let t = match &agg.arg_col {
                None => None, // COUNT(*)
                Some(name) => {
                    let idx = in_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                        .ok_or_else(|| {
                            PylonError::InvalidPlan(format!(
                                "aggregate: arg column {name} not found in input"
                            ))
                        })?;
                    Some(batch.column(idx).data_type().clone())
                }
            };
            types.push(t);
        }
        self.input_types = Some(types);

        // If the output schema is still empty, derive it now.
        if self.output_schema.fields().is_empty() {
            let mut fields: Vec<Field> = Vec::new();
            for name in &self.group_by_cols {
                let f = in_schema
                    .field_with_name(name)
                    .map_err(|_| {
                        PylonError::InvalidPlan(format!(
                            "aggregate: group_by column {name} not found in input"
                        ))
                    })?
                    .clone();
                fields.push(f);
            }
            for (agg, input_type) in self.aggregates.iter().zip(self.input_types.as_ref().unwrap().iter()) {
                let out_dt = match agg.func.as_str() {
                    "count" => DataType::Int64,
                    "sum" => match input_type.as_ref().unwrap() {
                        DataType::Int64 => DataType::Int64,
                        DataType::Float64 => DataType::Float64,
                        other => {
                            return Err(PylonError::InvalidPlan(format!(
                                "SUM does not support input type {other:?}"
                            )))
                        }
                    },
                    "min" | "max" => input_type.as_ref().unwrap().clone(),
                    other => {
                        return Err(PylonError::InvalidPlan(format!(
                            "aggregate {other} not supported in output-schema derivation"
                        )))
                    }
                };
                fields.push(Field::new(&agg.out_name, out_dt, true));
            }
            self.output_schema = Arc::new(Schema::new(fields));
        }
        Ok(())
    }

    /// Pre-resolve the output schema before any data arrives. Useful
    /// when the caller knows the post-aggregation schema (e.g. a
    /// coordinator that built the PhysicalPlan) and especially when
    /// some partitions may receive zero rows — those still need a
    /// well-formed zero-row batch emitted at `no_more_input`.
    pub fn resolve_output_schema(&mut self, schema: SchemaRef) {
        if self.output_schema.fields().is_empty() {
            self.output_schema = schema;
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

    /// Initialize the per-group state vector for a freshly-seen group.
    /// Caller must have resolved `input_types` already.
    fn initial_states(&self) -> Vec<AggState> {
        let input_types = self
            .input_types
            .as_ref()
            .expect("initial_states called before resolve_input_types");
        self.aggregates
            .iter()
            .zip(input_types.iter())
            .map(|(agg, input_type)| match agg.func.as_str() {
                "count" => AggState::Count(0),
                "sum" => match input_type.as_ref().expect("sum needs arg column") {
                    DataType::Int64 => AggState::SumI64(0),
                    DataType::Float64 => AggState::SumF64(0.0),
                    other => panic!("sum unsupported type {other:?} in AggSpec"),
                },
                "min" => match input_type.as_ref().expect("min needs arg column") {
                    DataType::Int64 => AggState::MinI64(None),
                    DataType::Float64 => AggState::MinF64(None),
                    DataType::Utf8 => AggState::MinUtf8(None),
                    other => panic!("min unsupported type {other:?} in AggSpec"),
                },
                "max" => match input_type.as_ref().expect("max needs arg column") {
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
            if self.output_schema.fields().is_empty() {
                return Err(PylonError::InvalidPlan(
                    "HashAggregate: empty input and no output schema provided;                      caller must either pass a non-empty output schema or                      feed at least one batch before no_more_input"
                        .into(),
                ));
            }
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

        // Aggregate columns. Output type comes from the field at
        // position `group_by_cols.len() + a_idx` in the output schema.
        for (a_idx, agg) in self.aggregates.iter().enumerate() {
            let out_type = self
                .output_schema
                .field(self.group_by_cols.len() + a_idx)
                .data_type()
                .clone();
            match (agg.func.as_str(), &out_type) {
                ("count", DataType::Int64) => {
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
                ("sum", DataType::Int64) => {
                    let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::SumI64(s) = &states[a_idx] {
                            buf.push(Some(*s));
                        } else {
                            return Err(PylonError::Internal("sum i64 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Int64Array::from(buf)));
                }
                ("sum", DataType::Float64) => {
                    let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::SumF64(s) = &states[a_idx] {
                            buf.push(Some(*s));
                        } else {
                            return Err(PylonError::Internal("sum f64 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Float64Array::from(buf)));
                }
                ("min", DataType::Int64) => {
                    let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::MinI64(s) = &states[a_idx] {
                            buf.push(*s);
                        } else {
                            return Err(PylonError::Internal("min i64 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Int64Array::from(buf)));
                }
                ("min", DataType::Float64) => {
                    let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::MinF64(s) = &states[a_idx] {
                            buf.push(*s);
                        } else {
                            return Err(PylonError::Internal("min f64 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Float64Array::from(buf)));
                }
                ("min", DataType::Utf8) => {
                    let mut buf: Vec<Option<String>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::MinUtf8(s) = &states[a_idx] {
                            buf.push(s.clone());
                        } else {
                            return Err(PylonError::Internal("min utf8 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(StringArray::from(buf)));
                }
                ("max", DataType::Int64) => {
                    let mut buf: Vec<Option<i64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::MaxI64(s) = &states[a_idx] {
                            buf.push(*s);
                        } else {
                            return Err(PylonError::Internal("max i64 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Int64Array::from(buf)));
                }
                ("max", DataType::Float64) => {
                    let mut buf: Vec<Option<f64>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::MaxF64(s) = &states[a_idx] {
                            buf.push(*s);
                        } else {
                            return Err(PylonError::Internal("max f64 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(Float64Array::from(buf)));
                }
                ("max", DataType::Utf8) => {
                    let mut buf: Vec<Option<String>> = Vec::with_capacity(n_groups);
                    for k in &groups {
                        let states = self.state.get(*k).unwrap();
                        if let AggState::MaxUtf8(s) = &states[a_idx] {
                            buf.push(s.clone());
                        } else {
                            return Err(PylonError::Internal("max utf8 state mismatch".into()));
                        }
                    }
                    columns.push(Arc::new(StringArray::from(buf)));
                }
                (f, t) => {
                    return Err(PylonError::Internal(format!(
                        "aggregate {f} with output type {t:?} not implemented in build_output"
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
        // RFC 0007 §3.1 conformance: estimate the bytes this batch
        // will cost us in group_map state and claim them up-front.
        // ~32 bytes per row is a conservative upper bound for one
        // group key + per-aggregate scalar state on the M3 cut; the
        // exact accounting (with spill) lands in M4.S2.
        if batch.num_rows() > 0 {
            let bytes_estimate = batch.num_rows().saturating_mul(32);
            self.pool.try_grow(bytes_estimate)?;
            self.pool_allocated += bytes_estimate;
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.emitted {
            return Err(PylonError::InvalidPlan(
                "HashAggregate received input after emitting final batch".into(),
            ));
        }
        // Resolve aggregate input types on the first non-empty batch.
        // After this, `input_types` is Some and we can use it in the
        // fold loop without further lookups.
        self.resolve_input_types(&batch)?;
        let input_types = self
            .input_types
            .as_ref()
            .expect("resolve_input_types just set this");

        let group_by_indices = self.group_by_indices(&batch)?;
        let arg_indices: Vec<Option<usize>> = self
            .aggregates
            .iter()
            .map(|agg| match &agg.arg_col {
                None => Ok(None),
                Some(name) => batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
                    .map(Some)
                    .ok_or_else(|| {
                        PylonError::InvalidPlan(format!(
                            "aggregate: arg column {name} not found in input"
                        ))
                    }),
            })
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
            for (a_idx, (agg, input_type)) in
                self.aggregates.iter().zip(input_types.iter()).enumerate()
            {
                Self::fold_value(
                    &mut states[a_idx],
                    &agg.func,
                    input_type.as_ref().unwrap_or(&DataType::Int64),
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

/// RFC 0007 §3.1 conformance: release every byte claimed in
/// `add_input`. We claim-and-add to `pool_allocated`; this Drop
/// impl is the symmetric counter-weight. If you find an op that
/// `try_grow`s but never `release`s in its drop, this is the bug
/// pattern to look for.
impl Drop for HashAggregateOp {
    fn drop(&mut self) {
        if self.pool_allocated > 0 {
            self.pool.release(self.pool_allocated);
            // reset so a (theoretical) re-use of the same struct
            // wouldn't double-release — currently nobody re-uses an
            // op struct post-drop, but it costs nothing to be tidy.
            self.pool_allocated = 0;
        }
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

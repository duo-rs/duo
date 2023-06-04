use std::{num::NonZeroU64, sync::Arc};

use anyhow::Result;
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array, UInt8Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{Log, Span, Trace};

pub fn schema_trace() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("duration", DataType::Int64, false),
        Field::new("time", DataType::Int64, false),
        Field::new("process_id", DataType::Utf8, false),
    ]))
}

pub fn schema_span() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("parent_id", DataType::UInt64, true),
        Field::new("trace_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, true),
    ]))
}

pub fn schema_log() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("span_id", DataType::UInt64, true),
        Field::new("trace_id", DataType::UInt64, true),
        Field::new("level", DataType::UInt8, false),
        Field::new("time", DataType::Int64, false),
    ]))
}

#[derive(Default)]
pub struct TraceRecordBatchBuilder {
    trace_ids: Vec<u64>,
    process_ids: Vec<String>,
    durations: Vec<i64>,
    trace_times: Vec<i64>,
}

#[derive(Default)]
pub struct SpanRecordBatchBuilder {
    span_ids: Vec<u64>,
    parent_ids: Vec<Option<u64>>,
    trace_ids: Vec<u64>,
    names: Vec<String>,
    start_times: Vec<i64>,
    end_times: Vec<Option<i64>>,
}

#[derive(Default)]
pub struct LogRecordBatchBuilder {
    span_ids: Vec<Option<u64>>,
    trace_ids: Vec<Option<u64>>,
    levels: Vec<u8>,
    times: Vec<i64>,
}

impl TraceRecordBatchBuilder {
    pub fn append_trace(&mut self, trace: &Trace) {
        self.trace_ids.push(trace.id.get());
        self.process_ids.push(trace.process_id.clone());
        self.durations
            .push(trace.duration.whole_milliseconds() as i64);
        self.trace_times.push(trace.time.unix_timestamp());
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            schema_trace(),
            vec![
                Arc::new(UInt64Array::from(self.trace_ids)),
                Arc::new(Int64Array::from(self.durations)),
                Arc::new(Int64Array::from(self.trace_times)),
                Arc::new(StringArray::from(self.process_ids)),
            ],
        )?)
    }
}

impl SpanRecordBatchBuilder {
    pub fn append_span(&mut self, trace_id: NonZeroU64, span: &Span) {
        self.span_ids.push(span.id.get());
        self.parent_ids.push(span.parent_id.map(|id| id.get()));
        self.trace_ids.push(trace_id.get());
        self.names.push(span.name.clone());
        self.start_times.push(span.start.unix_timestamp());
        self.end_times.push(span.end.map(|t| t.unix_timestamp()));
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            schema_span(),
            vec![
                Arc::new(UInt64Array::from(self.span_ids)),
                Arc::new(UInt64Array::from(self.parent_ids)),
                Arc::new(UInt64Array::from(self.trace_ids)),
                Arc::new(StringArray::from(self.names)),
                Arc::new(Int64Array::from(self.start_times)),
                Arc::new(Int64Array::from(self.end_times)),
            ],
        )?)
    }
}

impl LogRecordBatchBuilder {
    pub fn append_log(&mut self, log: &Log) {
        self.span_ids.push(log.span_id.map(NonZeroU64::get));
        self.trace_ids.push(log.trace_id.map(NonZeroU64::get));
        self.levels.push(1);
        self.times.push(log.time.unix_timestamp());
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            schema_log(),
            vec![
                Arc::new(UInt64Array::from(self.span_ids)),
                Arc::new(UInt64Array::from(self.trace_ids)),
                Arc::new(UInt8Array::from(self.levels)),
                Arc::new(Int64Array::from(self.times)),
            ],
        )?)
    }
}

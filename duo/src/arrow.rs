use std::{num::NonZeroU64, sync::Arc};

use anyhow::Result;
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array, UInt8Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{Log, Span};

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

impl SpanRecordBatchBuilder {
    pub fn append_span(&mut self, span: &Span) {
        self.span_ids.push(span.id.get());
        self.parent_ids.push(span.parent_id.map(|id| id.get()));
        self.trace_ids.push(span.trace_id.get());
        self.names.push(span.name.clone());
        self.start_times
            .push((span.start.unix_timestamp_nanos() / 1000) as i64);
        self.end_times
            .push(span.end.map(|t| (t.unix_timestamp_nanos() / 1000) as i64));
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
        self.times
            .push((log.time.unix_timestamp_nanos() / 1000) as i64);
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

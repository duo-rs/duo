use std::{num::NonZeroU64, sync::Arc};

use anyhow::Result;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array, UInt8Array};

use crate::{Log, Span, Trace};

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
        Ok(RecordBatch::try_from_iter_with_nullable(vec![
            (
                "id",
                Arc::new(UInt64Array::from(self.trace_ids)) as ArrayRef,
                false,
            ),
            (
                "duration",
                Arc::new(Int64Array::from(self.durations)) as ArrayRef,
                false,
            ),
            (
                "time",
                Arc::new(Int64Array::from(self.trace_times)) as ArrayRef,
                false,
            ),
            (
                "process_id",
                Arc::new(StringArray::from(self.process_ids)) as ArrayRef,
                false,
            ),
        ])?)
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
        Ok(RecordBatch::try_from_iter_with_nullable(vec![
            (
                "id",
                Arc::new(UInt64Array::from(self.span_ids)) as ArrayRef,
                false,
            ),
            (
                "parent_id",
                Arc::new(UInt64Array::from(self.parent_ids)) as ArrayRef,
                true,
            ),
            (
                "trace_id",
                Arc::new(UInt64Array::from(self.trace_ids)) as ArrayRef,
                false,
            ),
            (
                "name",
                Arc::new(StringArray::from(self.names)) as ArrayRef,
                false,
            ),
            (
                "start",
                Arc::new(Int64Array::from(self.start_times)) as ArrayRef,
                false,
            ),
            (
                "end",
                Arc::new(Int64Array::from(self.end_times)) as ArrayRef,
                true,
            ),
        ])?)
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
        Ok(RecordBatch::try_from_iter_with_nullable(vec![
            (
                "span_id",
                Arc::new(UInt64Array::from(self.span_ids)) as ArrayRef,
                true,
            ),
            (
                "trace_id",
                Arc::new(UInt64Array::from(self.trace_ids)) as ArrayRef,
                true,
            ),
            (
                "level",
                Arc::new(UInt8Array::from(self.levels)) as ArrayRef,
                false,
            ),
            (
                "time",
                Arc::new(Int64Array::from(self.times)) as ArrayRef,
                false,
            ),
        ])?)
    }
}

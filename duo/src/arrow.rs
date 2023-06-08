use duo_api as proto;
use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

use crate::{Log, Span};
use anyhow::Result;
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

pub fn schema_span() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("parent_id", DataType::UInt64, true),
        Field::new("trace_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("process_id", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, true),
        Field::new("tags", DataType::Utf8, true),
    ]))
}

pub fn schema_log() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("span_id", DataType::UInt64, true),
        Field::new("trace_id", DataType::UInt64, true),
        Field::new("level", DataType::Utf8, false),
        Field::new("time", DataType::Int64, false),
        Field::new("fields", DataType::Utf8, true),
    ]))
}

#[derive(Default)]
pub struct SpanRecordBatchBuilder {
    span_ids: Vec<u64>,
    parent_ids: Vec<Option<u64>>,
    trace_ids: Vec<u64>,
    names: Vec<String>,
    process_ids: Vec<String>,
    start_times: Vec<i64>,
    end_times: Vec<Option<i64>>,
    tags_list: Vec<HashMap<String, proto::Value>>,
}

#[derive(Default)]
pub struct LogRecordBatchBuilder {
    span_ids: Vec<Option<u64>>,
    trace_ids: Vec<Option<u64>>,
    levels: Vec<&'static str>,
    times: Vec<i64>,
    fields_list: Vec<HashMap<String, proto::Value>>,
}

impl SpanRecordBatchBuilder {
    pub fn append_span(&mut self, span: &Span) {
        self.span_ids.push(span.id.get());
        self.parent_ids.push(span.parent_id.map(|id| id.get()));
        self.trace_ids.push(span.trace_id.get());
        self.names.push(span.name.clone());
        self.process_ids.push(span.process_id.clone());
        self.start_times
            .push((span.start.unix_timestamp_nanos() / 1000) as i64);
        self.end_times
            .push(span.end.map(|t| (t.unix_timestamp_nanos() / 1000) as i64));
        self.tags_list.push(span.tags.clone());
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        if self.span_ids.is_empty() {
            return Ok(RecordBatch::new_empty(schema_span()));
        }

        Ok(RecordBatch::try_new(
            schema_span(),
            vec![
                Arc::new(UInt64Array::from(self.span_ids)),
                Arc::new(UInt64Array::from(self.parent_ids)),
                Arc::new(UInt64Array::from(self.trace_ids)),
                Arc::new(StringArray::from(self.names)),
                Arc::new(StringArray::from(self.process_ids)),
                Arc::new(Int64Array::from(self.start_times)),
                Arc::new(Int64Array::from(self.end_times)),
                build_field_array(&self.tags_list),
            ],
        )?)
    }
}

impl LogRecordBatchBuilder {
    pub fn append_log(&mut self, log: &Log) {
        self.span_ids.push(log.span_id.map(NonZeroU64::get));
        self.trace_ids.push(log.trace_id.map(NonZeroU64::get));
        self.levels.push(log.level.as_str());
        self.times
            .push((log.time.unix_timestamp_nanos() / 1000) as i64);
        self.fields_list.push(log.fields.clone());
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        if self.times.is_empty() {
            return Ok(RecordBatch::new_empty(schema_log()));
        }

        Ok(RecordBatch::try_new(
            schema_log(),
            vec![
                Arc::new(UInt64Array::from(self.span_ids)),
                Arc::new(UInt64Array::from(self.trace_ids)),
                Arc::new(StringArray::from(self.levels)),
                Arc::new(Int64Array::from(self.times)),
                build_field_array(&self.fields_list),
            ],
        )?)
    }
}

fn build_field_array(list: &[HashMap<String, proto::Value>]) -> Arc<StringArray> {
    Arc::new(StringArray::from(
        list.into_iter()
            .map(|map| {
                let fields = map
                    .iter()
                    .map(|(key, value)| crate::web::serialize::KvFields(key, value))
                    .collect::<Vec<_>>();
                serde_json::to_string(&fields).unwrap()
            })
            .collect::<Vec<_>>(),
    ))
}

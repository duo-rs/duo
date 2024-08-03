use datafusion::arrow::json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use crate::{Log, Span};
use anyhow::Result;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, UInt64Array};
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

#[derive(Default)]
pub struct SpanRecordBatchBuilder {
    span_ids: Vec<u64>,
    parent_ids: Vec<Option<u64>>,
    trace_ids: Vec<u64>,
    names: Vec<String>,
    process_ids: Vec<String>,
    start_times: Vec<i64>,
    end_times: Vec<Option<i64>>,
    tags_list: Vec<String>,
}

#[derive(Default)]
pub struct LogRecordBatchBuilder {
    data: Vec<JsonValue>,
}

impl SpanRecordBatchBuilder {
    pub fn append_span(&mut self, span: Span) {
        let start_time = span.start_as_micros();
        let end_time = span.end_as_micros();
        self.span_ids.push(span.id);
        self.parent_ids.push(span.parent_id);
        self.trace_ids.push(span.trace_id);
        self.names.push(span.name);
        self.process_ids.push(span.process_id);
        self.start_times.push(start_time);
        self.end_times.push(end_time);
        self.tags_list
            .push(serde_json::to_string(&span.tags).unwrap());
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
                Arc::new(StringArray::from(self.tags_list)),
            ],
        )?)
    }
}

impl LogRecordBatchBuilder {
    pub fn append_log(&mut self, log: Log) {
        let mut map = Map::new();
        let time = log.as_micros();
        map.insert("process_id".into(), log.process_id.into());
        map.insert("span_id".into(), log.span_id.into());
        map.insert("trace_id".into(), log.trace_id.into());
        map.insert("level".into(), log.level.as_str().into());
        map.insert("time".into(), time.into());
        for (key, value) in log.fields {
            map.insert(key, value);
        }
        self.data.push(JsonValue::Object(map));
    }

    pub fn into_record_batch(self) -> Result<RecordBatch> {
        let inferred_schema = infer_json_schema_from_iterator(self.data.iter().map(Ok))?;
        let mut decoder = ReaderBuilder::new(Arc::new(inferred_schema)).build_decoder()?;
        decoder.serialize(&self.data)?;
        let batch = decoder.flush()?.expect("Empty record batch");
        Ok(batch)
    }
}

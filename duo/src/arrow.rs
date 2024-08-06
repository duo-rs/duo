use datafusion::arrow::json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use crate::{Log, Span};
use anyhow::Result;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, UInt64Array};

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

pub fn convert_span_to_record_batch(spans: Vec<Span>) -> Result<RecordBatch> {
    let mut span_ids = Vec::<u64>::new();
    let mut parent_ids = Vec::<Option<u64>>::new();
    let mut trace_ids = Vec::<u64>::new();
    let mut names = Vec::<String>::new();
    let mut process_ids = Vec::<String>::new();
    let mut start_times = Vec::<i64>::new();
    let mut end_times = Vec::<Option<i64>>::new();
    let mut tags_list = Vec::<String>::new();

    for span in spans {
        let start_time = span.start_as_micros();
        let end_time = span.end_as_micros();
        span_ids.push(span.id);
        parent_ids.push(span.parent_id);
        trace_ids.push(span.trace_id);
        names.push(span.name);
        process_ids.push(span.process_id);
        start_times.push(start_time);
        end_times.push(end_time);
        tags_list.push(serde_json::to_string(&span.tags).unwrap());
    }

    if span_ids.is_empty() {
        return Ok(RecordBatch::new_empty(schema_span()));
    }

    Ok(RecordBatch::try_new(
        schema_span(),
        vec![
            Arc::new(UInt64Array::from(span_ids)),
            Arc::new(UInt64Array::from(parent_ids)),
            Arc::new(UInt64Array::from(trace_ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(process_ids)),
            Arc::new(Int64Array::from(start_times)),
            Arc::new(Int64Array::from(end_times)),
            Arc::new(StringArray::from(tags_list)),
        ],
    )?)
}

pub fn convert_log_to_record_batch(logs: Vec<Log>) -> Result<RecordBatch> {
    let mut data = vec![];
    for log in logs {
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
        data.push(JsonValue::Object(map));
    }

    let inferred_schema = infer_json_schema_from_iterator(data.iter().map(Ok))?;
    let mut decoder = ReaderBuilder::new(Arc::new(inferred_schema)).build_decoder()?;
    decoder.serialize(&data)?;
    let batch = decoder.flush()?.expect("Empty record batch");
    Ok(batch)
}

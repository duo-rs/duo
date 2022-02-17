use std::{num::NonZeroU64, time::SystemTime};

use jage_api as proto;
use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_json::value::Value::Null;

use crate::{Log, Process, Span, TraceExt};

use super::JaegerData;

struct SpanExt<'a> {
    inner: &'a Span,
    trace_id: NonZeroU64,
    process: String,
}

struct KvFields<'a>(&'a String, &'a proto::Value);

struct ReferenceType {
    trace_id: NonZeroU64,
    span_id: NonZeroU64,
}

impl Serialize for ReferenceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("refType", "CHILD_OF")?;
        map.serialize_entry("traceID", &self.trace_id.to_string())?;
        map.serialize_entry("spanID", &self.span_id.to_string())?;
        map.end()
    }
}

impl<'a> Serialize for KvFields<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("key", &self.0)?;
        if let Some(value) = self.1.inner.as_ref() {
            match value {
                proto::ValueEnum::StrVal(v) => {
                    map.serialize_entry("type", "string")?;
                    map.serialize_entry("value", &v)?
                }
                proto::ValueEnum::U64Val(v) => {
                    map.serialize_entry("type", "int64")?;
                    map.serialize_entry("value", &v)?
                }
                proto::ValueEnum::I64Val(v) => {
                    map.serialize_entry("type", "int64")?;
                    map.serialize_entry("value", &v)?
                }
                proto::ValueEnum::BoolVal(v) => {
                    map.serialize_entry("type", "bool")?;
                    map.serialize_entry("value", &v)?
                }
            }
        }
        map.end()
    }
}

impl Serialize for Log {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry(
            "timestamp",
            &(self
                .time
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("SystemTime before UNIX EPOCH!"))
            .as_micros(),
        )?;
        let fields: Vec<_> = self
            .fields
            .iter()
            .map(|(key, value)| KvFields(key, value))
            .collect();
        map.serialize_entry("fields", &fields)?;
        map.end()
    }
}

impl<'a> Serialize for SpanExt<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let trace_id = self.trace_id;
        let span = self.inner;

        let mut map = serializer.serialize_map(Some(11))?;
        map.serialize_entry("traceID", &trace_id.to_string())?;
        let references = if let Some(parent_span_id) = span.parent_id {
            vec![ReferenceType {
                span_id: parent_span_id,
                trace_id,
            }]
        } else {
            vec![]
        };
        map.serialize_entry("references", &references)?;

        map.serialize_entry("spanID", &span.id.to_string())?;
        if span.is_intact() {
            map.serialize_entry("operationName", &span.name)?;
        } else {
            // The span isn't intact, add * to the operationName for indication.
            map.serialize_entry("operationName", &format!("{}*", span.name))?;
        }
        map.serialize_entry(
            "startTime",
            &(span
                .start
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("SystemTime before UNIX EPOCH!"))
            .as_micros(),
        )?;
        map.serialize_entry("duration", &span.duration())?;

        let tags: Vec<_> = span
            .tags
            .iter()
            .map(|(key, value)| KvFields(key, value))
            .collect();
        map.serialize_entry("tags", &tags)?;
        map.serialize_entry("logs", &span.logs)?;

        map.serialize_entry("processID", &self.process)?;
        map.serialize_entry("warnings", &Null)?;
        map.serialize_entry("flags", &1)?;

        map.end()
    }
}

impl Serialize for TraceExt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let trace = &self.inner;

        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("traceID", &trace.id.to_string())?;
        map.serialize_entry(
            "spans",
            &trace
                .spans
                .iter()
                .map(|span| SpanExt {
                    trace_id: trace.id,
                    inner: span,
                    process: String::from("p1"),
                })
                .collect::<Vec<_>>(),
        )?;
        map.serialize_entry("processes", &self.processes)?;
        map.serialize_entry("warnings", &Null)?;
        map.end()
    }
}

impl Serialize for Process {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("serviceName", &self.name)?;
        let tags: Vec<_> = self
            .tags
            .iter()
            .map(|(key, value)| KvFields(key, value))
            .collect();
        map.serialize_entry("tags", &tags)?;
        map.end()
    }
}

impl<T: Serialize + IntoIterator> Serialize for JaegerData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("data", &self.0)?;
        map.serialize_entry("total", &0)?;
        map.serialize_entry("limit", &0)?;
        map.serialize_entry("offset", &0)?;
        map.serialize_entry("errors", &Null)?;
        map.end()
    }
}

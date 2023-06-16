use std::collections::HashMap;

use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_json::Value;

use crate::{Log, Process, Span, TraceExt};

use super::JaegerData;

struct SpanExt<'a> {
    inner: &'a Span,
    trace_id: u64,
    process_id: &'a String,
}

// Due to Jaeger has different format, here we
// use newtype to reimplement the searialization.
struct JaegerField<'a>((&'a String, &'a Value));
struct JaegerLog<'a>(&'a Log);
struct JaegerProcess<'a>(&'a Process);

struct ReferenceType {
    trace_id: u64,
    span_id: u64,
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

impl<'a> Serialize for JaegerField<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map: <S as Serializer>::SerializeMap = serializer.serialize_map(Some(3))?;
        let (key, value) = self.0;
        map.serialize_entry("key", key)?;
        match value {
            Value::Bool(v) => {
                map.serialize_entry("type", "bool")?;
                map.serialize_entry("value", v)?
            }
            Value::Number(v) => {
                map.serialize_entry("type", "int64")?;
                map.serialize_entry("value", v)?
            }
            Value::String(v) => {
                map.serialize_entry("type", "string")?;
                map.serialize_entry("value", v)?
            }
            _ => {
                // TODO: more types?
            }
        }

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
        map.serialize_entry("startTime", &span.as_micros())?;
        map.serialize_entry("duration", &span.duration().whole_microseconds())?;
        let tags: Vec<_> = span.tags.iter().map(JaegerField).collect();
        map.serialize_entry("tags", &tags)?;
        map.serialize_entry("logs", &span.logs.iter().map(JaegerLog).collect::<Vec<_>>())?;
        map.serialize_entry("processID", &self.process_id)?;
        map.serialize_entry("warnings", &Value::Null)?;
        map.serialize_entry("flags", &1)?;

        map.end()
    }
}

impl Serialize for TraceExt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("traceID", &self.trace_id.to_string())?;
        map.serialize_entry(
            "spans",
            &self
                .spans
                .iter()
                .map(|span| SpanExt {
                    trace_id: span.trace_id,
                    inner: span,
                    process_id: &span.process_id,
                })
                .collect::<Vec<_>>(),
        )?;

        let processes = self
            .processes
            .iter()
            .map(|(key, value)| (key, JaegerProcess(value)))
            .collect::<HashMap<_, _>>();
        map.serialize_entry("processes", &processes)?;
        map.serialize_entry("warnings", &Value::Null)?;
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
        map.serialize_entry("errors", &Value::Null)?;
        map.end()
    }
}

impl<'a> Serialize for JaegerLog<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("timestamp", &self.0.as_micros())?;
        let fields: Vec<_> = self.0.fields.iter().map(JaegerField).collect();
        map.serialize_entry("fields", &fields)?;
        map.end()
    }
}

impl<'a> Serialize for JaegerProcess<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let inner = self.0;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("id", &inner.id)?;
        map.serialize_entry("serviceName", &inner.service_name)?;
        let tags: Vec<_> = inner.tags.iter().map(JaegerField).collect();
        map.serialize_entry("tags", &tags)?;
        map.end()
    }
}

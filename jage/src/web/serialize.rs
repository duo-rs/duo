use std::num::NonZeroU64;

use jage_api as proto;
use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_json::value::Value::Null;

use crate::{Log, Span, Trace};

use super::JaegerData;

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

struct LogFields<'a>(&'a String, &'a proto::Value);

impl<'a> Serialize for LogFields<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("key", &self.0)?;
        if let Some(value) = self.1.inner.as_ref() {
            match value {
                proto::ValueEnum::DebugVal(v) | proto::ValueEnum::StrVal(v) => {
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

// Trace id and Span pair.
struct TraceSpan<'a>(NonZeroU64, &'a Span);

impl<'a> Serialize for TraceSpan<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        struct LogWrapper<'a>(&'a Log);

        impl<'a> Serialize for LogWrapper<'a> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut map = serializer.serialize_map(Some(2))?;
                // TODO: timestamp
                // map.serialize_entry("timestamp", &(self.0.time.timestamp_nanos() / 1000))?;
                let fields: Vec<_> = self
                    .0
                    .fields
                    .iter()
                    .map(|(key, value)| LogFields(key, value))
                    .collect();
                map.serialize_entry("fields", &fields)?;
                map.end()
            }
        }

        let trace_id = self.0;
        let span = self.1;

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
        map.serialize_entry("operationName", &span.name)?;
        // TODO: timestamp
        // map.serialize_entry("startTime", &(span.start_time.timestamp_nanos() / 1000))?;
        map.serialize_entry("duration", &span.duration())?;

        let tags: Vec<_> = span
            .tags
            .iter()
            .map(|(key, value)| LogFields(key, value))
            .collect();
        map.serialize_entry("tags", &tags)?;

        let logs = span
            .logs
            .iter()
            .map(|log| LogWrapper(log))
            .collect::<Vec<LogWrapper>>();
        map.serialize_entry("logs", &logs)?;

        // TODO: processID
        map.serialize_entry("processID", &Null)?;
        map.serialize_entry("warnings", &Null)?;
        map.serialize_entry("flags", &1)?;

        map.end()
    }
}

impl Serialize for Trace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("traceID", &self.id.to_string())?;
        map.serialize_entry(
            "spans",
            &self
                .spans
                .iter()
                .map(|span| TraceSpan(self.id, span))
                .collect::<Vec<_>>(),
        )?;
        // TODO: processes
        map.serialize_entry("processes", &Null)?;
        map.serialize_entry("warnings", &Null)?;
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

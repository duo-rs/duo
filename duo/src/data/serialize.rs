use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::num::NonZeroU64;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tracing::Level;

use duo_api as proto;

use crate::{Log, Process, Span, Trace};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PersistValue {
    String(String),
    U64(u64),
    I64(i64),
    Bool(bool),
    None,
}

impl From<proto::Value> for PersistValue {
    fn from(value: proto::Value) -> Self {
        if let Some(data) = value.inner {
            return match data {
                proto::ValueEnum::StrVal(v) => PersistValue::String(v),
                proto::ValueEnum::U64Val(v) => PersistValue::U64(v),
                proto::ValueEnum::I64Val(v) => PersistValue::I64(v),
                proto::ValueEnum::BoolVal(v) => PersistValue::Bool(v),
            };
        }
        PersistValue::None
    }
}

impl From<PersistValue> for proto::Value {
    fn from(persist_value: PersistValue) -> Self {
        match persist_value {
            PersistValue::String(data) => proto::Value {
                inner: Some(proto::ValueEnum::StrVal(data))
            },
            PersistValue::U64(data) => proto::Value {
                inner: Some(proto::ValueEnum::U64Val(data))
            },
            PersistValue::I64(data) => proto::Value {
                inner: Some(proto::ValueEnum::I64Val(data))
            },
            PersistValue::Bool(data) => proto::Value {
                inner: Some(proto::ValueEnum::BoolVal(data))
            },
            PersistValue::None => proto::Value {
                inner: None
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProcessPersist {
    pub id: String,
    pub service_name: String,
    pub tags: HashMap<String, PersistValue>,
}

impl From<Process> for ProcessPersist {
    fn from(process: Process) -> Self {
        Self {
            id: process.id,
            service_name: process.service_name,
            tags: process.tags.into_iter().map(|(k, v)| (k, PersistValue::from(v))).collect(),
        }
    }
}

impl From<ProcessPersist> for Process {
    fn from(process_persist: ProcessPersist) -> Self {
        Self {
            id: process_persist.id,
            service_name: process_persist.service_name,
            tags: process_persist.tags.into_iter().map(|(k, v)| (k, proto::Value::from(v))).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SpanPersist {
    pub id: NonZeroU64,
    pub parent_id: Option<NonZeroU64>,
    pub name: String,
    pub start: OffsetDateTime,
    pub end: Option<OffsetDateTime>,
    pub tags: HashMap<String, PersistValue>,
    pub process_id: String,
}

impl From<SpanPersist> for Span {
    fn from(span_persist: SpanPersist) -> Self {
        Self {
            id: span_persist.id,
            parent_id: span_persist.parent_id,
            name: span_persist.name,
            start: span_persist.start,
            end: span_persist.end,
            tags: span_persist.tags.into_iter().map(|(k, v)| (k, proto::Value::from(v))).collect(),
            logs: Vec::new(),
            process_id: span_persist.process_id,
        }
    }
}

impl From<Span> for SpanPersist {
    fn from(span: Span) -> Self {
        Self {
            id: span.id,
            parent_id: span.parent_id,
            name: span.name,
            start: span.start,
            end: span.end,
            tags: span.tags.into_iter().map(|(k, v)| (k, PersistValue::from(v))).collect(),
            process_id: span.process_id,
        }
    }
}

impl Hash for SpanPersist {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for SpanPersist {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.name == other.name
    }
}

impl Eq for SpanPersist {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TracePersist {
    pub id: NonZeroU64,
    pub duration: Duration,
    pub time: OffsetDateTime,
    pub spans: HashSet<SpanPersist>,
    pub process_id: String,
}

impl From<Trace> for TracePersist {
    fn from(trace: Trace) -> Self {
        Self {
            id: trace.id,
            duration: trace.duration,
            time: trace.time,
            spans: trace.spans.into_iter().map(|x| SpanPersist::from(x)).collect(),
            process_id: trace.process_id,
        }
    }
}

impl From<TracePersist> for Trace {
    fn from(trace_persist: TracePersist) -> Self {
        Self {
            id: trace_persist.id,
            duration: trace_persist.duration,
            time: trace_persist.time,
            spans: trace_persist.spans.into_iter().map(|x| Span::from(x)).collect(),
            process_id: trace_persist.process_id,
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogPersist {
    pub span_id: Option<NonZeroU64>,
    pub level: u8,
    pub time: OffsetDateTime,
    pub fields: HashMap<String, PersistValue>,
}

impl From<LogPersist> for Log {
    fn from(log_persist: LogPersist) -> Self {
        Self {
            idx: 0,
            span_id: log_persist.span_id,
            level: match log_persist.level {
                0 => Level::TRACE,
                1 => Level::DEBUG,
                2 => Level::INFO,
                3 => Level::WARN,
                // 4 and else
                _ => Level::ERROR,
            },
            time: log_persist.time,
            fields: log_persist.fields.into_iter().map(|(k, v)| (k, proto::Value::from(v))).collect(),
        }
    }
}


impl From<Log> for LogPersist {
    fn from(log: Log) -> Self {
        Self {
            span_id: log.span_id,
            level: match log.level {
                Level::TRACE => 0,
                Level::DEBUG => 1,
                Level::INFO => 2,
                Level::WARN => 3,
                Level::ERROR => 4,
            },
            time: log.time,
            fields: log.fields.into_iter().map(|(k, v)| (k, PersistValue::from(v))).collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::num::NonZeroU64;

    use time::{Duration, OffsetDateTime};

    use crate::data::serialize::{LogPersist, PersistValue, ProcessPersist, SpanPersist, TracePersist};

    #[test]
    fn bincode_process_serialize_test() {
        let process = ProcessPersist {
            id: "id".to_string(),
            service_name: "service_name".to_string(),
            tags: HashMap::from([
                ("Mercury".parse().unwrap(), PersistValue::Bool(true)),
                ("Venus".parse().unwrap(), PersistValue::U64(32)),
            ]),
        };
        let encoded: Vec<u8> = bincode::serialize(&process).unwrap();
        let decoded: ProcessPersist = bincode::deserialize(&encoded[..]).unwrap();
        assert_eq!(process.id, decoded.id);
        assert_eq!(process.service_name, decoded.service_name);
        assert_eq!(process.tags.len(), decoded.tags.len());
    }

    fn get_span_persist() -> SpanPersist {
        SpanPersist {
            id: NonZeroU64::new(1).unwrap(),
            parent_id: NonZeroU64::new(2),
            name: "name".to_string(),
            start: OffsetDateTime::now_utc(),
            end: Some(OffsetDateTime::now_utc()),
            tags: HashMap::from([
                ("Mercury".parse().unwrap(), PersistValue::Bool(true)),
                ("Venus".parse().unwrap(), PersistValue::U64(32)),
            ]),
            process_id: "process_id".to_string(),
        }
    }

    #[test]
    fn bincode_span_serialize_test() {
        let span = get_span_persist();
        let encoded: Vec<u8> = bincode::serialize(&span).unwrap();
        let decoded: SpanPersist = bincode::deserialize(&encoded[..]).unwrap();
        assert_eq!(span.id, decoded.id);
        assert_eq!(span.parent_id, decoded.parent_id);
        assert_eq!(span.name, decoded.name);
        assert_eq!(span.start, decoded.start);
        assert_eq!(span.end, decoded.end);
        assert_eq!(span.process_id, decoded.process_id);
        assert_eq!(span.tags.len(), decoded.tags.len());
    }

    #[test]
    fn bincode_trace_serialize_test() {
        let mut spans = HashSet::new();
        spans.insert(get_span_persist());
        let trace = TracePersist {
            id: NonZeroU64::new(1).unwrap(),
            duration: Duration::default(),
            time: OffsetDateTime::now_utc(),
            spans,
            process_id: "process_id".to_string(),
        };
        let encoded: Vec<u8> = bincode::serialize(&trace).unwrap();
        let decoded: TracePersist = bincode::deserialize(&encoded[..]).unwrap();
        let span = trace.spans.into_iter().next().unwrap();
        let decode_span = decoded.spans.into_iter().next().unwrap();
        // part span
        assert_eq!(span.id, decode_span.id);
        assert_eq!(span.parent_id, decode_span.parent_id);
        assert_eq!(span.name, decode_span.name);
        assert_eq!(span.start, decode_span.start);
        assert_eq!(span.end, decode_span.end);
        assert_eq!(span.process_id, decode_span.process_id);
        assert_eq!(span.tags.len(), decode_span.tags.len());
        // part trace
        assert_eq!(trace.id, decoded.id);
        assert_eq!(trace.duration, decoded.duration);
        assert_eq!(trace.time, decoded.time);
        assert_eq!(trace.process_id, decoded.process_id);
    }

    #[test]
    fn bincode_log_serialize_test() {
        let log = LogPersist {
            span_id: NonZeroU64::new(2),
            level: 0,
            time: OffsetDateTime::now_utc(),
            fields: HashMap::from([
                ("Mercury".parse().unwrap(), PersistValue::Bool(true)),
                ("Venus".parse().unwrap(), PersistValue::U64(32)),
            ]),
        };
        let encoded: Vec<u8> = bincode::serialize(&log).unwrap();
        let decoded: LogPersist = bincode::deserialize(&encoded[..]).unwrap();
        assert_eq!(log.span_id, decoded.span_id);
        assert_eq!(log.level, decoded.level);
        assert_eq!(log.time, decoded.time);
        assert_eq!(log.fields.len(), decoded.fields.len());
    }
}

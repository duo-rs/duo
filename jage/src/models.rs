use jage_api as proto;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    num::NonZeroU64,
    time::SystemTime,
};
use tracing::Level;

#[derive(Debug, Clone)]
pub struct Process {
    pub id: String,
    pub service_name: String,
    pub tags: HashMap<String, proto::Value>,
}

#[derive(Debug, Clone)]
pub struct Trace {
    pub id: NonZeroU64,
    pub duration: i64,
    pub time: SystemTime,
    pub spans: HashSet<Span>,
    pub process_id: String,
}

#[derive(Debug, Clone)]
pub struct Span {
    pub id: NonZeroU64,
    pub parent_id: Option<NonZeroU64>,
    pub name: String,
    pub start: SystemTime,
    pub end: Option<SystemTime>,
    pub tags: HashMap<String, proto::Value>,
    pub logs: Vec<Log>,
    pub process_id: String,
}

#[derive(Debug, Clone)]
pub struct Log {
    /// The numeric id in log collection.
    pub idx: usize,
    /// The span's id the log belong to.
    /// They have no span id if the log emitted out of tracing context.
    pub span_id: Option<NonZeroU64>,
    pub level: Level,
    pub time: SystemTime,
    pub fields: HashMap<String, proto::Value>,
}

#[derive(Debug)]
pub struct TraceExt {
    pub inner: Trace,
    pub processes: HashMap<String, Process>,
}

impl Hash for Span {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for Span {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.name == other.name
    }
}

impl Eq for Span {}

impl Span {
    pub fn duration(&self) -> i64 {
        self.end
            .map(|end| {
                end.duration_since(self.start)
                    .expect("Span start time is earlier than the end time")
                    .as_micros() as i64
            })
            .unwrap_or_default()
    }

    /// Whether the span is intact.
    #[inline]
    pub fn is_intact(&self) -> bool {
        self.end.is_some()
    }
}

impl Trace {
    pub(crate) fn convert_span(&mut self, span: &proto::Span) -> Span {
        let target = Span {
            id: NonZeroU64::new(span.id).expect("Span id cann not be 0"),
            parent_id: span.parent_id.map(NonZeroU64::new).flatten(),
            name: span.name.clone(),
            start: span
                .start
                .clone()
                .map(|timestamp| timestamp.try_into().ok())
                .flatten()
                .unwrap_or_else(SystemTime::now),
            end: span
                .end
                .clone()
                .map(|timestamp| timestamp.try_into().ok())
                .flatten()
                .or_else(|| Some(SystemTime::now())),
            tags: span.tags.clone(),
            logs: Vec::new(),
            process_id: self.process_id.clone(),
        };
        self.duration = self.duration.max(target.duration());
        self.time = self.time.min(target.start);

        target
    }
}

impl From<proto::Log> for Log {
    fn from(log: proto::Log) -> Self {
        Log {
            idx: 0,
            span_id: log.span_id.map(NonZeroU64::new).flatten(),
            level: proto::Level::from_i32(log.level)
                .map(tracing::Level::from)
                .unwrap_or(tracing::Level::DEBUG),
            time: log
                .time
                .map(|timestamp| timestamp.try_into().ok())
                .flatten()
                .unwrap_or_else(SystemTime::now),
            fields: log.fields,
        }
    }
}

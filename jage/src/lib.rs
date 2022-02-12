use jage_api as proto;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    time::SystemTime,
};
use tracing::Level;

mod aggregator;
mod server;

pub use aggregator::Aggregator;
pub use server::JageServer;

#[derive(Debug)]
pub struct Trace {
    pub app_name: String,
    pub id: u64,
    pub duration: i64,
    pub time: SystemTime,
    pub spans: HashSet<Span>,
    /// Whether the Trace is intact.
    /// Intact means all spans of this trace have both time values: start and end.
    pub intact: bool,
}

#[derive(Debug)]
pub struct Span {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub name: String,
    pub start: SystemTime,
    pub end: Option<SystemTime>,
    pub tags: HashMap<String, proto::Value>,
    pub logs: Vec<Log>,
}

#[derive(Debug)]
pub struct Log {
    pub span_id: u64,
    pub level: Level,
    pub time: SystemTime,
    pub fields: HashMap<String, proto::Value>,
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
}

impl From<&proto::Span> for Span {
    fn from(span: &proto::Span) -> Self {
        Span {
            id: span.id,
            parent_id: span.parent_id,
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
        }
    }
}

impl From<proto::Log> for Log {
    fn from(log: proto::Log) -> Self {
        Log {
            span_id: log.span_id.unwrap_or_default(),
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

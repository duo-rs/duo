use crate::web::deser;
use duo_api as proto;
use serde::Deserialize;
use std::{collections::HashMap, hash::Hash, num::NonZeroU64, time::SystemTime};
use time::{Duration, OffsetDateTime};
use tracing::Level;
#[derive(Debug, Clone)]
pub struct Process {
    pub id: String,
    pub service_name: String,
    pub tags: HashMap<String, proto::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Span {
    pub id: NonZeroU64,
    pub trace_id: NonZeroU64,
    pub parent_id: Option<NonZeroU64>,
    pub process_id: String,
    pub name: String,
    #[serde(deserialize_with = "deser::miscrosecond")]
    pub start: OffsetDateTime,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    pub end: Option<OffsetDateTime>,
    pub tags: serde_json::Value,
    #[serde(skip_deserializing)]
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone)]
pub struct Log {
    /// The numeric id in log collection.
    pub idx: usize,
    /// The span's id the log belong to.
    /// They have no span id if the log emitted out of tracing context.
    pub span_id: Option<NonZeroU64>,
    pub trace_id: Option<NonZeroU64>,
    pub level: Level,
    pub time: OffsetDateTime,
    pub fields: HashMap<String, proto::Value>,
}

#[derive(Debug)]
pub struct TraceExt {
    pub trace_id: NonZeroU64,
    pub spans: Vec<Span>,
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
    pub fn as_micros(&self) -> i64 {
        (self.start.unix_timestamp_nanos() / 1000) as i64
    }

    pub fn duration(&self) -> Duration {
        self.end.map(|end| end - self.start).unwrap_or_default()
    }

    /// Whether the span is intact.
    /// Intact means the span have both time values: start and end.
    #[inline]
    pub fn is_intact(&self) -> bool {
        self.end.is_some()
    }
}

impl Log {
    pub fn as_micros(&self) -> i64 {
        (self.time.unix_timestamp_nanos() / 1000) as i64
    }
}

impl From<&proto::Span> for Span {
    fn from(raw: &proto::Span) -> Self {
        let mut tags = raw.tags.clone();
        for key in ["@busy", "@idle"] {
            if let Some(proto::Value {
                inner: Some(proto::ValueEnum::U64Val(value)),
            }) = tags.remove(key)
            {
                tags.insert(key.into(), format_timing_value(value).into());
            }
        }

        let fields = tags
            .iter()
            .map(|(key, value)| crate::web::serialize::KvFields(key, value))
            .collect::<Vec<_>>();

        Span {
            id: NonZeroU64::new(raw.id).expect("Span id cann not be 0"),
            trace_id: NonZeroU64::new(raw.trace_id).expect("Trace id cann not be 0"),
            parent_id: raw.parent_id.and_then(NonZeroU64::new),
            process_id: raw.process_id.clone(),
            name: raw.name.clone(),
            start: raw
                .start
                .clone()
                .and_then(|timestamp| {
                    SystemTime::try_from(timestamp)
                        .ok()
                        .map(OffsetDateTime::from)
                })
                .unwrap_or_else(OffsetDateTime::now_utc),
            end: raw
                .end
                .clone()
                .and_then(|timestamp| {
                    SystemTime::try_from(timestamp)
                        .ok()
                        .map(OffsetDateTime::from)
                })
                .or_else(|| Some(OffsetDateTime::now_utc())),
            tags: serde_json::to_value(&fields).unwrap(),
            logs: Vec::new(),
        }
    }
}

impl From<proto::Log> for Log {
    fn from(log: proto::Log) -> Self {
        let level = proto::Level::from_i32(log.level)
            .map(tracing::Level::from)
            .unwrap_or(tracing::Level::DEBUG);

        let mut fields = log.fields;
        fields.insert("level".to_owned(), level.as_str().to_lowercase().into());

        Log {
            idx: 0,
            span_id: log.span_id.and_then(NonZeroU64::new),
            trace_id: log.trace_id.and_then(NonZeroU64::new),
            level,
            time: log
                .time
                .and_then(|timestamp| {
                    SystemTime::try_from(timestamp)
                        .ok()
                        .map(OffsetDateTime::from)
                })
                .unwrap_or_else(OffsetDateTime::now_utc),
            fields,
        }
    }
}

fn format_timing_value(value: u64) -> String {
    let value = value as f64;
    if value < 1000.0 {
        format!("{}us", value)
    } else if value < 1_000_000.0 {
        format!("{:.2}ms", value / 1000.0)
    } else {
        format!("{:.2}s", value / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::format_timing_value;

    #[test]
    fn test_timings_format() {
        assert_eq!(format_timing_value(3), "3us".to_string());
        assert_eq!(format_timing_value(303), "303us".to_string());
        assert_eq!(format_timing_value(3003), "3.00ms".to_string());
        assert_eq!(format_timing_value(3013), "3.01ms".to_string());
        assert_eq!(format_timing_value(300030), "300.03ms".to_string());
        assert_eq!(format_timing_value(3003300), "3.00s".to_string());
        assert_eq!(format_timing_value(3033300), "3.03s".to_string());
        assert_eq!(format_timing_value(3333300), "3.33s".to_string());
        assert_eq!(format_timing_value(33000330), "33.00s".to_string());
        assert_eq!(format_timing_value(33300330), "33.30s".to_string());
    }
}

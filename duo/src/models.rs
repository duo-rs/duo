use crate::web::deser;
use duo_api as proto;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{collections::HashMap, time::SystemTime};
use time::{Duration, OffsetDateTime};
use tracing::Level;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Process {
    pub id: String,
    #[serde(rename = "serviceName")]
    pub service_name: String,
    pub tags: HashMap<String, JsonValue>,
}

#[derive(Clone, Deserialize)]
pub struct Span {
    pub id: u64,
    pub trace_id: u64,
    pub parent_id: Option<u64>,
    pub process_id: String,
    pub name: String,
    #[serde(deserialize_with = "deser::miscrosecond::deserialize")]
    pub start: OffsetDateTime,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    pub end: Option<OffsetDateTime>,
    #[serde(default, deserialize_with = "deser::map_list")]
    pub tags: HashMap<String, JsonValue>,
    #[serde(skip_deserializing)]
    pub logs: Vec<Log>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Log {
    pub process_id: String,
    /// The span's id the log belong to.
    /// They have no span id if the log emitted out of tracing context.
    pub span_id: Option<u64>,
    pub trace_id: Option<u64>,
    // TODO: change level to i32
    #[serde(with = "deser::level")]
    pub level: Level,
    #[serde(with = "deser::miscrosecond")]
    pub time: OffsetDateTime,
    #[serde(flatten)]
    pub fields: HashMap<String, JsonValue>,
}

pub struct TraceExt {
    pub trace_id: u64,
    pub spans: Vec<Span>,
    pub processes: HashMap<String, Process>,
}

impl Span {
    pub fn start_as_micros(&self) -> i64 {
        (self.start.unix_timestamp_nanos() / 1000) as i64
    }

    pub fn end_as_micros(&self) -> Option<i64> {
        self.end.map(|t| (t.unix_timestamp_nanos() / 1000) as i64)
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

    pub fn correlate_span_logs(&mut self, logs: &[Log]) {
        let mut errors = 0;
        self.logs = logs
            .iter()
            .filter(|log| log.span_id == Some(self.id))
            .inspect(|log| errors += (log.level == Level::ERROR) as i32)
            .cloned()
            .collect();

        // Auto insert 'error = true' tag, this will help Jaeger UI show error icon.
        if errors > 0 {
            self.tags
                .insert(String::from("error"), serde_json::Value::Bool(true));
        }
    }
}

impl Log {
    pub fn as_micros(&self) -> i64 {
        (self.time.unix_timestamp_nanos() / 1000) as i64
    }
}

impl From<&proto::Span> for Span {
    fn from(span: &proto::Span) -> Self {
        let mut raw_tags = span.tags.clone();
        for key in ["busy", "idle"] {
            if let Some(proto::Value {
                inner: Some(proto::ValueEnum::U64Val(value)),
            }) = raw_tags.remove(key)
            {
                raw_tags.insert(key.into(), format_timing_value(value).into());
            }
        }

        let tags = raw_tags
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect();

        Span {
            id: span.id,
            trace_id: span.trace_id,
            parent_id: span.parent_id,
            process_id: span.process_id.clone(),
            name: span.name.clone(),
            start: span
                .start
                .clone()
                .and_then(|timestamp| {
                    SystemTime::try_from(timestamp)
                        .ok()
                        .map(OffsetDateTime::from)
                })
                .unwrap_or_else(OffsetDateTime::now_utc),
            end: span
                .end
                .clone()
                .and_then(|timestamp| {
                    SystemTime::try_from(timestamp)
                        .ok()
                        .map(OffsetDateTime::from)
                })
                .or_else(|| Some(OffsetDateTime::now_utc())),
            tags,
            logs: Vec::new(),
        }
    }
}

impl From<proto::Log> for Log {
    fn from(log: proto::Log) -> Self {
        let level = proto::Level::from_i32(log.level)
            .map(tracing::Level::from)
            .unwrap_or(tracing::Level::DEBUG);

        let fields = log
            .fields
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect();
        Log {
            process_id: log.process_id,
            span_id: log.span_id,
            trace_id: log.trace_id,
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

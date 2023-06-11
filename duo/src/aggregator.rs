use duo_api as proto;

use std::mem;

use crate::{Log, Span};

#[derive(Debug, Default)]
pub struct Aggregator {
    // <span_id, Span>
    spans: Vec<proto::Span>,
    logs: Vec<proto::Log>,
}

pub struct AggregatedData {
    pub spans: Vec<Span>,
    pub logs: Vec<Log>,
}

impl Aggregator {
    pub fn new() -> Self {
        Aggregator {
            spans: Vec::new(),
            logs: Vec::new(),
        }
    }

    pub fn record_span(&mut self, raw: proto::Span) {
        if let Some(span) = self.spans.iter_mut().find(|s| s.id == raw.id) {
            if raw.parent_id.is_some() {
                span.parent_id = raw.parent_id;
            }

            if !raw.tags.is_empty() {
                span.tags.extend(raw.tags);
            }
            span.end = raw.end;
        } else {
            self.spans.push(raw);
        }
    }

    #[inline]
    pub fn record_log(&mut self, log: proto::Log) {
        self.logs.push(log);
    }

    /// Aggregate recorded data into [`AggregatedData`].
    pub fn aggregate(&mut self) -> AggregatedData {
        // Remove all spans of intact spans.
        let mut spans = Vec::new();
        self.spans.retain(|span| {
            if span.end.is_some() {
                spans.push(Span::from(span));
                false
            } else {
                true
            }
        });
        let logs = mem::take(&mut self.logs);
        AggregatedData {
            spans,
            logs: logs.into_iter().map(Log::from).collect(),
        }
    }
}

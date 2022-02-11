use jage_api as proto;

use std::collections::{hash_map::Entry, HashMap};

#[derive(Debug, Default)]
pub struct Aggregator {
    spans: HashMap<u64, proto::Span>,
    logs: Vec<proto::Log>,
}

impl Aggregator {
    pub fn new() -> Self {
        Aggregator {
            spans: HashMap::default(),
            logs: Vec::new(),
        }
    }

    pub fn record_span(&mut self, span: proto::Span) {
        match self.spans.entry(span.id) {
            Entry::Occupied(mut entry) => {
                let target_span = entry.get_mut();

                if span.parent_id.is_some() {
                    target_span.parent_id = span.parent_id;
                }

                if !span.tags.is_empty() {
                    target_span.tags.extend(span.tags);
                }
                target_span.end = span.end;
            }
            Entry::Vacant(entry) => {
                entry.insert(span);
            }
        }
    }

    #[inline]
    pub fn record_log(&mut self, log: proto::Log) {
        self.logs.push(log);
    }

    pub fn aggregate(&mut self) {}
}

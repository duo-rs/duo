use duo_api as proto;
use time::{Duration, OffsetDateTime};

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    mem,
    num::NonZeroU64,
};

use crate::{Log, Trace};

#[derive(Debug, Default)]
pub struct Aggregator {
    // <span_id, Span>
    spans: HashMap<u64, proto::Span>,
    logs: Vec<proto::Log>,
}

#[derive(Debug)]
pub struct AggregatedData {
    // <trace_id, Trace>
    pub traces: HashMap<NonZeroU64, Trace>,
    pub logs: Vec<Log>,
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

    /// Aggregate recorded data into [`AggregatedData`].
    pub fn aggregate(&mut self) -> AggregatedData {
        let mut traces = HashMap::new();
        self.spans.values().for_each(|span| {
            let trace_id = NonZeroU64::new(span.trace_id).expect("trace id cannot be 0");
            traces
                .entry(trace_id)
                .or_insert(Trace {
                    process_id: span.process_id.clone(),
                    id: trace_id,
                    duration: Duration::default(),
                    time: OffsetDateTime::now_utc(),
                    spans: HashSet::new(),
                })
                .merge_span(span);
        });

        let intact_trace_ids = traces
            .iter()
            .filter_map(|(id, trace)| trace.is_intact().then(|| id.get()))
            .collect::<Vec<_>>();

        // Remove all spans of intact traces.
        self.spans
            .retain(|_, span| intact_trace_ids.contains(&span.trace_id));

        let capacity = self.logs.capacity();
        let logs = mem::replace(&mut self.logs, Vec::with_capacity(capacity));
        AggregatedData {
            traces,
            logs: logs.into_iter().map(Log::from).collect(),
        }
    }
}

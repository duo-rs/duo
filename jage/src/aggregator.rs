use jage_api as proto;
use time::OffsetDateTime;

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

    pub fn aggregate(&mut self) -> AggregatedData {
        let mut traces = HashMap::new();
        self.spans.values().for_each(|span| {
            let trace_id = span.trace_id;
            let (trace, is_intact) = traces.entry(trace_id).or_insert((
                Trace {
                    process_id: span.process_id.clone(),
                    id: NonZeroU64::new(trace_id).expect("trace id cannot be 0"),
                    duration: 0,
                    // FIXME: trace's time should be the first span's time
                    time: OffsetDateTime::now_utc(),
                    spans: HashSet::new(),
                },
                // Whether the trace is intact.
                // Intact means all spans of this trace have both time values: start and end.
                true,
            ));
            let target_span = trace.convert_span(span);

            if span.end.is_none() {
                *is_intact = false;
            }

            trace.spans.replace(target_span);
        });

        // Remove all spans of intact traces.
        traces.values().for_each(|(trace, is_intact)| {
            if *is_intact {
                self.spans.retain(|_, span| span.trace_id != trace.id.get());
            }
        });

        let capacity = self.logs.capacity();
        let logs = mem::replace(&mut self.logs, Vec::with_capacity(capacity));
        AggregatedData {
            traces: traces
                .into_iter()
                .map(|(_, (trace, _))| (trace.id, trace))
                .collect(),
            logs: logs.into_iter().map(Log::from).collect(),
        }
    }
}

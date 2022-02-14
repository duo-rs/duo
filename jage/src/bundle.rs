use std::{collections::HashMap, num::NonZeroU64};

use crate::{aggregator::AggregatedData, Log, Trace};

#[derive(Debug, Default)]
pub struct TraceBundle {
    // <trace_id, Trace>
    traces: HashMap<NonZeroU64, Trace>,
    logs: Vec<Log>,
    // <span_id, Vec<log id>>
    span_log_map: HashMap<NonZeroU64, Vec<usize>>,
}

impl TraceBundle {
    pub fn new() -> Self {
        TraceBundle::default()
    }

    // Merge aggregated data.
    pub(crate) fn merge_data(&mut self, data: AggregatedData) {
        data.traces.into_iter().for_each(|(id, trace)| {
            self.traces.insert(id, trace);
        });

        // Reserve capacity advanced.
        self.logs.reserve(data.logs.len());
        let base_idx = self.logs.len();
        data.logs.into_iter().enumerate().for_each(|(i, mut log)| {
            let idx = base_idx + i;

            // Exclude those logs without span_id,
            // normally they are not emitted in tracing context.
            if let Some(span_id) = log.span_id {
                let log_idxs = self.span_log_map.entry(span_id).or_default();
                log_idxs.push(idx);
            }

            log.idx = idx;
            self.logs.push(log);
        });

        println!(
            "After merge - traces: {}, logs: {}, span_log_map: {:?}",
            self.traces.len(),
            self.logs.len(),
            self.span_log_map
        );
    }
}

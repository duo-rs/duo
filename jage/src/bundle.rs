use std::{collections::HashMap, num::NonZeroU64};

use crate::{aggregator::AggregatedData, Log, Process, Trace};
use jage_api as proto;

#[derive(Default)]
pub struct TraceBundle {
    // Collection of process.
    prcoesses: Vec<Process>,
    // <trace_id, Trace>
    traces: HashMap<NonZeroU64, Trace>,
    logs: Vec<Log>,
    // <span_id, Vec<log id>>
    span_log_map: HashMap<NonZeroU64, Vec<usize>>,
}

impl std::fmt::Debug for TraceBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceBundle")
            .field("prcoesses", &self.prcoesses)
            .field("traces", &self.traces.len())
            .field("logs", &self.logs.len())
            .field("span_log_map", &self.span_log_map.len())
            .finish()
    }
}

impl TraceBundle {
    pub fn new() -> Self {
        TraceBundle::default()
    }

    /// Register new process and return the process id.
    pub(crate) fn register_process(&mut self, process: proto::Process) -> u32 {
        // TODO: generate new process id
        let process_id = self.prcoesses.len() as u32 + 1;
        self.prcoesses.push(Process {
            id: process_id,
            name: process.name,
            tags: process.tags,
        });
        process_id
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
    }
}

use std::{collections::HashMap, num::NonZeroU64};

use crate::{aggregator::AggregatedData, Log, Process, Trace, TraceExt};
use jage_api as proto;

#[derive(Default)]
pub struct Warehouse {
    // Collection of services.
    services: HashMap<String, Vec<Process>>,
    // <trace_id, Trace>
    traces: HashMap<NonZeroU64, Trace>,
    logs: Vec<Log>,
    // <span_id, Vec<log id>>
    span_log_map: HashMap<NonZeroU64, Vec<usize>>,
}

impl std::fmt::Debug for Warehouse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Warehouse")
            .field("services", &self.services)
            .field("traces", &self.traces.len())
            .field("logs", &self.logs.len())
            .field("span_log_map", &self.span_log_map.len())
            .finish()
    }
}

impl Warehouse {
    pub fn new() -> Self {
        Warehouse::default()
    }

    pub(crate) fn services(&self) -> Vec<String> {
        self.services.keys().cloned().collect()
    }

    fn processes(&self) -> HashMap<String, Process> {
        self.services
            .values()
            .enumerate()
            .flat_map(|(i, processes)| {
                processes
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(j, process)| (format!("p{}-{}", i, j), process))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(crate) fn transform_traces(&self, process_id: u32) -> Vec<TraceExt> {
        let processes = self.processes();
        self.traces
            .values()
            .filter(|trace| trace.process_id == process_id)
            .cloned()
            .map(|mut trace| {
                trace.spans = trace
                    .spans
                    .into_iter()
                    .filter_map(|mut span| {
                        if let Some(idxs) = self.span_log_map.get(&span.id) {
                            span.logs = idxs
                                .iter()
                                .filter_map(|idx| self.logs.get(*idx))
                                .cloned()
                                .collect();
                            Some(span)
                        } else {
                            None
                        }
                    })
                    .collect();
                TraceExt {
                    inner: trace,
                    processes: processes.clone(),
                }
            })
            .collect()
    }

    /// Register new process and return the process id.
    pub(crate) fn register_process(&mut self, process: proto::Process) -> u32 {
        let service_name = process.name;
        let service_processes = self.services.entry(service_name.clone()).or_default();

        // TODO: generate new process id
        let process_id = service_processes.len() as u32 + 1;
        service_processes.push(Process {
            id: process_id,
            service_name,
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

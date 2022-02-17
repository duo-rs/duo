use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
};

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

    pub(crate) fn span_names(&self, service: &str) -> HashSet<String> {
        let process_prefix = format!("{}:", service);
        self.traces
            .values()
            .filter(|trace| trace.process_id.starts_with(&process_prefix))
            .flat_map(|trace| {
                trace
                    .spans
                    .iter()
                    .map(|span| span.name.clone())
                    .collect::<HashSet<_>>()
            })
            .collect()
    }

    fn processes(&self) -> HashMap<String, Process> {
        self.services
            .iter()
            .flat_map(|(service_name, processes)| {
                processes
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, process)| (format!("{}:{}", &service_name, i), process))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(crate) fn transform_traces(&self, service: &str) -> Vec<TraceExt> {
        let process_prefix = format!("{}:", service);
        let processes = self.processes();
        self.traces
            .values()
            .filter(|trace| trace.process_id.starts_with(&process_prefix))
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
    pub(crate) fn register_process(&mut self, process: proto::Process) -> String {
        let service_name = process.name;
        let service_processes = self.services.entry(service_name.clone()).or_default();

        // TODO: generate new process id
        let process_id = format!("{}:{}", &service_name, service_processes.len());
        service_processes.push(Process {
            id: process_id.clone(),
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

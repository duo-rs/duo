use std::collections::{HashMap, HashSet};

use crate::{Process, TraceExt, Warehouse};

use super::routes::QueryParameters;

pub(super) struct TraceQuery<'a>(&'a Warehouse);

impl<'a> TraceQuery<'a> {
    pub(super) fn new(warehouse: &'a Warehouse) -> Self {
        TraceQuery(warehouse)
    }

    pub(super) fn filter_traces(&self, p: QueryParameters) -> Vec<TraceExt> {
        let traces = self.0.traces();
        let logs = self.0.logs();
        let span_log_map = self.0.span_log_map();

        let processes = self.processes();
        let process_prefix = format!("{}:", p.service);
        traces
            .values()
            .filter(|trace| trace.process_id.starts_with(&process_prefix))
            .cloned()
            .map(|mut trace| {
                trace.spans = trace
                    .spans
                    .into_iter()
                    .filter_map(|mut span| {
                        if let Some(idxs) = span_log_map.get(&span.id) {
                            span.logs = idxs
                                .iter()
                                .filter_map(|idx| logs.get(*idx))
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

    pub(super) fn span_names(&self, service: &str) -> HashSet<String> {
        let process_prefix = format!("{}:", service);
        self.0
            .traces()
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

    pub(super) fn processes(&self) -> HashMap<String, Process> {
        self.0
            .services()
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

    pub(super) fn service_names(&self) -> Vec<String> {
        self.0.services().keys().cloned().collect()
    }
}

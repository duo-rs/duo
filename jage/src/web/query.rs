use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
};

use crate::{Process, TraceExt, Warehouse};

use super::routes::QueryParameters;

const DEFAUT_TRACE_LIMIT: usize = 20;

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
        let limit = p.limit.unwrap_or(DEFAUT_TRACE_LIMIT);
        traces
            .values()
            .filter(|trace| {
                if !trace.process_id.starts_with(&process_prefix) {
                    return false;
                }

                if let Some(span_name) = p.operation.as_ref() {
                    if !trace.spans.iter().any(|span| &*span.name == span_name) {
                        return false;
                    }
                }

                match (p.start, p.end) {
                    (Some(start), None) if trace.time < start => return false,
                    (None, Some(end)) if trace.time > end => return false,
                    (Some(start), Some(end)) if trace.time < start || trace.time > end => {
                        return false
                    }
                    _ => {}
                }

                match (p.min_duration, p.max_duration) {
                    (Some(min), None) if trace.duration < min => return false,
                    (None, Some(max)) if trace.duration > max => return false,
                    (Some(min), Some(max)) if trace.duration < min || trace.duration > max => {
                        return false
                    }
                    _ => {}
                }

                true
            })
            .take(limit)
            .cloned()
            .map(|mut trace| {
                trace.spans = trace
                    .spans
                    .into_iter()
                    .map(|mut span| {
                        if let Some(idxs) = span_log_map.get(&span.id) {
                            span.logs = idxs
                                .iter()
                                .filter_map(|idx| logs.get(*idx))
                                .cloned()
                                .collect();
                        }
                        span
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

    pub(super) fn get_trace_by_id(&self, trace_id: &NonZeroU64) -> Option<TraceExt> {
        let processes = self.processes();
        self.0
            .traces()
            .get(trace_id)
            .cloned()
            .map(|trace| TraceExt {
                inner: trace,
                processes,
            })
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

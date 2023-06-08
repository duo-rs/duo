use crate::query::PartitionQuery;
use crate::{Process, Span, TraceExt, Warehouse};
use datafusion::prelude::*;
use std::borrow::Cow;
use std::vec;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
};

use super::routes::QueryParameters;

const DEFAUT_TRACE_LIMIT: usize = 20;

pub(super) struct TraceQuery<'a>(&'a Warehouse);

impl<'a> TraceQuery<'a> {
    pub(super) fn new(warehouse: &'a Warehouse) -> Self {
        TraceQuery(warehouse)
    }

    pub(super) async fn filter_traces(&self, p: QueryParameters) -> Vec<TraceExt> {
        let processes = self.processes();
        let process_prefix = format!("{}:", p.service);
        let limit = p.limit.unwrap_or(DEFAUT_TRACE_LIMIT);
        // <trace_id, spans>
        let mut traces = HashMap::<NonZeroU64, Vec<Cow<Span>>>::new();
        let pq = PartitionQuery::new(".".into(), p.start.unwrap(), p.end.unwrap());
        let expr = col("process_id").like(lit(format!("{process_prefix}%")));
        let spans = pq
            .query_span(expr)
            .await
            .unwrap()
            .into_iter()
            .map(|value| Cow::<Span>::Owned(serde_json::from_value::<Span>(value).unwrap()));
        println!("spans from parquet: {}", spans.len());
        let mut total_spans = vec![];
        total_spans.extend(self.0.spans().iter().map(Cow::Borrowed));
        for span in spans {
            if traces.contains_key(&span.trace_id) {
                traces
                    .entry(span.trace_id)
                    .and_modify(|spans| spans.push(span))
                    .or_insert_with(Vec::new);
                continue;
            }

            if !span.process_id.starts_with(&process_prefix) {
                continue;
            }
            if let Some(span_name) = p.operation.as_ref() {
                if &*span.name != span_name {
                    continue;
                }
            }

            if span.parent_id.is_some() {
                continue;
            }

            match (p.start, p.end) {
                (Some(start), None) if span.start < start => continue,
                (None, Some(end)) if span.start > end => continue,
                (Some(start), Some(end)) if span.start < start || span.start > end => continue,
                _ => {}
            }

            let duration = span.duration();
            match (p.min_duration, p.max_duration) {
                (Some(min), None) if duration < min => continue,
                (None, Some(max)) if duration > max => continue,
                (Some(min), Some(max)) if duration < min || duration > max => continue,
                _ => {}
            }

            traces
                .entry(span.trace_id)
                .and_modify(|spans| spans.push(span))
                .or_insert_with(Vec::new);
        }

        traces
            .into_iter()
            .take(limit)
            .map(|(trace_id, spans)| TraceExt {
                trace_id,
                spans: spans
                    .iter()
                    .map(|span| {
                        let mut span = span.clone().into_owned();
                        self.0.correlate_span_logs(&mut span);
                        span
                    })
                    .collect(),
                processes: processes.clone(),
            })
            .collect()
    }

    pub(super) fn span_names(&self, service: &str) -> HashSet<String> {
        let process_prefix = format!("{}:", service);

        self.0
            .spans()
            .iter()
            .filter_map(|span| {
                if span.process_id.starts_with(&process_prefix) {
                    Some(span.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn get_trace_by_id(&self, trace_id: NonZeroU64) -> Option<TraceExt> {
        let processes = self.processes();
        let spans = self
            .0
            .spans()
            .iter()
            .filter(|span| span.trace_id == trace_id)
            .cloned()
            .collect::<Vec<_>>();
        if spans.is_empty() {
            None
        } else {
            Some(TraceExt {
                trace_id,
                spans: spans
                    .into_iter()
                    .map(|mut span| {
                        self.0.correlate_span_logs(&mut span);
                        span
                    })
                    .collect(),
                processes,
            })
        }
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

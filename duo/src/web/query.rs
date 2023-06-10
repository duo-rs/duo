use crate::query::PartitionQuery;
use crate::{Span, TraceExt, Warehouse};
use datafusion::prelude::*;
use std::borrow::Cow;
use std::{collections::HashMap, num::NonZeroU64};

use super::routes::QueryParameters;

const DEFAUT_TRACE_LIMIT: usize = 20;

pub(super) struct TraceQuery<'a>(&'a Warehouse);

impl<'a> TraceQuery<'a> {
    pub(super) fn new(warehouse: &'a Warehouse) -> Self {
        TraceQuery(warehouse)
    }

    pub(super) async fn filter_traces(&self, p: QueryParameters) -> Vec<TraceExt> {
        let processes = self.0.processes();
        let process_prefix = p.service;
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
            .map(Cow::Owned);
        println!("spans from parquet: {}", spans.len());

        let mut total_spans = self.0.spans().iter().map(Cow::Borrowed).collect::<Vec<_>>();
        total_spans.extend(spans);
        for span in total_spans {
            if traces.contains_key(&span.trace_id) {
                traces
                    .entry(span.trace_id)
                    .or_insert_with(Vec::new)
                    .push(span);
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
                .or_insert_with(Vec::new)
                .push(span);
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

    pub(super) async fn get_trace_by_id(&self, trace_id: NonZeroU64) -> Option<TraceExt> {
        let mut trace_spans = self
            .0
            .spans()
            .iter()
            .filter(|span| span.trace_id == trace_id)
            .map(Cow::Borrowed)
            .collect::<Vec<_>>();
        if trace_spans.is_empty() {
            let spans = PartitionQuery::recent_hours(".".into(), 12)
                .query_span(col("trace_id").eq(lit(trace_id.get())))
                .await
                .unwrap()
                .into_iter()
                .map(Cow::Owned);
            trace_spans.extend(spans);
        }

        if trace_spans.is_empty() {
            None
        } else {
            Some(TraceExt {
                trace_id,
                spans: trace_spans
                    .into_iter()
                    .map(|span| {
                        let mut span = span.clone().into_owned();
                        self.0.correlate_span_logs(&mut span);
                        span
                    })
                    .collect(),
                processes: self.0.processes(),
            })
        }
    }
}

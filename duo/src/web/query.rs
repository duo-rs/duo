use crate::query::PartitionQuery;
use crate::{MemoryStore, Span, TraceExt};
use datafusion::prelude::*;
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};
use tracing::debug;

use super::trace::QueryParameters;

const DEFAUT_TRACE_LIMIT: usize = 20;

pub(super) struct TraceQuery<'a>(&'a MemoryStore);

impl<'a> TraceQuery<'a> {
    pub(super) fn new(memory_store: &'a MemoryStore) -> Self {
        TraceQuery(memory_store)
    }

    pub(super) async fn filter_traces(&self, p: QueryParameters) -> Vec<TraceExt> {
        let processes = self.0.processes();
        let process_prefix = p.service;
        let limit = p.limit.unwrap_or(DEFAUT_TRACE_LIMIT);
        // <trace_id, spans>
        let mut traces = HashMap::<u64, Vec<Span>>::new();

        let expr = col("process_id").like(lit(format!("{process_prefix}%")));
        let mut total_spans = self.0.query_span(expr.clone()).await.unwrap();

        // Don't query data from storage in memory mode
        let pq = if crate::is_memory_mode() {
            None
        } else {
            Some(PartitionQuery::new(
                ".".into(),
                p.start
                    .unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                p.end.unwrap_or(OffsetDateTime::now_utc()),
            ))
        };

        if let Some(pq) = pq.as_ref() {
            let spans = pq.query_span(expr).await.unwrap_or_default();
            debug!("spans from parquet: {}", spans.len());
            total_spans.extend(spans);
        }

        for span in total_spans {
            if traces.contains_key(&span.trace_id) {
                traces.entry(span.trace_id).or_default().push(span);
                continue;
            }

            if !span.process_id.starts_with(&process_prefix) {
                continue;
            }
            if let Some(span_name) = p.operation.as_ref() {
                if &span.name != span_name {
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

            traces.entry(span.trace_id).or_default().push(span);
        }

        let trace_ids = traces.keys().collect::<Vec<_>>();

        let expr =
            col("trace_id").in_list(trace_ids.into_iter().map(|id| lit(*id)).collect(), false);
        let mut trace_logs = self.0.query_log(expr.clone()).await.unwrap();
        if let Some(pq) = pq.as_ref() {
            let logs = pq.query_log(expr).await.unwrap_or_default();
            debug!("span logs from parquet: {}", logs.len());
            trace_logs.extend(logs);
        }
        traces
            .into_iter()
            .take(limit)
            .map(|(trace_id, spans)| TraceExt {
                trace_id,
                spans: spans
                    .iter()
                    .map(|span| {
                        let mut span = span.clone();
                        span.correlate_span_logs(&trace_logs);
                        span
                    })
                    .collect(),
                processes: processes.clone(),
            })
            .collect()
    }

    pub(super) async fn get_trace_by_id(&self, trace_id: u64) -> Option<TraceExt> {
        // Don't query data from storage in memory mode
        let pq = if crate::is_memory_mode() {
            None
        } else {
            Some(PartitionQuery::recent_hours(".".into(), 12))
        };

        let expr = col("trace_id").eq(lit(trace_id));
        let mut trace_spans: Vec<Span> = self.0.query_span(expr.clone()).await.unwrap_or_default();
        if let Some(pq) = pq.as_ref() {
            let spans = pq.query_span(expr.clone()).await.unwrap_or_default();
            trace_spans.extend(spans);
        }

        if trace_spans.is_empty() {
            None
        } else {
            let mut trace_logs = self.0.query_log(expr.clone()).await.unwrap_or_default();
            if let Some(pq) = pq.as_ref() {
                let logs = pq.query_log(expr).await.unwrap();
                debug!("trace `{trace_id}` logs from parquet: {}", logs.len());
                trace_logs.extend(logs);
            }
            Some(TraceExt {
                trace_id,
                spans: trace_spans
                    .into_iter()
                    .map(|span| {
                        let mut span = span.clone();
                        span.correlate_span_logs(&trace_logs);
                        span
                    })
                    .collect(),
                processes: self.0.processes(),
            })
        }
    }
}

use crate::query::QueryEngine;
use crate::{Log, MemoryStore, Span, TraceExt};
use datafusion::arrow::array::StringArray;
use datafusion::prelude::*;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::trace::QueryParameters;

const DEFAUT_TRACE_LIMIT: usize = 20;

pub(super) async fn filter_traces(
    memory_store: Arc<RwLock<MemoryStore>>,
    p: QueryParameters,
) -> Vec<TraceExt> {
    let process_prefix = p.service;
    let limit = p.limit.unwrap_or(DEFAUT_TRACE_LIMIT);
    // <trace_id, spans>
    let mut traces = HashMap::<u64, Vec<Span>>::new();

    let expr = col("process_id").like(lit(format!("{process_prefix}%")));

    let processes = { memory_store.read().processes() };
    let query_engine = QueryEngine::new(memory_store);
    let total_spans = query_engine
        .query_span(expr.clone())
        .range(p.start, p.end)
        .collect::<Span>()
        .await
        .unwrap_or_default();

    for span in total_spans {
        if traces.contains_key(&span.trace_id) {
            traces.entry(span.trace_id).or_default().push(span);
            continue;
        }

        // Filter the root span, the child spans will be added in above.
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

    let expr = col("trace_id").in_list(trace_ids.into_iter().map(|id| lit(*id)).collect(), false);
    let trace_logs = query_engine
        .query_log(expr.clone())
        .range(p.start, p.end)
        .collect::<Log>()
        .await
        .unwrap_or_default();

    traces
        .into_iter()
        .take(limit)
        .map(|(trace_id, spans)| TraceExt {
            trace_id,
            processes: processes.clone(),
            spans: spans
                .iter()
                .map(|span| {
                    let mut span = span.clone();
                    span.correlate_span_logs(&trace_logs);
                    span
                })
                .collect(),
        })
        .collect()
}

pub(super) async fn get_trace_by_id(
    memory_store: Arc<RwLock<MemoryStore>>,
    trace_id: u64,
) -> Option<TraceExt> {
    let expr = col("trace_id").eq(lit(trace_id));
    let processes = { memory_store.read().processes() };
    let query_engine = QueryEngine::new(memory_store);
    let trace_spans = query_engine
        .query_span(expr.clone())
        .collect::<Span>()
        .await
        .unwrap_or_default();

    if trace_spans.is_empty() {
        None
    } else {
        let trace_logs = query_engine
            .query_log(expr)
            .collect::<Log>()
            .await
            .unwrap_or_default();
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
            processes,
        })
    }
}

pub(super) async fn aggregate_span_names(
    memory_store: Arc<RwLock<MemoryStore>>,
    service: &str,
) -> HashSet<String> {
    let query_engine = QueryEngine::new(memory_store);
    let expr = col("process_id").like(lit(format!("{service}%")));
    let batches = query_engine
        .aggregate_span_names(expr)
        .collect()
        .await
        .unwrap_or_default();

    batches
        .into_iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>()
}

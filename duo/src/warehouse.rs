use std::{collections::HashMap, num::NonZeroU64};

use crate::{
    aggregator::AggregatedData,
    arrow::{LogRecordBatchBuilder, SpanRecordBatchBuilder, TraceRecordBatchBuilder},
    partition::PartitionWriter,
    Log, Process, Trace,
};
use duo_api as proto;

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

    pub(crate) fn services(&self) -> &HashMap<String, Vec<Process>> {
        &self.services
    }

    pub(crate) fn traces(&self) -> &HashMap<NonZeroU64, Trace> {
        &self.traces
    }

    pub(crate) fn logs(&self) -> &Vec<Log> {
        &self.logs
    }

    pub(crate) fn span_log_map(&self) -> &HashMap<NonZeroU64, Vec<usize>> {
        &self.span_log_map
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

    pub(crate) async fn write_parquet(&self) -> anyhow::Result<()> {
        let pw = PartitionWriter::with_minute();
        let mut trace_record_batch_builder = TraceRecordBatchBuilder::default();
        let mut span_record_batch_builder = SpanRecordBatchBuilder::default();

        for trace in self.traces.values() {
            trace_record_batch_builder.append_trace(trace);
            for span in &trace.spans {
                span_record_batch_builder.append_span(trace.id, span);
            }
        }

        pw.write_partition("trace", trace_record_batch_builder.into_record_batch()?)
            .await?;
        pw.write_partition("span", span_record_batch_builder.into_record_batch()?)
            .await?;

        let mut log_record_batch_builder = LogRecordBatchBuilder::default();
        for log in &self.logs {
            log_record_batch_builder.append_log(log);
        }
        pw.write_partition("log", log_record_batch_builder.into_record_batch()?)
            .await?;
        Ok(())
    }
}

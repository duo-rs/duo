use std::{collections::HashMap, num::NonZeroU64};

use crate::{
    aggregator::AggregatedData,
    arrow::{LogRecordBatchBuilder, SpanRecordBatchBuilder},
    partition::PartitionWriter,
    Log, Process, Span,
};
use duo_api as proto;
use tracing::Level;

#[derive(Default)]
pub struct Warehouse {
    // Collection of services.
    services: HashMap<String, Vec<Process>>,
    spans: Vec<Span>,
    logs: Vec<Log>,
    // <span_id, Vec<log id>>
    span_log_map: HashMap<NonZeroU64, Vec<usize>>,
}

impl std::fmt::Debug for Warehouse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Warehouse")
            .field("services", &self.services)
            .field("spans", &self.spans.len())
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

    pub(crate) fn spans(&self) -> &Vec<Span> {
        &self.spans
    }

    pub(crate) fn correlate_span_logs(&self, span: &mut Span) {
        if let Some(idxs) = self.span_log_map.get(&span.id) {
            let mut errors = 0;
            span.logs = idxs
                .iter()
                .filter_map(|idx| self.logs.get(*idx))
                .inspect(|log| errors += (log.level == Level::ERROR) as i32)
                .cloned()
                .collect();

            // Auto insert 'error = true' tag, this will help Jaeger UI show error icon.
            if errors > 0 {
                span.tags.insert("error".into(), true.into());
            }
        }
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
        self.spans.extend(data.spans);
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

        let mut span_record_batch_builder = SpanRecordBatchBuilder::default();
        for span in &self.spans {
            span_record_batch_builder.append_span(span);
        }
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

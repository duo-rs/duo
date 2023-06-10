use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    mem,
    num::NonZeroU64,
    path::Path,
};

use crate::{
    aggregator::AggregatedData,
    arrow::{LogRecordBatchBuilder, SpanRecordBatchBuilder},
    partition::PartitionWriter,
    web::serialize::KvFields,
    Log, Process, Span,
};
use anyhow::Result;
use duo_api as proto;
use tracing::Level;

#[derive(Default)]
pub struct Warehouse {
    // Collection of services.
    services: HashMap<String, Vec<Process>>,
    pub spans: Vec<Span>,
    pub logs: Vec<Log>,
    // <span_id, Vec<log id>>
    pub span_log_map: HashMap<NonZeroU64, Vec<usize>>,
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

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().join("process.json");
        if !path.exists() {
            return Ok(Self::new());
        }

        let file = File::open(path)?;
        let mut services = HashMap::<String, Vec<_>>::new();
        let data: Vec<Process> = match serde_json::from_reader(file) {
            Ok(data) => data,
            Err(err) => {
                println!("Warning: read process.json failed: {err}");
                return Ok(Self::new());
            }
        };
        data.into_iter().for_each(|process| {
            services
                .entry(process.service_name.clone())
                .and_modify(|vec| vec.push(process))
                .or_insert_with(Vec::new);
        });

        Ok(Warehouse {
            services,
            ..Default::default()
        })
    }

    pub(crate) fn spans(&self) -> &Vec<Span> {
        &self.spans
    }

    pub(super) fn processes(&self) -> HashMap<String, Process> {
        self.services
            .values()
            .flat_map(|processes| {
                processes
                    .iter()
                    .map(|process| (process.id.clone(), process.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(super) fn service_names(&self) -> Vec<String> {
        self.services.keys().cloned().collect()
    }

    pub(super) fn span_names(&self, service: &str) -> HashSet<String> {
        self.spans()
            .iter()
            .filter_map(|span| {
                if span.process_id.starts_with(service) {
                    Some(span.name.clone())
                } else {
                    None
                }
            })
            .collect()
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
                let key = String::from("error");
                let value = proto::Value::from(true);
                let tag = KvFields(&key, &value);
                span.tags.push(serde_json::to_value(tag).unwrap());
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
            tags: process
                .tags
                .iter()
                .map(|(key, value)| {
                    serde_json::to_value(crate::web::serialize::KvFields(key, value)).unwrap()
                })
                .collect::<Vec<_>>(),
        });
        self.write_process(".").unwrap();
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

    fn write_process<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = File::create(path.as_ref().join("process.json"))?;
        file.write_all(
            serde_json::to_string(&self.processes().values().collect::<Vec<_>>())?.as_bytes(),
        )?;
        Ok(())
    }

    pub(crate) async fn write_parquet(&mut self) -> Result<()> {
        let pw = PartitionWriter::with_minute();

        if !self.spans.is_empty() {
            let mut span_record_batch_builder = SpanRecordBatchBuilder::default();
            for span in mem::take(&mut self.spans) {
                span_record_batch_builder.append_span(span);
            }
            pw.write_partition("span", span_record_batch_builder.into_record_batch()?)
                .await?;
        }

        if !self.logs.is_empty() {
            let mut log_record_batch_builder = LogRecordBatchBuilder::default();
            for log in mem::take(&mut self.logs) {
                log_record_batch_builder.append_log(log);
            }
            pw.write_partition("log", log_record_batch_builder.into_record_batch()?)
                .await?;
        }
        Ok(())
    }
}

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    mem,
    path::Path,
};

use crate::{
    aggregator::AggregatedData,
    arrow::{LogRecordBatchBuilder, SpanRecordBatchBuilder},
    partition::PartitionWriter,
    Log, Process, Span,
};
use anyhow::Result;
use duo_api as proto;

#[derive(Default)]
pub struct Warehouse {
    // Collection of services.
    services: HashMap<String, Vec<Process>>,
    pub spans: Vec<Span>,
    pub logs: Vec<Log>,
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

        let data: Vec<Process> = match serde_json::from_reader(File::open(path)?) {
            Ok(data) => data,
            Err(err) => {
                println!("Warning: read process.json failed: {err}");
                return Ok(Self::new());
            }
        };
        let mut services = HashMap::<String, Vec<_>>::new();
        data.into_iter().for_each(|process| {
            services
                .entry(process.service_name.clone())
                .or_insert_with(Vec::new)
                .push(process);
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

    /// Register new process and return the process id.
    pub(crate) fn register_process(&mut self, process: proto::Process) -> Result<String> {
        let service_name = process.name;
        let service_processes = self.services.entry(service_name.clone()).or_default();

        // TODO: generate new process id
        let process_id = format!("{}-{}", &service_name, service_processes.len());
        service_processes.push(Process {
            id: process_id.clone(),
            service_name,
            tags: process
                .tags
                .into_iter()
                .map(|(key, value)| [(key, value.into())].into_iter().collect())
                .collect::<Vec<_>>(),
        });
        self.write_process(".")?;
        Ok(process_id)
    }

    // Merge aggregated data.
    pub(crate) fn merge_data(&mut self, data: AggregatedData) {
        self.spans.extend(data.spans);
        // Reserve capacity advanced.
        self.logs.reserve(data.logs.len());
        self.logs.extend(data.logs);
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

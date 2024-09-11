use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug, fs::File, io::Write, mem, path::Path};

use crate::arrow::{convert_log_to_record_batch, convert_span_to_record_batch};
use crate::ipc::IpcFile;
use crate::{config, schema, Log, Process, Span};
use anyhow::Result;
use arrow_schema::Schema;
use datafusion::arrow::array::RecordBatch;

use duo_api as proto;

pub struct MemoryStore {
    // Collection of services.
    services: HashMap<String, Vec<Process>>,
    pub log_schema: Arc<Schema>,
    pub span_batches: Vec<RecordBatch>,
    pub log_batches: Vec<RecordBatch>,
    pub is_dirty: bool,
}

impl Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("services", &self.services.len())
            .finish()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            services: HashMap::new(),
            log_schema: schema::get_log_schema(),
            span_batches: vec![],
            log_batches: vec![],
            is_dirty: false,
        }
    }

    pub fn load() -> Result<Self> {
        let config = config::load();
        let path = Path::new(&config.data_dir);
        let ipc_file = IpcFile::new();
        let span_batches = ipc_file.read_span_ipc()?;
        let log_batches = ipc_file.read_log_ipc()?;
        let mut store = Self {
            span_batches,
            log_batches,
            services: HashMap::new(),
            log_schema: schema::get_log_schema(),
            is_dirty: false,
        };
        let path = path.join("process.json");
        if !path.exists() {
            return Ok(store);
        }
        let data: Vec<Process> = match serde_json::from_reader(File::open(path)?) {
            Ok(data) => data,
            Err(err) => {
                println!("Warning: read process.json failed: {err}");
                return Ok(store);
            }
        };
        let mut services = HashMap::<String, Vec<_>>::new();
        data.into_iter().for_each(|process| {
            services
                .entry(process.service_name.clone())
                .or_insert_with(Vec::new)
                .push(process);
        });

        store.services = services;
        Ok(store)
    }

    pub(super) fn reset(&mut self) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        (
            mem::take(&mut self.span_batches),
            mem::take(&mut self.log_batches),
        )
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
                .map(|(key, value)| (key, value.into()))
                .collect(),
        });
        self.write_process()?;
        Ok(process_id)
    }

    pub fn merge_logs(&mut self, logs: Vec<Log>) {
        let batches = convert_log_to_record_batch(logs).unwrap();

        let schema = batches.schema();
        self.log_schema = schema::merge_log_schema(schema);
        self.log_batches.push(batches);
        self.is_dirty = true;
    }

    pub fn merge_spans(&mut self, spans: Vec<Span>) {
        self.span_batches
            .push(convert_span_to_record_batch(spans).unwrap());
        self.is_dirty = true;
    }

    fn write_process(&self) -> Result<()> {
        let config = config::load();
        let mut file = File::create(Path::new(&config.data_dir).join("process.json"))?;
        file.write_all(
            serde_json::to_string(&self.processes().values().collect::<Vec<_>>())?.as_bytes(),
        )?;
        Ok(())
    }
}

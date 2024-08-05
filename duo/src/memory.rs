use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fs::File,
    io::Write,
    mem,
    path::Path,
    sync::Arc,
};

use crate::arrow::{convert_log_to_record_batch, convert_span_to_record_batch};
use crate::{arrow::schema_span, Log, Process, Span};
use anyhow::Result;
use arrow_schema::Schema;
use datafusion::{
    arrow::{array::RecordBatch, json::ArrayWriter},
    datasource::MemTable,
    prelude::{Expr, SessionContext},
};
use duo_api as proto;
use serde::de::DeserializeOwned;

pub struct MemoryStore {
    // Collection of services.
    services: HashMap<String, Vec<Process>>,
    pub log_schema: Schema,
    pub span_batches: Vec<RecordBatch>,
    pub log_batches: Vec<RecordBatch>,
}

impl Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("services", &self.services)
            .finish()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            services: HashMap::new(),
            log_schema: Schema::empty(),
            span_batches: vec![],
            log_batches: vec![],
        }
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

        let mut store = Self::new();
        store.services = services;
        Ok(store)
    }

    pub async fn query_span(&self, expr: Expr) -> Result<Vec<Span>> {
        let ctx = SessionContext::new();
        ctx.register_table(
            "span",
            Arc::new(MemTable::try_new(
                schema_span(),
                vec![self.span_batches.clone()],
            )?),
        )?;

        let batches = ctx
            .table("span")
            .await?
            .filter(expr)?
            .collect()
            .await
            .unwrap();
        serialize_record_batche(&batches)
    }

    pub async fn query_log(&self, expr: Expr) -> Result<Vec<Log>> {
        let ctx = SessionContext::new();
        ctx.register_table(
            "log",
            Arc::new(MemTable::try_new(
                Arc::new(self.log_schema.clone()),
                vec![self.log_batches.clone()],
            )?),
        )?;

        let batches = ctx.table("log").await?.filter(expr)?.collect().await?;
        serialize_record_batche(&batches)
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
        // self.spans()
        //     .iter()
        //     .filter_map(|span| {
        //         if span.process_id.starts_with(service) {
        //             Some(span.name.clone())
        //         } else {
        //             None
        //         }
        //     })
        //     .collect()
        HashSet::new()
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
        self.write_process(".")?;
        Ok(process_id)
    }

    pub fn merge_logs(&mut self, logs: Vec<Log>) {
        let batches = convert_log_to_record_batch(logs).unwrap();

        let schema = batches.schema();
        self.log_schema = Schema::try_merge(vec![
            mem::replace(&mut self.log_schema, Schema::empty()),
            Schema::new_with_metadata(schema.fields().clone(), schema.metadata().clone()),
        ])
        .unwrap();
        self.log_batches.push(batches);
    }

    pub fn merge_spans(&mut self, spans: Vec<Span>) {
        self.span_batches
            .push(convert_span_to_record_batch(spans).unwrap());
    }

    fn write_process<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = File::create(path.as_ref().join("process.json"))?;
        file.write_all(
            serde_json::to_string(&self.processes().values().collect::<Vec<_>>())?.as_bytes(),
        )?;
        Ok(())
    }
}

fn serialize_record_batche<T: DeserializeOwned>(batch: &[RecordBatch]) -> Result<Vec<T>> {
    let buf = Vec::new();
    let mut writer = ArrayWriter::new(buf);
    writer.write_batches(&batch.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    let json_values = writer.into_inner();
    let json_rows: Vec<_> = serde_json::from_reader(json_values.as_slice())?;
    Ok(json_rows)
}

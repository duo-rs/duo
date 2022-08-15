use std::{collections::HashMap, io, num::NonZeroU64};
use std::path::Path;

use crate::{aggregator::AggregatedData, Log, PersistConfig, Process, Trace};
use duo_api as proto;
use crate::data::reader::PersistReader;
use crate::data::serialize::{LogPersist, ProcessPersist, TracePersist};

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
    pub(crate) fn register_process(&mut self, process: proto::Process) -> Process {
        let service_name = process.name;
        let service_processes = self.services.entry(service_name.clone()).or_default();

        // TODO: generate new process id
        let process_id = format!("{}:{}", &service_name, service_processes.len());
        let process = Process {
            id: process_id,
            service_name,
            tags: process.tags,
        };
        service_processes.push(process.clone());
        process
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

    /// replay the persist log store on file system, restore the data in the warehouse.
    pub async fn replay(&mut self, mut config: PersistConfig) -> io::Result<()> {
        let base_path = config.path;
        // the base path not exist, give up replay
        if !Path::new(&base_path).exists() {
            return Ok(());
        }
        config.path = format!("{}/{}", base_path, "process");
        let mut process_reader = PersistReader::new(config.clone())?;
        config.path = format!("{}/{}", base_path, "trace");
        let mut trace_reader = PersistReader::new(config.clone())?;
        config.path = format!("{}/{}", base_path, "log");
        let mut log_reader = PersistReader::new(config)?;
        let processes: Vec<ProcessPersist> = process_reader.parse().await?;
        let traces: Vec<TracePersist> = trace_reader.parse().await?;
        let logs: Vec<LogPersist> = log_reader.parse().await?;
        for process in processes {
            let service_processes = self.services.entry(process.service_name.clone()).or_default();
            service_processes.push(Process::from(process));
        }
        for trace in traces {
            self.traces.insert(trace.id, Trace::from(trace));
        }
        let mut i = 0;
        for log in logs {
            let mut local_log = Log::from(log);
            local_log.idx = i;
            // construct span_log_map
            if let Some(span_id) = local_log.span_id {
                let idx = self.span_log_map.entry(span_id).or_default();
                idx.push(i);
            }
            self.logs.push(local_log);
            i += 1;
        }
        Ok(())
    }
}

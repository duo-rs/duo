use std::{fs::File, mem, sync::Arc, time::Duration};

use crate::{
    arrow::SPAN_SCHEMA, partition::PartitionWriter, schema, Log, MemoryStore, SpanAggregator,
};
use datafusion::arrow::ipc::writer::FileWriter;
use duo_api::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse, RegisterProcessRequest, RegisterProcessResponse,
};
use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

pub struct DuoServer {
    memory_store: Arc<RwLock<MemoryStore>>,
    aggregator: Arc<RwLock<SpanAggregator>>,
    logs: Arc<RwLock<Vec<Log>>>,
}

impl DuoServer {
    pub fn new(memory_store: Arc<RwLock<MemoryStore>>) -> Self {
        Self {
            memory_store,
            aggregator: Arc::new(RwLock::new(SpanAggregator::new())),
            logs: Arc::new(RwLock::new(vec![])),
        }
    }

    pub fn spawn(&mut self) {
        let aggregator = Arc::clone(&self.aggregator);
        let memory_store = Arc::clone(&self.memory_store);
        let logs = Arc::clone(&self.logs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                let logs = mem::take(&mut *logs.write());
                let spans = aggregator.write().aggregate();
                if logs.is_empty() || spans.is_empty() {
                    continue;
                }

                let mut guard = memory_store.write();
                guard.merge_logs(logs);
                guard.merge_spans(spans);
            }
        });

        if crate::is_memory_mode() {
            // Never persist data in memory mode.
            return;
        }

        let memory_store = Arc::clone(&self.memory_store);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            interval.tick().await;
            loop {
                interval.tick().await;

                println!(
                    "ipc writing: is locked {}, is_locked_exclusive {}",
                    memory_store.is_locked(),
                    memory_store.is_locked_exclusive()
                );
                let guard = memory_store.read();
                if !guard.is_dirty {
                    continue;
                }

                if !guard.span_batches.is_empty() {
                    let mut span_writer =
                        FileWriter::try_new(File::create("span.arrow").unwrap(), &SPAN_SCHEMA)
                            .unwrap();
                    for batch in &guard.span_batches {
                        span_writer.write(batch).unwrap();
                    }
                    span_writer.finish().unwrap();
                }

                if !guard.log_batches.is_empty() {
                    let mut log_writer =
                        FileWriter::try_new(File::create("log.arrow").unwrap(), &guard.log_schema)
                            .unwrap();
                    for batch in &guard.log_batches {
                        log_writer.write(batch).unwrap();
                    }
                    log_writer.finish().unwrap();
                }
                drop(guard);

                memory_store.write().is_dirty = false;
                tokio::spawn(async {
                    schema::persit_log_schema().await;
                });
            }
        });

        let memory_store = Arc::clone(&self.memory_store);
        tokio::spawn(async move {
            // TODO: replace interval with job scheduler
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await;
            loop {
                interval.tick().await;

                let pw = PartitionWriter::with_minute();
                println!(
                    "write partition: is locked {}, is_locked_exclusive {}",
                    memory_store.is_locked(),
                    memory_store.is_locked_exclusive()
                );

                // clear the previous log schema
                let (span_batches, log_batches) = { memory_store.write().reset() };

                if !span_batches.is_empty() {
                    pw.write_partition("span", &span_batches).await.unwrap();
                    println!("write partition done: span");
                }

                if !log_batches.is_empty() {
                    pw.write_partition("log", &log_batches).await.unwrap();
                    println!("write partition done: log");
                }
            }
        });
    }
}

#[tonic::async_trait]
impl Instrument for DuoServer {
    async fn register_process(
        &self,
        request: Request<RegisterProcessRequest>,
    ) -> Result<Response<RegisterProcessResponse>, Status> {
        let process = request
            .into_inner()
            .process
            .ok_or_else(|| tonic::Status::invalid_argument("missing process"))?;
        info!("register process: {}", process.name);
        let process_id = self
            .memory_store
            .write()
            .register_process(process)
            .expect("Register process failed.");
        Ok(Response::new(RegisterProcessResponse { process_id }))
    }

    async fn record_span(
        &self,
        request: Request<RecordSpanRequest>,
    ) -> Result<Response<RecordSpanResponse>, Status> {
        let span = request
            .into_inner()
            .span
            .ok_or_else(|| tonic::Status::invalid_argument("missing span"))?;
        debug!(target: "duo_internal", "record span: {}", span.name);
        self.aggregator.write().record_span(span);
        Ok(Response::new(RecordSpanResponse {}))
    }

    async fn record_event(
        &self,
        request: Request<RecordEventRequest>,
    ) -> Result<Response<RecordEventResponse>, Status> {
        debug!(target: "duo_internal", "record event, {:?}", request);

        let log = request
            .into_inner()
            .log
            .ok_or_else(|| tonic::Status::invalid_argument("missing event"))?;
        self.logs.write().push(log.into());
        Ok(Response::new(RecordEventResponse {}))
    }
}

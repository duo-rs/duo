use std::{sync::Arc, time::Duration};

use duo_api::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse, RegisterProcessRequest, RegisterProcessResponse,
};
use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::{partition::PartitionWriter, MemoryStore, SpanAggregator};

pub struct DuoServer {
    memory_store: Arc<RwLock<MemoryStore>>,
    aggregator: Arc<RwLock<SpanAggregator>>,
}

impl DuoServer {
    pub fn new(memory_store: Arc<RwLock<MemoryStore>>) -> Self {
        Self {
            memory_store,
            aggregator: Arc::new(RwLock::new(SpanAggregator::new())),
        }
    }

    pub fn run(&mut self) {
        let aggregator = Arc::clone(&self.aggregator);
        let memory_store = Arc::clone(&self.memory_store);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let spans = aggregator.write().aggregate();
                if spans.is_empty() {
                    continue;
                }
                let mut memory_store = memory_store.write();
                memory_store.merge_spans(spans);
            }
        });

        if crate::is_memory_mode() {
            // Never persist data in memory mode.
            return;
        }

        let memory_store = Arc::clone(&self.memory_store);
        tokio::spawn(async move {
            // TODO: replace interval with job scheduler
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let (logs, spans) = {
                    let mut guard = memory_store.write();
                    (guard.take_logs(), guard.take_spans())
                };
                let mut pw = PartitionWriter::with_minute();
                pw.write_logs(logs).unwrap();
                pw.write_spans(spans).unwrap();
                pw.flush().await.expect("Write parquet failed");
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
        let mut guard = self.memory_store.write();
        guard.record_log(log);
        Ok(Response::new(RecordEventResponse {}))
    }
}

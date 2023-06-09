use std::{sync::Arc, time::Duration};

use duo_api as proto;
use parking_lot::RwLock;
use proto::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse, RegisterProcessRequest, RegisterProcessResponse,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::{Aggregator, Warehouse};

pub struct DuoServer {
    warehouse: Arc<RwLock<Warehouse>>,
    aggregator: Arc<RwLock<Aggregator>>,
}

impl DuoServer {
    pub fn new(warehouse: Arc<RwLock<Warehouse>>) -> Self {
        Self {
            warehouse,
            aggregator: Arc::new(RwLock::new(Aggregator::new())),
        }
    }

    pub fn bootstrap(&mut self) {
        let aggregator = Arc::clone(&self.aggregator);
        let warehouse = Arc::clone(&self.warehouse);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let data = aggregator.write().aggregate();
                let mut warehouse = warehouse.write();
                warehouse.merge_data(data);
                debug!("After merge: {:?}", warehouse);
            }
        });

        let warehouse = Arc::clone(&self.warehouse);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let mut warehouse = warehouse.write();
                warehouse
                    .write_parquet()
                    .await
                    .expect("Write parquet failed");
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
        info!("register process: {:?}", request);
        let process = request
            .into_inner()
            .process
            .ok_or_else(|| tonic::Status::invalid_argument("missing process"))?;
        let process_id = self.warehouse.write().register_process(process);
        Ok(Response::new(RegisterProcessResponse { process_id }))
    }

    async fn record_span(
        &self,
        request: Request<RecordSpanRequest>,
    ) -> Result<Response<RecordSpanResponse>, Status> {
        debug!("record span: {:?}", request);
        let span = request
            .into_inner()
            .span
            .ok_or_else(|| tonic::Status::invalid_argument("missing span"))?;
        self.aggregator.write().record_span(span);
        Ok(Response::new(RecordSpanResponse {}))
    }

    async fn record_event(
        &self,
        request: Request<RecordEventRequest>,
    ) -> Result<Response<RecordEventResponse>, Status> {
        debug!("record event, {:?}", request);

        let log = request
            .into_inner()
            .log
            .ok_or_else(|| tonic::Status::invalid_argument("missing event"))?;
        self.aggregator.write().record_log(log);
        Ok(Response::new(RecordEventResponse {}))
    }
}

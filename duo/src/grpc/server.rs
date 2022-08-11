use std::{sync::Arc, time::Duration};

use duo_api as proto;
use parking_lot::RwLock;
use proto::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse, RegisterProcessRequest, RegisterProcessResponse,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::{Aggregator, Warehouse};
use crate::data::persist::Persist;

pub struct DuoServer {
    warehouse: Arc<RwLock<Warehouse>>,
    aggregator: Arc<RwLock<Aggregator>>,
    sender: Sender<Message>,
    receiver: Arc<RwLock<Receiver<Message>>>,
    persist: Arc<RwLock<Persist>>,
}

#[derive(Debug)]
enum Message {
    Register(RegisterMessage),
    Span(proto::Span),
    Log(proto::Log),
}

#[derive(Debug)]
struct RegisterMessage {
    tx: Sender<String>,
    process: proto::Process,
}

impl DuoServer {
    pub fn new(warehouse: Arc<RwLock<Warehouse>>) -> Self {
        let (sender, receiver) = channel::<Message>(4096);
        Self {
            warehouse,
            aggregator: Arc::new(RwLock::new(Aggregator::new())),
            sender,
            receiver: Arc::new(RwLock::new(receiver)),
            persist: Arc::new(RwLock::new(Persist::new())),
        }
    }

    pub fn bootstrap(&mut self) {
        let warehouse = Arc::clone(&self.warehouse);
        let receiver = Arc::clone(&self.receiver);
        let aggregator = Arc::clone(&self.aggregator);
        let persist_for_receiver = Arc::clone(&self.persist);
        let persist_for_aggregate = Arc::clone(&self.persist);
        tokio::spawn(async move {
            loop {
                let mut receiver = receiver.write();
                match receiver.recv().await {
                    Some(Message::Register(RegisterMessage { tx, process })) => {
                        let process = warehouse.write().register_process(process);
                        tx.send(process.id.clone()).await.unwrap();
                        persist_for_receiver.read().persist_process(process).await;
                    }
                    Some(Message::Span(span)) => {
                        aggregator.write().record_span(span);
                    }
                    Some(Message::Log(log)) => {
                        aggregator.write().record_log(log);
                    }
                    None => {}
                }
            }
        });

        let aggregator = Arc::clone(&self.aggregator);
        let warehouse = Arc::clone(&self.warehouse);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let data = aggregator.write().aggregate();
                let mut warehouse = warehouse.write();
                warehouse.merge_data(data.clone());
                debug!("After merge: {:?}", warehouse);
                persist_for_aggregate.read().persist_data(data).await;
            }
        });
        self.persist.write().bootstrap();
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

        let (tx, mut rx) = channel(1024 * 100);
        self.sender
            .send(Message::Register(RegisterMessage { tx, process }))
            .await
            .map_err(|e| tonic::Status::internal(format!("register failed: {}", e)))?;

        let process_id = rx
            .recv()
            .await
            .ok_or_else(|| tonic::Status::internal("process id generated failed"))?;
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
        self.sender
            .send(Message::Span(span))
            .await
            .map_err(|e| tonic::Status::internal(format!("record span failed: {}", e)))?;
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
        self.sender
            .send(Message::Log(log))
            .await
            .map_err(|e| tonic::Status::internal(format!("record event failed: {}", e)))?;
        Ok(Response::new(RecordEventResponse {}))
    }
}

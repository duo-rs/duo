use std::sync::Arc;

use jage_api as proto;
use parking_lot::RwLock;
use proto::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::{Request, Response, Status};

use crate::Aggregator;

pub struct JageServer {
    aggregator: Arc<RwLock<Aggregator>>,
    sender: Sender<Message>,
    receiver: Arc<RwLock<Receiver<Message>>>,
}

#[derive(Debug)]
enum Message {
    Span(proto::Span),
    Log(proto::Log),
}

impl JageServer {
    pub fn new() -> Self {
        let (sender, receiver) = channel::<Message>(4096);
        Self {
            aggregator: Arc::new(RwLock::new(Aggregator::new())),
            sender,
            receiver: Arc::new(RwLock::new(receiver)),
        }
    }

    pub fn bootstrap(&mut self) {
        let receiver = Arc::clone(&self.receiver);
        let aggregator = Arc::clone(&self.aggregator);
        tokio::spawn(async move {
            loop {
                let mut receiver = receiver.write();
                match receiver.recv().await {
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
    }
}

#[tonic::async_trait]
impl Instrument for JageServer {
    async fn record_span(
        &self,
        request: Request<RecordSpanRequest>,
    ) -> Result<Response<RecordSpanResponse>, Status> {
        println!("record span: {:?}", request);
        if let Some(span) = request.into_inner().span {
            self.sender.send(Message::Span(span)).await.unwrap();
        }
        Ok(Response::new(RecordSpanResponse {}))
    }

    async fn record_event(
        &self,
        request: Request<RecordEventRequest>,
    ) -> Result<Response<RecordEventResponse>, Status> {
        println!("record event, {:?}", request);
        if let Some(log) = request.into_inner().log {
            self.sender.send(Message::Log(log)).await.unwrap();
        }
        Ok(Response::new(RecordEventResponse {}))
    }
}

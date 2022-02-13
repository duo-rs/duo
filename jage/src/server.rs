use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use jage_api as proto;
use parking_lot::RwLock;
use proto::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::{Request, Response, Status};

use crate::{Aggregator, Log, Trace};

pub struct JageServer {
    // <trace_id, Trace>
    traces: Arc<DashMap<u64, Trace>>,
    logs: Arc<RwLock<Vec<Log>>>,
    // <span_id, Vec<log id>>
    span_log_map: Arc<DashMap<u64, Vec<usize>>>,
    aggregator: Arc<RwLock<Aggregator>>,
    sender: Sender<Message>,
    receiver: Arc<RwLock<Receiver<Message>>>,
}

#[derive(Debug)]
enum Message {
    Span(proto::Span),
    Log(proto::Log),
}

impl Default for JageServer {
    fn default() -> Self {
        let (sender, receiver) = channel::<Message>(4096);
        Self {
            traces: Arc::new(DashMap::default()),
            logs: Arc::new(RwLock::new(Vec::new())),
            span_log_map: Arc::new(DashMap::default()),
            aggregator: Arc::new(RwLock::new(Aggregator::new())),
            sender,
            receiver: Arc::new(RwLock::new(receiver)),
        }
    }
}

impl JageServer {
    pub fn new() -> Self {
        Self::default()
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

        let aggregator = Arc::clone(&self.aggregator);
        let traces = Arc::clone(&self.traces);
        let logs = Arc::clone(&self.logs);
        let span_log_map = Arc::clone(&self.span_log_map);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let bundle = aggregator.write().aggregate();

                bundle.traces.into_iter().for_each(|(id, trace)| {
                    traces.insert(id, trace);
                });

                let mut logs = logs.write();
                // Reserve capacity advanced.
                logs.reserve(bundle.logs.len());
                let base_idx = logs.len();
                bundle
                    .logs
                    .into_iter()
                    .enumerate()
                    .for_each(|(i, mut log)| {
                        let idx = base_idx + i;

                        // Exclude those logs without span_id,
                        // normally they are not emitted in tracing context.
                        if let Some(span_id) = log.span_id {
                            let mut log_idxs = span_log_map.entry(span_id).or_default();
                            log_idxs.push(idx);
                        }

                        log.idx = idx;
                        logs.push(log);
                    });

                println!(
                    "After tick - traces: {}, logs: {}, span_log_map: {:?}",
                    traces.len(),
                    logs.len(),
                    span_log_map
                );
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

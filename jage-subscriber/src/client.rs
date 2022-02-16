use std::collections::HashMap;

use jage_api as proto;
use proto::instrument::{
    instrument_client::InstrumentClient, RecordEventRequest, RecordSpanRequest,
    RegisterProcessRequest,
};
use proto::process::Process;
use tonic::{transport::Channel, Request};

pub struct JageClient {
    name: &'static str,
    process_id: u32,
    inner: InstrumentClient<Channel>,
}

impl JageClient {
    #[must_use]
    pub fn new(name: &'static str, client: InstrumentClient<Channel>) -> JageClient {
        JageClient {
            name,
            process_id: 0,
            inner: client,
        }
    }

    pub(crate) async fn registry_process(&mut self) {
        let response = self
            .inner
            .register_process(Request::new(RegisterProcessRequest {
                process: Some(Process {
                    name: String::from(self.name),
                    tags: HashMap::default(),
                }),
            }))
            .await
            .unwrap();
        self.process_id = response.into_inner().process_id;
    }

    pub async fn record_span(&mut self, mut span: proto::Span) {
        span.process_id = self.process_id;
        self.inner
            .record_span(Request::new(RecordSpanRequest { span: Some(span) }))
            .await
            .unwrap();
    }

    pub async fn record_event(&mut self, log: proto::Log) {
        self.inner
            .record_event(Request::new(RecordEventRequest { log: Some(log) }))
            .await
            .unwrap();
    }
}

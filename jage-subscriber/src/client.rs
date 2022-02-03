use jage_api as proto;
use proto::instrument::{
    instrument_client::InstrumentClient, RecordEventRequest, RecordSpanRequest,
};
use tonic::{transport::Channel, Request};

pub struct JageClient {
    inner: InstrumentClient<Channel>,
}
impl JageClient {
    pub fn new(client: InstrumentClient<Channel>) -> JageClient {
        JageClient { inner: client }
    }

    pub async fn record_span(&mut self, span: proto::Span) {
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

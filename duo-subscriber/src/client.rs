use duo_api as proto;
use proto::instrument::{
    instrument_client::InstrumentClient, RecordEventRequest, RecordSpanRequest,
    RegisterProcessRequest,
};
use proto::process::Process;
use tonic::{transport::Channel, Request};

pub struct DuoClient {
    name: &'static str,
    process_id: String,
    inner: InstrumentClient<Channel>,
}

impl DuoClient {
    #[must_use]
    pub fn new(name: &'static str, client: InstrumentClient<Channel>) -> DuoClient {
        DuoClient {
            name,
            process_id: String::new(),
            inner: client,
        }
    }

    pub(crate) async fn registry_process(&mut self) {
        let response = self
            .inner
            .register_process(Request::new(RegisterProcessRequest {
                process: Some(Process {
                    name: String::from(self.name),
                    tags: super::grasp_process_info(),
                }),
            }))
            .await
            .unwrap();
        self.process_id = response.into_inner().process_id;
    }

    pub async fn record_span(&mut self, mut span: proto::Span) {
        span.process_id = self.process_id.clone();
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

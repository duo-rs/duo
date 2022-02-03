use jage_api as proto;
use proto::instrument::{
    instrument_server::Instrument, RecordEventRequest, RecordEventResponse, RecordSpanRequest,
    RecordSpanResponse,
};
use tonic::{Request, Response, Status};
use tracing::debug;
pub struct JageServer {}

#[tonic::async_trait]
impl Instrument for JageServer {
    async fn record_span(
        &self,
        request: Request<RecordSpanRequest>,
    ) -> Result<Response<RecordSpanResponse>, Status> {
        println!("record span");
        debug!(?request);
        Ok(Response::new(RecordSpanResponse {}))
    }

    async fn record_event(
        &self,
        request: Request<RecordEventRequest>,
    ) -> Result<Response<RecordEventResponse>, Status> {
        println!("record event");
        debug!(?request);
        Ok(Response::new(RecordEventResponse {}))
    }
}

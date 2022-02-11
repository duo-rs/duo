use jage_api as proto;
use proto::instrument::instrument_server::InstrumentServer;
use tonic::transport::Server;

use jage::JageServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello Jage!");
    let addr = "127.0.0.1:6000".parse().unwrap();
    let mut service = JageServer::new();
    service.bootstrap();
    Server::builder()
        .add_service(InstrumentServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

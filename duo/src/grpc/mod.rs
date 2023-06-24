use std::{net::SocketAddr, sync::Arc};

use crate::MemoryStore;

use self::server::DuoServer;

use duo_api as proto;
use parking_lot::RwLock;
use proto::instrument::instrument_server::InstrumentServer;
use tonic::transport::Server;

mod server;

pub fn spawn_server(memory_store: Arc<RwLock<MemoryStore>>, port: u16) {
    tokio::spawn(async move {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let mut service = DuoServer::new(memory_store);
        service.run();

        println!("gRPC server listening on grpc://{}", addr);
        Server::builder()
            .add_service(InstrumentServer::new(service))
            .serve(addr)
            .await
            .unwrap();
    });
}

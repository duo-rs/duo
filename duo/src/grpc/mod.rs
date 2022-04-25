use std::sync::Arc;

use crate::Warehouse;

use self::server::DuoServer;

use duo_api as proto;
use parking_lot::RwLock;
use proto::instrument::instrument_server::InstrumentServer;
use tonic::transport::Server;

mod server;

pub fn spawn_server(warehouse: Arc<RwLock<Warehouse>>) {
    tokio::spawn(async {
        let addr = "127.0.0.1:6000".parse().unwrap();
        let mut service = DuoServer::new(warehouse);
        service.bootstrap();

        println!("gRPC server listening on 127.0.0.1:6000\n");
        Server::builder()
            .add_service(InstrumentServer::new(service))
            .serve(addr)
            .await
            .unwrap();
    });
}

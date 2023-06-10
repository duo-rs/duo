use std::{net::SocketAddr, sync::Arc};

use crate::Warehouse;

use self::server::DuoServer;

use duo_api as proto;
use parking_lot::RwLock;
use proto::instrument::instrument_server::InstrumentServer;
use tonic::transport::Server;

mod server;

pub fn spawn_server(warehouse: Arc<RwLock<Warehouse>>, port: u16) {
    tokio::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut service = DuoServer::new(warehouse);
        service.run();

        println!("gRPC server listening on http://{}\n", addr);
        Server::builder()
            .add_service(InstrumentServer::new(service))
            .serve(addr)
            .await
            .unwrap();
    });
}

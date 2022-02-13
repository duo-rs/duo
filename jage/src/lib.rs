mod aggregator;
mod grpc;
mod models;
mod web;

pub use aggregator::Aggregator;
pub use grpc::spawn_server as spawn_grpc_server;
pub use models::{Log, Span, Trace};
pub use web::run_web_server;

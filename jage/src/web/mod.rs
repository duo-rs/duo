use std::sync::Arc;

use axum::{routing::get, AddExtensionLayer, Router};
use parking_lot::RwLock;
use tower::ServiceBuilder;

use crate::TraceBundle;

mod routes;
mod serialize;

pub struct JaegerData<I: IntoIterator>(pub I);

pub async fn run_web_server(bundle: Arc<RwLock<TraceBundle>>) {
    let layer = ServiceBuilder::new().layer(AddExtensionLayer::new(bundle));
    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/traces", get(routes::traces))
        .route("/services", get(|| async { "Hello, World!" }))
        .route(
            "/services/:service/operations",
            get(|| async { "Hello, World!" }),
        )
        .layer(layer);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

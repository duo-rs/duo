use std::sync::Arc;

use axum::{
    http::StatusCode,
    response::Html,
    routing::{get, get_service},
    AddExtensionLayer, Router,
};
use parking_lot::RwLock;
use tower::ServiceBuilder;
use tower_http::services::ServeDir;

use crate::Warehouse;

mod routes;
mod query;
mod serialize;

pub struct JaegerData<I: IntoIterator>(pub I);

pub async fn run_web_server(warehouse: Arc<RwLock<Warehouse>>) {
    let layer = ServiceBuilder::new().layer(AddExtensionLayer::new(warehouse));
    let app = Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../../ui/index.html")) }),
        )
        .nest(
            "/static",
            get_service(ServeDir::new("ui/static")).handle_error(|error| async move {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unhandled internal error: {}", error),
                )
            }),
        )
        .route("/api/traces", get(routes::traces))
        .route("/api/services", get(routes::services))
        .route("/api/services/:service/operations", get(routes::operations))
        .layer(layer);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

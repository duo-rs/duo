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

use crate::TraceBundle;

mod routes;
mod serialize;

pub struct JaegerData<I: IntoIterator>(pub I);

pub async fn run_web_server(bundle: Arc<RwLock<TraceBundle>>) {
    let layer = ServiceBuilder::new().layer(AddExtensionLayer::new(bundle));
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

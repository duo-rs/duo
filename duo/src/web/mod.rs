use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension,
    http::{StatusCode, Uri},
    response::{Html, IntoResponse},
    routing::{get, get_service},
    Router,
};
use parking_lot::RwLock;
use tower::ServiceBuilder;
use tower_http::services::ServeDir;

use crate::Warehouse;

pub mod deser;
mod query;
mod routes;
pub mod serialize;

// Frontend HTML page.
static ROOT_PAGE: Html<&'static str> = Html(include_str!("../../ui/index.html"));
static TMP_DUO_STATIC_DIR: &str = "/tmp/__duo_ui";
pub struct JaegerData<I: IntoIterator>(pub I);

pub async fn run_web_server(warehouse: Arc<RwLock<Warehouse>>, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let layer = ServiceBuilder::new().layer(Extension(warehouse));

    include_dir::include_dir!("$CARGO_MANIFEST_DIR/ui/static").extract(TMP_DUO_STATIC_DIR)?;
    let app = Router::new()
        .route("/", get(|| async { ROOT_PAGE }))
        .nest_service(
            "/static",
            get_service(ServeDir::new(TMP_DUO_STATIC_DIR)).handle_error(|error| async move {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unhandled internal error: {}", error),
                )
            }),
        )
        .route("/api/traces", get(routes::traces))
        .route("/api/traces/:id", get(routes::trace))
        .route("/api/services", get(routes::services))
        .route("/api/services/:service/operations", get(routes::operations))
        .route("/stats", get(routes::stats))
        .fallback(fallback)
        .layer(layer);

    println!("Web server listening on http://{}\n", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn fallback(uri: Uri) -> impl IntoResponse {
    let path = uri.path();
    if path.starts_with("/api") || path.starts_with("/static") {
        // For those routes, we simply return 404 text.
        (StatusCode::NOT_FOUND, "404 Not Found").into_response()
    } else {
        // Due to the frontend is a SPA (Single Page Application),
        // it has own frontend routes, we should return the ROOT PAGE
        // to avoid frontend route 404.
        (StatusCode::TEMPORARY_REDIRECT, ROOT_PAGE).into_response()
    }
}

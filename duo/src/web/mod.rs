use std::{env, net::SocketAddr, sync::Arc};

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
mod logs;
mod query;
pub mod serialize;
mod trace;

// Frontend HTML page.
static ROOT_PAGE: Html<&'static str> = Html(include_str!("../../ui/index.html"));
pub struct JaegerData<I: IntoIterator>(pub I);

pub async fn run_web_server(warehouse: Arc<RwLock<Warehouse>>, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let layer = ServiceBuilder::new().layer(Extension(warehouse));

    let tmp_duo_dir = env::temp_dir().join("__duo_ui");
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/ui/static").extract(&tmp_duo_dir)?;
    let app = Router::new()
        .route("/", get(|| async { ROOT_PAGE }))
        .nest_service(
            "/static",
            get_service(ServeDir::new(tmp_duo_dir)).handle_error(|error| async move {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unhandled internal error: {}", error),
                )
            }),
        )
        .route("/api/traces", get(trace::list))
        .route("/api/traces/:id", get(trace::get_by_id))
        .route("/api/services", get(trace::services))
        .route("/api/services/:service/operations", get(trace::operations))
        .route("/api/logs", get(logs::list))
        .route("/stats", get(self::stats))
        .fallback(fallback)
        .layer(layer);

    println!("Web server listening on http://{}", addr);
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

#[tracing::instrument]
async fn stats(Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>) -> impl IntoResponse {
    let warehouse = warehouse.read();
    serde_json::json!({
            "process": warehouse.processes(),
            "logs": warehouse.logs.len(),
            "spans": warehouse.spans.len(),
    })
    .to_string()
}

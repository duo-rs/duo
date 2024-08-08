use std::{net::SocketAddr, sync::Arc};

use axum::{
    body::Body,
    extract::Extension,
    http::{header, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use parking_lot::RwLock;
use rust_embed::RustEmbed;
use tower::ServiceBuilder;

use crate::MemoryStore;

pub mod deser;
mod logs;
mod services;
pub mod serialize;
mod trace;

// Frontend HTML page.
static ROOT_PAGE: Html<&'static str> = Html(include_str!("../../ui/index.html"));
pub struct JaegerData<I: IntoIterator>(pub I);

#[derive(RustEmbed)]
#[folder = "ui/static"]
struct UiAssets;

pub struct StaticFile<T>(pub T);

impl<T> IntoResponse for StaticFile<T>
where
    T: Into<String>,
{
    fn into_response(self) -> Response {
        let path = self.0.into();

        match UiAssets::get(path.as_str()) {
            Some(content) => {
                let body = Body::from(content.data);
                let mime = mime_guess::from_path(path).first_or_octet_stream();
                Response::builder()
                    .header(header::CONTENT_TYPE, mime.as_ref())
                    .body(body)
                    .unwrap()
            }
            None => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("404"))
                .unwrap(),
        }
    }
}

pub async fn run_web_server(
    memory_store: Arc<RwLock<MemoryStore>>,
    port: u16,
) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let layer = ServiceBuilder::new().layer(Extension(memory_store));

    let app = Router::new()
        .route("/", get(|| async { ROOT_PAGE }))
        .nest_service("/static", get(static_handler))
        .route("/api/traces", get(trace::list))
        .route("/api/traces/:id", get(trace::get_by_id))
        .route("/api/services", get(trace::services))
        .route("/api/services/:service/operations", get(trace::operations))
        .route("/api/logs", get(logs::list))
        .route("/stats", get(self::stats))
        .fallback(fallback)
        .layer(layer);

    println!("Web server listening on http://{}", addr);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    StaticFile(uri.path().trim_start_matches('/').to_string())
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
async fn stats(Extension(memory_store): Extension<Arc<RwLock<MemoryStore>>>) -> impl IntoResponse {
    let memory_store = memory_store.read();
    serde_json::json!({
            "process": memory_store.processes(),
            "logs": 0,
            "spans": 0,
    })
    .to_string()
}

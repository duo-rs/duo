use std::{net::SocketAddr, sync::Arc};

use axum::{
    body::Body,
    extract::Extension,
    http::{header, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use parking_lot::RwLock;
use rust_embed::RustEmbed;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};

use crate::MemoryStore;

pub mod deser;
mod logs;
pub mod serialize;
mod services;
mod trace;

pub struct JaegerData<I: IntoIterator>(pub I);

#[derive(RustEmbed)]
#[folder = "ui"]
struct UiAssets;

pub struct StaticFile(Uri);

impl IntoResponse for StaticFile {
    fn into_response(self) -> Response {
        let new_path = match self.0.path().trim_start_matches('/') {
            "" => "index.html",
            p if p.starts_with("trace") || p.starts_with("search") => "trace.html",
            p => p,
        };
        // println!("path: {}, new_path: {}", path, new_path);
        match UiAssets::get(new_path) {
            Some(content) => {
                let body = Body::from(content.data);
                let mime = mime_guess::from_path(new_path).first_or_octet_stream();
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
    let cors = CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([Method::GET, Method::POST])
        // allow requests from any origin
        .allow_origin(Any);
    let layer = ServiceBuilder::new()
        .layer(Extension(memory_store))
        .layer(cors);

    let app = Router::new()
        .nest_service("/", get(static_handler))
        .route("/api/traces", get(trace::list))
        .route("/api/traces/:id", get(trace::get_by_id))
        .route("/api/services", get(trace::services))
        .route("/api/services/:service/operations", get(trace::operations))
        .route("/api/logs", get(logs::list))
        .route("/api/logs/schema", get(logs::schema))
        .route("/api/logs/stats/:field", get(logs::field_stats))
        .route("/stats", get(self::stats))
        .layer(layer);

    println!("Web server listening on http://{}", addr);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    StaticFile(uri)
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

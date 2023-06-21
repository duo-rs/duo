use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use parking_lot::RwLock;
use serde::Deserialize;
use time::{Duration, OffsetDateTime};

use crate::{TraceExt, Warehouse};

use super::deser;
use super::query::TraceQuery;
use super::JaegerData;

#[derive(Debug, Deserialize)]
pub(super) struct QueryParameters {
    pub service: String,
    pub operation: Option<String>,
    #[serde(default, deserialize_with = "deser::option_ignore_error")]
    pub limit: Option<usize>,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    pub start: Option<OffsetDateTime>,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    pub end: Option<OffsetDateTime>,
    #[serde(rename = "maxDuration")]
    #[serde(default, deserialize_with = "deser::option_duration")]
    pub max_duration: Option<Duration>,
    #[serde(rename = "minDuration")]
    #[serde(default, deserialize_with = "deser::option_duration")]
    pub min_duration: Option<Duration>,
}

#[tracing::instrument]
pub(super) async fn traces(
    Query(parameters): Query<QueryParameters>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(
        TraceQuery::new(&warehouse).filter_traces(parameters).await,
    ))
}

#[tracing::instrument]
pub(super) async fn services(
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(warehouse.service_names()))
}

#[tracing::instrument]
pub(super) async fn operations(
    Path(service): Path<String>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(warehouse.span_names(&service)))
}

#[tracing::instrument]
pub(super) async fn trace(
    Path(id): Path<String>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    let trace_id = id.parse::<u64>().ok();

    match trace_id {
        Some(trace_id) => {
            if let Some(trace) = TraceQuery::new(&warehouse).get_trace_by_id(trace_id).await {
                Json(JaegerData(vec![trace])).into_response()
            } else {
                Json(JaegerData(Vec::<TraceExt>::new())).into_response()
            }
        }
        None => (StatusCode::NOT_FOUND, format!("trace {} not found", id)).into_response(),
    }
}

#[tracing::instrument]
pub(super) async fn stats(
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    serde_json::json!({
            "process": warehouse.processes(),
            "logs": warehouse.logs.len(),
            "spans": warehouse.spans.len(),
    })
    .to_string()
}

use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::response::IntoResponse;
use axum::Json;
use parking_lot::RwLock;
use serde::Deserialize;

use crate::Warehouse;

use super::JaegerData;

#[derive(Debug, Deserialize)]
pub struct TraceQuery {
    service: String,
}

pub async fn traces(
    Query(query): Query<TraceQuery>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(warehouse.transform_traces(&query.service)))
}

pub async fn services(
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(warehouse.services()))
}

pub(crate) async fn operations(
    Path(service): Path<String>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(warehouse.span_names(&service)))
}

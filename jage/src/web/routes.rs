use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::response::IntoResponse;
use axum::Json;
use parking_lot::RwLock;
use serde::Deserialize;
use time::{Duration, OffsetDateTime};

use crate::Warehouse;

use super::deser;
use super::query::TraceQuery;
use super::JaegerData;

#[derive(Debug, Deserialize)]
pub struct QueryParameters {
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

pub async fn traces(
    Query(parameters): Query<QueryParameters>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(
        TraceQuery::new(&warehouse).filter_traces(parameters),
    ))
}

pub async fn services(
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(TraceQuery::new(&warehouse).service_names()))
}

pub(crate) async fn operations(
    Path(service): Path<String>,
    Extension(warehouse): Extension<Arc<RwLock<Warehouse>>>,
) -> impl IntoResponse {
    let warehouse = warehouse.read();
    Json(JaegerData(TraceQuery::new(&warehouse).span_names(&service)))
}

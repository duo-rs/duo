use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::response::IntoResponse;
use axum::Json;
use parking_lot::RwLock;
use serde::{de, Deserialize};

use crate::Warehouse;

use super::query::TraceQuery;
use super::JaegerData;

fn deserialize_option_ignore_error<'de, T, D>(d: D) -> Result<Option<T>, D::Error>
where
    T: de::Deserialize<'de>,
    D: de::Deserializer<'de>,
{
    Ok(T::deserialize(d).ok())
}

#[derive(Debug, Deserialize)]
pub struct QueryParameters {
    pub service: String,
    pub operation: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_ignore_error")]
    pub limit: Option<usize>,
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

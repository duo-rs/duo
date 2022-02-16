use std::sync::Arc;

use axum::extract::Extension;
use axum::response::IntoResponse;
use axum::Json;
use parking_lot::RwLock;

use crate::TraceBundle;

use super::JaegerData;

pub async fn traces(Extension(bundle): Extension<Arc<RwLock<TraceBundle>>>) -> impl IntoResponse {
    let bundle = bundle.read();
    Json(JaegerData(bundle.transform_traces(1)))
}

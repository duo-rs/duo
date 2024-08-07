use std::sync::Arc;

use axum::extract::{Extension, Query};
use axum::response::IntoResponse;
use axum::Json;
use datafusion::prelude::*;
use parking_lot::RwLock;
use serde::Deserialize;
use time::{Duration, OffsetDateTime};

use crate::query::QueryEngine;
use crate::{Log, MemoryStore};

use super::deser;

const DEFAUT_LOG_LIMIT: usize = 100;

#[derive(Debug, Deserialize)]
pub(super) struct QueryParameters {
    pub service: String,
    #[serde(default, deserialize_with = "deser::option_ignore_error")]
    pub limit: Option<usize>,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    pub start: Option<OffsetDateTime>,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    pub end: Option<OffsetDateTime>,
}

#[tracing::instrument]
pub(super) async fn list(
    Query(p): Query<QueryParameters>,
    Extension(memory_store): Extension<Arc<RwLock<MemoryStore>>>,
) -> impl IntoResponse {
    let process_prefix = p.service;
    let limit = p.limit.unwrap_or(DEFAUT_LOG_LIMIT);
    let query_engine = QueryEngine::new(memory_store);
    let expr = col("process_id").like(lit(format!("{process_prefix}%")));
    let total_logs = query_engine
        .query_log(expr)
        .range(
            p.start
                .or_else(|| Some(OffsetDateTime::now_utc() - Duration::minutes(15))),
            p.end,
        )
        .collect::<Log>()
        .await
        .unwrap_or_default();
    Json(total_logs.into_iter().take(limit).collect::<Vec<_>>())
}

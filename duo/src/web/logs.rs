use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use datafusion::functions_aggregate::count::count;
use datafusion::prelude::*;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::query::QueryEngine;
use crate::{schema, Log, MemoryStore};

use super::deser;

const DEFAUT_LOG_LIMIT: usize = 100;

#[derive(Debug, Deserialize)]
pub(super) struct QueryParameters {
    service: String,
    #[serde(default, deserialize_with = "deser::option_ignore_error")]
    limit: Option<usize>,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    start: Option<OffsetDateTime>,
    #[serde(default, deserialize_with = "deser::option_miscrosecond")]
    end: Option<OffsetDateTime>,
    keyword: Option<String>,
    #[serde(default, deserialize_with = "deser::str_sequence")]
    levels: Vec<String>,
}

#[tracing::instrument]
pub(super) async fn schema() -> impl IntoResponse {
    Json(schema::get_log_schema())
}

impl QueryParameters {
    fn expr(&self) -> Expr {
        let process_prefix = &self.service;
        let mut expr = col("process_id").like(lit(format!("{process_prefix}%")));
        if let Some(keyword) = self.keyword.as_ref() {
            expr = expr.and(col("message").like(lit(format!("%{keyword}%"))));
        }
        if !self.levels.is_empty() {
            expr = expr.and(col("level").in_list(self.levels.iter().map(lit).collect(), false));
        }
        expr
    }
}

#[tracing::instrument]
pub(super) async fn field_stats(
    Path(field): Path<String>,
    Query(p): Query<QueryParameters>,
    Extension(memory_store): Extension<Arc<RwLock<MemoryStore>>>,
) -> Response {
    if schema::get_log_schema().index_of(&field).is_err() {
        return (StatusCode::NOT_FOUND, format!("Field {field} not exists")).into_response();
    }

    #[derive(Serialize, Deserialize)]
    struct FieldStats {
        // FIXME: need support int, bool
        value: String,
        count: i64,
    }
    let query_engine = QueryEngine::new(memory_store);
    let c = col(field);
    let stats = query_engine
        .query_log(p.expr())
        .range(p.start, p.end)
        // sort by count desc
        .sort(vec![col("count").sort(false, false)])
        .aggregate(
            vec![c.clone().alias("value")],
            vec![count(c).alias("count")],
        )
        .collect::<FieldStats>()
        .await
        .unwrap();
    Json(stats).into_response()
}

#[tracing::instrument]
pub(super) async fn list(
    Query(p): Query<QueryParameters>,
    Extension(memory_store): Extension<Arc<RwLock<MemoryStore>>>,
) -> impl IntoResponse {
    let limit = p.limit.unwrap_or(DEFAUT_LOG_LIMIT);
    let query_engine = QueryEngine::new(memory_store);
    let total_logs = query_engine
        .query_log(p.expr())
        .range(p.start, p.end)
        .collect::<Log>()
        .await
        .unwrap_or_default();
    Json(total_logs.into_iter().take(limit).collect::<Vec<_>>())
}

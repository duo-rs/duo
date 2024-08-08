use std::sync::Arc;

use crate::arrow::schema_span;
use crate::arrow::serialize_record_batches;
use crate::partition::PartitionQuery;
use crate::MemoryStore;

use anyhow::{Ok, Result};
use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::{arrow::array::RecordBatch, prelude::Expr};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use time::{Duration, OffsetDateTime};

#[derive(Debug, Clone)]
pub struct QueryEngine {
    memory_store: Arc<RwLock<MemoryStore>>,
}

impl QueryEngine {
    pub fn new(memory_store: Arc<RwLock<MemoryStore>>) -> Self {
        Self { memory_store }
    }

    pub fn query_trace(&self, expr: Expr) -> Query {
        Query {
            table_name: "span",
            batches: self.memory_store.read().span_batches.clone(),
            schema: schema_span(),
            expr,
            start: None,
            end: None,
        }
    }

    pub fn query_log(&self, expr: Expr) -> Query {
        let guard = self.memory_store.read();
        Query {
            table_name: "log",
            batches: guard.log_batches.clone(),
            schema: Arc::new(guard.log_schema.clone()),
            expr,
            start: None,
            end: None,
        }
    }
}

pub struct Query {
    table_name: &'static str,
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    expr: Expr,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
}

impl Query {
    pub fn range(self, start: Option<OffsetDateTime>, end: Option<OffsetDateTime>) -> Self {
        Self { start, end, ..self }
    }

    pub async fn collect<T: DeserializeOwned>(self) -> Result<Vec<T>> {
        let mut total_batches = if self.batches.is_empty() {
            vec![]
        } else {
            let ctx = SessionContext::new();
            let df = ctx.read_table(Arc::new(MemTable::try_new(
                self.schema,
                vec![self.batches.clone()],
            )?))?;
            df.filter(self.expr.clone())?.collect().await?
        };

        // Don't query data from storage in memory mode
        // TODO: make query parallel
        if !crate::is_memory_mode() {
            let pq = PartitionQuery::new(
                ".".into(),
                self.start
                    .unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                self.end.unwrap_or(OffsetDateTime::now_utc()),
            );
            let spans = pq
                .query_table(self.table_name, self.expr)
                .await
                .unwrap_or_default();
            tracing::debug!("{} from parquet: {}", self.table_name, spans.len());
            total_batches.extend(spans);
        }

        Ok(serialize_record_batches::<T>(&total_batches).unwrap_or_default())
    }
}

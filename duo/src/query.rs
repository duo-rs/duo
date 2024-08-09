use std::sync::Arc;

use crate::arrow::schema_span;
use crate::arrow::serialize_record_batches;
use crate::partition::PartitionQuery;
use crate::MemoryStore;

use anyhow::{Ok, Result};
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::prelude::{col, Expr};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use time::{Duration, OffsetDateTime};

pub struct QueryEngine {
    memory_store: Arc<RwLock<MemoryStore>>,
}

impl QueryEngine {
    pub fn new(memory_store: Arc<RwLock<MemoryStore>>) -> Self {
        Self { memory_store }
    }

    pub fn aggregate_span_names(self, expr: Expr) -> AggregateQuery {
        AggregateQuery {
            raw_query: self.query_span(expr),
            group_expr: vec![col("name")],
        }
    }

    pub fn query_span(&self, expr: Expr) -> Query {
        let guard = self.memory_store.read();
        let memtable: Option<MemTable> = if guard.span_batches.is_empty() {
            None
        } else {
            Some(
                MemTable::try_new(schema_span(), vec![guard.span_batches.clone()])
                    .expect("Create Memtable failed"),
            )
        };
        Query {
            table_name: "span",
            memtable,
            expr,
            start: None,
            end: None,
        }
    }

    pub fn query_log(&self, expr: Expr) -> Query {
        let guard = self.memory_store.read();
        let memtable = if guard.log_batches.is_empty() {
            None
        } else {
            Some(
                MemTable::try_new(
                    Arc::new(guard.log_schema.clone()),
                    vec![guard.log_batches.clone()],
                )
                .expect("Create Memtable failed"),
            )
        };
        Query {
            table_name: "log",
            memtable,
            expr,
            start: None,
            end: None,
        }
    }
}

pub struct Query {
    table_name: &'static str,
    expr: Expr,
    memtable: Option<MemTable>,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
}

pub struct AggregateQuery {
    raw_query: Query,
    group_expr: Vec<Expr>,
}

impl Query {
    pub fn range(self, start: Option<OffsetDateTime>, end: Option<OffsetDateTime>) -> Self {
        Self { start, end, ..self }
    }

    pub async fn collect<T: DeserializeOwned>(self) -> Result<Vec<T>> {
        let mut total_batches = vec![];
        if let Some(memtable) = self.memtable {
            let ctx = SessionContext::new();
            let df = ctx.read_table(Arc::new(memtable))?;
            total_batches = df.filter(self.expr.clone())?.collect().await?;
        }

        // Don't query data from storage in memory mode
        // TODO: make query parallel
        if !crate::is_memory_mode() {
            let pq = PartitionQuery::new(
                self.start
                    .unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                self.end.unwrap_or(OffsetDateTime::now_utc()),
            );
            let batches = pq
                .query_table(self.table_name, self.expr)
                .await
                .unwrap_or_default();
            tracing::debug!("{} from parquet: {}", self.table_name, batches.len());
            total_batches.extend(batches);
        }

        Ok(serialize_record_batches::<T>(&total_batches).unwrap())
    }
}

impl AggregateQuery {
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let mut total_batches = vec![];

        let Query {
            table_name,
            expr,
            memtable,
            start,
            end,
        } = self.raw_query;

        if let Some(memtable) = memtable {
            let ctx = SessionContext::new();
            let df = ctx.read_table(Arc::new(memtable))?;
            total_batches = df
                .filter(expr.clone())?
                .aggregate(self.group_expr.clone(), vec![])?
                .collect()
                .await?;
        }

        // Don't query data from storage in memory mode
        if !crate::is_memory_mode() {
            let pq = PartitionQuery::new(
                start.unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                end.unwrap_or(OffsetDateTime::now_utc()),
            );
            let df = pq.df(table_name, expr).await?;
            let batches = df.aggregate(self.group_expr, vec![])?.collect().await?;
            tracing::debug!("aggregate from parquet: {}", batches.len());
            total_batches.extend(batches);
        }
        Ok(total_batches)
    }
}

use std::sync::Arc;

use crate::arrow::serialize_record_batches;
use crate::partition::PartitionQuery;
use crate::schema;
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
        Query {
            table_name: "span",
            memtable: MemTable::try_new(
                schema::get_span_schema(),
                vec![guard.span_batches.clone()],
            )
            .expect("Create Memtable failed"),
            expr,
            start: None,
            end: None,
        }
    }

    pub fn query_log(&self, expr: Expr) -> Query {
        let guard = self.memory_store.read();
        Query {
            table_name: "log",
            memtable: MemTable::try_new(
                Arc::clone(&guard.log_schema),
                vec![guard.log_batches.clone()],
            )
            .expect("Create Memtable failed"),
            expr,
            start: None,
            end: None,
        }
    }
}

pub struct Query {
    table_name: &'static str,
    expr: Expr,
    memtable: MemTable,
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
        let ctx = SessionContext::new();
        let mut df = ctx.read_table(Arc::new(self.memtable))?;

        // Don't query data from storage in memory mode
        // TODO: make query parallel
        if !crate::is_memory_mode() {
            let pq = PartitionQuery::new(
                self.start
                    .unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                self.end.unwrap_or(OffsetDateTime::now_utc()),
            );
            df = df.union(pq.df(self.table_name).await?)?;
        }
        let batches = df.filter(self.expr)?.collect().await?;
        Ok(serialize_record_batches::<T>(&batches).unwrap())
    }
}

impl AggregateQuery {
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let Query {
            table_name,
            expr,
            memtable,
            start,
            end,
        } = self.raw_query;

        let ctx = SessionContext::new();
        let mut df = ctx.read_table(Arc::new(memtable))?;

        // Don't query data from storage in memory mode
        if !crate::is_memory_mode() {
            let pq = PartitionQuery::new(
                start.unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                end.unwrap_or(OffsetDateTime::now_utc()),
            );
            df = df.union(pq.df(table_name).await?)?;
        }
        let batches = df
            .filter(expr)?
            .aggregate(self.group_expr, vec![])?
            .collect()
            .await?;
        Ok(batches)
    }
}

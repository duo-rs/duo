use std::mem;
use std::sync::Arc;

use crate::arrow::serialize_record_batches;
use crate::partition::PartitionQuery;
use crate::schema;
use crate::MemoryStore;

use anyhow::{Ok, Result};
use datafusion::datasource::MemTable;
use datafusion::prelude::DataFrame;
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
        self.query_span(expr).aggregate(vec![col("name")], vec![])
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
            sort_expr: Vec::new(),
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
            sort_expr: Vec::new(),
        }
    }
}

pub struct Query {
    table_name: &'static str,
    expr: Expr,
    memtable: MemTable,
    start: Option<OffsetDateTime>,
    end: Option<OffsetDateTime>,
    sort_expr: Vec<Expr>,
}

pub struct AggregateQuery {
    raw_query: Query,
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
}

impl Query {
    pub fn range(self, start: Option<OffsetDateTime>, end: Option<OffsetDateTime>) -> Self {
        Self { start, end, ..self }
    }

    async fn df(self) -> Result<DataFrame> {
        let ctx = SessionContext::new();
        let mut df = ctx.read_table(Arc::new(self.memtable))?;

        // Don't query data from storage in memory mode
        if !crate::is_memory_mode() {
            let pq = PartitionQuery::new(
                self.start
                    .unwrap_or_else(|| OffsetDateTime::now_utc() - Duration::minutes(15)),
                self.end.unwrap_or(OffsetDateTime::now_utc()),
            );
            df = df.union(pq.df(self.table_name).await?)?;
        }
        Ok(df.filter(self.expr)?)
    }

    pub fn sort(self, sort_expr: Vec<Expr>) -> Self {
        Self { sort_expr, ..self }
    }

    pub fn aggregate(self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> AggregateQuery {
        AggregateQuery {
            raw_query: self,
            group_expr,
            aggr_expr,
        }
    }

    pub async fn collect<T: DeserializeOwned>(mut self) -> Result<Vec<T>> {
        let sort_expr = mem::take(&mut self.sort_expr);
        let mut df = self.df().await?;
        if !sort_expr.is_empty() {
            df = df.sort(sort_expr)?;
        }
        let batches = df.collect().await?;
        Ok(serialize_record_batches::<T>(&batches)?)
    }
}

impl AggregateQuery {
    pub async fn collect<T: DeserializeOwned>(mut self) -> Result<Vec<T>> {
        let sort_expr = mem::take(&mut self.raw_query.sort_expr);
        let mut df = self
            .raw_query
            .df()
            .await?
            .aggregate(self.group_expr, self.aggr_expr)?;
        if !sort_expr.is_empty() {
            df = df.sort(sort_expr)?;
        }
        let batches = df.collect().await?;
        Ok(serialize_record_batches::<T>(&batches)?)
    }
}

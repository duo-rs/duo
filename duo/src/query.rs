use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow_json::writer::record_batches_to_json_rows;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    prelude::{Expr, SessionContext},
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use time::{Duration, OffsetDateTime};

use crate::{arrow::schema_span, utils::TimePeriod, Log, Span};

static TABLE_SPAN: &str = "span";
static TABLE_LOG: &str = "log";

pub struct PartitionQuery {
    ctx: SessionContext,
    root_path: PathBuf,
    prefixes: Vec<String>,
}

impl PartitionQuery {
    pub fn new(root_path: PathBuf, start: OffsetDateTime, end: OffsetDateTime) -> Self {
        let ctx = SessionContext::new();
        PartitionQuery {
            ctx,
            root_path,
            prefixes: TimePeriod::new(start, end, 1).generate_prefixes(),
        }
    }

    pub fn recent_hours(root_path: PathBuf, hours: i64) -> Self {
        let now = OffsetDateTime::now_utc();
        let hours_ago = dbg!(now - Duration::hours(hours));
        Self::new(root_path, hours_ago, now)
    }

    fn table_paths(&self, table_name: &str) -> Vec<ListingTableUrl> {
        self.prefixes
            .iter()
            .filter_map(|prefix| {
                let path = self.root_path.join(table_name).join(prefix);
                ListingTableUrl::parse(path.to_str().unwrap()).ok()
            })
            .collect()
    }

    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        let listing_options = ListingOptions::new(Arc::new(
            ParquetFormat::default().with_enable_pruning(Some(true)),
        ))
        .with_file_extension(".parquet");
        let mut listing_table_config =
            ListingTableConfig::new_with_multi_paths(self.table_paths(table_name))
                .with_listing_options(listing_options);
        if table_name == TABLE_SPAN {
            listing_table_config = listing_table_config.with_schema(schema_span());
        } else {
            listing_table_config = listing_table_config.infer_schema(&self.ctx.state()).await?;
        }
        Ok(Arc::new(ListingTable::try_new(listing_table_config)?))
    }

    async fn query_table<T: DeserializeOwned>(
        &self,
        table_name: &str,
        expr: Expr,
    ) -> Result<impl IntoIterator<Item = T>> {
        let df = self.ctx.read_table(self.get_table(table_name).await?)?;
        let batch = df.filter(expr)?.collect().await.unwrap();
        let json_values = record_batches_to_json_rows(&batch.iter().collect::<Vec<_>>())
            .unwrap()
            .into_iter()
            .map(|value| serde_json::from_value::<T>(Value::Object(value)).unwrap());
        Ok(json_values)
    }

    pub async fn query_span(&self, expr: Expr) -> Result<Vec<Span>> {
        Ok(self
            .query_table(TABLE_SPAN, expr)
            .await?
            .into_iter()
            .collect())
    }

    pub async fn query_log(&self, expr: Expr) -> Result<Vec<Log>> {
        Ok(self
            .query_table(TABLE_LOG, expr)
            .await?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use time::format_description::well_known::Rfc3339;

    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_query() {
        let query = PartitionQuery::new(
            ".".into(),
            OffsetDateTime::parse("2023-06-04T14:45:00+00:00", &Rfc3339).unwrap(),
            OffsetDateTime::parse("2023-06-04T14:46:00+00:00", &Rfc3339).unwrap(),
        );
        let v = query
            .query_span(col("trace_id").eq(lit("15427617998887099000")))
            .await
            .unwrap();
        assert!(v.len() == 8);
    }
}

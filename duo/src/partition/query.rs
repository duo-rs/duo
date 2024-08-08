use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use datafusion::{
    arrow::array::RecordBatch,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    prelude::{DataFrame, Expr, SessionContext},
};
use time::{Duration, OffsetDateTime};

use crate::{arrow::schema_span, utils::TimePeriod};

static TABLE_SPAN: &str = "span";

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
        let hours_ago = now - Duration::hours(hours);
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
        let listing_options =
            ListingOptions::new(Arc::new(ParquetFormat::default().with_enable_pruning(true)))
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

    pub async fn df(&self, table_name: &str, expr: Expr) -> Result<DataFrame> {
        Ok(self
            .ctx
            .read_table(self.get_table(table_name).await?)?
            .filter(expr)?)
    }

    pub async fn query_table(&self, table_name: &str, expr: Expr) -> Result<Vec<RecordBatch>> {
        let df = self.df(table_name, expr).await?;
        Ok(df.collect().await.unwrap_or_default())
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
            .query_table("span", col("trace_id").eq(lit("15427617998887099000")))
            .await
            .unwrap();
        assert!(v.len() == 8);
    }
}

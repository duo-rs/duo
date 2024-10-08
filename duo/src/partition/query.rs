use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    arrow::array::RecordBatch,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    prelude::{DataFrame, Expr, SessionConfig, SessionContext},
};
use time::{Duration, OffsetDateTime};
use url::Url;

use crate::{config, schema, utils::TimePeriod};

static TABLE_SPAN: &str = "span";

pub struct PartitionQuery {
    ctx: SessionContext,
    object_store_url: Url,
    prefixes: Vec<String>,
}

impl PartitionQuery {
    pub fn new(start: OffsetDateTime, end: OffsetDateTime) -> Self {
        let ctx = SessionContext::new_with_config(
            // Enable bloom filter pruning for parquet readers
            SessionConfig::new().with_parquet_bloom_filter_pruning(true),
        );
        let config = config::load();
        let object_store_url = config.object_store_url();
        ctx.register_object_store(&object_store_url, config.object_store());
        PartitionQuery {
            ctx,
            object_store_url,
            prefixes: TimePeriod::new(start, end, 1).generate_prefixes(),
        }
    }

    pub fn recent_hours(hours: i64) -> Self {
        let now = OffsetDateTime::now_utc();
        let hours_ago = now - Duration::hours(hours);
        Self::new(hours_ago, now)
    }

    fn table_paths(&self, table_name: &str) -> Vec<ListingTableUrl> {
        self.prefixes
            .iter()
            .filter_map(|prefix| {
                ListingTableUrl::parse(
                    self.object_store_url
                        .join(&format!("{table_name}/{prefix}"))
                        .unwrap(),
                )
                .ok()
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
            listing_table_config = listing_table_config.with_schema(schema::get_span_schema());
        } else {
            // FIXME: log dynamic fields schema
            listing_table_config = listing_table_config.with_schema(schema::get_log_schema());
            // listing_table_config = listing_table_config.infer_schema(&self.ctx.state()).await?;
            // println!("listing schema: {:?}", listing_table_config.file_schema);
        }
        Ok(Arc::new(ListingTable::try_new(listing_table_config)?))
    }

    pub async fn df(&self, table_name: &str) -> Result<DataFrame> {
        Ok(self.ctx.read_table(self.get_table(table_name).await?)?)
    }

    pub async fn query_table(&self, table_name: &str, expr: Expr) -> Result<Vec<RecordBatch>> {
        let df = self.df(table_name).await?;
        Ok(df.filter(expr)?.collect().await.unwrap_or_default())
    }
}

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    prelude::{Expr, SessionContext},
};
use time::OffsetDateTime;

pub struct PartitionQuery {
    ctx: SessionContext,
    start: OffsetDateTime,
    end: OffsetDateTime,
    root_path: PathBuf,
    prefixes: Vec<String>,
}

impl PartitionQuery {
    pub fn new(root_path: PathBuf, start: OffsetDateTime, end: OffsetDateTime) -> Self {
        let ctx = SessionContext::new();
        PartitionQuery {
            ctx,
            start,
            end,
            root_path,
            prefixes: vec![],
        }
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

    fn get_table(&self, table_name: &str) -> Result<Arc<dyn TableProvider>> {
        let listing_options = ListingOptions::new(Arc::new(
            ParquetFormat::default().with_enable_pruning(Some(true)),
        ))
        .with_file_extension(".parquet");
        let listing_table_config =
            ListingTableConfig::new_with_multi_paths(self.table_paths(table_name))
                .with_listing_options(listing_options);
        Ok(Arc::new(ListingTable::try_new(listing_table_config)?))
    }

    pub async fn query_span(&self, expr: Expr) -> Result<()> {
        let df = self.ctx.read_table(self.get_table("span")?)?;
        let batch = df.collect().await?;
        todo!()
    }
}

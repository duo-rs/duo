use std::sync::Arc;

use anyhow::Result;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::{arrow::array::RecordBatch, parquet::file::properties::WriterProperties};
use object_store::{path::Path, ObjectStore};
use rand::{rngs::ThreadRng, Rng};
use time::OffsetDateTime;

use crate::config;

pub struct PartitionWriter {
    object_store: Arc<dyn ObjectStore>,
    partition_path: String,
}

impl PartitionWriter {
    pub fn with_minute() -> Self {
        let now = OffsetDateTime::now_utc();
        let config = config::load();
        PartitionWriter {
            object_store: config.object_store(),
            partition_path: format!(
                "date={}/hour={:02}/minute={:02}",
                now.date(),
                now.hour(),
                now.minute()
            ),
        }
    }

    pub async fn write_partition(
        &self,
        table_name: &str,
        record_batchs: &[RecordBatch],
    ) -> Result<()> {
        let schema = if let Some(rb) = record_batchs.first() {
            rb.schema()
        } else {
            return Ok(());
        };

        let mut buffer = vec![];
        // Enable bloom filter for trace_id column,
        // both span and log have trace_id column
        let properties = WriterProperties::builder()
            .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
            .build();
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, schema, Some(properties))?;
        for rb in record_batchs {
            writer.write(rb).await?;
        }
        writer.close().await?;
        let path = Path::from(format!(
            "{table_name}/{}/{}.parquet",
            self.partition_path,
            ThreadRng::default().gen::<u32>()
        ));
        self.object_store.put(&path, buffer.into()).await?;

        Ok(())
    }
}

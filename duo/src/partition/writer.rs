use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::AsyncArrowWriter;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use rand::{rngs::ThreadRng, Rng};
use time::OffsetDateTime;

pub struct PartitionWriter {
    object_store: Arc<dyn ObjectStore>,
    partition_path: String,
}

impl PartitionWriter {
    pub fn with_minute() -> Self {
        let now = OffsetDateTime::now_utc();
        PartitionWriter {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(".").unwrap()),
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
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, schema, None)?;
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

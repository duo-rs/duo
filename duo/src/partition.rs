use std::{fs::File, io::Write, vec};

use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::AsyncArrowWriter;
use rand::{rngs::ThreadRng, Rng};
use time::OffsetDateTime;

pub struct PartitionWriter {
    partition_path: String,
}

impl PartitionWriter {
    pub fn with_minute() -> Self {
        let now = OffsetDateTime::now_utc();
        PartitionWriter {
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

        let path = std::path::Path::new(table_name).join(&self.partition_path);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        let mut file =
            File::create(path.join(format!("{}.parquet", ThreadRng::default().gen::<u32>())))?;

        let mut buffer = vec![];
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, schema, None)?;
        for rb in record_batchs {
            if rb.num_rows() == 0 {
                continue;
            }
            writer.write(rb).await?;
        }
        writer.close().await?;

        file.write_all(buffer.as_slice())?;
        Ok(())
    }
}

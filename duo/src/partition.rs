use std::{fs::File, io::Write, vec};

use arrow_array::RecordBatch;
use parquet::arrow::AsyncArrowWriter;
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
                "date={}/hour={}/minute={}",
                now.date(),
                now.hour(),
                now.minute()
            ),
        }
    }

    pub async fn write_partition(
        &self,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> anyhow::Result<()> {
        if record_batch.num_rows() == 0 {
            return Ok(());
        }

        let path = std::path::Path::new(table_name).join(&self.partition_path);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        let mut file =
            File::create(path.join(format!("{}.parquet", ThreadRng::default().gen::<u32>())))?;
        let mut buffer = vec![];
        let mut writer = AsyncArrowWriter::try_new(&mut buffer, record_batch.schema(), 0, None)?;
        writer.write(&record_batch).await?;
        writer.close().await?;

        file.write_all(buffer.as_slice())?;
        Ok(())
    }
}

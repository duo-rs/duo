use std::{fs::File, io::Write, vec};

use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::AsyncArrowWriter;
use rand::{rngs::ThreadRng, Rng};
use time::OffsetDateTime;
use tracing::debug;

use crate::{
    arrow::{LogRecordBatchBuilder, SpanRecordBatchBuilder},
    Log, Span,
};

pub struct PartitionWriter {
    partition_path: String,
    log_batches: Vec<RecordBatch>,
    span_batches: Vec<RecordBatch>,
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
            log_batches: Vec::new(),
            span_batches: Vec::new(),
        }
    }

    pub fn write_logs(&mut self, logs: Vec<Log>) -> Result<()> {
        if !logs.is_empty() {
            let mut log_record_batch_builder = LogRecordBatchBuilder::default();
            for log in logs {
                log_record_batch_builder.append_log(log);
            }
            self.log_batches
                .push(log_record_batch_builder.into_record_batch()?);
        }
        Ok(())
    }

    pub fn write_spans(&mut self, spans: Vec<Span>) -> Result<()> {
        if !spans.is_empty() {
            let mut span_record_batch_builder = SpanRecordBatchBuilder::default();
            for span in spans {
                span_record_batch_builder.append_span(span);
            }
            self.span_batches
                .push(span_record_batch_builder.into_record_batch()?);
        }
        Ok(())
    }

    pub async fn flush(self) -> Result<()> {
        debug!(
            "Flush record batch to parquet file, logs: {}, spans: {}",
            self.log_batches.len(),
            self.span_batches.len()
        );
        self.write_partition("log", &self.log_batches).await?;
        self.write_partition("span", &self.span_batches).await?;
        Ok(())
    }

    async fn write_partition(&self, table_name: &str, record_batchs: &[RecordBatch]) -> Result<()> {
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

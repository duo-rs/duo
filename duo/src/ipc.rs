use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::Result;
use arrow_schema::Schema;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::{array::RecordBatch, ipc::reader::FileReader};

use crate::{config, schema};

pub struct IpcFile {
    path: PathBuf,
}

impl IpcFile {
    pub fn new() -> Self {
        let config = config::load();
        Self {
            path: Path::new(&config.data_dir).join("ipc"),
        }
    }

    pub fn read_log_ipc(&self) -> Result<Vec<RecordBatch>> {
        self.read_ipc("log")
    }

    pub fn read_span_ipc(&self) -> Result<Vec<RecordBatch>> {
        self.read_ipc("span")
    }

    fn read_ipc(&self, name: &'static str) -> Result<Vec<RecordBatch>> {
        let ipc_path = self.path.join(format!("{name}.arrow"));
        if !ipc_path.exists() {
            return Ok(vec![]);
        }
        let reader = FileReader::try_new(File::open(ipc_path)?, None)?;
        Ok(reader.filter_map(Result::ok).collect::<Vec<_>>())
    }

    fn write_ipc(
        &self,
        name: &'static str,
        batches: &[RecordBatch],
        schema: &Schema,
    ) -> Result<()> {
        if !self.path.exists() {
            fs::create_dir_all(&self.path)?;
        }
        let ipc_path = self.path.join(format!("{name}.arrow"));
        let mut writer = FileWriter::try_new(File::create(ipc_path)?, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn write_log_ipc(&self, batches: &[RecordBatch], schema: &Schema) -> Result<()> {
        self.write_ipc("log", batches, schema)
    }

    pub fn write_span_ipc(&self, batches: &[RecordBatch]) -> Result<()> {
        self.write_ipc("span", batches, &schema::get_span_schema())
    }

    pub fn clear(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_dir_all(&self.path)?;
        }
        Ok(())
    }
}

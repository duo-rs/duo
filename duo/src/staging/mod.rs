use std::{fs::File, io::BufWriter};

use datafusion::arrow::ipc::writer::StreamWriter;

pub struct StagingWriter {
    ipc_writer: StreamWriter<BufWriter<File>>,
}

impl StagingWriter {
    pub fn new() -> Self {
        StagingWriter { ipc_writer: () }
    }
}

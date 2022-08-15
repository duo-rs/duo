use std::path::Path;
use serde::Serialize;
use time::{Date, OffsetDateTime};
use tokio::fs::{create_dir_all, File, OpenOptions};
use tokio::io;
use tokio::io::{AsyncWriteExt, BufWriter};
use crate::data::persist::PersistConfig;


pub struct PersistWriter {
    writer: BufWriter<File>,
    buffer: Vec<u8>,
    current_time: Date,
    op: PersistConfig,
    // is the data be write but not flush
    dirty: bool,
}

impl PersistWriter {
    pub async fn new(op: PersistConfig) -> io::Result<PersistWriter> {
        let current_time = OffsetDateTime::now_utc().date();
        if !Path::new(&op.path).exists() {
            create_dir_all(&op.path).await?;
        }
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(format!("{}/{}", op.path, current_time.to_string()))
            .await?;
        Ok(Self {
            buffer: Vec::with_capacity(1000),
            op,
            current_time,
            writer: BufWriter::new(f),
            dirty: false,
        })
    }
    pub async fn write(&mut self, data: impl Serialize) -> io::Result<()> {
        let mut encoded: Vec<u8> = bincode::serialize(&data).unwrap();
        // let mut align = Vec::with_capacity(encoded.len() % 4);
        let current = OffsetDateTime::now_utc().date();
        // a new day has come, flush current buffer and start a new log file to write
        if current != self.current_time {
            self.flush().await?;
            self.writer.shutdown().await?;
            self.current_time = current;
            let f = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(format!("{}/{}", self.op.path, self.current_time.to_string()))
                .await?;
            self.writer = BufWriter::new(f);
        }
        let mut len = Self::len_to_byte(encoded.len());
        self.buffer.append(&mut len);
        self.buffer.append(&mut encoded);
        // self.buffer.append(&mut align);
        self.dirty = true;
        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            io::copy(&mut &self.buffer[..], &mut self.writer).await?;
            self.writer.flush().await?;
            self.buffer.clear();
        }
        self.dirty = false;
        Ok(())
    }

    /// convert length to 4 bytes vec
    pub fn len_to_byte(len: usize) -> Vec<u8> {
        vec![(len as u32 >> 24) as u8, (len as u32 >> 16) as u8, (len as u32 >> 8) as u8, len as u8]
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::io;
    use crate::data::reader::PersistReader;
    use crate::data::serialize::{PersistValue, ProcessPersist};
    use crate::data::writer::{PersistConfig, PersistWriter};
    use rand::{thread_rng, Rng};
    use rand::distributions::Alphanumeric;


    #[test]
    fn serialize_len_test() {
        let len: u32 = 0x12345678;
        let v = PersistWriter::len_to_byte(len as usize);
        assert_eq!(v, vec![0x12, 0x34, 0x56, 0x78]);
        assert_eq!(len, PersistReader::get_len(&v) as u32);
    }

    #[tokio::test]
    // use writer write 100 random Process to file system, and use reader to parse writer's output
    async fn reader_and_writer_test() {
        // writer write
        let mut writer = PersistWriter::new(PersistConfig {
            path: "/tmp/duo/data/process/".to_string(),
            log_load_time: 14,
        }).await.unwrap();
        let mut process = ProcessPersist {
            id: String::new(),
            service_name: String::new(),
            tags: HashMap::new(),
        };
        let mut process_vec = Vec::new();
        for _ in 1..100 {
            process.id = String::from_utf8(thread_rng()
                .sample_iter(&Alphanumeric)
                .take(64)
                .collect()).unwrap();
            process.service_name = String::from_utf8(thread_rng()
                .sample_iter(&Alphanumeric)
                .take(64)
                .collect()).unwrap();
            process.tags = HashMap::from([
                (String::from_utf8(thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(64)
                    .collect()).unwrap(), PersistValue::Bool(true)),
            ]);
            process_vec.push(process.clone());
            writer.write(process.clone()).await.unwrap();
        }
        writer.flush().await.unwrap();
        // reader read
        let _error = PersistReader::new(PersistConfig {
            path: "/tmp/duo/not_exist/".to_string(),
            log_load_time: 14,
        });
        assert!(matches!(io::ErrorKind::NotFound,_error));
        let mut reader = PersistReader::new(PersistConfig {
            path: "/tmp/duo/data/process/".to_string(),
            log_load_time: 14,
        }).unwrap();
        let data: Vec<ProcessPersist> = reader.parse().await.unwrap();
        let mut process_map = HashMap::new();
        for x in data {
            process_map.insert(x.id.clone(), x);
        }
        for ele in process_vec {
            assert_eq!(process_map.contains_key(&ele.id), true);
            let process = process_map.get(&ele.id).unwrap().clone();
            assert_eq!(process.service_name, ele.service_name);
            let process_tag = process.tags.into_iter().next().unwrap();
            let ele_tag = ele.tags.into_iter().next().unwrap();
            assert_eq!(process_tag.0, ele_tag.0);
        }
    }
}
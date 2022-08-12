use std::fs;
use std::path::{Path, PathBuf};
use serde::de::DeserializeOwned;
use tokio::fs::{OpenOptions};
use tokio::io;
use tokio::io::AsyncReadExt;
use crate::data::writer::PersistConfig;

pub struct PersistReader {
    path_list: Vec<PathBuf>,
}

impl PersistReader {
    pub const PATH_NOT_EXIST: Result<Self, &'static str> = Err("PersistReader's config read path not exist");

    pub fn new(config: PersistConfig) -> Result<Self, &'static str> {
        let dir = Path::new(&config.path);
        if !dir.exists() {
            return Self::PATH_NOT_EXIST;
        }
        let mut path_list = Vec::with_capacity(config.log_reserve_time as usize);
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            path_list.push(entry.path())
        }
        Ok(Self {
            path_list
        })
    }

    pub async fn parse<T>(&mut self) -> io::Result<Vec<T>> where T: DeserializeOwned {
        let mut data_vec: Vec<T> = Vec::new();
        for file_path in &self.path_list {
            Self::parse_file(file_path, &mut data_vec).await?;
        }
        Ok(data_vec)
    }

    async fn parse_file<T>(file_path: &PathBuf, data_vec: &mut Vec<T>) -> io::Result<()> where T: DeserializeOwned {
        let mut f = OpenOptions::new()
            .read(true)
            .open(file_path)
            .await?;
        let mut buffer = Vec::new();
        let mut current_index: usize = 0;
        f.read_to_end(&mut buffer).await?;
        while current_index < buffer.len() {
            let len = Self::get_len(&buffer[current_index..current_index + 4]);
            current_index += 4;
            let data: T = bincode::deserialize(&buffer[current_index..current_index + len]).unwrap();
            data_vec.push(data);
            current_index += len;
        }
        Ok(())
    }

    pub fn get_len(len: &[u8]) -> usize {
        (((len[0] as u32) << 24) | ((len[1] as u32) << 16) | ((len[2] as u32) << 8) | (len[3] as u32)) as usize
    }
}
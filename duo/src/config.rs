use std::{
    env, fs,
    path::Path,
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result};
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use serde::Deserialize;
use url::Url;

static DUO_CONFIG: OnceLock<Arc<DuoConfig>> = OnceLock::new();

#[derive(Debug, Deserialize)]
pub struct DuoConfig {
    storage: StorageConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StorageConfig {
    Local {
        dir: String,
    },
    S3 {
        bucket: String,
        region: String,
        aws_access_key: Option<String>,
        aws_access_secret: Option<String>,
    },
}

pub fn load() -> Arc<DuoConfig> {
    Arc::clone(DUO_CONFIG.get().expect("DuoConfig not initialized"))
}

pub fn set(config: DuoConfig) {
    DUO_CONFIG
        .set(Arc::new(config))
        .expect("DuoConfig already initialized")
}

impl DuoConfig {
    pub fn parse_from_toml<P: AsRef<Path>>(source: P) -> Result<Self> {
        let source = source.as_ref();
        let content = fs::read_to_string(source)
            .with_context(|| format!("Read `{}` failed", source.display()))?;

        Ok(toml::from_str::<DuoConfig>(&content)
            .unwrap_or_else(|err| panic!("Parse `{}` failed: {}", source.display(), err)))
    }

    pub fn object_store_url(&self) -> Url {
        match &self.storage {
            StorageConfig::Local { dir } => {
                let path = Path::new(dir);
                if path.is_relative() {
                    if let Ok(cwd) = env::current_dir() {
                        let path = cwd.join(path);
                        if path.is_relative() {
                            panic!("Invalid path: {}", path.display());
                        }
                        // A trailing slash is significant. Without it, the last path component
                        // is considered to be a “file” name to be removed to get at the “directory”
                        // that is used as the base.
                        // https://docs.rs/url/latest/url/struct.Url.html#method.join
                        return Url::parse(&format!("file://{}/", path.display())).unwrap();
                    }
                }
                Url::parse(&format!("file://{dir}/")).unwrap()
            }
            StorageConfig::S3 { bucket, .. } => Url::parse(&format!("s3://{bucket}/")).unwrap(),
        }
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self.storage {
            StorageConfig::Local { dir } => {
                let path = Path::new(dir);
                if !path.exists() {
                    std::fs::create_dir_all(path).unwrap();
                }

                Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
            }
            StorageConfig::S3 {
                bucket,
                region,
                aws_access_key,
                aws_access_secret,
            } => {
                let s3 = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_access_key_id(
                        env::var("AWS_ACCESS_KEY_ID")
                            .ok()
                            .as_ref()
                            .or(aws_access_key.as_ref())
                            .unwrap(),
                    )
                    .with_secret_access_key(
                        env::var("AWS_SECRET_ACCESS_KEY")
                            .ok()
                            .as_ref()
                            .or(aws_access_secret.as_ref())
                            .unwrap(),
                    )
                    .build()
                    .unwrap();
                Arc::new(s3)
            }
        }
    }
}

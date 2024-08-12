use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, LazyLock, OnceLock,
};

use anyhow::Result;
use arrow_schema::{DataType, Field, Schema};
use object_store::path::Path;
use parking_lot::RwLock;

use crate::config;

static LOG_SCHEMA: OnceLock<RwLock<Arc<Schema>>> = OnceLock::new();
static LOG_SCHEMA_DIRTY: AtomicBool = AtomicBool::new(false);

static SPAN_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("parent_id", DataType::UInt64, true),
        Field::new("trace_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("process_id", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, true),
        Field::new("tags", DataType::Utf8, true),
    ]))
});

#[inline]
fn default_log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("process_id", DataType::Utf8, false),
        Field::new("time", DataType::Int64, false),
        Field::new("trace_id", DataType::UInt64, true),
        Field::new("span_id", DataType::UInt64, true),
        Field::new("level", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, true),
    ]))
}

pub fn get_span_schema() -> Arc<Schema> {
    Arc::clone(&SPAN_SCHEMA)
}

pub async fn load() -> Result<()> {
    let config = config::load();
    let object_store = config.object_store();
    match object_store
        .get(&Path::from("schema/log_schema.json"))
        .await
    {
        Ok(data) => {
            let schema = serde_json::from_slice::<Schema>(&data.bytes().await?)?;
            LOG_SCHEMA
                .set(RwLock::new(Arc::new(schema)))
                .expect("LogSchema already initialized");
        }
        Err(_err) => {
            LOG_SCHEMA
                .set(RwLock::new(default_log_schema()))
                .expect("LogSchema already initialized");
        }
    }

    Ok(())
}

pub fn get_log_schema() -> Arc<Schema> {
    Arc::clone(&LOG_SCHEMA.get().expect("LogSchema not initialized").read())
}

pub fn merge_log_schema(schema: Arc<Schema>) -> Arc<Schema> {
    let mut guard = LOG_SCHEMA.get().expect("LogSchema not initialized").write();
    if guard.contains(&schema) {
        return Arc::clone(&*guard);
    }

    let new_schema =
        Arc::new(Schema::try_merge(vec![(**guard).clone(), (*schema).clone()]).unwrap());
    *guard = Arc::clone(&new_schema);
    LOG_SCHEMA_DIRTY.store(true, Ordering::Relaxed);
    new_schema
}

pub async fn persit_log_schema() {
    if LOG_SCHEMA_DIRTY.load(Ordering::Relaxed) {
        let object_store = config::load().object_store();
        let payload = serde_json::to_vec(&get_log_schema()).unwrap();
        object_store
            .put(&Path::from("schema/log_schema.json"), payload.into())
            .await
            .unwrap();
        LOG_SCHEMA_DIRTY.store(false, Ordering::Relaxed);
    }
}

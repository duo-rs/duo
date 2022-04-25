//! Duo subscriber for tracing.
//!
use std::collections::HashMap;

use duo_api as proto;
mod client;
mod conn;
mod subscriber;
mod visitor;

pub use subscriber::DuoLayer;

// Grasp basic process info, this will collect to server
// when register process.
fn grasp_process_info() -> HashMap<String, proto::Value> {
    let mut tags = HashMap::default();
    tags.insert("duo-version".into(), env!("CARGO_PKG_VERSION").into());
    tags
}

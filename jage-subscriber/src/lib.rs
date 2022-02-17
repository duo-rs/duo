//! Jage subscriber for tracing.
//!
use std::collections::HashMap;

use jage_api as proto;
mod client;
mod conn;
mod subscriber;
mod visitor;

pub use subscriber::JageLayer;

// Grasp basic process info, this will collect to server
// when register process.
fn grasp_process_info() -> HashMap<String, proto::Value> {
    let mut tags = HashMap::default();
    tags.insert("jage-version".into(), env!("CARGO_PKG_VERSION").into());
    tags
}

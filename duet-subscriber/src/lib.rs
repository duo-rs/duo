//! Duet subscriber for tracing.
//!
use std::collections::HashMap;

use duet_api as proto;
mod client;
mod conn;
mod subscriber;
mod visitor;

pub use subscriber::DuetLayer;

// Grasp basic process info, this will collect to server
// when register process.
fn grasp_process_info() -> HashMap<String, proto::Value> {
    let mut tags = HashMap::default();
    tags.insert("duet-version".into(), env!("CARGO_PKG_VERSION").into());
    tags
}

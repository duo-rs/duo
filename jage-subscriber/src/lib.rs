//! Jage subscriber for tracing.
//!
use jage_api as proto;
mod client;
mod conn;
mod subscriber;
mod visitor;

pub use subscriber::JageLayer;

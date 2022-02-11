use std::time::Duration;

use jage_subscriber::JageLayer;
use tonic::transport::Uri;
use tracing::Level;
use tracing_subscriber::{
    self, filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

#[tracing::instrument]
fn foo() {
    bar();
}

#[tracing::instrument]
fn bar() {
    baz();
}

#[tracing::instrument]
fn baz() {}

#[tokio::main]
async fn main() {
    let fmt_layer = fmt::layer();
    let uri = Uri::from_static("http://127.0.0.1:6000");
    let jage_layer = JageLayer::new(uri).await;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(jage_layer)
        .with(
            Targets::new()
                .with_target("main", Level::DEBUG)
                .with_target("tracing_subscriber", Level::DEBUG),
        )
        .init();

    tracing::debug!("Bootstrap...");
    foo();
    tokio::time::sleep(Duration::from_secs(3)).await;
}

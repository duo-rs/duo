use std::time::Duration;

use duo_subscriber::DuoLayer;
use tonic::transport::Uri;
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::{
    self, filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

#[tracing::instrument]
fn foo() {
    info!(test = true, "hello foo!");
    bar();
    debug!("called bar!");
    foz();
}

#[tracing::instrument]
fn bar() {
    baz();
}

#[tracing::instrument]
fn baz() {
    warn!("hello baz!");
}

#[tracing::instrument]
fn foz() {
    debug!("hello foz!");
    error!(flag = 1, data = "data", "Oops!");
}

#[tokio::main]
async fn main() {
    let fmt_layer = fmt::layer();
    let uri = Uri::from_static("http://127.0.0.1:6000");
    let duo_layer = DuoLayer::new("example", uri).await;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(duo_layer)
        .with(
            Targets::new()
                .with_target("main", Level::DEBUG)
                .with_target("tracing_subscriber", Level::DEBUG),
        )
        .init();

    tracing::info!("Bootstrap...");
    foo();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

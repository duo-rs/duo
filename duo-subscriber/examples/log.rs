// cargo run --example log --features=log-compat

use duo_subscriber::DuoLayer;
use log::debug;
use tonic::transport::Uri;
use tracing::Level;
use tracing_subscriber::{self, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

#[tracing::instrument]
fn foo() {
    debug!("hello foo!");
    bar();
    debug!("called bar!");
}

#[tracing::instrument]
fn bar() {
    debug!("hello bar!");
    baz();
}

#[tracing::instrument]
fn baz() {
    debug!("hello baz!");
}

#[tokio::main]
async fn main() {
    let name = "log";
    let uri = Uri::from_static("http://127.0.0.1:6000");
    let (duo_layer, handle) = DuoLayer::with_handle(name, uri).await;
    tracing_subscriber::registry()
        .with(duo_layer)
        .with(
            Targets::new()
                .with_target(name, Level::DEBUG)
                .with_target("tracing_subscriber", Level::DEBUG),
        )
        .init();

    debug!("Bootstrap...");
    foo();

    handle.await.unwrap();
}

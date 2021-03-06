use duo_subscriber::DuoLayer;
use tonic::transport::Uri;
use tracing::{debug, error, Level};
use tracing_subscriber::{
    self, filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

#[tracing::instrument]
fn foo() {
    debug!(test = true, "hello foo!");
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
    debug!("hello baz!");
}

#[tracing::instrument]
fn foz() {
    debug!("hello foz!");
    error!("Oops!");
}

#[tokio::main]
async fn main() {
    let fmt_layer = fmt::layer();
    let uri = Uri::from_static("http://127.0.0.1:6000");
    let (duo_layer, handle) = DuoLayer::with_handle("example", uri).await;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(duo_layer)
        .with(
            Targets::new()
                .with_target("main", Level::DEBUG)
                .with_target("tracing_subscriber", Level::DEBUG),
        )
        .init();

    tracing::debug!("Bootstrap...");
    foo();

    handle.await.unwrap();
}

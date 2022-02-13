use axum::{routing::get, Router};

mod serialize;

pub struct JaegerData<I: IntoIterator>(pub I);

pub async fn run_web_server() {
    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/traces", get(|| async { "Hello, World!" }))
        .route("/services", get(|| async { "Hello, World!" }))
        .route(
            "/services/:service/operattions",
            get(|| async { "Hello, World!" }),
        );

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

# Duo

[![Crates.io](https://img.shields.io/crates/v/duo.svg)](https://crates.io/crates/duo)
![Crates.io](https://img.shields.io/crates/d/duo)
[![license-mit](https://img.shields.io/badge/license-MIT-yellow.svg)](./LICENSE)

**Observability duo: Logging and Tracing.**

> **Notice: this project is in the experimental stage and not production-ready. Use at your own risk.**

## What is duo?

Duo is an easy-to-use observability solution that provides both logging and tracing capabilities for Rust applications. While traditional observability solutions are powerful (such as [ELK](https://elastic.co), [jaegertracing](https://jaegertracing.io), etc), it is also complex to deploy and maintain. Duo aimed to provide a less-powerful but complete set of observability features, with extremely simple deployment and maintenance. 

This project was inspired by [tracing](https://github.com/tokio-rs/tracing) and [console](https://github.com/tokio-rs/console), which mainly consist of multiple components:

- **duo-api** - a wire protocol for logging and tracing data. The wire format is defined using gRPC and protocol buffers.
- **duo-subscriber** - instrumentation for collecting logging and tracing data from a process and exposing it over the wire format. `duo-subscriber` crate in this repository contains an implementation of the instrumentation-side API as a [tracing-subscriber](https://crates.io/crates/tracing-subscriber) [Layer](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/trait.Layer.html), for projects using Tokio and tracing.
- **duo-ui** - the web UI for duo. Currently, we just use the [jaeger-ui](https://github.com/jaegertracing/jaeger-ui) for tracing and have no UI for logging. The future repository is here: [duo-rs/duo-ui](https://github.com/duo-rs/duo-ui).
- **duo-server** - the aggregating server to collect tracing and logging data and interact with duo web UI.

## Why called duo?

Duo is mainly a musical terminology meaning a musical composition for two performers in which the performers have equal importance to the piece, often a composition involving two singers or two pianists.

The famous duo band is [Brooklyn Duo](https://www.youtube.com/c/BrooklynDuo), you can visit this video ([Canon in D (Pachelbel's Canon) - Cello & Piano](https://www.youtube.com/watch?v=Ptk_1Dc2iPY)) to learn more about them.

![](https://i.ytimg.com/vi/Ptk_1Dc2iPY/maxresdefault.jpg)

I personally think the logging and tracing have equal importance to observability, they are just like a duo band to help you diagnose your application.

## Get started

### Installation

```
cargo install duo
```

Run `duo start`.

```
$ duo start

gRPC server listening on http://127.0.0.1:6000

Web server listening on http://127.0.0.1:3000
```

Open https://127.0.0.1:3000 at your local browser to wait application report data.

### Application

```toml
duo-subscriber = "0.1"
```

```rs
#[tokio::main]
async fn main() {
    let fmt_layer = fmt::layer();
    let uri = Uri::from_static("http://127.0.0.1:6000");
    let (duo_layer, handle) = DuoLayer::with_handle("example", uri).await;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(duo_layer)
        .init();

    tracing::debug!("Bootstrap...");
    foo();

    handle.await.unwrap();
}
```
> For more example, please see [examples directory](./duo-subscriber/examples/).

Run your application then check the http://127.0.0.1:3000 to see the tracing data.

![](./duo-ui.png)

## Roadmap

- [x] Support tracing diagnosing with Jaeger UI.

- [ ] Build duo web UI.

- [ ] Support logging diagnosing.

- [ ] Support arrow-ipc WAL.

- [ ] Support OpenTelemetry specification, aimed to be a lightweight OpenTelemetry backend.

## License

This project is licensed under the [MIT license](./LICENSE).

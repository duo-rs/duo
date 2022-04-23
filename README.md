# Duet

[![Crates.io](https://img.shields.io/crates/v/duet.svg)](https://crates.io/crates/duet)
![Crates.io](https://img.shields.io/crates/d/duet)
[![license-mit](https://img.shields.io/badge/license-MIT-yellow.svg)](./LICENSE)

**Observability duet: Logging and Tracing.**

> **Notice: this project is in the experimental stage and not production-ready. Use at your own risk.**

## What is duet?

Duet is a simple toolkit to provide observability to rust applications with logging and tracing. This project was inspired by [tracing](https://github.com/tokio-rs/tracing) and [console](https://github.com/tokio-rs/console), which mainly consist of multiple components:

- **duet-api** - a wire protocol for logging and tracing data. The wire format is defined using gRPC and protocol buffers.
- **duet-subscriber** - instrumentation for collecting logging and tracing data from a process and exposing it over the wire format. `duet-subscriber` crate in this repository contains an implementation of the instrumentation-side API as a [tracing-subscriber](https://crates.io/crates/tracing-subscriber) [Layer](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/trait.Layer.html), for projects using Tokio and tracing.
- **duet-ui** - the web UI for duet. Currently, we just use the [jaeger-ui](https://github.com/jaegertracing/jaeger-ui) for tracing and have no UI for logging. The future repository is here: [duet-rs/duet-ui](https://github.com/duet-rs/duet-ui).
- **duet-server** - the aggregating server to collect tracing and logging data and interact with duet web UI.

## Why called duet?

Duet is mainly a musical terminology meaning a musical composition for two performers in which the performers have equal importance to the piece, often a composition involving two singers or two pianists.

The famous duet band is [Brooklyn Duo](https://www.youtube.com/c/BrooklynDuo), you can visit this video ([Canon in D (Pachelbel's Canon) - Cello & Piano](https://www.youtube.com/watch?v=Ptk_1Dc2iPY)) to learn more about them.

![](https://i.ytimg.com/vi/Ptk_1Dc2iPY/maxresdefault.jpg)

I personally think the logging and tracing have equal importance to observability, they are just like a duet band to help you diagnose your application.

## Get started

### Installation

```
cargo install duet-server
```

Run `duet`.

```
$ duet

gRPC server listening on 127.0.0.1:6000

Web server listening on 127.0.0.1:3000
```

Open `127.0.0.1:3000` at your local browser to wait application report data.

### Application

```toml
duet-subscriber = "0.1"
```

```rs
#[tokio::main]
async fn main() {
    let fmt_layer = fmt::layer();
    let uri = Uri::from_static("http://127.0.0.1:6000");
    let (duet_layer, handle) = DuetLayer::with_handle("example", uri).await;
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(duet_layer)
        .init();

    tracing::debug!("Bootstrap...");
    foo();

    handle.await.unwrap();
}
```
> For more example, please see [examples directory](./duet-subscriber/examples/).

Run your application then check the `127.0.0.1:3000` to see the tracing data.

## Roadmap

- [x] Support tracing diagnosing with Jaeger UI.

- [ ] Build duet web UI.

- [ ] Support logging diagnosing.

- [ ] Support persist tracing and logging data into the database.

## License

This project is licensed under the [MIT license](./LICENSE).

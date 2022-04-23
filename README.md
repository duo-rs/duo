# Duet

**Observability duet: Logging and Tracing.**

## What is duet?

Duet is a simple toolkit to provide observability to rust applications with logging and tracing. This project was inspired by [tracing](https://github.com/tokio-rs/tracing) and [console](https://github.com/tokio-rs/console), which mainly consist of multiple components:

- **duet-api** - a wire protocol for logging and tracing data. The wire format is defined using gRPC and protocol buffers.
- **duet-subscriber** - instrumentation for collecting logging and tracing data from a process and exposing it over the wire format. `duet-subscriber` crate in this repository contains an implementation of the instrumentation-side API as a [tracing-subscriber](https://crates.io/crates/tracing-subscriber) [Layer](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/trait.Layer.html), for projects using Tokio and tracing.
- **duet-ui** - the web UI for duet. Currently, we just use the [jaeger-ui]() for tracing and have no UI for logging. The future repository is here: [duet-rs/duet-ui](https://github.com/duet-rs/duet-ui).
- **duet server** - the aggregating server to collect tracing and logging data and interact with duet web UI.

> **Notice: this project is in the experimental stage and not production-ready. Use at your own risk.**

## Why called duet?

Duet is mainly a musical terminology meaning a musical composition for two performers in which the performers have equal importance to the piece, often a composition involving two singers or two pianists.

The famous duet band is [Brooklyn Duo](https://www.youtube.com/c/BrooklynDuo), you can visit this video ([Canon in D (Pachelbel's Canon) - Cello & Piano](https://www.youtube.com/watch?v=Ptk_1Dc2iPY)) to learn more about them.

![](https://i.ytimg.com/vi/Ptk_1Dc2iPY/maxresdefault.jpg)

I personally think the logging and tracing have equal importance to observability, they are just like a duet band to help you diagnose your application.

## License

This project is licensed under the [MIT license](./LICENSE).

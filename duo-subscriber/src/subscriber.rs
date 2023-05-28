use std::{
    collections::HashMap,
    time::{Instant, SystemTime},
};

use crate::{
    conn::Connection,
    proto,
    visitor::{EventAttributeVisitor, SpanAttributeVisitor},
};
use rand::rngs::ThreadRng;
use rand::Rng;
use tokio::{
    sync::mpsc::{self, error::TrySendError, Sender},
    task::JoinHandle,
};
use tonic::transport::Uri;
use tracing::{
    span::{self, Attributes},
    Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

pub struct DuoLayer {
    sender: Sender<Message>,
}

#[derive(Debug)]
enum Message {
    NewSpan(proto::Span),
    CloseSpan(proto::Span),
    Event(proto::Log),
}

struct Timings {
    // unit:  us
    idle: u32,
    // unit:  us
    busy: u32,
    last: Instant,
}

impl Timings {
    fn new() -> Self {
        Self {
            idle: 0,
            busy: 0,
            last: Instant::now(),
        }
    }
}

impl DuoLayer {
    pub async fn new(name: &'static str, uri: Uri) -> Self {
        let (layer, _) = Self::with_handle(name, uri).await;
        layer
    }

    pub async fn with_handle(name: &'static str, uri: Uri) -> (Self, JoinHandle<()>) {
        let (sender, mut receiver) = mpsc::channel(2048);
        let handler = tokio::spawn(async move {
            let mut client = Connection::connect(name, uri).await;
            while let Some(message) = receiver.recv().await {
                match message {
                    Message::NewSpan(span) | Message::CloseSpan(span) => {
                        client.record_span(span).await
                    }
                    Message::Event(log) => {
                        client.record_event(log).await;
                    }
                }
            }
        });
        (DuoLayer { sender }, handler)
    }

    #[inline]
    fn send_message(&self, message: Message) {
        match self.sender.try_send(message) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(_)) => {}
        }
    }
}

impl<S> Layer<S> for DuoLayer
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extension = span.extensions_mut();

            let parent_span = if let Some(parent) = attrs.parent() {
                ctx.span(parent)
            } else if attrs.is_contextual() {
                ctx.lookup_current()
            } else {
                None
            };

            let rand_id = ThreadRng::default().gen();
            // Obtain parent_id and trace_id from parent span.
            let (parent_id, trace_id) = parent_span
                .and_then(|span_ref| {
                    span_ref
                        .extensions()
                        .get::<proto::Span>()
                        .map(|s| (Some(s.id), s.trace_id))
                })
                // If parent's trace_id not exists, use the newly generated one.
                .unwrap_or((None, rand_id));

            let metadata = attrs.metadata();
            let mut tags = HashMap::with_capacity(3 + metadata.fields().len());
            if let (Some(file), Some(line)) = (metadata.file(), metadata.line()) {
                tags.insert("@line".into(), format!("{}:{}", file, line).into());
            }
            let mut span = proto::Span {
                id: rand_id,
                trace_id,
                parent_id,
                name: metadata.name().into(),
                start: Some(SystemTime::now().into()),
                end: None,
                tags,
                // Set a temporary process id, we'll set a real value in send stage.
                process_id: String::new(),
            };
            attrs.record(&mut SpanAttributeVisitor(&mut span));
            self.send_message(Message::NewSpan(span.clone()));
            extension.insert(span);
            extension.insert(Timings::new());
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let parent_span_ref = if let Some(parent) = event.parent() {
            ctx.span(parent)
        } else if event.is_contextual() {
            ctx.lookup_current()
        } else {
            None
        };

        let (trace_id, span_id) = parent_span_ref
            .and_then(|span_ref| {
                span_ref
                    .extensions()
                    .get::<proto::Span>()
                    .map(|span| (Some(span.trace_id), Some(span.id)))
            })
            .unwrap_or_default();

        let metadata = event.metadata();
        let fields = HashMap::with_capacity(metadata.fields().len());
        let mut log = proto::Log {
            span_id,
            trace_id,
            level: proto::Level::from(*metadata.level()) as i32,
            time: Some(SystemTime::now().into()),
            fields,
        };
        event.record(&mut EventAttributeVisitor(&mut log));
        self.send_message(Message::Event(log));
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(timings) = extensions.get_mut::<Timings>() {
                let now = Instant::now();
                timings.idle += now.saturating_duration_since(timings.last).as_micros() as u32;
                timings.last = now;
            }
        }
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(id) {
            let mut extension = span_ref.extensions_mut();
            if let Some(span) = extension.get_mut::<proto::Span>() {
                values.record(&mut SpanAttributeVisitor(span));
            }
        }
    }

    fn on_follows_from(&self, id: &span::Id, follows: &span::Id, ctx: Context<'_, S>) {
        if let (Some(current), Some(follows)) = (ctx.span(id), ctx.span(follows)) {
            if let (Some(child), Some(parent)) = (
                current.extensions_mut().get_mut::<proto::Span>(),
                follows.extensions().get::<proto::Span>(),
            ) {
                child.parent_id = Some(parent.id);
            }
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(timings) = extensions.get_mut::<Timings>() {
                let now = Instant::now();
                timings.busy += now.saturating_duration_since(timings.last).as_micros() as u32;
                timings.last = now;
            }
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(&id) {
            let mut extensions = span_ref.extensions_mut();
            if let Some(mut span) = extensions.remove::<proto::Span>() {
                span.end = Some(SystemTime::now().into());

                if let Some(timings) = extensions.remove::<Timings>() {
                    span.tags.insert("@idle".into(), timings.idle.into());
                    span.tags.insert("@busy".into(), timings.busy.into());
                }

                self.send_message(Message::CloseSpan(span));
            }
        }
    }
}

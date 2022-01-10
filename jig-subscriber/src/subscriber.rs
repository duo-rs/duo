use std::{collections::HashMap, time::SystemTime};

use crate::{proto, visitor::SpanAttributeVisitor};
use rand::rngs::ThreadRng;
use rand::Rng;
use tracing::{
    span::{self, Attributes},
    Subscriber,
};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};
pub struct JigLayer {}

impl<S> Layer<S> for JigLayer
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

            let parent_id = parent_span
                .and_then(|span_ref| span_ref.extensions().get::<proto::Span>().map(|s| s.id));

            let rand_id = ThreadRng::default().gen();
            let trace_id = if parent_id.is_none() {
                Some(rand_id)
            } else {
                None
            };

            let metadata = attrs.metadata();
            let mut tags = HashMap::with_capacity(3 + metadata.fields().len());
            if let (Some(file), Some(line)) = (metadata.file(), metadata.line()) {
                tags.insert("line".into(), format!("{}:{}", file, line).into());
            }
            let mut span = proto::Span {
                id: rand_id,
                trace_id,
                parent_id,
                name: metadata.name().into(),
                start: Some(SystemTime::now().into()),
                end: None,
                tags,
            };
            attrs.record(&mut SpanAttributeVisitor(&mut span));
            extension.insert(span);
        }
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {}

    fn on_enter(&self, _id: &span::Id, _ctx: Context<'_, S>) {}

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

    fn on_exit(&self, _id: &span::Id, _ctx: Context<'_, S>) {}

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(&id) {
            let mut extensions = span_ref.extensions_mut();
            if let Some(mut span) = extensions.remove::<proto::Span>() {
                span.end = Some(SystemTime::now().into());
            }
        }
    }
}

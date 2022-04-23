use core::fmt;

use crate::proto;
use tracing::field::{Field, Visit};
pub(crate) struct SpanAttributeVisitor<'a>(pub(crate) &'a mut proto::Span);

pub(crate) struct EventAttributeVisitor<'a>(pub(crate) &'a mut proto::Log);

impl<'a> Visit for SpanAttributeVisitor<'a> {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.tags.insert(field.name().into(), value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.tags.insert(field.name().into(), value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.tags.insert(field.name().into(), value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.tags.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.tags.insert(field.name().into(), value.into());
    }
}

impl<'a> Visit for EventAttributeVisitor<'a> {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.fields.insert(field.name().into(), value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.fields.insert(field.name().into(), value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.fields.insert(field.name().into(), value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.fields.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.fields.insert(field.name().into(), value.into());
    }
}

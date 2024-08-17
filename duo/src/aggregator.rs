use std::mem;

use duo_api as proto;

use crate::Span;

#[derive(Debug, Default)]
pub struct SpanAggregator {
    spans: Vec<proto::Span>,
}

impl SpanAggregator {
    pub fn new() -> Self {
        SpanAggregator { spans: Vec::new() }
    }

    pub fn record_span(&mut self, raw: proto::Span) {
        if let Some(span) = self.spans.iter_mut().find(|s| s.id == raw.id) {
            if raw.parent_id.is_some() {
                span.parent_id = raw.parent_id;
            }

            if !raw.tags.is_empty() {
                span.tags.extend(raw.tags);
            }
            span.end = raw.end;
        } else {
            self.spans.push(raw);
        }
    }

    pub fn aggregate(&mut self) -> Vec<Span> {
        // Remove all intact spans.
        let (intact_spans, ongoing_spans): (Vec<_>, Vec<_>) = mem::take(&mut self.spans)
            .into_iter()
            .partition(|span| span.end.is_some());
        self.spans = ongoing_spans;
        intact_spans.into_iter().map(Span::from).collect()
    }
}

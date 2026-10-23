//! Protocol-agnostic normalization, applied after a protocol's extraction (OTLP today; the M6 runs API next):
//! token totals, provider-from-model, cost (master decision 14) per span, then dotted_order per batch.
use crate::normalize::pricing::{Pricing, Usage, provider_from_model};
use forge_model::row::SpanRow;

/// What extraction read but did not decide: token usage, the producer's own total and cost.
#[derive(Debug, Default, Clone, Copy)]
pub struct Measured {
    pub usage: Usage,
    pub producer_total: Option<i64>,
    pub producer_cost: Option<f64>,
}

/// Fills `prompt/completion/total_tokens`, a missing `provider` from a `vendor/model` name, and `cost_usd`.
///
/// `total_tokens` is the producer's total, else prompt + completion only when BOTH are on this post: a sparse
/// post (one count) must not write a partial total that would win the merge over a full post's total (the same
/// rule as P9(c) for cost).
pub fn finalize_span(r: &mut SpanRow, m: Measured, pricing: &Pricing) {
    let u = m.usage;
    r.prompt_tokens = u.prompt;
    r.completion_tokens = u.completion;
    r.total_tokens = m.producer_total.or(u.prompt.zip(u.completion).map(|(p, c)| p + c));
    if r.provider.is_none() {
        r.provider = r.model.as_deref().and_then(provider_from_model);
    }
    // Master decision 14: only leaf model calls carry cost; agent/chain tokens are aggregates.
    r.cost_usd = if r.run_type.is_some_and(|t| t.is_costed()) {
        m.producer_cost
            .or_else(|| r.model.as_deref().and_then(|model| pricing.cost(model, &u)))
    } else {
        None
    };
}

/// Batch-level step: resolves `dotted_order` across the rows of one request.
pub fn finalize_rows(rows: &mut [SpanRow]) {
    forge_model::dotted::resolve_dotted_orders(rows);
}

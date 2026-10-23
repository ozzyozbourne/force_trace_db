//! `total_tokens` in SQL (decision 15).
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, when};
use forge_model::schema::col as c;

/// The SQL form of `forge_model::derived::effective_total_tokens` (the 4-branch rule; the provider's differential
/// tests pin the two together): the producer total, else prompt + completion if both, else whichever count is
/// set, else null. Applied after the merge only.
/// Written as CASE: `coalesce()` is a logical-only function in DF 55 (the simplifier rewrites it to CASE;
/// `create_physical_expr` on it fails with "coalesce should have been simplified to case").
pub fn effective_total_tokens_expr() -> Expr {
    let (t, p, q) = (col(c::TOTAL_TOKENS), col(c::PROMPT_TOKENS), col(c::COMPLETION_TOKENS));
    when(t.clone().is_not_null(), t)
        .when(
            p.clone().is_not_null().and(q.clone().is_not_null()),
            p.clone() + q.clone(),
        )
        .when(p.clone().is_not_null(), p)
        .otherwise(q)
        .expect("static CASE")
}

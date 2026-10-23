#![allow(unused_imports)]
use crate::pv::*;
// ------------------------------------------------------------------ total_tokens (decision 15)
/// The ONE definition of the effective total. "prompt + completion if either is present":
/// both -> sum, one -> that one, none -> null. Applied after the merge only.
/// Written as CASE: `coalesce()` is a logical-only function in DF 55 (the simplifier rewrites it to CASE;
/// `create_physical_expr` on it fails with "coalesce should have been simplified to case").
pub fn effective_total_tokens_expr() -> Expr {
    let (t, p, q) = (col(c::TOTAL_TOKENS), col(c::PROMPT_TOKENS), col(c::COMPLETION_TOKENS));
    when(t.clone().is_not_null(), t)
        .when(p.clone().is_not_null().and(q.clone().is_not_null()), p.clone() + q.clone())
        .when(p.clone().is_not_null(), p)
        .otherwise(q)
        .expect("static CASE")
}

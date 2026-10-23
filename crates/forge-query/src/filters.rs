#![allow(unused_imports)]
use crate::pv::*;
// ------------------------------------------------------------------ filter classification
/// What the provider learned from the (ANDed) filters. Used for pruning only; DataFusion re-applies every
/// filter above the merge (all are Inexact).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyFilters {
    /// None = any project. Some(empty) = no row can match.
    pub projects: Option<BTreeSet<String>>,
    pub traces: Option<BTreeSet<TraceId>>,
    /// [start_ns, end_ns) on start_time: segment pruning only (decision 5).
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

pub(crate) enum Pushed {
    Projects(BTreeSet<String>),
    Traces(BTreeSet<TraceId>),
    Time { lo: Option<i64>, hi: Option<i64> },
}

fn column_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::Column(col) => Some(col.name.as_str()),
        Expr::Cast(cast) => column_name(&cast.expr),
        _ => None,
    }
}
fn literal(e: &Expr) -> Option<&ScalarValue> {
    match e {
        Expr::Literal(v, _) => Some(v),
        _ => None,
    }
}
fn str_lit(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Some(s.clone())
        }
        _ => None,
    }
}
/// A key literal of the wrong length matches no row: `Some(None)`.
fn trace_lit(v: &ScalarValue) -> Option<Option<TraceId>> {
    match v {
        ScalarValue::Binary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Some(TraceId::from_slice(b).ok()),
        _ => None,
    }
}
fn ts_ns(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::TimestampNanosecond(Some(x), _) => Some(*x),
        ScalarValue::TimestampMicrosecond(Some(x), _) => x.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(x), _) => x.checked_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(x), _) => x.checked_mul(1_000_000_000),
        _ => None,
    }
}
fn flip(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        o => o,
    }
}

/// `col = lit`, `lit = col`, a non-negated IN list, or an OR chain of those on one key column
/// (DataFusion turns IN lists of <= 3 items into OR chains).
fn key_set(e: &Expr) -> Option<(String, Vec<ScalarValue>)> {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
            let (name, v) = match (column_name(left), literal(right), column_name(right), literal(left)) {
                (Some(n), Some(v), _, _) | (_, _, Some(n), Some(v)) => (n, v),
                _ => return None,
            };
            Some((name.to_string(), vec![v.clone()]))
        }
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Or, right }) => {
            let (a, mut va) = key_set(left)?;
            let (b, vb) = key_set(right)?;
            (a == b).then(|| {
                va.extend(vb);
                (a, va)
            })
        }
        Expr::InList(InList { expr, list, negated: false }) => {
            let name = column_name(expr)?;
            let vals = list.iter().map(|e| literal(e).cloned()).collect::<Option<Vec<_>>>()?;
            Some((name.to_string(), vals))
        }
        _ => None,
    }
}

pub(crate) fn classify(e: &Expr) -> Option<Pushed> {
    if let Some((name, vals)) = key_set(e) {
        return match name.as_str() {
            c::PROJECT_ID => vals.iter().map(str_lit).collect::<Option<_>>().map(Pushed::Projects),
            c::TRACE_ID => {
                let ids = vals.iter().map(trace_lit).collect::<Option<Vec<_>>>()?;
                Some(Pushed::Traces(ids.into_iter().flatten().collect()))
            }
            _ => None,
        };
    }
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (op, v) = match (column_name(left), literal(right), column_name(right), literal(left)) {
                (Some(c::START_TIME), Some(v), _, _) => (*op, v),
                (_, _, Some(c::START_TIME), Some(v)) => (flip(*op), v),
                _ => return None,
            };
            let ns = ts_ns(v)?;
            match op {
                Operator::GtEq => Some(Pushed::Time { lo: Some(ns), hi: None }),
                Operator::Gt => Some(Pushed::Time { lo: ns.checked_add(1), hi: None }),
                Operator::Lt => Some(Pushed::Time { lo: None, hi: Some(ns) }),
                Operator::LtEq => Some(Pushed::Time { lo: None, hi: ns.checked_add(1) }),
                Operator::Eq => Some(Pushed::Time { lo: Some(ns), hi: ns.checked_add(1) }),
                _ => None,
            }
        }
        Expr::Between(Between { expr, negated: false, low, high }) if column_name(expr) == Some(c::START_TIME) => {
            let lo = literal(low).and_then(ts_ns);
            let hi = literal(high).and_then(ts_ns).and_then(|h| h.checked_add(1));
            (lo.is_some() || hi.is_some()).then_some(Pushed::Time { lo, hi })
        }
        _ => None,
    }
}

fn intersect<T: Ord + Clone>(cur: &mut Option<BTreeSet<T>>, new: BTreeSet<T>) {
    *cur = Some(match cur.take() {
        None => new,
        Some(old) => old.intersection(&new).cloned().collect(),
    });
}

impl KeyFilters {
    pub fn from_exprs(filters: &[Expr]) -> Self {
        let mut k = Self::default();
        for f in filters {
            match classify(f) {
                Some(Pushed::Projects(p)) => intersect(&mut k.projects, p),
                Some(Pushed::Traces(t)) => intersect(&mut k.traces, t),
                Some(Pushed::Time { lo, hi }) => {
                    if let Some(lo) = lo {
                        k.start_ns = Some(k.start_ns.map_or(lo, |x| x.max(lo)));
                    }
                    if let Some(hi) = hi {
                        k.end_ns = Some(k.end_ns.map_or(hi, |x| x.min(hi)));
                    }
                }
                None => {}
            }
        }
        k
    }
    pub(crate) fn empty(&self) -> bool {
        self.projects.as_ref().is_some_and(|p| p.is_empty()) || self.traces.as_ref().is_some_and(|t| t.is_empty())
    }
    /// The memtable rows that pass the KEY filters (never time: start_time is mutable before the merge).
    pub(crate) fn key_mask(&self, b: &RecordBatch) -> Option<BooleanArray> {
        if self.projects.is_none() && self.traces.is_none() {
            return None;
        }
        let p = b.column(col_idx(c::PROJECT_ID)).as_string::<i32>();
        let t = b.column(col_idx(c::TRACE_ID)).as_binary::<i32>();
        Some(
            (0..b.num_rows())
                .map(|i| {
                    self.projects.as_ref().is_none_or(|s| s.contains(p.value(i)))
                        && self
                            .traces
                            .as_ref()
                            .is_none_or(|s| TraceId::from_slice(t.value(i)).is_ok_and(|x| s.contains(&x)))
                })
                .map(Some)
                .collect(),
        )
    }
}

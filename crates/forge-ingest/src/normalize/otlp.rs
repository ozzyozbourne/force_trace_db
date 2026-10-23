//! OTLP ExportTraceServiceRequest -> Vec<SpanRow> (pure, synchronous; versions are assigned later).
use crate::normalize::attrs::{Attrs, any_to_json, json_payload, kvs_to_json, parse_json_text, unflatten};
use crate::normalize::finalize::{Measured, finalize_rows, finalize_span};
use crate::normalize::pricing::{Pricing, Usage, normalize_provider};
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::{ProjectId, SpanId, TraceId};
use forge_model::row::SpanRow;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value::Value};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{Span, status::StatusCode};
use serde_json::{Map, Value as J, json};
use std::sync::Arc;

/// gRPC metadata key / HTTP header naming the project (M1; M6 derives it from the API key).
pub const PROJECT_HEADER: &str = "x-forge-project";
/// Resource attribute naming the project (wins over `openinference.project.name`).
pub const PROJECT_RESOURCE_ATTR: &str = "forge.project";

#[derive(Debug, Clone)]
pub struct NormalizeConfig {
    /// `ingest.default_project` ("default").
    pub default_project: ProjectId,
    pub pricing: Arc<Pricing>,
}

impl NormalizeConfig {
    pub fn new(default_project: ProjectId) -> Self {
        Self {
            default_project,
            pricing: Arc::new(Pricing::builtin()),
        }
    }
}

/// `ingest.default_project`'s default ("default") and the built-in prices.
impl Default for NormalizeConfig {
    fn default() -> Self {
        let p = forge_model::config::IngestConfig::default().default_project;
        Self::new(ProjectId::new(p).expect("the default project id is valid"))
    }
}

#[derive(Debug, Default)]
pub struct Normalized {
    /// In request order (resource -> scope -> span), so per-row versions make a later duplicate win.
    pub rows: Vec<SpanRow>,
    pub rejected: u64,
    /// First rejection, prefixed with its position; goes into partial_success.error_message.
    pub first_error: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SpanError {
    #[error("{what} must be {expected} bytes, got {got}")]
    IdLength {
        what: &'static str,
        expected: usize,
        got: usize,
    },
    #[error("{0} must not be all zeros")]
    ZeroId(&'static str),
    #[error("{0} does not fit in i64 nanoseconds")]
    Time(&'static str),
}

pub fn normalize_request(
    req: ExportTraceServiceRequest,
    header_project: Option<&ProjectId>,
    cfg: &NormalizeConfig,
) -> Normalized {
    let mut out = Normalized::default();
    for (ri, rs) in req.resource_spans.into_iter().enumerate() {
        let res = Attrs::new(rs.resource.map(|r| r.attributes).unwrap_or_default());
        let project = header_project
            .cloned()
            .or_else(|| project_from_resource(&res))
            .unwrap_or_else(|| cfg.default_project.clone());
        let resource_json = J::Object(res.into_json());
        for (si, ss) in rs.scope_spans.into_iter().enumerate() {
            let scope = ss.scope.and_then(|s| {
                let mut m = Map::new();
                if !s.name.is_empty() {
                    m.insert("name".into(), J::String(s.name));
                }
                if !s.version.is_empty() {
                    m.insert("version".into(), J::String(s.version));
                }
                if !s.attributes.is_empty() {
                    m.insert("attributes".into(), J::Object(kvs_to_json(s.attributes)));
                }
                (!m.is_empty()).then_some(J::Object(m))
            });
            for (i, span) in ss.spans.into_iter().enumerate() {
                match normalize_span(span, &project, &resource_json, scope.as_ref(), cfg) {
                    Ok(row) => out.rows.push(row),
                    Err(e) => {
                        out.rejected += 1;
                        out.first_error
                            .get_or_insert_with(|| format!("resource_spans[{ri}].scope_spans[{si}].spans[{i}]: {e}"));
                    }
                }
            }
        }
    }
    finalize_rows(&mut out.rows);
    out
}

/// `forge.project` > `openinference.project.name`. Resource values are sanitized, not rejected
/// (`ProjectId::sanitize`).
pub fn project_from_resource(res: &Attrs) -> Option<ProjectId> {
    ProjectId::sanitize(res.first_str(&[PROJECT_RESOURCE_ATTR, "openinference.project.name"])?)
}

fn ts(ns: u64, what: &'static str) -> Result<Option<i64>, SpanError> {
    match ns {
        0 => Ok(None),
        n => i64::try_from(n).map(Some).map_err(|_| SpanError::Time(what)),
    }
}

fn id<T>(
    b: &[u8],
    what: &'static str,
    len: usize,
    mk: impl FnOnce(&[u8]) -> T,
    zero: impl FnOnce(&T) -> bool,
) -> Result<T, SpanError> {
    if b.len() != len {
        return Err(SpanError::IdLength {
            what,
            expected: len,
            got: b.len(),
        });
    }
    let v = mk(b);
    if zero(&v) {
        return Err(SpanError::ZeroId(what));
    }
    Ok(v)
}

const MODEL_KEYS: &[&str] = &[
    "gen_ai.response.model",
    "gen_ai.request.model",
    "llm.response.model_name",
    "llm.model_name",
    "llm.request.model_name",
    "embedding.model_name",
    "reranker.model_name",
    "ai.model.id",
];
const PROVIDER_KEYS: &[&str] = &["gen_ai.provider.name", "gen_ai.system", "llm.provider", "llm.system"];
const PROMPT_KEYS: &[&str] = &[
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.prompt_tokens",
    "llm.token_count.prompt",
];
const COMPLETION_KEYS: &[&str] = &[
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.completion_tokens",
    "llm.token_count.completion",
];
const TOTAL_KEYS: &[&str] = &[
    "llm.token_count.total",
    "gen_ai.usage.total_tokens",
    "llm.usage.total_tokens",
];
const CACHE_READ_KEYS: &[&str] = &[
    "gen_ai.usage.cache_read.input_tokens",
    "llm.token_count.prompt_details.cache_read",
    "gen_ai.usage.cache_read_input_tokens",
];
const CACHE_WRITE_KEYS: &[&str] = &[
    "gen_ai.usage.cache_write.input_tokens",
    "gen_ai.usage.cache_creation.input_tokens",
    "llm.token_count.prompt_details.cache_write",
    "gen_ai.usage.cache_creation_input_tokens",
];
/// Producer-computed cost (trusted when present).
const COST_KEYS: &[&str] = &["llm.cost.total", "gen_ai.usage.cost"];
const SESSION_KEYS: &[&str] = &["gen_ai.conversation.id", "session.id"];
/// Fallback session keys looked up in the merged metadata (LangSmith order).
const SESSION_METADATA_KEYS: &[&str] = &["session_id", "conversation_id", "thread_id"];

pub fn normalize_span(
    span: Span,
    project: &ProjectId,
    resource: &J,
    scope: Option<&J>,
    cfg: &NormalizeConfig,
) -> Result<SpanRow, SpanError> {
    let trace_id = id(
        &span.trace_id,
        "trace_id",
        TraceId::LEN,
        |b| TraceId::from_slice(b).expect("len"),
        TraceId::is_zero,
    )?;
    let span_id = id(
        &span.span_id,
        "span_id",
        SpanId::LEN,
        |b| SpanId::from_slice(b).expect("len"),
        SpanId::is_zero,
    )?;
    // Empty or all-zero parent = root (some SDKs send 8 zero bytes).
    let parent_span_id = match span.parent_span_id.len() {
        0 => None,
        _ => Some(id(
            &span.parent_span_id,
            "parent_span_id",
            SpanId::LEN,
            |b| SpanId::from_slice(b).expect("len"),
            |_| false,
        )?)
        .filter(|p| !p.is_zero()),
    };
    let start = ts(span.start_time_unix_nano, "start_time")?;
    let end = ts(span.end_time_unix_nano, "end_time")?;
    let open = end.is_none();

    let mut a = Attrs::new(span.attributes);
    let mut events = span.events;

    // ---- status / error ----
    let (code, status_msg) = span
        .status
        .map_or((StatusCode::Unset as i32, String::new()), |s| (s.code, s.message));
    let status = match StatusCode::try_from(code).unwrap_or(StatusCode::Unset) {
        StatusCode::Ok => Some(SpanStatus::Ok),
        StatusCode::Error => Some(SpanStatus::Error),
        // An open span's Unset is "not known yet": leave it null so a late-arriving start post cannot
        // overwrite the final status of the end post (latest non-null wins).
        StatusCode::Unset if open => None,
        StatusCode::Unset => Some(SpanStatus::Unset),
    };
    let error = (status == Some(SpanStatus::Error))
        .then(|| error_text(&status_msg, &events, &a))
        .flatten();

    // ---- scalar columns (borrowed reads; attributes stay) ----
    let run_type = derive_run_type(&a);
    let mut model = a.first_str(MODEL_KEYS).map(str::to_owned);
    let mut provider = a.first_str(PROVIDER_KEYS).and_then(normalize_provider);
    let usage = Usage {
        prompt: a.first_int(PROMPT_KEYS),
        completion: a.first_int(COMPLETION_KEYS),
        cache_read: a.first_int(CACHE_READ_KEYS),
        cache_write: a.first_int(CACHE_WRITE_KEYS),
    };
    let measured = Measured {
        usage,
        producer_total: a.first_int(TOTAL_KEYS),
        producer_cost: a.first_f64(COST_KEYS),
    };
    let mut session_id = a.first_str(SESSION_KEYS).map(str::to_owned);

    // ---- consumed attributes (removed) ----
    // P9: once a payload is extracted, the OpenInference `*.value` copies are redundant (and would push a
    // large prompt into `attributes` a second time): `redundant_on_hit`.
    let inputs = extract_payload(&INPUTS, &mut a, &mut events);
    let outputs = extract_payload(&OUTPUTS, &mut a, &mut events);
    let metadata = extract_metadata(&mut a);
    let tags = extract_tags(&mut a);

    // Fallbacks that live in metadata (LangSmith ls_* keys, thread ids).
    let meta_str = |k: &str| {
        metadata
            .get(k)
            .and_then(J::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    if model.is_none() {
        model = meta_str("ls_model_name");
    }
    if provider.is_none() {
        provider = meta_str("ls_provider").and_then(|p| normalize_provider(&p));
    }
    if session_id.is_none() {
        session_id = SESSION_METADATA_KEYS.iter().find_map(|k| meta_str(k));
    }

    // ---- attributes: everything left + forge.* envelope ----
    let mut attrs = a.into_json();
    if let Some(k) = span_kind(span.kind) {
        attrs.insert("forge.kind".into(), J::String(k.into()));
    }
    if let Some(s) = scope {
        attrs.insert("forge.scope".into(), s.clone());
    }
    if resource.as_object().is_some_and(|m| !m.is_empty()) {
        attrs.insert("forge.resource".into(), resource.clone());
    }
    if !span.trace_state.is_empty() {
        attrs.insert("forge.trace_state".into(), J::String(span.trace_state));
    }
    if !span.links.is_empty() {
        attrs.insert(
            "forge.links".into(),
            J::Array(span.links.into_iter().map(link_json).collect()),
        );
    }
    let dropped = json!({
        "attributes": span.dropped_attributes_count,
        "events": span.dropped_events_count,
        "links": span.dropped_links_count,
    });
    if dropped
        .as_object()
        .expect("obj")
        .values()
        .any(|v| v.as_u64() != Some(0))
    {
        attrs.insert("forge.dropped".into(), dropped);
    }

    let mut row = SpanRow {
        project_id: Some(project.clone()),
        trace_id,
        span_id,
        parent_span_id,
        dotted_order: None, // finalize_rows
        name: (!span.name.is_empty()).then_some(span.name),
        run_type,
        start_time_ns: start,
        end_time_ns: end,
        status,
        error,
        model,
        provider,
        prompt_tokens: None,     // finalize_span
        completion_tokens: None, // finalize_span
        total_tokens: None,      // finalize_span
        cost_usd: None,          // finalize_span
        session_id,
        tags,
        metadata: (!metadata.is_empty()).then(|| J::Object(metadata).to_string()),
        attributes: (!attrs.is_empty()).then(|| J::Object(attrs).to_string()),
        events: (!events.is_empty()).then(|| J::Array(events.into_iter().map(event_json).collect()).to_string()),
        inputs: inputs.map(|v| v.to_string()),
        outputs: outputs.map(|v| v.to_string()),
        version: 0, // Ingest::submit stamps versions
        deleted: false,
    };
    finalize_span(&mut row, measured, &cfg.pricing);
    Ok(row)
}

// -------------------------------------------------------------------------------------------------
// run_type
// -------------------------------------------------------------------------------------------------

fn has_input_messages(a: &Attrs) -> bool {
    a.has("gen_ai.input.messages") || a.has_indexed("llm.input_messages.") || a.has_indexed("gen_ai.prompt.")
}

/// Explicit producer kinds first (most specific), then gen_ai.operation.name, then llm.request.type,
/// then attribute sniffing. None when nothing identifies the span (P9: a sparse post never
/// overwrites a known run_type; readers use `COALESCE(run_type, 'other')`).
pub fn derive_run_type(a: &Attrs) -> Option<RunType> {
    use RunType::*;
    let chat_or_llm = || if has_input_messages(a) { ChatModel } else { Llm };
    if let Some(r) = a
        .str("langsmith.span.kind")
        .and_then(|k| match k.to_ascii_lowercase().as_str() {
            "llm" => Some(Llm),
            "chat_model" => Some(ChatModel),
            "tool" => Some(Tool),
            "chain" | "prompt" | "parser" => Some(Chain),
            "agent" => Some(Agent),
            "retriever" => Some(Retriever),
            "embedding" => Some(Embedding),
            _ => None,
        })
    {
        return Some(r);
    }
    if let Some(k) = a.str("openinference.span.kind") {
        return Some(match k.to_ascii_uppercase().as_str() {
            "LLM" => chat_or_llm(),
            "EMBEDDING" => Embedding,
            "CHAIN" | "PROMPT" => Chain,
            "RETRIEVER" | "RERANKER" => Retriever,
            "TOOL" => Tool,
            "AGENT" => Agent,
            _ => Other, // GUARDRAIL, EVALUATOR, DECISION, UNKNOWN
        });
    }
    if let Some(r) = a.str("traceloop.span.kind").and_then(|k| match k {
        "workflow" | "task" => Some(Chain),
        "agent" => Some(Agent),
        "tool" => Some(Tool),
        _ => None, // "unknown": keep looking
    }) {
        return Some(r);
    }
    if let Some(op) = a.str("gen_ai.operation.name") {
        return Some(match op {
            "chat" => ChatModel,
            "text_completion" => Llm,
            "generate_content" | "llm_request" => chat_or_llm(),
            "embeddings" => Embedding,
            "retrieval" | "search_memory" | "vector_db_retrieve" => Retriever,
            "execute_tool" => Tool,
            "invoke_agent" | "create_agent" | "plan" => Agent,
            "fetch_response" => Other,
            _ => Chain, // invoke_workflow, execute_task, other memory ops, future ops
        });
    }
    if let Some(r) = a.str("llm.request.type").and_then(|t| match t {
        "chat" => Some(ChatModel),
        "completion" => Some(Llm),
        "embedding" => Some(Embedding),
        "rerank" => Some(Retriever),
        _ => None,
    }) {
        return Some(r);
    }
    if a.has("gen_ai.tool.call.arguments") || a.has("gen_ai.tool.call.result") {
        return Some(Tool);
    }
    if a.first_str(MODEL_KEYS).is_some() || a.first_str(PROVIDER_KEYS).is_some() {
        return Some(chat_or_llm());
    }
    None
}

// -------------------------------------------------------------------------------------------------
// inputs / outputs (first source present wins; only the winner is removed from attributes)
// -------------------------------------------------------------------------------------------------

fn take_json(a: &mut Attrs, k: &str) -> Option<J> {
    a.take(k).map(json_payload)
}

/// OpenInference `input.value` + `input.mime_type`: JSON-parse only for application/json (or no mime and JSON-looking).
fn take_mime_value(a: &mut Attrs, value: &str, mime: &str) -> Option<J> {
    let mime_v = a.str(mime).map(str::to_owned);
    let v = a.take(value)?;
    a.take(mime);
    Some(match (mime_v.as_deref(), v.value) {
        (Some("text/plain"), Some(Value::StringValue(s))) => J::String(s),
        (_, val) => json_payload(AnyValue { value: val }),
    })
}

/// LangChain via traceloop wraps I/O as {"inputs": .., "tags": .., "metadata": .., "kwargs": ..}: keep `inner`.
fn unwrap_langchain(v: J, inner: &str) -> J {
    match v {
        J::Object(mut m)
            if m.contains_key(inner)
                && m.keys()
                    .all(|k| k == inner || matches!(k.as_str(), "tags" | "metadata" | "kwargs")) =>
        {
            m.remove(inner).expect("checked")
        }
        v => v,
    }
}

fn messages(v: J) -> J {
    json!({ "messages": v })
}

/// Legacy GenAI content events -> messages; removes the consumed events.
fn take_message_events(events: &mut Vec<Event>, names: &[&str]) -> Option<J> {
    let mut out = Vec::new();
    events.retain_mut(|e| {
        if !names.contains(&e.name.as_str()) {
            return true;
        }
        let mut m = kvs_to_json(std::mem::take(&mut e.attributes));
        for v in m.values_mut() {
            if let J::String(s) = v {
                *v = parse_json_text(std::mem::take(s));
            }
        }
        if let Some(role) = e.name.strip_prefix("gen_ai.").and_then(|r| r.strip_suffix(".message")) {
            m.entry("role").or_insert_with(|| J::String(role.into()));
        }
        // gen_ai.content.prompt {gen_ai.prompt: "[...]"} carries the whole message list.
        match m.remove("gen_ai.prompt").or_else(|| m.remove("gen_ai.completion")) {
            Some(J::Array(list)) => out.extend(list),
            Some(other) => out.push(other),
            None => out.push(J::Object(m)),
        }
        false
    });
    (!out.is_empty()).then(|| messages(J::Array(out)))
}

/// Where one payload source lives in a span.
enum Loc {
    /// Several JSON attributes combined into one object: `(attribute, output key)`.
    Group(&'static [(&'static str, &'static str)]),
    Json(&'static str),
    /// `prefix<digit>...` flattened attributes, unflattened.
    Indexed(&'static str),
    /// OpenInference `(value, mime_type)`.
    MimeValue(&'static str, &'static str),
    /// Legacy GenAI content events (already shaped as messages).
    Events(&'static [&'static str]),
}

struct Source {
    from: Loc,
    shape: fn(J) -> J,
    /// Duplicates of this source, removed when it wins.
    also_consume: &'static [&'static str],
}

/// One payload column: sources in precedence order (the first present wins; only it is consumed), plus
/// attributes made redundant by any hit (P9: the OpenInference `*.value` copies).
pub struct Payload {
    sources: &'static [Source],
    redundant_on_hit: &'static [&'static str],
}

const fn src(from: Loc, shape: fn(J) -> J) -> Source {
    Source {
        from,
        shape,
        also_consume: &[],
    }
}
fn same(v: J) -> J {
    v
}
fn query(v: J) -> J {
    json!({ "query": v })
}
fn prompts(v: J) -> J {
    json!({ "prompts": v })
}
fn documents(v: J) -> J {
    json!({ "documents": v })
}
fn choices(v: J) -> J {
    json!({ "choices": v })
}
fn langchain_inputs(v: J) -> J {
    unwrap_langchain(v, "inputs")
}
fn langchain_outputs(v: J) -> J {
    unwrap_langchain(v, "outputs")
}

pub static INPUTS: Payload = Payload {
    sources: &[
        // GenAI semconv (current)
        src(
            Loc::Group(&[
                ("gen_ai.input.messages", "messages"),
                ("gen_ai.system_instructions", "system_instructions"),
                ("gen_ai.tool.definitions", "tools"),
            ]),
            same,
        ),
        src(Loc::Json("gen_ai.tool.call.arguments"), same),
        src(Loc::Json("gen_ai.retrieval.query.text"), query),
        // OpenInference
        src(Loc::Indexed("llm.input_messages."), messages),
        src(Loc::Indexed("llm.prompts."), prompts),
        src(Loc::MimeValue("input.value", "input.mime_type"), same),
        // OpenLLMetry legacy + Traceloop
        src(Loc::Indexed("gen_ai.prompt."), messages),
        Source {
            from: Loc::Json("traceloop.entity.input"),
            shape: langchain_inputs,
            also_consume: &["gen_ai.task.input"],
        },
        src(Loc::Json("gen_ai.task.input"), langchain_inputs),
        // Obsolete single attribute and event-based content
        src(Loc::Json("gen_ai.prompt"), messages),
        src(
            Loc::Events(&[
                "gen_ai.content.prompt",
                "gen_ai.system.message",
                "gen_ai.user.message",
                "gen_ai.assistant.message",
                "gen_ai.tool.message",
            ]),
            same,
        ),
    ],
    redundant_on_hit: &["input.value", "input.mime_type"],
};

pub static OUTPUTS: Payload = Payload {
    sources: &[
        src(Loc::Json("gen_ai.output.messages"), messages),
        src(Loc::Json("gen_ai.tool.call.result"), same),
        src(Loc::Json("gen_ai.retrieval.documents"), documents),
        src(Loc::Indexed("llm.output_messages."), messages),
        src(Loc::Indexed("retrieval.documents."), documents),
        src(Loc::Indexed("llm.choices."), choices),
        src(Loc::MimeValue("output.value", "output.mime_type"), same),
        src(Loc::Indexed("gen_ai.completion."), messages),
        Source {
            from: Loc::Json("traceloop.entity.output"),
            shape: langchain_outputs,
            also_consume: &["gen_ai.task.output"],
        },
        src(Loc::Json("gen_ai.task.output"), langchain_outputs),
        src(Loc::Json("gen_ai.completion"), messages),
        src(Loc::Events(&["gen_ai.content.completion", "gen_ai.choice"]), same),
    ],
    redundant_on_hit: &["output.value", "output.mime_type"],
};

fn take_unflattened(a: &mut Attrs, prefix: &str) -> Option<J> {
    let kv = a.take_indexed(prefix);
    (!kv.is_empty()).then(|| unflatten(kv))
}

fn take_source(a: &mut Attrs, events: &mut Vec<Event>, from: &Loc) -> Option<J> {
    match from {
        Loc::Group(keys) => {
            let mut g = Map::new();
            for (k, out) in *keys {
                if let Some(v) = take_json(a, k) {
                    g.insert((*out).into(), v);
                }
            }
            (!g.is_empty()).then_some(J::Object(g))
        }
        Loc::Json(k) => take_json(a, k),
        Loc::Indexed(p) => take_unflattened(a, p),
        Loc::MimeValue(v, m) => take_mime_value(a, v, m),
        Loc::Events(names) => take_message_events(events, names),
    }
}

/// The first source present wins; it and its duplicates are consumed, and on a hit `redundant_on_hit` too.
pub fn extract_payload(p: &Payload, a: &mut Attrs, events: &mut Vec<Event>) -> Option<J> {
    let v = p.sources.iter().find_map(|s| {
        let v = take_source(a, events, &s.from)?;
        for k in s.also_consume {
            a.take(k);
        }
        Some((s.shape)(v))
    })?;
    for k in p.redundant_on_hit {
        a.take(k);
    }
    Some(v)
}

/// OpenInference `metadata` (JSON object string) + `user.id`, Traceloop association properties and
/// LangSmith `langsmith.metadata.*` (prefixes stripped). All consumed.
pub fn extract_metadata(a: &mut Attrs) -> Map<String, J> {
    let mut m = Map::new();
    if let Some(v) = a.take("metadata") {
        match json_payload(v) {
            J::Object(o) => m.extend(o),
            J::Null => {}
            other => {
                m.insert("metadata".into(), other);
            }
        }
    }
    for p in ["traceloop.association.properties.", "langsmith.metadata."] {
        let mut kv = a.take_prefix(p);
        kv.sort_by(|x, y| x.0.cmp(&y.0));
        for (k, v) in kv {
            m.insert(k, any_to_json(v));
        }
    }
    if let Some(u) = a.take("user.id") {
        m.insert("user.id".into(), any_to_json(u));
    }
    m
}

/// OpenInference `tag.tags` (string[]) or LangSmith `langsmith.span.tags` (comma-joined).
pub fn extract_tags(a: &mut Attrs) -> Option<Vec<String>> {
    let to_vec = |v: AnyValue| -> Vec<String> {
        match any_to_json(v) {
            J::Array(xs) => xs
                .into_iter()
                .filter_map(|x| match x {
                    J::String(s) => Some(s),
                    J::Null => None,
                    o => Some(o.to_string()),
                })
                .collect(),
            J::String(s) => s
                .split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
            _ => Vec::new(),
        }
    };
    let mut tags: Vec<String> = Vec::new();
    for k in ["tag.tags", "langsmith.span.tags"] {
        if let Some(v) = a.take(k) {
            for t in to_vec(v) {
                if !tags.contains(&t) {
                    tags.push(t);
                }
            }
        }
    }
    (!tags.is_empty()).then_some(tags)
}

/// status.message -> last `exception` event ("type: message") -> `error.type` attribute.
fn error_text(status_msg: &str, events: &[Event], a: &Attrs) -> Option<String> {
    if !status_msg.trim().is_empty() {
        return Some(status_msg.to_string());
    }
    let from_event = events.iter().rev().filter(|e| e.name == "exception").find_map(|e| {
        let get = |k: &str| {
            e.attributes
                .iter()
                .find(|kv| kv.key == k)
                .and_then(|kv| match kv.value.as_ref()?.value.as_ref()? {
                    Value::StringValue(s) if !s.is_empty() => Some(s.as_str()),
                    _ => None,
                })
        };
        match (get("exception.type"), get("exception.message")) {
            (Some(t), Some(m)) => Some(format!("{t}: {m}")),
            (t, m) => t.or(m).map(str::to_owned),
        }
    });
    from_event.or_else(|| a.str("error.type").map(str::to_owned))
}

fn span_kind(k: i32) -> Option<&'static str> {
    Some(match k {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => return None,
    })
}

fn rfc3339(ns: u64) -> J {
    match i64::try_from(ns) {
        Ok(n) if n > 0 => J::String(
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(n)
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        ),
        _ => J::Null,
    }
}

/// `{"name", "time" (RFC 3339, UTC), "attributes"?, "dropped_attributes_count"?}`.
fn event_json(e: Event) -> J {
    let mut m = Map::new();
    m.insert("name".into(), J::String(e.name));
    m.insert("time".into(), rfc3339(e.time_unix_nano));
    if !e.attributes.is_empty() {
        m.insert("attributes".into(), J::Object(kvs_to_json(e.attributes)));
    }
    if e.dropped_attributes_count > 0 {
        m.insert("dropped_attributes_count".into(), J::from(e.dropped_attributes_count));
    }
    J::Object(m)
}

fn link_json(l: Link) -> J {
    let mut m = Map::new();
    m.insert("trace_id".into(), J::String(hex::encode(&l.trace_id)));
    m.insert("span_id".into(), J::String(hex::encode(&l.span_id)));
    if !l.trace_state.is_empty() {
        m.insert("trace_state".into(), J::String(l.trace_state));
    }
    if !l.attributes.is_empty() {
        m.insert("attributes".into(), J::Object(kvs_to_json(l.attributes)));
    }
    J::Object(m)
}

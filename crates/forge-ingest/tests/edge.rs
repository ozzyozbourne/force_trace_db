use forge_ingest::blob::offload_blobs;
use forge_ingest::normalize::pricing::Pricing;
use forge_ingest::normalize::{NormalizeConfig, normalize_request};
use forge_ingest::version::{VersionAllocator, stamp};
use forge_model::blob::{blob_ref, parse_blob_ref};
use forge_model::enums::RunType;
use forge_model::ids::ProjectId;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::Event;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use serde_json::{Value as J, json};
use std::sync::Arc;

fn cfg() -> NormalizeConfig {
    NormalizeConfig {
        default_project: ProjectId::new("default").unwrap(),
        pricing: Arc::new(Pricing::builtin()),
    }
}
fn s(v: &str) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(Value::StringValue(v.into())),
    })
}
fn i(v: i64) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(Value::IntValue(v)),
    })
}
fn kv(k: &str, value: Option<AnyValue>) -> KeyValue {
    KeyValue {
        key: k.into(),
        value,
        ..Default::default()
    }
}
fn span(t: u8, id: u8, parent: Option<u8>, start_ms: u64, attrs: Vec<KeyValue>) -> Span {
    Span {
        trace_id: vec![t; 16],
        span_id: vec![id; 8],
        parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
        name: format!("s{id}"),
        start_time_unix_nano: 1_792_663_200_000_000_000 + start_ms * 1_000_000,
        end_time_unix_nano: 1_792_663_200_000_000_000 + (start_ms + 5) * 1_000_000,
        attributes: attrs,
        status: Some(Status {
            code: 1,
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn req(res: Vec<KeyValue>, spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: res,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}
fn j(x: &Option<String>) -> J {
    x.as_deref().map_or(J::Null, |s| serde_json::from_str(s).unwrap())
}

#[test]
fn invalid_ids_are_rejected_individually() {
    let mut bad_trace = span(1, 2, None, 0, vec![]);
    bad_trace.trace_id.pop();
    let zero_span = span(1, 0, None, 0, vec![]);
    let mut bad_parent = span(1, 3, None, 0, vec![]);
    bad_parent.parent_span_id = vec![1, 2, 3, 4];
    let mut zero_parent = span(1, 4, None, 0, vec![]);
    zero_parent.parent_span_id = vec![0; 8]; // treated as root
    let mut huge_time = span(1, 5, None, 0, vec![]);
    huge_time.start_time_unix_nano = u64::MAX;
    let n = normalize_request(
        req(vec![], vec![bad_trace, zero_span, bad_parent, zero_parent, huge_time]),
        None,
        &cfg(),
    );
    assert_eq!(n.rejected, 4);
    assert_eq!(
        n.first_error.as_deref(),
        Some("resource_spans[0].scope_spans[0].spans[0]: trace_id must be 16 bytes, got 15")
    );
    assert_eq!(n.rows.len(), 1);
    assert_eq!(n.rows[0].parent_span_id, None);
}

#[test]
fn dotted_order_only_when_chain_reaches_a_root_in_batch() {
    // 1 (root) <- 2 <- 3 ; 5 <- 6 where 5 is not in the batch ; 7 <- 8 <- 7 cycle
    let n = normalize_request(
        req(
            vec![],
            vec![
                span(1, 3, Some(2), 30, vec![]),
                span(1, 6, Some(5), 60, vec![]),
                span(1, 2, Some(1), 20, vec![]),
                span(1, 1, None, 10, vec![]),
                span(1, 7, Some(8), 70, vec![]),
                span(1, 8, Some(7), 80, vec![]),
                span(2, 2, Some(1), 20, vec![]), // other trace: parent 1 of trace 2 is absent
            ],
        ),
        None,
        &cfg(),
    );
    let d: Vec<Option<usize>> = n
        .rows
        .iter()
        .map(|r| r.dotted_order.as_ref().map(|x| forge_model::dotted::depth(x)))
        .collect();
    assert_eq!(d, [Some(3), None, Some(2), Some(1), None, None, None]);
}

#[test]
fn project_resolution() {
    let res = vec![
        kv("forge.project", s("My Project/α")),
        kv("openinference.project.name", s("oi")),
    ];
    let n = normalize_request(req(res.clone(), vec![span(1, 1, None, 0, vec![])]), None, &cfg());
    assert_eq!(n.rows[0].project_id.as_ref().unwrap().as_str(), "My-Project--");
    let hdr = ProjectId::new("hdr").unwrap();
    let n = normalize_request(req(res, vec![span(1, 1, None, 0, vec![])]), Some(&hdr), &cfg());
    assert_eq!(n.rows[0].project_id.as_ref().unwrap().as_str(), "hdr");
    let n = normalize_request(
        req(vec![kv("service.name", s("svc"))], vec![span(1, 1, None, 0, vec![])]),
        None,
        &cfg(),
    );
    assert_eq!(n.rows[0].project_id.as_ref().unwrap().as_str(), "default");
}

#[test]
fn start_and_end_posts_in_one_batch_get_increasing_versions() {
    let mut start = span(1, 1, None, 0, vec![]);
    start.end_time_unix_nano = 0;
    start.status = None;
    let end = span(1, 1, None, 0, vec![kv("gen_ai.usage.input_tokens", i(3))]);
    let mut n = normalize_request(req(vec![], vec![start, end]), None, &cfg());
    let a = VersionAllocator::new(100);
    let r = a.reserve(2);
    stamp(&mut n.rows, r);
    assert_eq!((n.rows[0].version, n.rows[1].version), (101, 102));
    assert_eq!((n.rows[0].status, n.rows[0].end_time_ns.is_some()), (None, false));
    assert!(n.rows.iter().all(|r| r.dotted_order.is_some()));
}

#[test]
fn indexed_messages_sort_numerically_and_nest() {
    let mut attrs = vec![kv("openinference.span.kind", s("LLM"))];
    for k in 0..12 {
        attrs.push(kv(&format!("llm.input_messages.{k}.message.role"), s("user")));
        attrs.push(kv(
            &format!("llm.input_messages.{k}.message.content"),
            s(&k.to_string()),
        ));
    }
    attrs.push(kv(
        "llm.input_messages.1.message.tool_calls.0.tool_call.function.name",
        s("f"),
    ));
    attrs.push(kv(
        "llm.input_messages.1.message.tool_calls.0.tool_call.function.arguments",
        s("{\"a\":1}"),
    ));
    attrs.push(kv(
        "llm.cost.total",
        Some(AnyValue {
            value: Some(Value::DoubleValue(0.5)),
        }),
    ));
    attrs.push(kv("llm.model_name", s("gpt-4o")));
    attrs.push(kv("llm.token_count.prompt", s("1000")));
    let n = normalize_request(req(vec![], vec![span(1, 1, None, 0, attrs)]), None, &cfg());
    let r = &n.rows[0];
    assert_eq!(r.run_type, Some(RunType::ChatModel));
    assert_eq!(r.cost_usd, Some(0.5)); // producer cost wins over the table
    assert_eq!(r.prompt_tokens, Some(1000)); // string coerced
    let m = j(&r.inputs);
    let contents: Vec<&str> = m["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["content"].as_str().unwrap())
        .collect();
    assert_eq!(contents, ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"]);
    assert_eq!(
        m["messages"][1]["tool_calls"][0],
        json!({"function": {"name": "f", "arguments": "{\"a\":1}"}})
    );
}

#[test]
fn legacy_content_events_become_messages() {
    let mut sp = span(
        1,
        1,
        None,
        0,
        vec![
            kv("gen_ai.system", s("az.ai.openai")),
            kv("gen_ai.request.model", s("gpt-4o")),
        ],
    );
    sp.events = vec![
        Event {
            name: "gen_ai.user.message".into(),
            attributes: vec![kv("content", s("hi"))],
            ..Default::default()
        },
        Event {
            name: "gen_ai.choice".into(),
            attributes: vec![kv("message", s("{\"role\":\"assistant\",\"content\":\"yo\"}"))],
            ..Default::default()
        },
        Event {
            name: "log".into(),
            ..Default::default()
        },
    ];
    let n = normalize_request(req(vec![], vec![sp]), None, &cfg());
    let r = &n.rows[0];
    assert_eq!(r.provider.as_deref(), Some("azure.ai.openai"));
    assert_eq!(r.run_type, Some(RunType::Llm)); // sniffed from model (no operation name)
    assert_eq!(j(&r.inputs), json!({"messages": [{"role": "user", "content": "hi"}]}));
    assert_eq!(
        j(&r.outputs),
        json!({"messages": [{"message": {"role": "assistant", "content": "yo"}}]})
    );
    assert_eq!(j(&r.events), json!([{"name": "log", "time": null}]));
}

#[tokio::test]
async fn blob_offload() {
    let big = format!("[{{\"content\":\"{}\"}}]", "x".repeat(2 << 20));
    let attrs = vec![
        kv("gen_ai.operation.name", s("chat")),
        kv("gen_ai.input.messages", s(&big)),
    ];
    let mut n = normalize_request(
        req(
            vec![],
            vec![span(1, 1, None, 0, attrs.clone()), span(1, 2, None, 0, attrs)],
        ),
        None,
        &cfg(),
    );
    let store = InMemory::new();
    let written = offload_blobs(&mut n.rows, &store, 1 << 20).await.unwrap();
    assert_eq!(written, 1); // same content, one object
    let r = n.rows[0].inputs.clone().unwrap();
    let (path, size) = parse_blob_ref(&r).unwrap();
    assert!(path.starts_with("blobs/default/") && path.len() == "blobs/default/".len() + 64);
    let got = store.get(&Path::from(path)).await.unwrap().bytes().await.unwrap();
    assert_eq!(size as usize, got.len());
    let original: J = serde_json::from_slice(&got).unwrap();
    assert_eq!(original["messages"][0]["content"].as_str().unwrap().len(), 2 << 20);
    assert_eq!(serde_json::from_str::<J>(&r).unwrap()["size"], size); // the ref itself is valid JSON
    assert_eq!(r, blob_ref(path, size as usize));
    // Idempotent: re-running (e.g. after an ambiguous error) is a no-op on refs and AlreadyExists is OK.
    let mut again = n.rows.clone();
    assert_eq!(offload_blobs(&mut again, &store, 1 << 20).await.unwrap(), 0);
    assert_eq!(again, n.rows);
    let attrs = vec![
        kv("gen_ai.operation.name", s("chat")),
        kv("gen_ai.input.messages", s(&big)),
    ];
    let mut fresh = normalize_request(req(vec![], vec![span(1, 3, None, 0, attrs)]), None, &cfg());
    assert_eq!(offload_blobs(&mut fresh.rows, &store, 1 << 20).await.unwrap(), 1); // AlreadyExists counts as written
    assert_eq!(fresh.rows[0].inputs, n.rows[0].inputs);
    assert!(n.rows[0].outputs.is_none() && n.rows[0].attributes.as_ref().unwrap().len() < 1000);
}

#[test]
fn unidentified_span_has_null_run_type_and_cost() {
    // No kind, model, provider or operation attributes: nothing identifies it, so a sparse post (e.g. an end
    // post) never overwrites a run_type or cost set by an earlier post.
    let attrs = vec![kv("gen_ai.usage.input_tokens", i(10)), kv("http.method", s("GET"))];
    let n = normalize_request(req(vec![], vec![span(1, 1, None, 0, attrs)]), None, &cfg());
    let r = &n.rows[0];
    assert_eq!(r.run_type, None);
    assert_eq!(r.cost_usd, None);
    assert_eq!(r.prompt_tokens, Some(10));
}

//! Normalization never panics, and every row it produces is a valid spans row.
use forge_ingest::normalize::{NormalizeConfig, normalize_request};
use forge_model::row::validate_batch;
use forge_model::testing::batch_from_rows;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::Event;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use proptest::prelude::*;

const KEYS: &[&str] = &[
    "gen_ai.operation.name",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "gen_ai.provider.name",
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.output_tokens",
    "gen_ai.tool.call.arguments",
    "gen_ai.conversation.id",
    "openinference.span.kind",
    "llm.model_name",
    "llm.cost.total",
    "llm.token_count.prompt",
    "input.value",
    "input.mime_type",
    "output.value",
    "metadata",
    "tag.tags",
    "traceloop.entity.input",
    "traceloop.span.kind",
    "langsmith.span.kind",
    "session.id",
    "forge.project",
    "openinference.project.name",
];

fn any_value() -> impl Strategy<Value = AnyValue> {
    let leaf = prop_oneof![
        any::<String>().prop_map(Value::StringValue),
        "[{\\[\"a-z0-9:,\\]}]{0,12}".prop_map(Value::StringValue), // JSON-looking text
        any::<bool>().prop_map(Value::BoolValue),
        prop_oneof![Just(i64::MIN), Just(i64::MAX), Just(0i64), any::<i64>()].prop_map(Value::IntValue),
        any::<f64>().prop_map(Value::DoubleValue),
        prop::collection::vec(any::<u8>(), 0..8).prop_map(Value::BytesValue),
    ];
    let leaf = prop_oneof![9 => leaf.prop_map(Some), 1 => Just(None)].prop_map(|value| AnyValue { value });
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(|values| AnyValue {
                value: Some(Value::ArrayValue(ArrayValue { values })),
            }),
            prop::collection::vec((key(), inner), 0..4).prop_map(|kvs| AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: kvs.into_iter().map(|(k, v)| kv(k, v)).collect(),
                })),
            }),
        ]
    })
}

fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(KEYS).prop_map(str::to_string),
        (
            0u32..20,
            prop::sample::select(&["role", "content", "message.role", "message.content"][..])
        )
            .prop_map(|(n, f)| format!("gen_ai.prompt.{n}.{f}")),
        (0u32..5).prop_map(|n| format!("llm.input_messages.{n}.message.content")),
        any::<String>(),
    ]
}

fn kv(key: String, v: AnyValue) -> KeyValue {
    KeyValue {
        key,
        value: Some(v),
        ..Default::default()
    }
}

fn attrs() -> impl Strategy<Value = Vec<KeyValue>> {
    prop::collection::vec((key(), any_value()).prop_map(|(k, v)| kv(k, v)), 0..10)
}

fn id(len: usize) -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        8 => prop::collection::vec(any::<u8>(), len),
        1 => Just(vec![0u8; len]),
        1 => prop::collection::vec(any::<u8>(), 0..20),
    ]
}

fn span() -> impl Strategy<Value = Span> {
    (
        id(16),
        id(8),
        prop_oneof![Just(vec![]), id(8)],
        any::<String>(),
        prop_oneof![Just(0u64), any::<u64>(), Just(u64::MAX)],
        prop_oneof![Just(0u64), any::<u64>()],
        attrs(),
        prop::collection::vec(("[a-z_.]{0,20}", attrs()), 0..3),
        prop::option::of((0i32..4, any::<String>())),
        0i32..7,
    )
        .prop_map(|(t, s, p, name, start, end, attributes, events, status, kind)| Span {
            trace_id: t,
            span_id: s,
            parent_span_id: p,
            name,
            kind,
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            attributes,
            events: events
                .into_iter()
                .map(|(name, attributes)| Event {
                    name,
                    attributes,
                    ..Default::default()
                })
                .collect(),
            status: status.map(|(code, message)| Status { code, message }),
            ..Default::default()
        })
}

fn request() -> impl Strategy<Value = ExportTraceServiceRequest> {
    prop::collection::vec((attrs(), prop::collection::vec(span(), 0..6)), 1..3).prop_map(|rs| {
        ExportTraceServiceRequest {
            resource_spans: rs
                .into_iter()
                .map(|(attributes, spans)| ResourceSpans {
                    resource: Some(Resource {
                        attributes,
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        spans,
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn normalize_never_panics(req in request()) {
        let cfg = NormalizeConfig::default();
        let spans: usize = req.resource_spans.iter().flat_map(|r| &r.scope_spans).map(|s| s.spans.len()).sum();
        let n = normalize_request(req, None, &cfg);
        prop_assert_eq!(n.rows.len() as u64 + n.rejected, spans as u64);
        let mut rows = n.rows.clone();
        for (i, r) in rows.iter_mut().enumerate() {
            for (c, v) in [
                ("inputs", &r.inputs),
                ("outputs", &r.outputs),
                ("events", &r.events),
                ("attributes", &r.attributes),
                ("metadata", &r.metadata),
            ] {
                if let Some(v) = v {
                    prop_assert!(serde_json::from_str::<serde_json::Value>(v).is_ok(), "{} is not JSON: {}", c, v);
                }
            }
            r.version = i as u64 + 1;
        }
        validate_batch(&batch_from_rows(&rows)).unwrap();
    }
}

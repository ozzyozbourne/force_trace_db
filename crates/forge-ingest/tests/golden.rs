//! Golden fixtures: research/fixtures/*.json -> exact normalized SpanRow fields.
mod common;
use common::j;
use forge_ingest::normalize::{NormalizeConfig, Normalized, normalize_request};
use forge_ingest::version::{VersionAllocator, stamp};
use forge_model::enums::{RunType, SpanStatus};
use forge_model::ids::SpanId;
use forge_model::row::{SpanRow, validate_batch};
use forge_model::testing::batch_from_rows;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use serde_json::json;

const BASE: i64 = 1_792_663_200_000_000_000; // 2026-10-22T10:00:00Z
fn ms(m: i64) -> Option<i64> {
    Some(BASE + m * 1_000_000)
}

fn load(name: &str) -> ExportTraceServiceRequest {
    serde_json::from_slice(&forge_model::testing::fixture_bytes(name)).unwrap()
}
fn norm(name: &str) -> Normalized {
    let n = normalize_request(load(name), None, &NormalizeConfig::default());
    assert_eq!(n.rejected, 0, "{:?}", n.first_error);
    n
}
fn row<'a>(n: &'a Normalized, span: &str) -> &'a SpanRow {
    let id: SpanId = span.parse().unwrap();
    n.rows
        .iter()
        .find(|r| r.span_id == id)
        .unwrap_or_else(|| panic!("no row {span}"))
}

/// Scalar columns of one row.
#[derive(Debug, PartialEq)]
struct Scalars<'a> {
    parent: Option<&'a str>,
    name: &'a str,
    run_type: RunType,
    model: Option<&'a str>,
    provider: Option<&'a str>,
    tokens: (Option<i64>, Option<i64>, Option<i64>),
    status: Option<SpanStatus>,
    session: Option<&'a str>,
}
/// `parent` is the row's parent span id in hex (owned by the caller).
fn scalars<'a>(r: &'a SpanRow, parent: Option<&'a str>) -> Scalars<'a> {
    Scalars {
        parent,
        name: r.name.as_deref().unwrap(),
        run_type: r.run_type.unwrap(),
        model: r.model.as_deref(),
        provider: r.provider.as_deref(),
        tokens: (r.prompt_tokens, r.completion_tokens, r.total_tokens),
        status: r.status,
        session: r.session_id.as_deref(),
    }
}
fn check(n: &Normalized, span: &str, want: Scalars<'_>, cost: Option<f64>) {
    let r = row(n, span);
    let parent = r.parent_span_id.map(|p| p.to_string());
    assert_eq!(scalars(r, parent.as_deref()), want, "{span}");
    match (r.cost_usd, cost) {
        (None, None) => {}
        (Some(a), Some(b)) => assert!((a - b).abs() < 1e-12, "{span} cost {a} != {b}"),
        (a, b) => panic!("{span} cost {a:?} != {b:?}"),
    }
}
use RunType::*;
use SpanStatus::Ok as SOk;

#[test]
fn openllmetry_langgraph() {
    let n = norm("openllmetry_langgraph.json");
    assert_eq!(n.rows.len(), 4);
    assert!(
        n.rows
            .iter()
            .all(|r| r.project_id.as_ref().unwrap().as_str() == "default")
    );
    let t = (None, None, None);
    check(
        &n,
        "f0e1d2c3b4a59687",
        Scalars {
            parent: None,
            name: "invoke_agent LangGraph",
            run_type: Agent,
            model: None,
            provider: Some("langgraph"),
            tokens: t,
            status: Some(SOk),
            session: None,
        },
        None,
    );
    check(
        &n,
        "a1a2a3a4a5a6a7a8",
        Scalars {
            parent: Some("f0e1d2c3b4a59687"),
            name: "LangGraph.workflow",
            run_type: Chain,
            model: None,
            provider: Some("langgraph"),
            tokens: t,
            status: Some(SOk),
            session: Some("thread-42"),
        },
        None,
    );
    check(
        &n,
        "c1d2e3f4a5b6c7d8",
        Scalars {
            parent: Some("a1a2a3a4a5a6a7a8"),
            name: "execute_task calculate",
            run_type: Chain,
            model: None,
            provider: Some("langgraph"),
            tokens: t,
            status: Some(SOk),
            session: Some("thread-42"),
        },
        None,
    );
    check(
        &n,
        "b7ad6b7169203331",
        Scalars {
            parent: Some("c1d2e3f4a5b6c7d8"),
            name: "openai.chat",
            run_type: ChatModel,
            model: Some("gpt-4o-2024-08-06"),
            provider: Some("openai"),
            tokens: (Some(24), Some(11), Some(35)),
            status: Some(SOk),
            session: Some("thread-42"),
        },
        Some(0.00017),
    );

    let leaf = row(&n, "b7ad6b7169203331");
    // The leaf arrives first in the request; its ancestors are in the same batch, so it resolves.
    assert_eq!(
        leaf.dotted_order.as_deref(),
        Some(
            "20261022T100000000000Zf0e1d2c3b4a59687.20261022T100000010000Za1a2a3a4a5a6a7a8.20261022T100000020000Zc1d2e3f4a5b6c7d8.20261022T100000040000Zb7ad6b7169203331"
        )
    );
    assert_eq!(
        row(&n, "f0e1d2c3b4a59687").dotted_order.as_deref(),
        Some("20261022T100000000000Zf0e1d2c3b4a59687")
    );
    assert_eq!((leaf.start_time_ns, leaf.end_time_ns), (ms(40), ms(900)));
    assert_eq!(
        j(&leaf.metadata),
        json!({"thread_id": "thread-42", "langgraph_node": "calculate"})
    );
    assert_eq!(
        j(&leaf.inputs),
        json!({"messages": [
            {"role": "system", "parts": [{"type": "text", "content": "You are a mathematician."}]},
            {"role": "user", "parts": [{"type": "text", "content": "What's 5 + 5?"}]}]})
    );
    assert_eq!(
        j(&leaf.outputs),
        json!({"messages": [{"role": "assistant", "parts": [{"type": "text", "content": "5 + 5 = 10."}], "finish_reason": "stop"}]})
    );
    let attrs = j(&leaf.attributes);
    assert!(
        attrs.get("gen_ai.input.messages").is_none()
            && attrs.get("traceloop.association.properties.thread_id").is_none()
    );
    assert_eq!(attrs["gen_ai.request.model"], "gpt-4o");
    assert_eq!(attrs["forge.kind"], "client");
    assert_eq!(
        attrs["forge.scope"],
        json!({"name": "opentelemetry.instrumentation.langchain", "version": "0.47.3"})
    );
    assert_eq!(attrs["forge.resource"]["service.name"], "langgraph-calculator");
    assert_eq!(leaf.events, None);
    assert_eq!(leaf.tags, None);

    let task = row(&n, "c1d2e3f4a5b6c7d8");
    assert_eq!(j(&task.inputs), json!({"request": "What's 5 + 5?"}));
    assert_eq!(j(&task.outputs), json!({"result": "5 + 5 = 10."}));
    let wf = row(&n, "a1a2a3a4a5a6a7a8");
    assert_eq!(
        j(&wf.outputs),
        json!({"request": "What's 5 + 5?", "result": "5 + 5 = 10."})
    );
    assert_eq!(j(&wf.metadata), json!({"thread_id": "thread-42"}));
    let root = row(&n, "f0e1d2c3b4a59687");
    assert_eq!(
        (root.inputs.as_ref(), root.outputs.as_ref(), root.metadata.as_ref()),
        (None, None, None)
    );
}

#[test]
fn openinference_langchain_rag() {
    let n = norm("openinference_langchain_rag.json");
    assert_eq!(n.rows.len(), 5);
    assert!(
        n.rows
            .iter()
            .all(|r| r.project_id.as_ref().unwrap().as_str() == "support-bot")
    );
    assert!(n.rows.iter().all(|r| r.dotted_order.is_some()));
    let t = (None, None, None);
    let s = Some("sess-7f3a");
    let root = "00f067aa0ba902b7";
    check(
        &n,
        root,
        Scalars {
            parent: None,
            name: "RunnableSequence",
            run_type: Chain,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: s,
        },
        None,
    );
    check(
        &n,
        "1111111111111111",
        Scalars {
            parent: Some(root),
            name: "VectorStoreRetriever",
            run_type: Retriever,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: s,
        },
        None,
    );
    check(
        &n,
        "2222222222222222",
        Scalars {
            parent: Some(root),
            name: "ChatPromptTemplate",
            run_type: Chain,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: s,
        },
        None,
    );
    check(
        &n,
        "3333333333333333",
        Scalars {
            parent: Some(root),
            name: "ChatOpenAI",
            run_type: ChatModel,
            model: Some("gpt-4o-mini-2024-07-18"),
            provider: Some("openai"),
            tokens: (Some(52), Some(12), Some(64)),
            status: Some(SOk),
            session: s,
        },
        Some(0.000015),
    );
    check(
        &n,
        "4444444444444444",
        Scalars {
            parent: Some(root),
            name: "lookup_order",
            run_type: Tool,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SpanStatus::Error),
            session: s,
        },
        None,
    );

    let r = row(&n, root);
    assert_eq!(r.tags, Some(vec!["rag".into(), "prod".into()]));
    assert_eq!(
        j(&r.metadata),
        json!({"env": "prod", "customer_tier": "gold", "user.id": "user-123"})
    );
    assert_eq!(j(&r.inputs), json!("What is the refund policy?"));
    assert_eq!(
        j(&r.outputs),
        json!("You can request a refund within 30 days of purchase.")
    );

    let ret = row(&n, "1111111111111111");
    assert_eq!(j(&ret.inputs), json!("What is the refund policy?"));
    assert_eq!(
        j(&ret.outputs),
        json!({"documents": [
            {"id": "doc-17", "score": 0.91, "content": "Refunds are accepted within 30 days of purchase.", "metadata": {"source": "policy.md"}},
            {"id": "doc-4", "score": 0.62, "content": "Shipping takes 3-5 business days."}]})
    );
    assert_eq!(
        j(&row(&n, "2222222222222222").inputs),
        json!({"question": "What is the refund policy?", "context": "Refunds are accepted within 30 days of purchase."})
    );

    let llm = row(&n, "3333333333333333");
    assert_eq!(
        j(&llm.inputs),
        json!({"messages": [
            {"role": "system", "content": "Answer using the context.\n\nContext: Refunds are accepted within 30 days of purchase."},
            {"role": "user", "content": "What is the refund policy?"}]})
    );
    assert_eq!(
        j(&llm.outputs),
        json!({"messages": [{"role": "assistant", "content": "You can request a refund within 30 days of purchase."}]})
    );
    assert_eq!(
        j(&llm.metadata),
        json!({"ls_provider": "openai", "ls_model_name": "gpt-4o-mini", "env": "prod", "user.id": "user-123"})
    );
    let a = j(&llm.attributes);
    // P9: once inputs/outputs are extracted, the OpenInference `*.value` copies are dropped; winners and
    // metadata/tags are consumed.
    assert!(a.get("input.value").is_none() && a.get("input.mime_type").is_none());
    assert!(a.get("output.value").is_none() && a.get("output.mime_type").is_none());
    assert!(
        a.get("llm.input_messages.0.message.role").is_none()
            && a.get("metadata").is_none()
            && a.get("tag.tags").is_none()
    );
    assert_eq!(a["llm.token_count.prompt"], 52);

    let tool = row(&n, "4444444444444444");
    assert_eq!(tool.error.as_deref(), Some("KeyError: 'A-1001'"));
    assert_eq!(j(&tool.inputs), json!({"order_id": "A-1001"}));
    let ev = j(&tool.events);
    assert_eq!(ev.as_array().unwrap().len(), 1);
    assert_eq!(ev[0]["name"], "exception");
    assert_eq!(ev[0]["time"], "2026-10-22T10:00:02.289Z");
    assert_eq!(ev[0]["attributes"]["exception.type"], "KeyError");
}

#[test]
fn genai_semconv_agent() {
    let n = norm("genai_semconv_agent.json");
    assert_eq!(n.rows.len(), 7);
    let c = Some("conv_5j66UpCpwteGg4YSxUnt7lPY");
    let root = "eee19b7ec3c1b100";
    let t = (None, None, None);
    check(
        &n,
        root,
        Scalars {
            parent: None,
            name: "invoke_agent WeatherBot",
            run_type: Agent,
            model: Some("gpt-4o"),
            provider: Some("openai"),
            tokens: (Some(230), Some(31), Some(261)),
            status: Some(SOk),
            session: c,
        },
        None,
    );
    check(
        &n,
        "eee19b7ec3c1b101",
        Scalars {
            parent: Some(root),
            name: "chat gpt-4o",
            run_type: ChatModel,
            model: Some("gpt-4o-2024-08-06"),
            provider: Some("openai"),
            tokens: (Some(120), Some(18), Some(138)),
            status: Some(SOk),
            session: c,
        },
        Some(0.0004),
    );
    check(
        &n,
        "eee19b7ec3c1b102",
        Scalars {
            parent: Some(root),
            name: "execute_tool get_weather",
            run_type: Tool,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: c,
        },
        None,
    );
    check(
        &n,
        "eee19b7ec3c1b103",
        Scalars {
            parent: Some(root),
            name: "chat gpt-4o",
            run_type: ChatModel,
            model: Some("gpt-4o-2024-08-06"),
            provider: Some("openai"),
            tokens: (Some(110), Some(13), Some(123)),
            status: Some(SOk),
            session: c,
        },
        Some(0.000405),
    );
    check(
        &n,
        "eee19b7ec3c1b104",
        Scalars {
            parent: Some(root),
            name: "embeddings text-embedding-3-small",
            run_type: Embedding,
            model: Some("text-embedding-3-small"),
            provider: Some("openai"),
            tokens: (Some(5), None, Some(5)),
            status: Some(SOk),
            session: None,
        },
        Some(0.0000001),
    );
    check(
        &n,
        "eee19b7ec3c1b105",
        Scalars {
            parent: Some(root),
            name: "retrieval weather_kb",
            run_type: Retriever,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: None,
        },
        None,
    );
    // Open span: end_time null and status null (not "unset"), so a late start post cannot clobber the end post.
    check(
        &n,
        "eee19b7ec3c1b106",
        Scalars {
            parent: Some(root),
            name: "plan WeatherBot",
            run_type: Agent,
            model: None,
            provider: None,
            tokens: t,
            status: None,
            session: None,
        },
        None,
    );
    let open = row(&n, "eee19b7ec3c1b106");
    assert_eq!((open.start_time_ns, open.end_time_ns), (ms(3160), None));
    assert_eq!(
        open.dotted_order.as_deref(),
        Some("20261022T100000000000Zeee19b7ec3c1b100.20261022T100003160000Zeee19b7ec3c1b106")
    );

    let leaf_cost: f64 = n.rows.iter().filter_map(|r| r.cost_usd).sum();
    assert!((leaf_cost - 0.0008051).abs() < 1e-12, "{leaf_cost}");

    let user = json!([{"role": "user", "parts": [{"type": "text", "content": "Weather in Paris?"}]}]);
    let sys = json!([{"type": "text", "content": "You are a helpful weather assistant."}]);
    let r = row(&n, root);
    assert_eq!(j(&r.inputs), json!({"messages": user, "system_instructions": sys}));
    assert_eq!(
        j(&r.outputs)["messages"][0]["parts"][0]["content"],
        "It's rainy and 57°F in Paris."
    );
    let chat = row(&n, "eee19b7ec3c1b101");
    let ci = j(&chat.inputs);
    assert_eq!(ci["messages"], user);
    assert_eq!(ci["system_instructions"], sys);
    assert_eq!(ci["tools"][0]["name"], "get_weather");
    assert_eq!(j(&chat.outputs)["messages"][0]["parts"][0]["type"], "tool_call");
    assert_eq!(j(&chat.attributes)["gen_ai.usage.cache_read.input_tokens"], 64);
    let tool = row(&n, "eee19b7ec3c1b102");
    assert_eq!(j(&tool.inputs), json!({"location": "Paris"}));
    assert_eq!(j(&tool.outputs), json!("rainy, 57°F"));
    assert_eq!(
        j(&tool.attributes)["gen_ai.tool.call.id"],
        "call_VSPygqKTWdrhaFErNvMV18Yl"
    );
    assert_eq!(
        j(&row(&n, "eee19b7ec3c1b105").inputs),
        json!({"query": "Paris weather"})
    );
    assert_eq!(row(&n, "eee19b7ec3c1b104").inputs, None);
}

#[test]
fn openllmetry_legacy() {
    let n = norm("openllmetry_legacy.json");
    assert_eq!(n.rows.len(), 4);
    let root = "1000000000000001";
    let t = (None, None, None);
    let s = Some("sess-legacy-1");
    check(
        &n,
        root,
        Scalars {
            parent: None,
            name: "joke_generator.workflow",
            run_type: Chain,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: s,
        },
        None,
    );
    check(
        &n,
        "1000000000000002",
        Scalars {
            parent: Some(root),
            name: "openai.chat",
            run_type: ChatModel,
            model: Some("gpt-3.5-turbo-0125"),
            provider: Some("openai"),
            tokens: (Some(22), Some(15), Some(37)),
            status: Some(SOk),
            session: s,
        },
        Some(0.0000335),
    );
    check(
        &n,
        "1000000000000003",
        Scalars {
            parent: Some(root),
            name: "get_weather.tool",
            run_type: Tool,
            model: None,
            provider: None,
            tokens: t,
            status: Some(SOk),
            session: None,
        },
        None,
    );
    check(
        &n,
        "1000000000000004",
        Scalars {
            parent: Some(root),
            name: "openai.embeddings",
            run_type: Embedding,
            model: Some("text-embedding-ada-002"),
            provider: Some("openai"),
            tokens: (Some(3), None, Some(3)),
            status: Some(SOk),
            session: None,
        },
        Some(0.0000003),
    );

    let r = row(&n, root);
    assert_eq!(j(&r.metadata), json!({"session_id": "sess-legacy-1", "user_id": "u-9"}));
    assert_eq!(j(&r.inputs), json!({"args": [], "kwargs": {"topic": "opentelemetry"}}));
    assert_eq!(
        j(&r.outputs),
        json!("Why did the span cross the road? To get to the other trace.")
    );
    let chat = row(&n, "1000000000000002");
    assert_eq!(
        j(&chat.inputs),
        json!({"messages": [{"role": "system", "content": "You are a comedian."}, {"role": "user", "content": "Tell me a joke about opentelemetry"}]})
    );
    assert_eq!(
        j(&chat.outputs),
        json!({"messages": [{"role": "assistant", "finish_reason": "stop", "content": "Why did the span cross the road? To get to the other trace."}]})
    );
    assert_eq!(j(&chat.attributes)["gen_ai.system"], "OpenAI"); // raw value kept; column is lowercased
    assert_eq!(
        j(&row(&n, "1000000000000003").inputs),
        json!({"args": ["Paris"], "kwargs": {}})
    );
    assert_eq!(
        j(&row(&n, "1000000000000004").inputs),
        json!({"messages": [{"content": "opentelemetry"}]})
    );
}

/// Every fixture's rows stamp versions and build a valid spans batch.
#[test]
fn rows_build_valid_batches() {
    let alloc = VersionAllocator::new(41);
    for f in [
        "openllmetry_langgraph.json",
        "openinference_langchain_rag.json",
        "genai_semconv_agent.json",
        "openllmetry_legacy.json",
    ] {
        let mut n = norm(f);
        let r = alloc.reserve(n.rows.len() as u64);
        stamp(&mut n.rows, r);
        validate_batch(&batch_from_rows(&n.rows)).unwrap();
    }
    assert_eq!(alloc.high_water(), 41 + 4 + 5 + 7 + 4);
}

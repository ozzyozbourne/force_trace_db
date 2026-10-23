# Forge

Forge is a single-binary trace database for LLM applications. It ingests OpenTelemetry (OTLP) spans,
stores them as Vortex segments on local disk or object storage, keeps its catalog in SQLite, and serves
trace lookups, SQL and full-text search.

**Status: M2 (read path).** OTLP spans are normalized, written to a WAL (acked after fsync), and flushed to L0
Vortex segments plus the SQLite catalog; the WAL replays after a crash. Acknowledged spans are queryable at once
(memtable + segments, merged per span): trace trees, a runs list and SQL. Compaction and search land later.

## Build and test

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

## Run

```sh
forge serve                                  # no config: data under ./forge-data
forge serve --config forge.example.toml      # the spec's layout under /var/lib/forge
```

| Port | Listener | Behaviour |
|---|---|---|
| 4317 | OTLP gRPC (`TraceService/Export`) | ingest (gzip/zstd); project from `x-forge-project` metadata |
| 4318 | OTLP HTTP (`POST /v1/traces`) | ingest, protobuf or JSON, gzip/zstd; project from `x-forge-project` |
| 8080 | API (`/v1/...`) | projects, traces, runs, SQL (below); other routes 501 |
| 9090 | Ops (`/healthz`, `/readyz`, `/metrics`) | 200 / 200 once listening / placeholder |

The first SIGTERM or SIGINT drains all listeners (bounded by `server.shutdown_grace`, default 10s), fsyncs the WAL,
flushes the memtable (also bounded by the grace; on timeout the WAL replays next start) and exits 0.
A second signal exits immediately with code 130.

`forge inspect segment <path> [--verify] [--layout]` prints a segment's footer, column stats and forge metadata;
`--verify` re-checks the segment contract (sort order, one row per span).

## Query API

Responses are `application/json`; the API router gzips when the client sends `Accept-Encoding: gzip`. Errors are
`{"error": "..."}` with 400 (bad input, including a query over the memory limit), 404 (no such trace), 503 +
`Retry-After` (too many concurrent SQL queries), 504 (timeout, `query.timeout`, default 30 s).
The project comes from `?project=`, else the `x-forge-project` header, else `ingest.default_project`.

- `GET /v1/traces/{trace_id}?payloads=true|false&format=tree|flat`: one trace, every span merged.
  `trace_id` is 32 hex digits (any case) or a dashed UUID. `tree` nests spans under `children` (built from
  `parent_span_id`; siblings by start µs, then span id; spans without a start last); spans whose parent is missing
  are extra roots with `"orphan": true`, a parent cycle is broken at its smallest member with `"cycle": true`.
  `flat` lists the same order with `depth`. `payloads=false` leaves out `inputs/outputs/events/attributes`.
- `GET /v1/runs?start=&end=&run_type=&status=&q=&limit=&cursor=`: root spans (null parent, non-null start),
  newest first (`start_time DESC, trace_id DESC, span_id DESC`). `start`/`end` are RFC3339 (`end` exclusive),
  `run_type`/`status` comma-separated, `q` a case-insensitive substring of `name`, `limit` 1..1000 (default 50).
  Page with the returned `next_cursor` until it is `null`; a cursor only continues the same filters.
- `POST /v1/sql {"query": "...", "max_rows": 10000, "timeout_ms": ...}`: read-only SQL (DataFusion) over the
  table `spans`. Returns `{"columns":[{name,type}],"rows":[{...}],"row_count","truncated","elapsed_ms"}`; rows stop
  at `max_rows` (≤ 100,000) or `query.max_response_bytes` (64 MiB) with `truncated: true`.

The fast paths and SQL format values differently: traces and runs write timestamps as RFC3339 with exactly 9
fractional digits, JSON columns (`metadata`, `inputs`, …) as embedded JSON, and `latency_ms`; SQL keeps SQL types
(timestamps with as many fractional digits as needed, JSON columns as strings, ids as hex). `total_tokens` is the
producer's total, else `prompt_tokens + completion_tokens` when both are set, else whichever is set. `version`
can exceed 2^53, which JavaScript numbers cannot represent exactly.

`forge sql "<query>" [--server URL | --data-dir DIR] [--format table|json]`:
- `--server` POSTs to `URL/v1/sql` and sees everything the server sees.
- `--data-dir` (expects `DIR/meta.db` and `DIR/data`) or no flag (the config's URLs) runs the query in process over
  a read-only metastore. It reads flushed segments only: rows still in the server's memtable are invisible (a note
  on stderr says so). It can run beside a live server and never creates or migrates anything.

**M2 limits.** `start_time` is treated as immutable per span: a later post that changes it may be missed by time
pruning and by the runs list. Deep runs pages re-read every segment that overlaps the window (bounded by M3's
compaction). The trace and runs fast paths are not behind the SQL concurrency limit; they reserve their input
against `query.memory_limit` per call. A merge of a whole project (`SELECT *`) is bounded by the memory limit.

## Configuration

Settings come from defaults, then `--config <file>` (or `./forge.toml` in the current directory if it exists),
then environment variables named `FORGE_<SECTION>__<KEY>` (double underscore), for example
`FORGE_SERVER__API_ADDR=127.0.0.1:8081` or `FORGE_MEMTABLE__FLUSH_BYTES=128MiB`. Unknown keys are errors.
Variables with a single underscore (such as a typo like `FORGE_SERVER_API_ADDR`) are silently ignored.

Logs go to stderr. `--log` / `FORGE_LOG` (or `RUST_LOG`) sets the filter; `FORGE_LOG_FORMAT=json` selects JSON.

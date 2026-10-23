# Forge

Forge is a single-binary trace database for LLM applications. It ingests OpenTelemetry (OTLP) spans,
stores them as Vortex segments on local disk or object storage, keeps its catalog in SQLite, and serves
trace lookups, SQL and full-text search.

**Status: M3 (compaction and lifecycle).** OTLP spans are normalized, written to a WAL (acked after fsync), and
flushed to L0 Vortex segments plus the SQLite catalog; the WAL replays after a crash. Acknowledged spans are
queryable at once (memtable + segments, merged per span): trace trees, a runs list and SQL. L0 segments are
compacted into hourly L1 segments, retention and GC run in the background. Search lands later.

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

## Compaction, retention and GC

The server compacts and collects in the background (`[compaction]` in `forge.example.toml`):
- **Compaction.** Every `interval` (default 10s) a picker merges the oldest L0 segments into hour-partitioned L1
  segments of about `target_segment` (256 MiB) once `l0_trigger` (4) L0s exist or the oldest is `l0_max_age` (10m)
  old, folding small hours into the same job. Hours with overlapping L1 runs are rewritten newest runs first.
  Up to `concurrency` (2) jobs run at once. Queries see the same answers before and after a job.
- **Memory bound.** A job holds up to about 3 x `slice_bytes` (128 MiB) of decoded rows plus its open writers'
  buffers, about 0.9 GiB at the defaults, per job. On small hosts lower `max_job_bytes` (512 MiB), `slice_bytes`
  and `concurrency`, for example `max_job_bytes = "128MiB"`, `slice_bytes = "32MiB"`, `concurrency = 1`.
- **Retention.** Per project, set when it is created: `POST /v1/projects {"id": "p", "name": "p",
  "retention_days": 30}` (omit `retention_days` to keep forever). Every `gc_interval` (10m) whole hours older than
  the retention are deleted (L1 by partition hour; an L0 once all its rows are older than the cutoff).
- **GC.** Deleted segments' objects are removed `gc_grace` (1h) after deletion; objects the catalog does not know
  (cut uploads) after `orphan_grace` (24h; at least 4 x `memtable.flush_interval` + 10m, i.e. 12m at the defaults).
  A reused blob is refreshed once older than min(1h, `orphan_grace` / 4). A compaction job older than half of
  `orphan_grace` cannot commit.
- `interval = "0s"` / `gc_interval = "0s"` turn the background tasks off.

Offline tools (they refuse with exit code 3 while a server holds the data dir, i.e. its `LOCK` next to `meta.db` or a
WAL node dir's `LOCK`; stop the server first; `forge serve` likewise refuses to start while one of them runs):
- `forge compact --project P [--data-dir D] [--format table|json]` runs up to 64 jobs of one project with the triggers
  off (run it again if the output says capped). Exit 1 on an unknown project or a failed job.
- `forge gc [--data-dir D] [--dry-run] [--gc-grace 0s] [--orphan-grace 0s] [--format table|json]` runs one retention
  + purge + orphan pass. Blobs are skipped while the WAL holds unflushed records (start and stop the server once to
  flush them). Neither command changes the WAL; both (`--dry-run` too) migrate the metastore and reset compaction
  jobs an earlier process left running.

## Configuration

Settings come from defaults, then `--config <file>` (or `./forge.toml` in the current directory if it exists),
then environment variables named `FORGE_<SECTION>__<KEY>` (double underscore), for example
`FORGE_SERVER__API_ADDR=127.0.0.1:8081` or `FORGE_MEMTABLE__FLUSH_BYTES=128MiB`. Unknown keys are errors.
Variables with a single underscore (such as a typo like `FORGE_SERVER_API_ADDR`) are silently ignored.

Logs go to stderr. `--log` / `FORGE_LOG` (or `RUST_LOG`) sets the filter; `FORGE_LOG_FORMAT=json` selects JSON.

# Forge

Forge is a single-binary trace database for LLM applications. It ingests OpenTelemetry (OTLP) spans,
stores them as Vortex segments on local disk or object storage, keeps its catalog in SQLite, and serves
trace lookups, SQL and full-text search.

**Status: M1 (write path).** OTLP spans are normalized, written to a WAL (acked after fsync), and flushed to L0
Vortex segments plus the SQLite catalog; the WAL replays after a crash. Queries and search land in later milestones.

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
| 8080 | API (`/v1/...`) | `GET`/`POST /v1/projects`; other routes 501 |
| 9090 | Ops (`/healthz`, `/readyz`, `/metrics`) | 200 / 200 once listening / placeholder |

The first SIGTERM or SIGINT drains all listeners (bounded by `server.shutdown_grace`, default 10s), fsyncs the WAL,
flushes the memtable (also bounded by the grace; on timeout the WAL replays next start) and exits 0.
A second signal exits immediately with code 130.

`forge inspect segment <path> [--verify] [--layout]` prints a segment's footer, column stats and forge metadata;
`--verify` re-checks the segment contract (sort order, one row per span).

## Configuration

Settings come from defaults, then `--config <file>` (or `./forge.toml` in the current directory if it exists),
then environment variables named `FORGE_<SECTION>__<KEY>` (double underscore), for example
`FORGE_SERVER__API_ADDR=127.0.0.1:8081` or `FORGE_MEMTABLE__FLUSH_BYTES=128MiB`. Unknown keys are errors.
Variables with a single underscore (such as a typo like `FORGE_SERVER_API_ADDR`) are silently ignored.

Logs go to stderr. `--log` / `FORGE_LOG` (or `RUST_LOG`) sets the filter; `FORGE_LOG_FORMAT=json` selects JSON.

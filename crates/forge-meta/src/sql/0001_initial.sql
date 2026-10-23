-- forge-meta migration 1 (schema_versions is created by the runner before migrations)
CREATE TABLE projects (
  id              TEXT    PRIMARY KEY,                -- ProjectId (path-safe)
  name            TEXT    NOT NULL,                   -- display name; not unique
  api_key_hash    BLOB    UNIQUE CHECK (api_key_hash IS NULL OR length(api_key_hash) = 32), -- sha256
  retention_days  INTEGER CHECK (retention_days IS NULL OR retention_days > 0),
  created_at_ms   INTEGER NOT NULL
) STRICT;

CREATE TABLE segments (
  id               TEXT    PRIMARY KEY,               -- ULID
  project_id       TEXT    NOT NULL REFERENCES projects(id),
  level            INTEGER NOT NULL CHECK (level IN (0, 1)),
  partition_hour   INTEGER NOT NULL,                  -- hours since epoch
  path             TEXT    NOT NULL UNIQUE,           -- object key of .vortex
  row_count        INTEGER NOT NULL CHECK (row_count >= 0),
  byte_size        INTEGER NOT NULL,
  fts_byte_size    INTEGER,                           -- NULL = no .fts sidecar
  min_start_ns     INTEGER,                           -- NULL = all start_time null
  max_start_ns     INTEGER,
  start_time_nulls INTEGER NOT NULL DEFAULT 0,
  min_trace_id     BLOB    NOT NULL CHECK (length(min_trace_id) = 16),
  max_trace_id     BLOB    NOT NULL CHECK (length(max_trace_id) = 16),
  min_version      INTEGER NOT NULL,
  max_version      INTEGER NOT NULL,
  schema_version   INTEGER NOT NULL,
  created_at_ms    INTEGER NOT NULL,
  state            TEXT    NOT NULL CHECK (state IN ('live', 'compacting', 'deleted')),
  deleted_at_ms    INTEGER,
  CHECK ((state = 'deleted') = (deleted_at_ms IS NOT NULL))
) STRICT;
-- candidate listing: project + visibility + newest partition first
CREATE INDEX segments_by_project ON segments (project_id, state, partition_hour DESC, level);
-- time pruning / retention
CREATE INDEX segments_by_time    ON segments (project_id, max_start_ns);
-- gc
CREATE INDEX segments_deleted    ON segments (deleted_at_ms) WHERE state = 'deleted';

CREATE TABLE trace_index (
  project_id  TEXT    NOT NULL,
  trace_id    BLOB    NOT NULL CHECK (length(trace_id) = 16),
  segment_id  TEXT    NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  row_start   INTEGER NOT NULL CHECK (row_start >= 0),
  row_count   INTEGER NOT NULL CHECK (row_count > 0),
  PRIMARY KEY (project_id, trace_id, segment_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX trace_index_by_segment ON trace_index (segment_id);  -- swap/purge deletes + FK cascade

CREATE TABLE wal_checkpoints (
  node_id        TEXT    PRIMARY KEY,
  flushed_seq    INTEGER NOT NULL,
  max_version    INTEGER NOT NULL,
  updated_at_ms  INTEGER NOT NULL
) STRICT;

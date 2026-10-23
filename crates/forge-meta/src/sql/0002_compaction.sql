-- forge-meta migration 2: compaction jobs (plans/phase-4 D2, D6) + blob refs (LC §1.4).
CREATE TABLE compaction_jobs (
  id              INTEGER PRIMARY KEY,
  project_id      TEXT    NOT NULL REFERENCES projects(id),
  kind            TEXT    NOT NULL CHECK (kind IN ('l0', 'l1')),
  status          TEXT    NOT NULL CHECK (status IN ('running', 'done', 'failed')),
  created_at_ms   INTEGER NOT NULL,                 -- wall clock (job-age guard, D15)
  finished_at_ms  INTEGER,
  error           TEXT,
  CHECK ((status = 'running') = (finished_at_ms IS NULL))
) STRICT;
-- Backstop: one running L0 job per project (the D1 prefix check already rejects a second one).
CREATE UNIQUE INDEX compaction_jobs_one_l0 ON compaction_jobs (project_id) WHERE status = 'running' AND kind = 'l0';
CREATE INDEX compaction_jobs_running ON compaction_jobs (status) WHERE status = 'running';

CREATE TABLE compaction_inputs (
  job_id      INTEGER NOT NULL REFERENCES compaction_jobs(id) ON DELETE CASCADE,
  segment_id  TEXT    NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  PRIMARY KEY (job_id, segment_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX compaction_inputs_by_segment ON compaction_inputs (segment_id);

-- Picker, begin checks (whole hour / suffix) and L1 retention.
CREATE INDEX segments_by_hour ON segments (project_id, level, partition_hour) WHERE state <> 'deleted';

CREATE TABLE blob_refs (
  project_id  TEXT NOT NULL,
  path        TEXT NOT NULL,                        -- blobs/<project>/<sha256>
  segment_id  TEXT NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
  PRIMARY KEY (project_id, path, segment_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX blob_refs_by_segment ON blob_refs (segment_id);

-- forge-meta migration 3: prune_compaction_jobs seeks finished jobs instead of scanning the table.
CREATE INDEX compaction_jobs_finished ON compaction_jobs (finished_at_ms) WHERE status <> 'running';

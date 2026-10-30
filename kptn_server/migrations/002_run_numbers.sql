-- Each run's number within its project: 1 for the project's first run, then
-- one more for each run after it. The UI names a run by it; the run id stays
-- the key, and what URLs and run files are named by.
ALTER TABLE runs ADD COLUMN number INTEGER;

-- Runs made before this migration are numbered in the order they were made.
UPDATE runs SET number = (
  SELECT COUNT(*) FROM runs AS earlier
  WHERE earlier.project_root = runs.project_root
    AND (earlier.created_at < runs.created_at
         OR (earlier.created_at = runs.created_at AND earlier.run_id <= runs.run_id))
);

CREATE UNIQUE INDEX runs_project_number ON runs(project_root, number);

INSERT INTO schema_version(version) VALUES (2);

CREATE TABLE schema_version(version INTEGER PRIMARY KEY);
INSERT INTO schema_version(version) VALUES (1);

CREATE TABLE runs(
  run_id TEXT PRIMARY KEY,
  project_root TEXT NOT NULL,
  pipeline TEXT NOT NULL,
  profile TEXT,
  force INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  worker_pid INTEGER,
  worker_started_at REAL,
  heartbeat_at TEXT,
  current_task TEXT,
  exit_code INTEGER,
  log_path TEXT NOT NULL
);

CREATE TABLE run_events(
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  sequence INTEGER NOT NULL,
  timestamp TEXT NOT NULL,
  kind TEXT NOT NULL,
  task_name TEXT,
  payload_json TEXT NOT NULL,
  log_start INTEGER,
  log_end INTEGER,
  PRIMARY KEY(run_id, sequence)
);

CREATE TABLE project_run_locks(
  project_root TEXT PRIMARY KEY,
  run_id TEXT NOT NULL UNIQUE REFERENCES runs(run_id)
);

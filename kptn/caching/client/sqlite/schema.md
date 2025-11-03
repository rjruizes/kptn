# SQLite Database Schema for kptn Caching System

## Overview
This schema mirrors the DynamoDB structure but uses relational design optimized for SQLite.

## Tables

### 1. tasks
Main task metadata table
```sql
CREATE TABLE tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_key TEXT NOT NULL,           -- Branch/storage key
    pipeline TEXT NOT NULL,              -- Pipeline ID
    task_id TEXT NOT NULL,               -- Task ID
    code_hashes TEXT,                    -- JSON string
    input_hashes TEXT,                   -- JSON string
    input_data_hashes TEXT,              -- JSON string
    outputs_version TEXT,
    output_data_version TEXT,
    status TEXT,
    start_time TEXT,
    end_time TEXT,
    subtask_count INTEGER DEFAULT 0,
    taskdata_count INTEGER DEFAULT 0,
    subset_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(storage_key, pipeline, task_id)
);
```

### 2. taskdata_bins  
Stores task data in bins to handle large datasets
```sql
CREATE TABLE taskdata_bins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_key TEXT NOT NULL,
    pipeline TEXT NOT NULL,
    task_id TEXT NOT NULL,
    bin_type TEXT NOT NULL,              -- 'TASKDATABIN', 'SUBSETBIN'
    bin_id TEXT NOT NULL,                -- '0', '1', '2', etc.
    data TEXT NOT NULL,                  -- JSON serialized data
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(storage_key, pipeline, task_id, bin_type, bin_id),
    FOREIGN KEY(storage_key, pipeline, task_id) REFERENCES tasks(storage_key, pipeline, task_id) ON DELETE CASCADE
);
```

### 3. subtask_bins
Stores subtask progress information in bins
```sql
CREATE TABLE subtask_bins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_key TEXT NOT NULL,
    pipeline TEXT NOT NULL,
    task_id TEXT NOT NULL,
    bin_id TEXT NOT NULL,                -- '0', '1', '2', etc.
    data TEXT NOT NULL,                  -- JSON serialized subtask list
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(storage_key, pipeline, task_id, bin_id),
    FOREIGN KEY(storage_key, pipeline, task_id) REFERENCES tasks(storage_key, pipeline, task_id) ON DELETE CASCADE
);
```

## Indexes
```sql
CREATE INDEX idx_tasks_lookup ON tasks(storage_key, pipeline, task_id);
CREATE INDEX idx_tasks_pipeline ON tasks(storage_key, pipeline);
CREATE INDEX idx_taskdata_bins_lookup ON taskdata_bins(storage_key, pipeline, task_id, bin_type);
CREATE INDEX idx_subtask_bins_lookup ON subtask_bins(storage_key, pipeline, task_id);
```

## Key Design Decisions

1. **Normalized Structure**: Unlike DynamoDB's single table, we use separate tables for tasks, taskdata, and subtasks with foreign key relationships.

2. **JSON Storage**: Complex data types (hashes, data payloads) stored as JSON strings for flexibility.

3. **Binning Preserved**: Maintain the binning strategy from DynamoDB for consistency, even though SQLite doesn't have the same batch limitations.

4. **Composite Keys**: Use composite UNIQUE constraints to mirror DynamoDB's key structure.

5. **Cascading Deletes**: Foreign keys with CASCADE to ensure data integrity when tasks are deleted.

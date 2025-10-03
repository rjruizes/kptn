import sqlite3
import json
import os
from typing import Optional, Dict, Any

def init_database(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database with required tables and indexes.
    
    :param db_path: Path to SQLite database file
    :return: SQLite connection object
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
    conn.execute("PRAGMA foreign_keys=ON")   # Enable foreign key constraints
    
    # Create tables
    _create_tables(conn)
    _create_indexes(conn)
    
    conn.commit()
    return conn

def _create_tables(conn: sqlite3.Connection) -> None:
    """Create all required tables."""
    
    # Tasks table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storage_key TEXT NOT NULL,
            pipeline TEXT NOT NULL,
            task_id TEXT NOT NULL,
            code_hashes TEXT,
            input_hashes TEXT,
            input_data_hashes TEXT,
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
        )
    """)
    
    # TaskData bins table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS taskdata_bins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storage_key TEXT NOT NULL,
            pipeline TEXT NOT NULL,
            task_id TEXT NOT NULL,
            bin_type TEXT NOT NULL,
            bin_id TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(storage_key, pipeline, task_id, bin_type, bin_id),
            FOREIGN KEY(storage_key, pipeline, task_id) 
                REFERENCES tasks(storage_key, pipeline, task_id) 
                ON DELETE CASCADE
        )
    """)
    
    # Subtask bins table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS subtask_bins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            storage_key TEXT NOT NULL,
            pipeline TEXT NOT NULL,
            task_id TEXT NOT NULL,
            bin_id TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(storage_key, pipeline, task_id, bin_id),
            FOREIGN KEY(storage_key, pipeline, task_id) 
                REFERENCES tasks(storage_key, pipeline, task_id) 
                ON DELETE CASCADE
        )
    """)

def _create_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes for better query performance."""
    
    # Primary lookup indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tasks_lookup 
        ON tasks(storage_key, pipeline, task_id)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tasks_pipeline 
        ON tasks(storage_key, pipeline)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_taskdata_bins_lookup 
        ON taskdata_bins(storage_key, pipeline, task_id, bin_type)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_subtask_bins_lookup 
        ON subtask_bins(storage_key, pipeline, task_id)
    """)

def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Get SQLite connection, creating database if it doesn't exist.
    
    :param db_path: Path to SQLite database file
    :return: SQLite connection object
    """
    # Always initialize database to ensure tables exist
    return init_database(db_path)

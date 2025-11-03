import sqlite3
import json
import datetime
from typing import Dict, Any, Optional


def create_task(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    task_data: Dict[str, Any]
) -> None:
    """
    Create a new task in the SQLite database.
    
    :param conn: SQLite connection
    :param storage_key: The branch or desired key name to group task state by
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param task_data: A dictionary of task attributes
    """
    timestamp = datetime.datetime.now().isoformat()
    
    # Prepare the base fields
    fields = {
        'storage_key': storage_key,
        'pipeline': pipeline_id,
        'task_id': task_id,
        'created_at': timestamp,
        'updated_at': timestamp
    }
    
    # Add task-specific fields from task_data
    for key, value in task_data.items():
        if key in ['code_hashes', 'input_hashes', 'input_data_hashes']:
            # Store complex objects as JSON strings
            fields[key] = json.dumps(value) if value is not None else None
        elif key in ['outputs_version', 'output_data_version', 'status', 
                     'start_time', 'end_time']:
            fields[key] = value
        elif key in ['subtask_count', 'taskdata_count', 'subset_count']:
            fields[key] = value or 0
    
    # Build the INSERT query dynamically
    columns = list(fields.keys())
    placeholders = ['?' for _ in columns]
    values = list(fields.values())
    
    query = f"""
        INSERT OR REPLACE INTO tasks ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
    """
    
    conn.execute(query, values)
    conn.commit()


def get_single_task(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a single task from the SQLite database.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :return: Task data dictionary or None if not found
    """
    cursor = conn.execute("""
        SELECT * FROM tasks
        WHERE storage_key = ? AND pipeline = ? AND task_id = ?
    """, (storage_key, pipeline_id, task_id))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    # Convert row to dictionary
    columns = [description[0] for description in cursor.description]
    task_data = dict(zip(columns, row))
    
    # Parse JSON fields back to objects
    for field in ['code_hashes', 'input_hashes', 'input_data_hashes']:
        if task_data.get(field):
            try:
                task_data[field] = json.loads(task_data[field])
            except (json.JSONDecodeError, TypeError):
                pass
    
    return task_data


def update_task(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    updates: Dict[str, Any]
) -> None:
    """
    Update an existing task in the SQLite database.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param updates: Dictionary of fields to update
    """
    if not updates:
        return
    
    # Add updated timestamp
    updates = updates.copy()
    updates['updated_at'] = datetime.datetime.now().isoformat()
    
    # Handle JSON fields
    processed_updates = {}
    for key, value in updates.items():
        if key in ['code_hashes', 'input_hashes', 'input_data_hashes']:
            processed_updates[key] = json.dumps(value) if value is not None else None
        else:
            processed_updates[key] = value
    
    # Build UPDATE query
    set_clauses = [f"{key} = ?" for key in processed_updates.keys()]
    values = list(processed_updates.values())
    values.extend([storage_key, pipeline_id, task_id])
    
    query = f"""
        UPDATE tasks
        SET {', '.join(set_clauses)}
        WHERE storage_key = ? AND pipeline = ? AND task_id = ?
    """
    
    conn.execute(query, values)
    conn.commit()


def get_tasks_for_pipeline(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str
) -> list[Dict[str, Any]]:
    """
    Get all tasks for a specific pipeline.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :return: List of task dictionaries
    """
    cursor = conn.execute("""
        SELECT * FROM tasks
        WHERE storage_key = ? AND pipeline = ?
        ORDER BY task_id
    """, (storage_key, pipeline_id))
    
    columns = [description[0] for description in cursor.description]
    tasks = []
    
    for row in cursor.fetchall():
        task_data = dict(zip(columns, row))
        
        # Parse JSON fields
        for field in ['code_hashes', 'input_hashes', 'input_data_hashes']:
            if task_data.get(field):
                try:
                    task_data[field] = json.loads(task_data[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        
        tasks.append(task_data)
    
    return tasks


def delete_task(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str
) -> None:
    """
    Delete a task and all associated data from the SQLite database.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    """
    conn.execute("""
        DELETE FROM tasks
        WHERE storage_key = ? AND pipeline = ? AND task_id = ?
    """, (storage_key, pipeline_id, task_id))
    
    # Foreign key constraints will automatically delete related data
    conn.commit()

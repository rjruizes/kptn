import sqlite3
import json
import datetime
from typing import Any, List, Optional


def create_subtaskbin(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_id: str,
    subtasks: List[Any]
) -> None:
    """
    Create a subtask bin in the SQLite database.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_id: The bin ID (e.g., '0', '1', '2')
    :param subtasks: List of subtasks to store
    """
    timestamp = datetime.datetime.now().isoformat()
    
    conn.execute("""
        INSERT OR REPLACE INTO subtask_bins 
        (storage_key, pipeline, task_id, bin_id, data, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        storage_key,
        pipeline_id,
        task_id,
        bin_id,
        json.dumps(subtasks),
        timestamp,
        timestamp
    ))
    
    conn.commit()


def get_subtaskbins(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_ids: List[str]
) -> List[Any]:
    """
    Retrieve multiple subtask bins and combine their data.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_ids: List of bin IDs to retrieve
    :return: Combined subtask data from all bins
    """
    if not bin_ids:
        return []
    
    # Create placeholders for IN clause
    placeholders = ','.join(['?' for _ in bin_ids])
    
    cursor = conn.execute(f"""
        SELECT bin_id, data FROM subtask_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ? 
        AND bin_id IN ({placeholders})
        ORDER BY CAST(bin_id AS INTEGER)
    """, [storage_key, pipeline_id, task_id] + bin_ids)
    
    combined_subtasks = []
    
    for row in cursor.fetchall():
        bin_id, data_json = row
        try:
            bin_data = json.loads(data_json)
            if isinstance(bin_data, list):
                combined_subtasks.extend(bin_data)
            else:
                combined_subtasks.append(bin_data)
        except (json.JSONDecodeError, TypeError):
            # Handle case where data isn't valid JSON
            combined_subtasks.append(data_json)
    
    return combined_subtasks


def set_time_in_subitem_in_bin(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_id: str,
    index: int,
    time_field: str,
    timestamp: str,
    output_hash: Optional[str] = None
) -> None:
    """
    Update time information for a specific subtask within a bin.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_id: The bin ID containing the subtask
    :param index: The index of the subtask within the bin
    :param time_field: The time field to update ('startTime' or 'endTime')
    :param timestamp: The timestamp value
    :param output_hash: Optional output hash for completed subtasks
    """
    # First, retrieve the current bin data
    cursor = conn.execute("""
        SELECT data FROM subtask_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_id = ?
    """, (storage_key, pipeline_id, task_id, bin_id))
    
    row = cursor.fetchone()
    if not row:
        return  # Bin doesn't exist
    
    try:
        subtasks = json.loads(row[0])
        if not isinstance(subtasks, list) or index >= len(subtasks):
            return  # Invalid data structure or index out of bounds
        
        # Update the specified subtask
        subtasks[index][time_field] = timestamp
        if output_hash and time_field == 'endTime':
            subtasks[index]['outputHash'] = output_hash
        
        # Save the updated data back to the database
        updated_timestamp = datetime.datetime.now().isoformat()
        conn.execute("""
            UPDATE subtask_bins 
            SET data = ?, updated_at = ?
            WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_id = ?
        """, (
            json.dumps(subtasks),
            updated_timestamp,
            storage_key,
            pipeline_id,
            task_id,
            bin_id
        ))
        
        conn.commit()
    
    except (json.JSONDecodeError, TypeError, KeyError, IndexError):
        # Handle invalid JSON or data structure
        pass


def delete_subtask_bins(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_ids: List[str] = None
) -> None:
    """
    Delete subtask bins for a specific task.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_ids: Optional list of specific bin IDs to delete
    """
    if bin_ids:
        placeholders = ','.join(['?' for _ in bin_ids])
        conn.execute(f"""
            DELETE FROM subtask_bins
            WHERE storage_key = ? AND pipeline = ? AND task_id = ? 
            AND bin_id IN ({placeholders})
        """, [storage_key, pipeline_id, task_id] + bin_ids)
    else:
        conn.execute("""
            DELETE FROM subtask_bins
            WHERE storage_key = ? AND pipeline = ? AND task_id = ?
        """, (storage_key, pipeline_id, task_id))
    
    conn.commit()


def get_subtask_bin_ids(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str
) -> List[str]:
    """
    Get all bin IDs for subtasks of a specific task.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :return: List of bin IDs
    """
    cursor = conn.execute("""
        SELECT DISTINCT bin_id FROM subtask_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ?
        ORDER BY CAST(bin_id AS INTEGER)
    """, (storage_key, pipeline_id, task_id))
    
    return [row[0] for row in cursor.fetchall()]


def update_subtask_subset(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    subset_keys: List[str],
    reset_times: bool = True
) -> None:
    """
    Reset subtasks to only include those with keys in the subset.
    Used for subset operations where only specific subtasks should be processed.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param subset_keys: List of subtask keys to keep
    :param reset_times: Whether to reset start/end times
    """
    # Get all subtask bins
    cursor = conn.execute("""
        SELECT bin_id, data FROM subtask_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ?
        ORDER BY CAST(bin_id AS INTEGER)
    """, (storage_key, pipeline_id, task_id))
    
    updated_bins = []
    
    for row in cursor.fetchall():
        bin_id, data_json = row
        try:
            subtasks = json.loads(data_json)
            if isinstance(subtasks, list):
                # Filter subtasks to only include those in the subset
                filtered_subtasks = []
                for subtask in subtasks:
                    if subtask.get('key') in subset_keys:
                        if reset_times:
                            subtask['startTime'] = None
                            subtask['endTime'] = None
                            subtask['outputHash'] = None
                        filtered_subtasks.append(subtask)
                
                if filtered_subtasks:
                    updated_bins.append((bin_id, filtered_subtasks))
        
        except (json.JSONDecodeError, TypeError):
            continue
    
    # Delete all existing subtask bins for this task
    delete_subtask_bins(conn, storage_key, pipeline_id, task_id)
    
    # Re-create bins with filtered data
    timestamp = datetime.datetime.now().isoformat()
    for bin_id, subtasks in updated_bins:
        conn.execute("""
            INSERT INTO subtask_bins 
            (storage_key, pipeline, task_id, bin_id, data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            storage_key,
            pipeline_id,
            task_id,
            bin_id,
            json.dumps(subtasks),
            timestamp,
            timestamp
        ))
    
    conn.commit()
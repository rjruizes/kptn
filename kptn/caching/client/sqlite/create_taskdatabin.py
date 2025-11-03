import sqlite3
import json
import datetime
from typing import Any, List, Optional


def create_taskdatabin(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_name: str,
    bin_id: str,
    data: Any
) -> None:
    """
    Create a taskdata bin in the SQLite database.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_name: The bin type (e.g., 'TASKDATABIN', 'SUBSETBIN')
    :param bin_id: The bin ID (e.g., '0', '1', '2')
    :param data: The data to store (will be JSON serialized)
    """
    timestamp = datetime.datetime.now().isoformat()
    
    conn.execute("""
        INSERT OR REPLACE INTO taskdata_bins 
        (storage_key, pipeline, task_id, bin_type, bin_id, data, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        storage_key, 
        pipeline_id, 
        task_id, 
        bin_name, 
        bin_id, 
        json.dumps(data), 
        timestamp, 
        timestamp
    ))
    
    conn.commit()


def get_taskdatabins(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_ids: List[str],
    bin_name: str
) -> List[Any]:
    """
    Retrieve multiple taskdata bins and combine their data.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_ids: List of bin IDs to retrieve
    :param bin_name: The bin type to retrieve
    :return: Combined data from all bins
    """
    if not bin_ids:
        return []
    
    # Create placeholders for IN clause
    placeholders = ','.join(['?' for _ in bin_ids])
    
    cursor = conn.execute(f"""
        SELECT bin_id, data FROM taskdata_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ? 
        AND bin_type = ? AND bin_id IN ({placeholders})
        ORDER BY CAST(bin_id AS INTEGER)
    """, [storage_key, pipeline_id, task_id, bin_name] + bin_ids)
    
    combined_data = []
    
    for row in cursor.fetchall():
        bin_id, data_json = row
        try:
            bin_data = json.loads(data_json)
            if isinstance(bin_data, list):
                combined_data.extend(bin_data)
            else:
                combined_data.append(bin_data)
        except (json.JSONDecodeError, TypeError):
            # Handle case where data isn't valid JSON
            combined_data.append(data_json)
    
    # If there's only one bin and one item, and it was stored as a single item (not a list),
    # return the single item directly like DynamoDB implementation
    if len(bin_ids) == 1 and len(combined_data) == 1:
        # Check if the original data was stored as a single item by looking at the raw JSON
        cursor = conn.execute("""
            SELECT data FROM taskdata_bins
            WHERE storage_key = ? AND pipeline = ? AND task_id = ? 
            AND bin_type = ? AND bin_id = ?
        """, [storage_key, pipeline_id, task_id, bin_name, bin_ids[0]])
        
        row = cursor.fetchone()
        if row:
            try:
                original_data = json.loads(row[0])
                # If the original data was not a list, return the single item
                if not isinstance(original_data, list):
                    return combined_data[0]
            except (json.JSONDecodeError, TypeError):
                pass
    
    return combined_data


def get_single_taskdatabin(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_id: str,
    bin_name: str
) -> Optional[Any]:
    """
    Retrieve a single taskdata bin.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_id: The bin ID to retrieve
    :param bin_name: The bin type to retrieve
    :return: Data from the bin or None if not found
    """
    cursor = conn.execute("""
        SELECT data FROM taskdata_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ? 
        AND bin_type = ? AND bin_id = ?
    """, (storage_key, pipeline_id, task_id, bin_name, bin_id))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    data_json = row[0]
    try:
        return json.loads(data_json)
    except (json.JSONDecodeError, TypeError):
        return data_json


def delete_taskdata_bins(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_type: str,
    bin_ids: List[str] = None
) -> None:
    """
    Delete taskdata bins for a specific task and bin type.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_type: The bin type to delete
    :param bin_ids: Optional list of specific bin IDs to delete
    """
    if bin_ids:
        placeholders = ','.join(['?' for _ in bin_ids])
        conn.execute(f"""
            DELETE FROM taskdata_bins
            WHERE storage_key = ? AND pipeline = ? AND task_id = ? 
            AND bin_type = ? AND bin_id IN ({placeholders})
        """, [storage_key, pipeline_id, task_id, bin_type] + bin_ids)
    else:
        conn.execute("""
            DELETE FROM taskdata_bins
            WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_type = ?
        """, (storage_key, pipeline_id, task_id, bin_type))
    
    conn.commit()


def get_taskdata_bin_ids(
    conn: sqlite3.Connection,
    storage_key: str,
    pipeline_id: str,
    task_id: str,
    bin_type: str
) -> List[str]:
    """
    Get all bin IDs for a specific task and bin type.
    
    :param conn: SQLite connection
    :param storage_key: The branch or storage key
    :param pipeline_id: The pipeline ID
    :param task_id: The task ID
    :param bin_type: The bin type
    :return: List of bin IDs
    """
    cursor = conn.execute("""
        SELECT DISTINCT bin_id FROM taskdata_bins
        WHERE storage_key = ? AND pipeline = ? AND task_id = ? AND bin_type = ?
        ORDER BY CAST(bin_id AS INTEGER)
    """, (storage_key, pipeline_id, task_id, bin_type))
    
    return [row[0] for row in cursor.fetchall()]
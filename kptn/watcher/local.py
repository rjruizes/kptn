import ast
import os
from datetime import datetime
from pathlib import Path
from kptn.caching.Hasher import Hasher
from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.client.DbClientDDB import DbClientDDB
from kptn.deploy.authproxy_endpoint import authproxy_data
from kptn.deploy.get_active_branch_name import get_active_branch_name
from kptn.deploy.storage_key import read_branch_storage_key
from kptn.util.hash import hash_obj
from kptn.util.logger import get_logger
from kptn.util.pipeline_config import generateConfig, get_scratch_dir, get_storage_key
from kptn.util.read_tasks_config import all_tasks_configs_with_paths
from kptn.util.filepaths import test_r_tasks_dir, test_py_module_path, project_root
from kptn.util.rscript import r_script_log_path
from kptn.watcher.util import is_mock


def ddb_resp_to_item_types(resp) -> tuple[dict, dict, dict]:
    tasks = {}
    taskdata = {}
    subtasks = {}
    def is_task(task):
        return "TaskId" in task
    def is_taskdata(task):
        return "data" in task
    def is_subtask(task):
        return "items" in task
    def parse_taskdata_task_id(item):
        return item["SK"].split("#")[3]
    def parse_subtask_task_id(item):
        # 'SK': 'PIPELINE#srs#TASK#missing_months#SUBTASKBIN#0' -> 'missing_months'
        return item["SK"].split("#")[3]

    for item in resp:
        if is_task(item):
            tasks[item["TaskId"]] = item
        if is_taskdata(item):
            task_id = parse_taskdata_task_id(item)
            # Add as list if not already
            if task_id not in taskdata:
                taskdata[task_id] = [item]
            else:
                taskdata[task_id].append(item)
        if is_subtask(item):
            task_id = parse_subtask_task_id(item)
            if task_id not in subtasks:
                subtasks[task_id] = [item]
            else:
                subtasks[task_id].append(item)
    
    return tasks, taskdata, subtasks


def _normalize_code_hashes(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value
    if isinstance(value, list):
        normalized = []
        for element in value:
            if isinstance(element, dict) and len(element) == 1:
                inner = next(iter(element.values()))
            else:
                inner = element
            if isinstance(inner, str):
                try:
                    normalized.append(ast.literal_eval(inner))
                    continue
                except (ValueError, SyntaxError):
                    pass
            normalized.append(inner)
        return normalized
    return value


def _infer_code_kind(task: dict) -> str | None:
    if "r_script" in task:
        return "R"
    file_spec = task.get("file")
    if isinstance(file_spec, str):
        file_path = file_spec.split(":", 1)[0].strip()
        suffix = Path(file_path).suffix.lower()
        if suffix == ".sql":
            return "DuckDB SQL"
        if suffix in {".py", ".pyw"}:
            return "Python"
    if "py_script" in task:
        return "Python"
    return None

def setup_clients(stack: str, pipeline_name: str, tasks_conf, tasks_config_paths, authproxy_endpoint: str):
    branch = get_active_branch_name()
    possible_storage_key = read_branch_storage_key(branch)
    if is_mock(pipeline_name):
        r_tasks_dir = test_r_tasks_dir
        py_module_path = test_py_module_path
    else:
        r_tasks_dir = project_root
        py_module_path = "py_src.tasks"
    pipeline_config = generateConfig(
        pipeline_name,
        r_tasks_dir_path = r_tasks_dir,
        py_module_path = py_module_path,
        tasks_config_path = str(tasks_config_paths[0]) if tasks_config_paths else "",
        authproxy_endpoint = authproxy_endpoint,
        storage_key = possible_storage_key
    )
    if stack == "local":
        table = os.getenv("DYNAMODB_TABLE_NAME", "tasks")
        region = "local"
        aws_auth = { "endpoint_url": "http://dynamodb:8001" }
    elif stack:
        resp = authproxy_data(authproxy_endpoint)
        table = resp.headers["X-Aws-Dynamodb-Table"]
        region = resp.headers["X-Aws-Region"]
        resp_json = resp.json()
        aws_auth = {
            "aws_access_key_id": resp_json["AccessKeyId"],
            "aws_secret_access_key": resp_json["SecretAccessKey"],
            "aws_session_token": resp_json["Token"]
        }
    if stack:
        storage_key = get_storage_key(pipeline_config)
        cache_client = DbClientDDB(table_name=table, storage_key=storage_key, pipeline=pipeline_name, region=region, aws_auth = aws_auth)
    else:
        cache_client = -1
    tscache = TaskStateCache(pipeline_config, cache_client, tasks_conf, tasks_config_paths=tasks_config_paths)
    return tscache, cache_client, pipeline_config

def get_db_state_for_local(cache_client):
    logger = get_logger()
    resp = cache_client.get_tasks()
    logger.info(f"Items from DynamoDB {resp}")
    return ddb_resp_to_item_types(resp)


def get_duration(start_time: str, end_time: str) -> str:
    start = datetime.fromisoformat(start_time)
    end = datetime.fromisoformat(end_time)
    duration = end - start
    
    days = duration.days
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = duration.microseconds // 1000
    
    parts = []
    if days > 0:
        parts.append(f"{days} d")
    if hours > 0:
        parts.append(f"{hours} hr")
    if minutes > 0:
        parts.append(f"{minutes} min")
    if not hours and not minutes and seconds > 0:
        parts.append(f"{seconds}s")
    if not days and not hours and not minutes and not seconds and milliseconds > 0:
        parts.append("1s")

    
    return " ".join(parts)


def enrich_tasks(stack: str, graph: str, authproxy_endpoint: str = None):
    """Enrich the task data with additional information"""
    tasks_conf, tasks_config_paths = all_tasks_configs_with_paths()
    if not stack or not graph:
        return tasks_conf
    tscache, cache_client, pipeline_config = setup_clients(
        stack, graph, tasks_conf, tasks_config_paths, authproxy_endpoint
    )
    task_db_states, taskdata, subtasks = get_db_state_for_local(cache_client)
    tasks_dict = tasks_conf["tasks"]
    project_root_path = Path(project_root).resolve()
    base_dir_env = os.getenv("BASE_DIR")
    base_dir_override = Path(base_dir_env) if base_dir_env else None
    for task_name, task in tasks_dict.items():
        # First set YAML-based values
        resolved_path: Path | None = None
        if "r_script" in task:
            try:
                script_candidates, _ = tscache.hasher.get_full_r_script_paths(task_name, task["r_script"])
                if script_candidates:
                    resolved_path = script_candidates[0]
            except FileNotFoundError:
                resolved_path = None
        elif "py_script" in task:
            py_spec = task.get("py_script")
            py_filename = py_spec if isinstance(py_spec, str) else f"{task_name}.py"
            try:
                resolved_path = tscache.hasher.get_full_py_script_path(task_name, py_filename)
            except FileNotFoundError:
                resolved_path = None

        if resolved_path is None:
            raw_spec = task.get("r_script") or task.get("py_script")
            if isinstance(raw_spec, str):
                resolved_path = (project_root_path / raw_spec).resolve() if not Path(raw_spec).is_absolute() else Path(raw_spec)

        if resolved_path is not None:
            if base_dir_override:
                try:
                    relative_script = resolved_path.relative_to(project_root_path)
                    final_path = base_dir_override / relative_script
                except ValueError:
                    final_path = resolved_path
            else:
                final_path = resolved_path
            tasks_dict[task_name]["filepath"] = str(final_path)
        else:
            tasks_dict[task_name]["filepath"] = None
        
        # Now set cache-based values
        local_code_hashes, code_kind = tscache.build_task_code_hashes(task_name, task)
        if code_kind:
            tasks_dict[task_name]["code_kind"] = code_kind
        if local_code_hashes is not None:
            tasks_dict[task_name]["local_code_hashes"] = local_code_hashes
            tasks_dict[task_name]["local_code_version"] = hash_obj(local_code_hashes)

        if task_name in task_db_states:
            state = task_db_states[task_name]
            cached_code_hashes = _normalize_code_hashes(state.get("code_hashes"))
            if cached_code_hashes is not None:
                tasks_dict[task_name]["code_hashes"] = cached_code_hashes
                cached_version = state.get("code_version")
                if cached_version:
                    tasks_dict[task_name]["code_version"] = cached_version
                else:
                    tasks_dict[task_name]["code_version"] = hash_obj(cached_code_hashes)
            
            if "input_hashes" in state:
                tasks_dict[task_name]["cached_input_hashes"] = state["input_hashes"]
                tasks_dict[task_name]["cached_inputs_version"] = state["inputs_version"]
                dep_states = tscache.get_dep_states(task_name)
                tasks_dict[task_name]["live_input_hashes"] = tscache.get_input_hashes(task_name, dep_states)
                tasks_dict[task_name]["live_inputs_version"] = hash_obj(tasks_dict[task_name]["live_input_hashes"])

            if "outputs_version" in state:
                tasks_dict[task_name]["outputs_version"] = state["outputs_version"]
            
            if task_name in taskdata:
                tasks_dict[task_name]["data"] = taskdata[task_name][0]["data"]

            if "input_data_hashes" in state:
                tasks_dict[task_name]["cached_input_data_hashes"] = state["input_data_hashes"]
                tasks_dict[task_name]["cached_input_data_version"] = state["input_data_version"]
                tasks_dict[task_name]["live_input_data_hashes"] = tscache.get_data_hashes(task_name)
                tasks_dict[task_name]["live_input_data_version"] = hash_obj(tasks_dict[task_name]["live_input_data_hashes"])

            if task_name in subtasks:
                subtask_bins = subtasks[task_name]
                # For each subtask, get "i", "key", "startTime", "endTime"
                transformed_subtasks = []
                for subtask_bin in subtask_bins:
                    for subtask in subtask_bin["items"]:
                        subtask_data = {
                            "i": subtask["M"]["i"]["S"],
                            "key": subtask["M"]["key"]["S"],
                        }
                        if "startTime" in subtask["M"]:
                            subtask_data["startTime"] = subtask["M"]["startTime"]["S"]
                        if "endTime" in subtask["M"]:
                            subtask_data["endTime"] = subtask["M"]["endTime"]["S"]
                        transformed_subtasks.append(subtask_data)
                tasks_dict[task_name]["subtasks"] = transformed_subtasks

            if "r_script" in task and "start_time" in task_db_states[task_name]:
                custom_log_path = tscache.get_custom_log_path(task_name)
                _, relative_path = r_script_log_path(task_name, pipeline_config, custom_log_path)
                rel_url = relative_path.parent if "map_over" in task else relative_path
                if stack == "local":
                    storage_key = get_storage_key(pipeline_config)
                    rel_url = f"vscode://file/{os.getenv('BASE_DIR')}/scratch/{storage_key}/{rel_url}"
                    if "map_over" in task:
                        rel_url = f"{rel_url}?windowId=_blank" # Open in new window rather than killing the current one
                else:
                    scratch_dir = get_scratch_dir(pipeline_config)
                    rel_url = f"{authproxy_endpoint.replace('/authproxy', '/efs')}/{scratch_dir}/{rel_url}"
                tasks_dict[task_name]["log_path"] = rel_url

            start = task_db_states[task_name]["start_time"] if "start_time" in task_db_states[task_name] else None
            end = task_db_states[task_name]["end_time"] if "end_time" in task_db_states[task_name] else None
            if start and end:
                duration = get_duration(start, end)
                tasks_dict[task_name]["duration"] = duration
                print(f"Task {task_name} took {duration}")
    tasks_conf["tasks"] = tasks_dict
    return tasks_conf

def hash_code_for_tasks(task_names: list[str]):
    tasks_conf, tasks_config_paths = all_tasks_configs_with_paths()
    hasher = Hasher(
        py_dirs=[
            "py_src/tasks",
            "tests/mock_pipeline/py_tasks"
        ],
        r_dirs=[".", test_r_tasks_dir],
        tasks_config=tasks_conf,
        tasks_config_paths=[str(path) for path in tasks_config_paths],
    )
    updates = []
    for task_name in task_names:
        if task_name in tasks_conf["tasks"]:
            update = { "task_name": task_name }
            task = tasks_conf["tasks"][task_name]
            try:
                local_code_hashes = hasher.hash_code_for_task(task_name)
            except Exception:
                local_code_hashes = None
            if local_code_hashes is not None:
                update["local_code_hashes"] = local_code_hashes
                update["local_code_version"] = hash_obj(local_code_hashes)
            code_kind = _infer_code_kind(task)
            if code_kind:
                update["code_kind"] = code_kind
            updates.append(update)
    return updates

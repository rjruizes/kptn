import os
from datetime import datetime
from pathlib import Path
from kapten.caching.Hasher import Hasher
from kapten.caching.TaskStateCache import TaskStateCache
from kapten.caching.client.DbClientDDB import DbClientDDB
from kapten.deploy.authproxy_endpoint import authproxy_data
from kapten.deploy.get_active_branch_name import get_active_branch_name
from kapten.deploy.storage_key import read_branch_storage_key
from kapten.util.hash import hash_obj
from kapten.util.logger import get_logger
from kapten.util.pipeline_config import generateConfig, get_scratch_dir, get_storage_key
from kapten.util.read_tasks_config import all_tasks_configs
from kapten.util.filepaths import test_r_tasks_dir, test_py_module_path, project_root
from kapten.util.rscript import r_script_log_path
from kapten.watcher.util import is_mock


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

def setup_clients(stack: str, pipeline_name: str, tasks_conf, authproxy_endpoint: str):
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
    tscache = TaskStateCache(pipeline_config, cache_client, tasks_conf)
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
    tasks_conf = all_tasks_configs()
    if not stack or not graph:
        return tasks_conf
    tscache, cache_client, pipeline_config = setup_clients(stack, graph, tasks_conf, authproxy_endpoint)
    task_db_states, taskdata, subtasks = get_db_state_for_local(cache_client)
    tasks_dict = tasks_conf["tasks"]
    for task_name, task in tasks_dict.items():
        # First set YAML-based values
        if is_mock(graph):
            scriptpath = Path("r_tasks") / task["r_script"] if "r_script" in task else Path("py_tasks") / f"{task_name}.py"
            fpath = Path(os.getenv("BASE_DIR")) / "tests" / "mock_pipeline" / scriptpath
        else:
            scriptpath = task["r_script"] if "r_script" in task else Path("py_src") / "tasks" / f"{task_name}.py"
            fpath = Path(os.getenv("BASE_DIR")) / scriptpath
        tasks_dict[task_name]["filepath"] = str(fpath)
        
        # Now set cache-based values
        if task_name in task_db_states:
            if "r_code_hashes" in task_db_states[task_name]:
                tasks_dict[task_name]["r_code_hashes"] = task_db_states[task_name]["r_code_hashes"]
                tasks_dict[task_name]["r_code_version"] = task_db_states[task_name]["r_code_version"]
                tasks_dict[task_name]["local_r_code_hashes"] = tscache.hasher.build_r_code_hashes(task_name, task)
                tasks_dict[task_name]["local_r_code_version"] = hash_obj(tasks_dict[task_name]["local_r_code_hashes"])
            elif "py_code_hashes" in task_db_states[task_name]:
                tasks_dict[task_name]["py_code_hashes"] = task_db_states[task_name]["py_code_hashes"]
                tasks_dict[task_name]["py_code_version"] = task_db_states[task_name]["py_code_version"]
                tasks_dict[task_name]["local_py_code_hashes"] = tscache.hasher.build_py_code_hashes(task_name, task)
                tasks_dict[task_name]["local_py_code_version"] = hash_obj(tasks_dict[task_name]["local_py_code_hashes"])
            
            if "input_hashes" in task_db_states[task_name]:
                tasks_dict[task_name]["cached_input_hashes"] = task_db_states[task_name]["input_hashes"]
                tasks_dict[task_name]["cached_inputs_version"] = task_db_states[task_name]["inputs_version"]
                dep_states = tscache.get_dep_states(task_name)
                tasks_dict[task_name]["live_input_hashes"] = tscache.get_input_hashes(task_name, dep_states)
                tasks_dict[task_name]["live_inputs_version"] = hash_obj(tasks_dict[task_name]["live_input_hashes"])

            if "outputs_version" in task_db_states[task_name]:
                tasks_dict[task_name]["outputs_version"] = task_db_states[task_name]["outputs_version"]
            
            if task_name in taskdata:
                tasks_dict[task_name]["data"] = taskdata[task_name][0]["data"]

            if "input_data_hashes" in task_db_states[task_name]:
                tasks_dict[task_name]["cached_input_data_hashes"] = task_db_states[task_name]["input_data_hashes"]
                tasks_dict[task_name]["cached_input_data_version"] = task_db_states[task_name]["input_data_version"]
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
    tasks_conf = all_tasks_configs()
    hasher = Hasher(
        py_dirs=[
            "py_src/tasks",
            "tests/mock_pipeline/py_tasks"
        ],
        r_dirs=[".", test_r_tasks_dir],
        tasks_config=tasks_conf
    )
    updates = []
    for task_name in task_names:
        if task_name in tasks_conf["tasks"]:
            update = { "task_name": task_name }
            task = tasks_conf["tasks"][task_name]
            if "r_script" in task:
                update["local_r_code_hashes"] = hasher.build_r_code_hashes(task_name, task)
                update["local_r_code_version"] = hash_obj(update["local_r_code_hashes"])
            elif "py_script" in task:
                update["local_py_code_hashes"] = hasher.build_py_code_hashes(task_name, task)
                update["local_py_code_version"] = hash_obj(update["local_py_code_hashes"])
            updates.append(update)
    return updates
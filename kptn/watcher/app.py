import asyncio
from contextlib import asynccontextmanager
import importlib
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from kptn.caching.TaskStateCache import run_task
from kptn.codegen.codegen import generate_files
from kptn.deploy.ecr_image import get_full_image_uri
from kptn.deploy.prefect_deploy import local_deploy, prefect_deploy, run_deploy
from kptn.deploy.storage_key import read_branch_storage_key
from kptn.read_config import read_config
from kptn.util.pipeline_config import PipelineConfig, get_storage_key
from kptn.deploy.get_active_branch_name import get_active_branch_name
from kptn.watcher.local import enrich_tasks, hash_code_for_tasks
from kptn.watcher.stacks import read_stacks_list, stack_to_authproxy, stack_to_prefect_api, stacks_to_dict
from kptn.watcher.util import is_mock


logger = logging.getLogger('uvicorn.error')

state = {}
manager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global state, manager
    branch = get_active_branch_name()
    storage_key = read_branch_storage_key(branch)
    state["branch"] = branch
    state["storage_key"] = storage_key
    # state["tasks"] = enrich_tasks()
    state["stacks"] = read_stacks_list()
    state["stackDict"] = stacks_to_dict(state["stacks"])
    manager = ConnectionManager()
    yield
    state.clear()
    await shutdown_handler()

app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
"""
A simple file watcher that sends events to a WebSocket client.
"""

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.shutdown_event = asyncio.Event()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")

    async def close_all_connections(self):
        logger.info("Closing all WebSocket connections...")
        for connection in self.active_connections[:]:  # Create a copy of the list to iterate over
            try:
                await connection.close(code=1000, reason="Server shutdown")
                self.disconnect(connection)
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

@app.get("/api/state")
def get_conf(stack: str, graph: str):
    authproxy_endpoint = stack_to_authproxy(stack, state["stackDict"]) if stack else None
    state["tasks"] = enrich_tasks(stack, graph, authproxy_endpoint)
    return state


class DeployPayload(BaseModel):
    branch: str
    stack: str
    graph: str

class RunPayload(BaseModel):
    branch: str
    stack: str
    deployment: str
    graph: str
    tasks: list[str]
    ignore_cache: bool = False

class CodeChangePayload(BaseModel):
    file: str
    updated_tasks: list[str]

@app.post("/api/deploy")
def api_deploy(payload: DeployPayload):
    """
    Calls Prefect API to create/update a Deployment
    """
    branch = payload.branch
    possible_storage_key = read_branch_storage_key(branch)
    if payload.stack != "local":
        image = get_full_image_uri(branch, stack_to_authproxy(payload.stack, state["stackDict"]))
    else:
        kap_conf = read_config()
        image = kap_conf["docker_image"]
    if is_mock(payload.graph):
        py_tasks_module_path = "tests.mock_pipeline.py_tasks"
        tasks_config_path = "tests/mock_pipeline/kptn.yaml"
        r_tasks_dir_path = "/code/tests/mock_pipeline/r_tasks"
        module_path = f"tests.mock_pipeline.flows.{payload.graph}"
        module = importlib.import_module(module_path)
        flow = getattr(module, payload.graph)
    else:
        generate_files(payload.graph) # codegen
        py_tasks_module_path = "py_src.tasks"
        tasks_config_path = "/code/py_src/kptn.yaml"
        r_tasks_dir_path = "/code"
        module_path = f"py_src.flows.{payload.graph}"
        module = importlib.import_module(module_path)
        flow = getattr(module, payload.graph)
    pipeline_config = PipelineConfig(
        IMAGE=image,
        BRANCH=branch,
        PIPELINE_NAME=payload.graph,
        PY_MODULE_PATH=py_tasks_module_path,
        TASKS_CONFIG_PATH=tasks_config_path,
        R_TASKS_DIRS=(r_tasks_dir_path,) if r_tasks_dir_path else (),
        STORAGE_KEY=possible_storage_key,
    )
    storage_key = get_storage_key(pipeline_config)
    graph_deployment_name = f"{pipeline_config.PIPELINE_NAME}-{storage_key}"
    task_deployment_name = f"{pipeline_config.PIPELINE_NAME}-RunTask-{storage_key}"
    parameters = {
        "pipeline_config": pipeline_config.model_dump(),
    }
    if payload.stack == "local":
        run_graph_id = local_deploy(flow, graph_deployment_name, parameters)
        run_task_id = local_deploy(run_task, task_deployment_name, parameters)
    else:
        print("Deploying image:", image)
        job_variables = {
            "image": image,
        }
        url = stack_to_prefect_api(payload.stack, state["stackDict"])
        run_graph_id = prefect_deploy(flow, graph_deployment_name, url, job_variables, parameters, work_pool_name="ecs-pool", image=image)
        run_task_id = prefect_deploy(run_task, task_deployment_name, url, job_variables, parameters, work_pool_name="ecs-pool", image=image)
    return {"graph_deployment": run_graph_id, "task_deployment": run_task_id}

@app.post("/api/run")
def run_deployment(payload: RunPayload):
    """
    Calls Prefect API to run a Deployment
    """
    image = get_full_image_uri(payload.branch, stack_to_authproxy(payload.stack, state["stackDict"]))
    flow_run_id = run_deploy(image, stack_to_prefect_api(payload.stack, state["stackDict"]), payload.deployment, payload.graph, payload.tasks, payload.ignore_cache)
    return {"flow_run_id": flow_run_id}

@app.post("/api/codechange")
async def code_change(payload: CodeChangePayload):
    """
    Called when code changes are detected; hashes updated tasks and broadcasts to WebSocket clients
    """
    logger.info(f"Code change detected: {payload.file}")
    updates = hash_code_for_tasks(payload.updated_tasks)
    print(updates)
    await manager.broadcast({ "updateType": "code_change", "data": updates })
    return {"status": "success"}
 
@app.websocket("/ws") 
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try: 
        while True:
            data = await websocket.receive_text()
            # await manager.broadcast(f"Client says: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)
    finally:
        if websocket in manager.active_connections:
            manager.disconnect(websocket)

async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Initiating shutdown...")
    manager.shutdown_event.set()
    await manager.close_all_connections()
    logger.info("Shutdown complete")

def start():
    """Launched with `uv run dev` at root level"""
    uvicorn.run(
        "kptn.watcher.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_delay=0.5,
        reload_excludes=["py_src/tasks/__init__.py", "py_src/tasks/*.py", "tests/mock_pipeline/py_tasks/*.py"],
    )

if __name__ == "__main__":
    start()

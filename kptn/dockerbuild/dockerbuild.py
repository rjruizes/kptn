from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import subprocess
from pydantic import BaseModel

from kptn.deploy.push import docker_push
from kptn.read_config import read_config

app = FastAPI()

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

class PushPayload(BaseModel):
    authproxy_url: str
    branch: str

@app.post("/api/build")
def post_build():
    kap_conf = read_config()
    image = kap_conf["docker_image"]
    subprocess.run(["docker", "build", "-t", image, "."])
    return {"message": "Build complete"}

@app.post("/api/push")
def post_push(payload: PushPayload):
    docker_push(payload.authproxy_url, payload.branch)
    return {"message": "Push complete"}

def docker_api_server():
    uvicorn.run(app, host="localhost", port=8002)

if __name__ == "__main__":
    docker_api_server()

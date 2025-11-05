from datetime import datetime
import os
from prefect.settings import temporary_settings, PREFECT_API_URL
from prefect.deployments import run_deployment

from kptn.read_config import read_config

def prefect_deploy(flow, flow_name, public_url, job_variables, parameters={}, work_pool_name="docker-pool", image=None):
    if not image:
        kap_conf = read_config()
        image = kap_conf["docker_image"]
    settings = { PREFECT_API_URL: public_url }
    with temporary_settings(updates=settings):
        print(PREFECT_API_URL.value())
        return flow.deploy(
            name=flow_name,
            work_pool_name=work_pool_name,
            image=image,
            parameters=parameters,
            job_variables=job_variables,
            build=False,
            push=False,
        )

def local_deploy(flow, flow_name, parameters={}):
    public_url = "http://0.0.0.0:4200/api"
    private_url = "http://prefect_server:4200/api"
    job_variables = {
        "env": {
            "PREFECT_API_URL": private_url,
            "LOCAL_DYNAMODB": "true",
            "AWS_REGION": "local",
            # Fake creds mandatory https://github.com/aws/aws-cli/issues/5531#issuecomment-724814668
            # "AWS_ACCESS_KEY_ID": "local",
            # "AWS_SECRET_ACCESS_KEY": "local",
            "EXTERNAL_STORE": "s3://nibrs-estimation-externals",
            "ARTIFACT_STORE": "/code/artifacts",
            "AWS_PROFILE": "nibrs",
            "AWS_SHARED_CREDENTIALS_FILE": "/root/.aws/credentials",
            "PGHOST": os.getenv("PGHOST"),
            "PGPORT": os.getenv("PGPORT"),
            "PGPASSWORD": os.getenv("PGPASSWORD"),
            "PGUSER": os.getenv("PGUSER"),
            "PGDATABASE": os.getenv("PGDATABASE"),
        }
    }
    os.environ['AWS_REGION'] = "local"
    return prefect_deploy(flow, flow_name, private_url, job_variables, parameters)

def run_deploy(image: str, prefect_api_url: str, deployment_name: str, graph: str, task_names: list[str], ignore_cache: bool):
    """
    Given a prefect API URL, deployment name, and task name, runs the deployment.
    """
    settings = { PREFECT_API_URL: prefect_api_url }
    with temporary_settings(updates=settings):
        print(PREFECT_API_URL.value())
        # Disabling RunTask flow (it's faster, but it bypasses the cache check)
        # if len(task_names) == 1:
        #     flow_run_name = f"{task_names[0]}-{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}"
        #     parameters = {
        #         "task_name": task_names[0],
        #     }
        # else:
        if len(task_names) == 0:
            flow_run_name = f"full-{graph}-{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}"
        else:
            flow_run_name = f"partial-{graph}-{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}"
        parameters = {
            "task_list": task_names,
            "ignore_cache": ignore_cache,
        }
        result = run_deployment(
            name=deployment_name,
            flow_run_name=flow_run_name,
            as_subflow=False,
            parameters=parameters,
            timeout=0,
            job_variables={ "image": image },
        )
        return result.id

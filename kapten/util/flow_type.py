import os

def is_flow_prefect() -> bool:
    return "PREFECT_API_URL" in os.environ

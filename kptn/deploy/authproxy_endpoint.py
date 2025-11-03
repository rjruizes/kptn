import os
from pathlib import Path
import requests
from kptn.util.filepaths import project_root

def get_authproxy_endpoint():
    import dotenv
    print("Loading", Path(project_root) / ".env")
    dotenv.load_dotenv(Path(project_root) / ".env")
    endpoint = os.getenv("AUTHPROXY_ENDPOINT")
    return endpoint

def authproxy_data(authproxy_url=None) -> requests.Response:
    authproxy_endpoint = authproxy_url or get_authproxy_endpoint()
    # Do a simple HEAD request with a short timeout to check if authproxy is up
    try:
        requests.head(url=authproxy_endpoint, timeout=3)
    except requests.exceptions.RequestException:
        raise
    response = requests.get(url=authproxy_endpoint)
    return response
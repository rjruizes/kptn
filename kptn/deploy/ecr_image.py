import requests
from kptn.deploy.authproxy_endpoint import get_authproxy_endpoint
from kptn.deploy.get_active_branch_name import get_active_branch_name
from kptn.read_config import read_config

TIMEOUT = 3

def get_ecr_repo(authproxy_endpoint=None):
    """Fetch the flow runner repo name from the authproxy endpoint"""
    if not authproxy_endpoint:
        authproxy_endpoint = get_authproxy_endpoint()
    # Do a simple HEAD request w/ a short timeout to check if authproxy is up
    try:
        requests.head(url=authproxy_endpoint, timeout=TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise
    response = requests.get(url=authproxy_endpoint)
    return response.headers["X-Aws-Flow-Runner-Repo"]

def get_full_image_uri(branch: str, authproxy_endpoint):
    """Return the full image name"""
    if authproxy_endpoint:
        repo = get_ecr_repo(authproxy_endpoint)
    else:
        kap_conf = read_config()
        image = kap_conf["docker_image"]
        return image
    return f"{repo}:{branch}"

def get_full_image_and_branch(authproxy_endpoint=None):
    """Return the full image name and the branch"""
    repo = get_ecr_repo(authproxy_endpoint)
    branch = get_active_branch_name()
    return (f"{repo}:{branch}", branch)


if __name__ == "__main__":
    image, branch = get_full_image_and_branch()
    print(image)
    print(branch)

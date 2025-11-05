import base64
import logging
import requests
import subprocess
from kptn.deploy.authproxy_endpoint import get_authproxy_endpoint
from kptn.deploy.ecr_image import get_full_image_uri
from kptn.deploy.get_active_branch_name import get_active_branch_name
from kptn.read_config import read_config

logger = logging.getLogger(__name__)


def docker_push(authproxy_url=None, branch=None, image=None):
    import boto3
    if not image:
        kap_conf = read_config()
        image = kap_conf["docker_image"]
    authproxy_endpoint = authproxy_url or get_authproxy_endpoint()
    branch = branch or get_active_branch_name()
    full_image_uri = get_full_image_uri(branch, authproxy_endpoint)

    # retrieve AWS creds
    resp = requests.get(url=authproxy_endpoint)
    resp_json = resp.json()
    region = resp.headers.get("X-AWS-Region")
    access_key_id = resp_json.get("AccessKeyId")
    secret_access_key = resp_json.get("SecretAccessKey")
    token = resp_json.get("Token")
    # Use boto3 to get ECR token
    client = boto3.client(
        "ecr",
        region_name=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=token,
    )
    resp = client.get_authorization_token()
    decoded_token = base64.b64decode(resp["authorizationData"][0]["authorizationToken"])
    pw = decoded_token.decode().split(":")[1]
    registry = resp["authorizationData"][0]["proxyEndpoint"].replace("https://", "")
    # Construct docker login, tag, and push commands
    login_cmd = f"docker login --username AWS -p {pw} {registry}"
    tag_cmd = f"docker tag {image} {full_image_uri}"
    push_cmd = f"docker push {full_image_uri}"
    # Use subprocess to docker login w/ AWS creds and push image
    subprocess.run(f"{login_cmd}; {tag_cmd}; {push_cmd}", shell=True)
    return full_image_uri, branch, authproxy_endpoint


if __name__ == "__main__":
    docker_push()

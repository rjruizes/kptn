import os
import logging
import requests
import prefect.settings

logger = logging.getLogger(__name__)

"""
This module contains functions to get AWS credentials from the Prefect API URL.
"""

def get_prefect_api_url():
    """Get the Prefect API URL from the environment or settings."""
    url = os.environ.get("PREFECT_API_URL")
    if url is None:
        url = prefect.settings.PREFECT_API_URL.value()
    if url is None:
        print("\nError: PREFECT_API_URL not set")
        print("Please set the environment variable or use a Prefect profile.\n")
        print("Example: PREFECT_API_URL=http://3.123.456.78:4200/api\n")
        exit(1)
    logger.debug(f"PREFECT_API_URL: {url}")
    return url

def get_ip_from_prefect_url(url):
    """Get the IP address from the URL."""
    ip = url.split("//")[1].split(":")[0]
    logger.debug(f"IP: {ip}")
    return ip

def get_creds(ip, port=8080):
    """Get credentials and bucket name from the NGINX server."""
    resp = requests.get(f"http://{ip}:{port}")
    resp_json = resp.json()
    resp_json['ExternalsBucket'] = resp.headers.get("X-AWS-Externals-Bucket")
    resp_json['ArtifactsBucket'] = resp.headers.get("X-AWS-Artifacts-Bucket")
    
    id = resp_json.get("AccessKeyId")
    logger.debug(f"AccessKeyId: {id}")
    return resp_json

def get_aws_creds_from_prefect_url():
    """Get AWS credentials from the Prefect API URL."""
    url = get_prefect_api_url()
    ip = get_ip_from_prefect_url(url)
    return get_creds(ip)
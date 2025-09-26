import tomllib
import yaml
import os

def read_config():
    # Only allow kapten.yaml as the configuration file
    config_file = "kapten.yaml"
    
    if os.path.exists(config_file):
        with open(config_file) as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    
    # If no config file is found, raise an error
    raise FileNotFoundError("No configuration file found. Please ensure kapten.yaml exists")
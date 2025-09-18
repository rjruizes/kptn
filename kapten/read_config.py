import tomllib
import yaml
import os

def read_config():
    # Search for config files in order of preference
    config_files = ["kapten.yaml", "kapten.yml", "pyproject.toml"]
    
    for config_file in config_files:
        if os.path.exists(config_file):
            if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                with open(config_file) as f:
                    return yaml.load(f, Loader=yaml.FullLoader)
            elif config_file.endswith('.toml'):
                with open(config_file, "rb") as f:
                    config = tomllib.load(f)
                    if config_file == "pyproject.toml":
                        kapten_config = config["tool"]["kapten"]
                        return kapten_config
                    else:
                        return config
    
    # If no config file is found, raise an error
    raise FileNotFoundError("No configuration file found. Please ensure one of the following files exists: kapten.yaml, kapten.yml, or pyproject.toml")
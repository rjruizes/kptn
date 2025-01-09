import tomllib

def read_config():
    with open("pyproject.toml", "rb") as f:
        config = tomllib.load(f)
        kapten_config = config["tool"]["kapten"]
        return kapten_config
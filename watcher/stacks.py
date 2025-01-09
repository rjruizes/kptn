import json

def read_stacks_list():
    with open("stacks.json", "r") as f:
        stacks_json = json.load(f)
    return stacks_json

def stacks_to_dict(stacks):
    return {stack["name"]: stack for stack in stacks}

def stack_to_prefect_api(stack: str, stackDict = None):
    if not stackDict:
        stackDict = stacks_to_dict(read_stacks_list())
    return stackDict[stack]["prefect_api"]

def stack_to_authproxy(stack: str, stackDict):
    if stack == "local":
        return None
    return f'{stackDict[stack]["prefect_web_url"]}/authproxy'

def get_stack_endpoints(stack_name: str):
    stacks = read_stacks_list()
    stack = [stack for stack in stacks if stack["name"] == stack_name][0]
    authproxy_url = f'{stack["prefect_web_url"]}/authproxy'
    return stack["prefect_api"], authproxy_url

interface DeployPayload {
  branch: string;
  stack: string;
  graph: string;
  tasks: string[];
  ignore_cache: boolean;
  endpoint: string;
}
interface DeployResponse {
  graph_deployment: string;
  task_deployment: string;
}
interface RunResponse {
  flow_run_id: string;
}

interface TaskUpdate {
  task_name: string;
  local_code_hashes?: Record<string, string>[];
  local_code_version?: string;
  code_kind?: string;
}

type Graph = {
  tasks: Record<string, string[]>
}

type TaskConf = {
  graphs: Record<string, Graph>
  tasks: Record<string, any>
}
type Stack = {
  name: string,
  url: string,
}
type State = {
  branch: string,
  storage_key: string|null,
  tasks: TaskConf,
  stacks: Stack[]
}
type StateParams = {
  graph: string,
  stack: string,
}

type StateContextType = {
  state: State | null,
  setState: (state: State) => void,
  updateTask: (partialUpdate: any) => void,
}

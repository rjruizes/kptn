import { mande } from 'mande'
const deployApi = mande('http://localhost:8000/api/deploy')
const buildApi = mande('http://localhost:8002/api/build')
const pushApi = mande('http://localhost:8002/api/push')
const runApi = mande('http://localhost:8000/api/run')

function urlJoin(baseUrl: string, path: string) {
  // Remove trailing slashes from base URL
  baseUrl = baseUrl.replace(/\/+$/, '');
  // Remove leading slashes from path
  path = path.replace(/^\/+/, '');
  // Handle empty path
  if (!path) {
    return baseUrl;
  }
  // Join with single slash
  return `${baseUrl}/${path}`;
}

async function buildAndDeploy (payload: DeployPayload): Promise<[DeployResponse, any, any]> {
  // Push if payload.stack is not 'local'
  return Promise.all([
    deployApi.post({ ...payload }) as Promise<DeployResponse>,
    buildApi.post().then((response: any) => {
      return (payload.stack === 'local' ? Promise.resolve() : pushApi.post({ branch: payload.branch, authproxy_url: urlJoin(payload.endpoint, 'authproxy') }))
    })
  ])
}

export async function buildAndRun (payload: DeployPayload) {
  return buildAndDeploy(payload).then(([deployResult, _, __]: [DeployResponse, any, any]) => {
    console.log('Deploy/Build/Push requests have finished', deployResult);
    // const deploymentType = payload.tasks.length === 0 || payload.tasks.length > 1 ? 'graph_deployment' : 'task_deployment';
    // For now, disabling task deployment because it bypasses the cache check and it's confusing UI to not skip the cache check without a warning
    const deploymentType = 'graph_deployment';
    (runApi.post({ ...payload, deployment: deployResult[deploymentType] }) as Promise<RunResponse>).then((runResult: RunResponse) => {
      const url = `${payload.endpoint}/flow-runs/flow-run/${runResult.flow_run_id}`
      // Open the flow run in a new tab
      window.open(url, '_blank')
    })
  }).catch(error => {
    console.error('An error occurred:', error);
  });
}
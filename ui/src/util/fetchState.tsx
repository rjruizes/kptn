import { useStateStore } from '@/hooks/use-state'
import { mande } from 'mande'

const api = mande('http://localhost:8000/api/state')

export async function fetchState({ graph, stack }: StateParams) {
  const data: State = await api.get({ query: { graph: graph || '', stack: stack || '' } })
  useStateStore.setState(data)
  return data
}
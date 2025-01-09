import { create } from 'zustand'

type Graph = {
  tasks: Record<string, string[]>
}

type TaskConf = {
  graphs: Record<string, Graph>
  tasks: Record<string, any>
}
type Stack = {
  name: string,
  prefect_web_url: string,
}
interface AppState {
  branch: string,
  storage_key: string|null,
  tasks: TaskConf,
  stacks: Stack[],
  stackDict: Record<string, Stack>,
  setState: (state: AppState) => void,
  updateTask: (partialUpdate: TaskUpdate[]) => void,
}

export const useStateStore = create<AppState>()((set) => ({
  branch: '',
  storage_key: null,
  tasks: { graphs: {}, tasks: {} },
  stacks: [],
  stackDict: {},
  setState: (state) => set(() => (state)),
  updateTask: (partialUpdate: TaskUpdate[]) => {
    set((prevState) => {
      const newState = {
        ...prevState,
        tasks: {
          ...prevState.tasks,
          tasks: {
            ...prevState.tasks.tasks,
            ...(Object.fromEntries(
            partialUpdate.map(update => [
              update.task_name,
              { ...prevState.tasks.tasks[update.task_name], ...update }
            ])
          ))}
        }
      }
      console.log("partialUpdate", partialUpdate, prevState, newState)
      return (newState)});
  }
}))

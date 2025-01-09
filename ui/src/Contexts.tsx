import { createContext, useState } from 'react';

export const StateContext = createContext<StateContextType | null>(null);

const StateProvider: React.FC<{ children: React.ReactNode, initialState: State }> = ({ children, initialState }) => {
  const [state, setState] = useState(initialState);
  const updateTask = (partialUpdate: TaskUpdate[]) => {
    setState((prevState) => {
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
  return <StateContext.Provider value={{ state, setState, updateTask }}>{children}</StateContext.Provider>;
}

export default StateProvider;
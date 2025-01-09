import { create } from 'zustand'

interface RowState {
  rows: string[]
  setRows: (rows: string[]) => void
}

export const useRowStore = create<RowState>()((set) => ({
  rows: [],
  setRows: (rows) => set((state) => ({ rows })),
}))

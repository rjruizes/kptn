import { create } from 'zustand'

interface IgnoreState {
  ignore: boolean
  setIgnore: (ignore: boolean) => void
}

export const useIgnoreCacheStore = create<IgnoreState>()((set) => ({
  ignore: false,
  setIgnore: (ignore) => set((state) => ({ ignore })),
}))

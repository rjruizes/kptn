import { fetchState } from '@/util/fetchState'
import { createRootRoute, Outlet } from '@tanstack/react-router'
import { TanStackRouterDevtools } from '@tanstack/router-devtools'
import { z } from 'zod'
import Layout from '@/layout'
import { ThemeProvider } from '@/components/theme-provider'

export const Route = createRootRoute({
  component: RootComponent,
  loader: ({ deps }) => fetchState(deps),
  loaderDeps: ({ search: { stack, graph } }) => ({ stack, graph }),
  // staleTime: 2000, //60000, // cache for 1 minute
  validateSearch: z.object({
    branch: z.string().optional(),
    stack: z.string().optional(),
    graph: z.string().optional(),
  }),
})

function RootComponent() {
  return (
    <>
      <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
        <Layout>
          <Outlet />
          <TanStackRouterDevtools position="bottom-right" />
        </Layout>
      </ThemeProvider>
    </>
  )
}
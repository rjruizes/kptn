import { Table } from '@/components/Table'
import { createLazyFileRoute, getRouteApi } from '@tanstack/react-router'

export const Route = createLazyFileRoute('/')({
  component: Index,
})

function Index() {
  const routeApi = getRouteApi('/')
  const routeSearch = routeApi.useSearch()
  
  return (
    <div className="w-full h-full">
      {(!routeSearch.stack && !routeSearch.graph) ? "No stack or graph selected" : (routeSearch.graph ? (
        routeSearch.stack ? (
          <Table />
        ) : (
          "No stack selected"
        )
      ) : (
        "No graph selected"
      ))}
    </div>
  )
}
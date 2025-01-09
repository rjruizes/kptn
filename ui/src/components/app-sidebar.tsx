import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import { getRouteApi, Link } from '@tanstack/react-router'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faCodeBranch, faLayerGroup, faDiagramProject, faDatabase } from '@fortawesome/free-solid-svg-icons'
import { ThemeToggle } from "./theme-toggle"
import RunTasksBtn from "./RunTasksBtn"
import { Switch } from "@/components/ui/switch"
import { useStateStore } from "@/hooks/use-state"
import { useIgnoreCacheStore } from "@/hooks/use-ignorecache"

export function AppSidebar() {
  const state = useStateStore()
  const routeApi = getRouteApi('/')
  const routeSearch = routeApi.useSearch()
  const setIgnore = useIgnoreCacheStore(store => store.setIgnore)
  return (
    <Sidebar>
      <SidebarContent>
        <SidebarGroup>
          <ThemeToggle />
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem key={'branch'}>
                <SidebarGroupLabel>Branch</SidebarGroupLabel>
                <SidebarMenuButton disabled className="!opacity-100">
                  <FontAwesomeIcon icon={faCodeBranch} />
                  <span>{state?.branch}</span>
                </SidebarMenuButton>
              </SidebarMenuItem>

              {state?.storage_key && (
                <SidebarMenuItem key={'storage_key'}>
                  <SidebarGroupLabel>Storage Key</SidebarGroupLabel>
                  <SidebarMenuButton isActive={true} disabled className="!opacity-100">
                    <FontAwesomeIcon icon={faDatabase} />
                    <span>{state?.storage_key}</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              )}

              <SidebarGroupLabel>Stack</SidebarGroupLabel>
              {state?.stacks.map((stack) => (
                <SidebarMenuItem key={stack.name}>
                  <SidebarMenuButton asChild isActive={routeSearch.stack === stack.name}>
                    <Link from='/' search={(prev) => ({ ...prev, stack: stack.name })} className="[&.active]:font-bold">
                      <FontAwesomeIcon icon={faLayerGroup} />
                      {stack.name}
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
              <SidebarGroupLabel>Graphs</SidebarGroupLabel>
              {Object.keys(state?.tasks.graphs).map((graph) => (
                <SidebarMenuItem key={graph}>
                  <SidebarMenuButton asChild isActive={routeSearch.graph === graph}>
                    <Link from='/' search={(prev) => ({ ...prev, graph })} className="[&.active]:font-bold">
                      <FontAwesomeIcon icon={faDiagramProject} />
                      {graph}
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
              <SidebarGroupLabel>Actions</SidebarGroupLabel>
              <SidebarMenuItem key='deploy'>
                <RunTasksBtn branch={state?.branch} stack={routeSearch.stack!} graph={routeSearch.graph!} />
              </SidebarMenuItem>
              {/* <SidebarMenuItem key='open-prefect'>
                <div className="flex pt-2 space-x-2 px-0.5">
                  <Switch />
                  <span className="opacity-90">Auto-open Prefect</span>
                </div>
              </SidebarMenuItem> */}
              <SidebarMenuItem key='cache-control'>
                <div className="flex pt-2 space-x-2 px-0.5">
                  <Switch onCheckedChange={(ignore) => setIgnore(ignore)} />
                  <span className="opacity-90">Ignore cache</span>
                </div>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  )
}

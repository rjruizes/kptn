import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faPlay } from '@fortawesome/free-solid-svg-icons'
import { Button } from "./ui/button"
import { Badge } from "@/components/ui/badge"
import { useRowStore } from "@/hooks/use-row"
import { buildAndRun } from './buildAndDeploy'
import { useIgnoreCacheStore } from '@/hooks/use-ignorecache'
import { useStateStore } from '@/hooks/use-state'

export default function RunTasksBtn({branch, stack, graph}: {branch:string, stack: string, graph: string}) {
  const stacks = useStateStore((state) => state.stackDict)
  const tasks = useRowStore((state) => state.rows)
  const ignore = useIgnoreCacheStore((state) => state.ignore)
  const endpoint = stacks[stack]?.prefect_web_url
  return (
    <Button disabled={endpoint === undefined}
      variant='secondary'
      onClick={() => buildAndRun({ branch, stack, graph, tasks, ignore_cache: ignore, endpoint })}
    >
      <FontAwesomeIcon size="xs" icon={faPlay} />
      Run {tasks.length == 0 ? 'All' : 'Selected'} Tasks {tasks.length > 0 && <Badge>{tasks.length}</Badge>}
    </Button>
  )
}
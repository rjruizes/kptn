import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/stack/$stack')({
  component: StackComponent,
})

function StackComponent() {
  const { stack } = Route.useParams()
  return (
    <div>
      <h1>Stack: {stack}</h1>
      <p>
        This is the Stack route. It is a sibling of the Home and About routes.
      </p>
    </div>
  )
}
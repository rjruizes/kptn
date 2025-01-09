import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/stack/')({
  component: () => <div>Hello /stack/!</div>,
})

import { CustomCellRendererProps } from 'ag-grid-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

export default (params: CustomCellRendererProps) => {
  return !params.value.isMapped ? null : (
    <div className='flex h-full items-center font-mono'>
      {params.value.subtasks?.length > 0 ? (
          <Dialog>
            <DialogTrigger>
              <span>[{params.value.subtasks.length}]</span>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>{params.value.taskName}</DialogTitle>
                <DialogDescription>
                  <code className='whitespace-pre'>
                    {JSON.stringify(params.value.subtasks, null, 2)}
                  </code>
                </DialogDescription>
              </DialogHeader>
            </DialogContent>
          </Dialog>
        ) : <span className='opacity-30'>[]</span>}
    </div>
  );
};

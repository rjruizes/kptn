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
  return (
    <div className='flex h-full items-center font-mono'>
      {!params.value.isCached ? null : (
        !params.value.data ? <span className='opacity-30'>{"{}"}</span> : (
          <Dialog>
            <DialogTrigger>
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                  <span>{"{…}"}</span>
                  </TooltipTrigger>
                  <TooltipContent>
                    {/* Truncate */}
                    {params.value.data?.length > 70 ? params.value.data.slice(0, 70) + '...' : params.value.data}
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>{params.value.taskName}</DialogTitle>
                <DialogDescription>
                  {params.value.data}
                </DialogDescription>
              </DialogHeader>
            </DialogContent>
          </Dialog>
        )
      )}
    </div>
  );
};

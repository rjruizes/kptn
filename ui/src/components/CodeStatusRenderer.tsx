import { CustomCellRendererProps } from 'ag-grid-react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faXmark, faCheck } from '@fortawesome/free-solid-svg-icons'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"


export default (params: CustomCellRendererProps) => {
  return (
    // If no values, display nothing
    (!params.value.live && !params.value.cached) ? null :
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger>
          <div>
            {params.value.live === params.value.cached ? (
              <FontAwesomeIcon icon={faCheck} className='h-5 text-green-600 opacity-60' />
            ) : (
              <FontAwesomeIcon icon={faXmark} className='h-5 text-red-500' />
            )}
          </div>
        </TooltipTrigger>
        <TooltipContent>
          Live: {params.value.live}
          <br />
          Cached: {params.value.cached}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
};

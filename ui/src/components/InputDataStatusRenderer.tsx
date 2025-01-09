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
    (!params.value.live_version && !params.value.cached_version) ? null :
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger>
          <div className='flex h-full items-center'>
            {params.value.live_version === params.value.cached_version ? (
              <FontAwesomeIcon icon={faCheck} className='h-5 text-green-600 opacity-60' />
            ) : (
              <FontAwesomeIcon icon={faXmark} className='h-5 text-red-500' />
            )}
          </div>
        </TooltipTrigger>
        <TooltipContent>
          Live: {params.value.live_version}
          <br />
          Cached: {params.value.cached_version}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
};

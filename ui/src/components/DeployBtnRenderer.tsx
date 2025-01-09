import { CustomCellRendererProps } from 'ag-grid-react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faPlay } from '@fortawesome/free-solid-svg-icons'
import { Button } from './ui/button';
import { buildAndRun } from './buildAndDeploy';

export default (params: CustomCellRendererProps) => {
  return (
    <div className='flex h-full items-center'>
      <Button variant="outline" onClick={() => buildAndRun(params.value)}>
        <FontAwesomeIcon icon={faPlay} />
      </Button>
    </div>
  );
};

import { CustomCellRendererProps } from 'ag-grid-react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faFileLines } from '@fortawesome/free-solid-svg-icons'
// Assets from https://github.com/vscode-icons/vscode-icons/wiki/ListOfFiles

export default (params: CustomCellRendererProps) => {
  return params.value.url ? (
    // Stop propagation to prevent row selection; use ref to attach onClick event handler to the real DOM element
    <a
      ref={(el) => el?.addEventListener('click', (e) => e.stopPropagation())}
      href={params.value.url}
    >
      <FontAwesomeIcon icon={faFileLines} size='lg' />
    </a>
  ) : null;
};

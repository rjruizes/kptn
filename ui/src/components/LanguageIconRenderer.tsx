import { CustomCellRendererProps } from 'ag-grid-react';
import FileTypePython from '../assets/file_type_python.svg';
import FileTypeR from '../assets/file_type_r.svg';
// Assets from https://github.com/vscode-icons/vscode-icons/wiki/ListOfFiles

export default (params: CustomCellRendererProps) => {
  return (
    <a href={"vscode://file"+params.value.filepath} className='flex justify-center items-center pt-2'>
      {params.value.lang === 'py' ? (
        <img src={FileTypePython} alt='Python' className='h-5' />
      ) : (
        <img src={FileTypeR} alt='R' className='h-5' />
      )}
    </a>
  );
};

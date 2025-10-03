import { AgGridReact } from 'ag-grid-react'; // React Data Grid Component
import { useMemo } from 'react';
import { ColDef, colorSchemeDark, themeQuartz } from 'ag-grid-community';
import { useTheme } from './theme-provider';
import { getRouteApi } from '@tanstack/react-router';
import LanguageIconRenderer from './LanguageIconRenderer';
import CodeStatusRenderer from './CodeStatusRenderer';
import DeployBtnRenderer from './DeployBtnRenderer';
import { useRowStore } from '@/hooks/use-row';
import useGridApi from '@/hooks/use-grid-api';
import { WebSocketDemo } from './WebSocketDemo';
import { useStateStore } from '@/hooks/use-state';
import MappedRenderer from './MappedRenderer';
import DataRenderer from './DataRenderer';
import InputStatusRenderer from './InputStatusRenderer';
import LogsIconRenderer from './LogsIconRenderer';
import InputDataStatusRenderer from './InputDataStatusRenderer';


export const Table = () => {
  const routeApi = getRouteApi('/')
  const routeSearch = routeApi.useSearch()
  const state = useStateStore()
  if (!state) return null
  const taskNames = Object.keys(state.tasks.graphs[routeSearch.graph!].tasks)
  const taskList = taskNames.map((taskName) => {
    return {
      taskName,
      ...state.tasks.tasks[taskName]
    }
  })
  const { gridApi, onGridReady } = useGridApi();
  const setSelectedRows = useRowStore((state) => state.setRows)
  const rowData = taskList.map((task) => {
    const codeKind = task.code_kind ?? (task.py_script ? 'Python' : 'R')
    const langKey = codeKind === 'R' ? 'R' : 'py'
    return {
      taskName: task.taskName,
      duration: task.duration,
      lang: {lang: langKey, filepath: task.filepath},
      code: { live: task.local_code_version, cached: task.code_version },
      inputFiles: { live: task.live_inputs_version, cached: task.cached_inputs_version },
      inputData: {
        live_hashes: task.live_input_data_hashes,
        live_version: task.live_input_data_version,
        cached_hashes: task.cached_input_data_hashes,
        cached_version: task.cached_input_data_version
      },
      data: { isCached: task.cache_result, taskName: task.taskName, data: task.data},
      subtasks: { isMapped: "map_over" in task, subtasks: task.subtasks },
      logs: { url: task.log_path },
      // deploy: { branch: state.branch, tasks: [task.taskName], stack: routeSearch.stack!, graph: routeSearch.graph! },
    }
  })
  
  interface Task {
    taskName: string;
    duration: string;
    lang: string;
    code: object;
    inputFiles: object;
    inputData: object;
    data: object;
    subtasks: object;
    logs: string;
    // deploy: object;
  }
  // Column Definitions: Defines the columns to be displayed.
  const colDefs: ColDef<Task>[] = [
    { field: "taskName" },
    { field: "duration" },
    { field: "lang", cellRenderer: LanguageIconRenderer },
    { field: "code", cellRenderer: CodeStatusRenderer, maxWidth: 70,  },
    { field: "inputFiles", cellRenderer: InputStatusRenderer, maxWidth: 70, wrapHeaderText: true },
    { field: "inputData", maxWidth: 70, wrapHeaderText: true, cellRenderer: InputDataStatusRenderer },
    { field: "data", cellRenderer: DataRenderer },
    { field: "subtasks", cellRenderer: MappedRenderer },
    { field: "logs", cellRenderer: LogsIconRenderer, maxWidth: 70 },
    // { field: "deploy", cellRenderer: DeployBtnRenderer },
  ];
  const { theme } = useTheme()
  let actualTheme = theme === "system" ? (window.matchMedia("(prefers-color-scheme: dark)")
    .matches
    ? "dark"
    : "light") : theme
  const agTheme = actualTheme === 'light' ? themeQuartz : themeQuartz.withPart(colorSchemeDark);
  const rowSelection = useMemo(() => { 
    return { 
      mode: 'multiRow',
      enableClickSelection: true,
    };
  }, []);
  const onSelectionChanged = () => {
    const selectedData = gridApi?.getSelectedRows();
    // console.log('Selection Changed', selectedData);
    setSelectedRows(selectedData?.map(row => row.taskName) || [])
  };
  
  return (
    // wrapping container with theme & size
    <div
     className="w-full h-full" // applying the Data Grid theme
     style={{ }} // the Data Grid will fill the size of the parent container
    >
      <WebSocketDemo />
      <AgGridReact
        onGridReady={onGridReady}
        rowData={rowData}
        columnDefs={colDefs}
        theme={agTheme}
        rowSelection={rowSelection}
        enableCellTextSelection={true}
        autoSizeStrategy={{ type: 'fitCellContents'}}
        onSelectionChanged={onSelectionChanged}
      />
    </div>
   )
   
 }
 

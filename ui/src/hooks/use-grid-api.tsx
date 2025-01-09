import { useRef, useState } from "react";
import {
  GridApi,
  // ColumnApi,
  GridOptions,
} from "ag-grid-community";

/*
 * Cribbed from https://codesandbox.io/p/sandbox/ag-grid-examples-with-typescript-3v0zt?file=/src/hooks/useGridApi.ts:30,33
 * I removed the ColumnApi because it doesn't seem to exist in the ag-grid-community package.
 */

type UseGridApiReturnValue = {
  gridApi: GridApi | null;
  // columnApi: ColumnApi | null;
  onGridReady: GridOptions["onGridReady"];
};

const useGridApi = (): UseGridApiReturnValue => {
  const gridApiRef = useRef<UseGridApiReturnValue["gridApi"]>(null);
  // const columnApiRef = useRef<UseGridApiReturnValue["columnApi"]>(null);

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [_isInitialized, setIsInitialized] = useState(false);

  const onGridReady: UseGridApiReturnValue["onGridReady"] = (params) => {
    gridApiRef.current = params.api;
    // columnApiRef.current = params.columnApi;

    setIsInitialized(true);
  };

  return {
    // columnApi: columnApiRef.current,
    gridApi: gridApiRef.current,
    onGridReady,
  };
};

export default useGridApi;

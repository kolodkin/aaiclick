// The backend-agnostic kernel: result parsing, cell rendering, query params,
// presentation pickers, and the dashboard sandbox. Nothing here fetches, routes,
// or reads app state (enforced by eslint.config.js), so this folder is what
// another app reuses. The app imports it only through this file.

export {
  EMPTY_ROWS,
  cellText,
  columnNames,
  columnTypes,
  isContainer,
  type Cell,
  type ColumnMeta,
  type QueryRows,
} from './results/rows'
export { ResultsTable } from './results/ResultsTable'
export { escapeHtml, substituteCellTemplate } from './cells/cellView'
export { parseCellViewYaml, renderCell, type CellView, type CellViewMap } from './cells/cellViewYaml'
export {
  complexCellItems,
  parseComplexType,
  PREVIEW_COUNT,
  type CellItem,
  type ComplexType,
} from './cells/complexCells'
export { ComplexCell } from './cells/ComplexCell'
export { CellViewModal } from './cells/CellViewModal'
export {
  applyParams,
  parseQueryParams,
  parseYamlObject,
  type ParamDef,
  type ParamSpec,
} from './params/queryParams'
export { presentationForSave, shownColumnIndices } from './presentation/presentation'
export { FieldPickers, type Field, type OrderCol } from './presentation/FieldPickers'
export { DashboardFrame } from './dashboard/DashboardFrame'
export { buildSrcDoc, type DashboardResults } from './dashboard/srcDoc'

// A cell with no design of its own: collections (a ClickHouse Array / Map /
// Tuple, or any JSON array or object) get the built-in collapsible view, the
// rest is plain text.

import { ComplexCell } from "./ComplexCell";
import type { ComplexType } from "./complexCells";
import { cellText, isContainer, type Cell } from "../results/rows";

// `complex` is the column's parsed type (parseComplexType), worked out once
// per column rather than per cell.
export function renderDefaultCell(colName: string, value: Cell, complex: ComplexType | null): React.ReactNode {
  if (complex || isContainer(value)) return <ComplexCell type={complex} value={value} col={colName} />;
  return cellText(value);
}

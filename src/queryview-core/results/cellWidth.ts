// How wide a result cell gets, and when a value can't be read inside one.
// Lives with the grid that renders the box, not with the cell contents.

export const CELL_WIDTH_CH = 50;

// The cap on a column, as `max-width` on each cell's scroller and the header:
// the browser fits columns to their widest cell, and this keeps one long-text
// column from stretching the grid. The table is monospace throughout, so `ch`
// means the same in every cell.
export const CELL_MAX_WIDTH = `${CELL_WIDTH_CH}ch`;

// A value needs the cell's scroller and its popup button when it can't be read
// in place: too wide for the column, or spanning lines — `whitespace-pre` turns
// those into extra rows, and multi-line text is exactly what the parsed view is
// for, however short it is.
//
// Character count stands in for rendered width, which holds for the monospace
// Latin text these grids mostly carry; double-width glyphs (CJK) overflow a
// little before it notices.
export function isOverflowing(text: string): boolean {
  return text.length > CELL_WIDTH_CH || text.includes("\n");
}

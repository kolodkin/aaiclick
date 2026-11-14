// Case-insensitive substring match of each item's name on the trimmed query,
// keeping input order. A blank query keeps everything.
export function filterNames<T>(items: T[], query: string, nameOf: (item: T) => string = String): T[] {
  const q = query.trim().toLowerCase();
  if (!q) return items;
  return items.filter((item) => nameOf(item).toLowerCase().includes(q));
}

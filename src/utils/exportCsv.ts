/**
 * Shared CSV export utility.
 * Accepts columns + rows data and triggers a browser download.
 */

export function downloadCsv(
  columns: string[],
  rows: Record<string, unknown>[],
  filename?: string
): void {
  const header = columns.map(c => `"${c.replace(/"/g, '""')}"`).join(",");
  const body = rows.map(row =>
    columns.map(c => {
      const val = row[c];
      if (val === null || val === undefined) return "";
      const str = String(val);
      return `"${str.replace(/"/g, '""')}"`;
    }).join(",")
  ).join("\n");

  const csv = header + "\n" + body;
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename ?? `aradune-export-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

/**
 * Extract table data from markdown content (pipe-delimited tables).
 * Returns { columns, rows } or null if no table found.
 */
export function extractMarkdownTable(
  markdown: string
): { columns: string[]; rows: Record<string, string>[] } | null {
  const lines = markdown.split("\n");
  const tableLines = lines.filter(l => l.includes("|") && l.trim().startsWith("|"));
  if (tableLines.length < 3) return null; // header + separator + at least 1 data row

  const allRows = tableLines.map(l =>
    l.split("|").slice(1, -1).map(c => c.trim())
  );
  // First row is header, second is separator (---|---), rest are data
  const columns = allRows[0];
  const dataRows = allRows.slice(1).filter(r => !r.every(c => /^[\s\-:|]+$/.test(c)));

  if (columns.length === 0 || dataRows.length === 0) return null;

  const rows = dataRows.map(r => {
    const obj: Record<string, string> = {};
    columns.forEach((col, i) => {
      obj[col] = r[i] ?? "";
    });
    return obj;
  });

  return { columns, rows };
}

/**
 * Shared markdown parser for report exports (DOCX, PDF, Excel).
 * Parses Intelligence response markdown into structured blocks.
 */

export type Block =
  | { type: "heading"; level: number; text: string }
  | { type: "paragraph"; runs: Run[] }
  | { type: "table"; columns: string[]; rows: string[][] }
  | { type: "list"; ordered: boolean; items: string[] }
  | { type: "hr" }
  | { type: "code"; lang: string; text: string };

export type Run =
  | { bold: true; text: string }
  | { bold?: false; text: string };

/** Parse inline bold markers into runs. */
export function parseInline(text: string): Run[] {
  const runs: Run[] = [];
  const re = /\*\*(.+?)\*\*/g;
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) runs.push({ text: text.slice(last, m.index) });
    runs.push({ bold: true, text: m[1] });
    last = m.index + m[0].length;
  }
  if (last < text.length) runs.push({ text: text.slice(last) });
  if (runs.length === 0) runs.push({ text });
  return runs;
}

/** Parse a markdown string into an array of blocks. */
export function parseMarkdown(md: string): Block[] {
  const lines = md.split("\n");
  const blocks: Block[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Blank line — skip
    if (line.trim() === "") {
      i++;
      continue;
    }

    // Horizontal rule
    if (/^---+\s*$/.test(line.trim()) || /^\*\*\*+\s*$/.test(line.trim())) {
      blocks.push({ type: "hr" });
      i++;
      continue;
    }

    // Heading
    const headingMatch = line.match(/^(#{1,4})\s+(.+)$/);
    if (headingMatch) {
      blocks.push({
        type: "heading",
        level: headingMatch[1].length,
        text: headingMatch[2].replace(/\*\*/g, ""),
      });
      i++;
      continue;
    }

    // Code block
    if (line.trim().startsWith("```")) {
      const lang = line.trim().slice(3).trim();
      const codeLines: string[] = [];
      i++;
      while (i < lines.length && !lines[i].trim().startsWith("```")) {
        codeLines.push(lines[i]);
        i++;
      }
      blocks.push({ type: "code", lang, text: codeLines.join("\n") });
      i++; // skip closing ```
      continue;
    }

    // Table (pipe-delimited)
    if (line.includes("|") && line.trim().startsWith("|")) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].includes("|") && lines[i].trim().startsWith("|")) {
        tableLines.push(lines[i]);
        i++;
      }
      if (tableLines.length >= 3) {
        const parseCells = (l: string) =>
          l.split("|").slice(1, -1).map(c => c.trim());
        const columns = parseCells(tableLines[0]);
        const dataRows = tableLines
          .slice(1)
          .filter(l => !parseCells(l).every(c => /^[\s\-:|]+$/.test(c)))
          .map(parseCells);
        blocks.push({ type: "table", columns, rows: dataRows });
      }
      continue;
    }

    // Ordered list
    if (/^\d+\.\s/.test(line.trim())) {
      const items: string[] = [];
      while (i < lines.length && /^\d+\.\s/.test(lines[i].trim())) {
        items.push(lines[i].trim().replace(/^\d+\.\s+/, ""));
        i++;
      }
      blocks.push({ type: "list", ordered: true, items });
      continue;
    }

    // Unordered list
    if (/^[-*]\s/.test(line.trim())) {
      const items: string[] = [];
      while (i < lines.length && /^[-*]\s/.test(lines[i].trim())) {
        items.push(lines[i].trim().replace(/^[-*]\s+/, ""));
        i++;
      }
      blocks.push({ type: "list", ordered: false, items });
      continue;
    }

    // Paragraph — accumulate consecutive non-blank, non-special lines
    const paraLines: string[] = [];
    while (
      i < lines.length &&
      lines[i].trim() !== "" &&
      !lines[i].trim().startsWith("#") &&
      !lines[i].trim().startsWith("```") &&
      !lines[i].trim().startsWith("---") &&
      !(lines[i].includes("|") && lines[i].trim().startsWith("|")) &&
      !/^\d+\.\s/.test(lines[i].trim()) &&
      !/^[-*]\s/.test(lines[i].trim())
    ) {
      paraLines.push(lines[i].trim());
      i++;
    }
    if (paraLines.length > 0) {
      blocks.push({ type: "paragraph", runs: parseInline(paraLines.join(" ")) });
    }
  }

  return blocks;
}

/** Extract all tables from markdown. Returns array of { columns, rows }. */
export function extractAllTables(
  md: string
): { columns: string[]; rows: string[][] }[] {
  return parseMarkdown(md)
    .filter((b): b is Extract<Block, { type: "table" }> => b.type === "table")
    .map(b => ({ columns: b.columns, rows: b.rows }));
}

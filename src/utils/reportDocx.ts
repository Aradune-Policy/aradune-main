/**
 * DOCX Report Export
 * Generates a branded Word document from accumulated report sections.
 * Uses the `docx` library for client-side DOCX generation.
 */

import {
  Document,
  Paragraph,
  TextRun,
  Table,
  TableRow,
  TableCell,
  WidthType,
  AlignmentType,
  HeadingLevel,
  BorderStyle,
  Packer,
  PageBreak,
  ShadingType,
  TableLayoutType,
} from "docx";
import { saveAs } from "file-saver";
import { parseMarkdown, parseInline } from "./reportMarkdown";
import type { Block, Run } from "./reportMarkdown";

// Brand colors
const BRAND = "2E6B4A";
const INK = "0A2540";
const INK_LIGHT = "425A70";
const BORDER_COLOR = "E4EAE4";
const SURFACE = "F5F7F5";

interface ReportSection {
  id: string;
  prompt: string;
  response: string;
  queries: string[];
  createdAt: Date;
}

function runsToTextRuns(runs: Run[], baseFontSize = 20): TextRun[] {
  return runs.map(
    (r) =>
      new TextRun({
        text: r.text,
        bold: r.bold || false,
        font: "Helvetica Neue",
        size: baseFontSize,
        color: INK,
      })
  );
}

function blockToParagraphs(block: Block): (Paragraph | Table)[] {
  switch (block.type) {
    case "heading": {
      const level =
        block.level === 1
          ? HeadingLevel.HEADING_1
          : block.level === 2
            ? HeadingLevel.HEADING_2
            : HeadingLevel.HEADING_3;
      return [
        new Paragraph({
          heading: level,
          spacing: { before: 240, after: 120 },
          children: [
            new TextRun({
              text: block.text,
              bold: true,
              font: "Helvetica Neue",
              size: block.level === 1 ? 28 : block.level === 2 ? 24 : 22,
              color: BRAND,
            }),
          ],
        }),
      ];
    }

    case "paragraph":
      return [
        new Paragraph({
          spacing: { after: 120 },
          children: runsToTextRuns(block.runs),
        }),
      ];

    case "table":
      return [buildTable(block.columns, block.rows)];

    case "list":
      return block.items.map(
        (item, idx) =>
          new Paragraph({
            spacing: { after: 60 },
            indent: { left: 360 },
            children: [
              new TextRun({
                text: block.ordered ? `${idx + 1}. ` : "- ",
                font: "Helvetica Neue",
                size: 20,
                color: INK_LIGHT,
              }),
              ...runsToTextRuns(parseInline(item)),
            ],
          })
      );

    case "hr":
      return [
        new Paragraph({
          spacing: { before: 200, after: 200 },
          border: {
            bottom: { style: BorderStyle.SINGLE, size: 1, color: BORDER_COLOR },
          },
          children: [],
        }),
      ];

    case "code":
      return [
        new Paragraph({
          spacing: { before: 120, after: 120 },
          shading: { type: ShadingType.CLEAR, fill: SURFACE },
          children: [
            new TextRun({
              text: block.text,
              font: "Menlo",
              size: 16,
              color: INK,
            }),
          ],
        }),
      ];
  }
}

function buildTable(columns: string[], rows: string[][]): Table {
  const headerRow = new TableRow({
    tableHeader: true,
    children: columns.map(
      (col) =>
        new TableCell({
          shading: { type: ShadingType.CLEAR, fill: BRAND },
          children: [
            new Paragraph({
              spacing: { before: 40, after: 40 },
              children: [
                new TextRun({
                  text: col,
                  bold: true,
                  font: "Helvetica Neue",
                  size: 18,
                  color: "FFFFFF",
                }),
              ],
            }),
          ],
        })
    ),
  });

  const dataRows = rows.map(
    (row, rowIdx) =>
      new TableRow({
        children: columns.map(
          (_, colIdx) =>
            new TableCell({
              shading:
                rowIdx % 2 === 1
                  ? { type: ShadingType.CLEAR, fill: SURFACE }
                  : undefined,
              children: [
                new Paragraph({
                  spacing: { before: 30, after: 30 },
                  children: [
                    new TextRun({
                      text: row[colIdx] ?? "",
                      font: "Helvetica Neue",
                      size: 18,
                      color: INK,
                    }),
                  ],
                }),
              ],
            })
        ),
      })
  );

  return new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    layout: TableLayoutType.AUTOFIT,
    rows: [headerRow, ...dataRows],
  });
}

export async function generateReportDocx(
  sections: ReportSection[],
  title?: string
): Promise<void> {
  const reportTitle = title || "Medicaid Intelligence Report";
  const date = new Date().toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });

  // Collect all data sources from queries
  const allQueries = sections.flatMap((s) => s.queries || []);
  const tableNames = [
    ...new Set(
      allQueries
        .join(" ")
        .match(/(?:FROM|JOIN)\s+(fact_\w+|dim_\w+|ref_\w+)/gi)
        ?.map((m) => m.replace(/^(?:FROM|JOIN)\s+/i, "")) || []
    ),
  ];

  const docChildren: (Paragraph | Table)[] = [];

  // ── Cover page ──────────────────────────────────────────────────────

  // Brand bar (simulated with shaded paragraph)
  docChildren.push(
    new Paragraph({
      shading: { type: ShadingType.CLEAR, fill: BRAND },
      spacing: { after: 0 },
      children: [
        new TextRun({
          text: "  ARADUNE",
          bold: true,
          font: "Helvetica Neue",
          size: 28,
          color: "FFFFFF",
        }),
      ],
    })
  );

  // Spacer
  docChildren.push(new Paragraph({ spacing: { before: 600 }, children: [] }));

  // Title
  docChildren.push(
    new Paragraph({
      alignment: AlignmentType.LEFT,
      spacing: { after: 200 },
      children: [
        new TextRun({
          text: reportTitle,
          bold: true,
          font: "Helvetica Neue",
          size: 48,
          color: INK,
        }),
      ],
    })
  );

  // Date + section count
  docChildren.push(
    new Paragraph({
      spacing: { after: 80 },
      children: [
        new TextRun({
          text: date,
          font: "Helvetica Neue",
          size: 22,
          color: INK_LIGHT,
        }),
      ],
    })
  );
  docChildren.push(
    new Paragraph({
      spacing: { after: 400 },
      children: [
        new TextRun({
          text: `${sections.length} analysis section${sections.length !== 1 ? "s" : ""}`,
          font: "Helvetica Neue",
          size: 22,
          color: INK_LIGHT,
        }),
      ],
    })
  );

  // Data sources on cover
  if (tableNames.length > 0) {
    docChildren.push(
      new Paragraph({
        spacing: { after: 120 },
        children: [
          new TextRun({
            text: "Data Sources",
            bold: true,
            font: "Helvetica Neue",
            size: 20,
            color: BRAND,
          }),
        ],
      })
    );
    docChildren.push(
      new Paragraph({
        spacing: { after: 400 },
        children: [
          new TextRun({
            text: tableNames.join(", "),
            font: "Helvetica Neue",
            size: 18,
            color: INK_LIGHT,
          }),
        ],
      })
    );
  }

  // Page break after cover
  docChildren.push(
    new Paragraph({ children: [new PageBreak()] })
  );

  // ── Sections ────────────────────────────────────────────────────────

  sections.forEach((section, idx) => {
    // Section header
    docChildren.push(
      new Paragraph({
        spacing: { before: idx > 0 ? 360 : 0, after: 60 },
        children: [
          new TextRun({
            text: `Section ${idx + 1}`,
            bold: true,
            font: "Helvetica Neue",
            size: 16,
            color: INK_LIGHT,
          }),
        ],
      })
    );

    // User prompt
    docChildren.push(
      new Paragraph({
        shading: { type: ShadingType.CLEAR, fill: SURFACE },
        spacing: { after: 200 },
        children: [
          new TextRun({
            text: section.prompt,
            italics: true,
            font: "Helvetica Neue",
            size: 22,
            color: INK,
          }),
        ],
      })
    );

    // Parse response markdown into blocks
    const blocks = parseMarkdown(section.response);
    for (const block of blocks) {
      docChildren.push(...blockToParagraphs(block));
    }

    // Section divider (except after last)
    if (idx < sections.length - 1) {
      docChildren.push(
        new Paragraph({
          spacing: { before: 240, after: 240 },
          border: {
            bottom: {
              style: BorderStyle.SINGLE,
              size: 1,
              color: BORDER_COLOR,
            },
          },
          children: [],
        })
      );
    }
  });

  // ── Footer note ─────────────────────────────────────────────────────

  docChildren.push(
    new Paragraph({ spacing: { before: 600 }, children: [] })
  );
  docChildren.push(
    new Paragraph({
      border: {
        top: { style: BorderStyle.SINGLE, size: 1, color: BORDER_COLOR },
      },
      spacing: { before: 200 },
      children: [
        new TextRun({
          text: "Generated by Aradune | aradune.co",
          font: "Helvetica Neue",
          size: 16,
          color: INK_LIGHT,
        }),
      ],
    })
  );

  // ── Build and save ──────────────────────────────────────────────────

  const doc = new Document({
    styles: {
      default: {
        document: {
          run: {
            font: "Helvetica Neue",
            size: 20,
            color: INK,
          },
        },
      },
    },
    sections: [
      {
        properties: {
          page: {
            margin: { top: 720, right: 720, bottom: 720, left: 720 },
          },
        },
        children: docChildren,
      },
    ],
  });

  const blob = await Packer.toBlob(doc);
  const filename = `aradune-report-${new Date().toISOString().slice(0, 10)}.docx`;
  saveAs(blob, filename);
}

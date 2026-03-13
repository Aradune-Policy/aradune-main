import { useState, useRef, useCallback } from "react";
import { useAradune } from "../context/AraduneContext";
import type { ImportedFile } from "../context/AraduneContext";
import { C, FONT, SHADOW_LG } from "../design";
import { API_BASE } from "../lib/api";

const API = API_BASE;

interface ImportPanelProps {
  open: boolean;
  onClose: () => void;
}

interface UploadPreview {
  id: string;
  name: string;
  type: "csv" | "xlsx" | "json";
  columns: string[];
  rowCount: number;
  preview: Record<string, unknown>[];
  tableName: string;
}

export default function ImportPanel({ open, onClose }: ImportPanelProps) {
  const { importedFiles, addImportedFile, removeImportedFile } = useAradune();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<UploadPreview | null>(null);
  const [dragOver, setDragOver] = useState(false);

  const resetState = useCallback(() => {
    setUploading(false);
    setError(null);
    setPreview(null);
    setDragOver(false);
  }, []);

  const handleClose = useCallback(() => {
    resetState();
    onClose();
  }, [onClose, resetState]);

  const uploadFile = useCallback(async (file: File) => {
    // Validate size (50MB)
    if (file.size > 50 * 1024 * 1024) {
      setError("File exceeds the 50MB limit.");
      return;
    }

    // Validate extension
    const ext = file.name.split(".").pop()?.toLowerCase();
    if (!ext || !["csv", "xlsx", "json"].includes(ext)) {
      setError("Unsupported file type. Please upload a CSV, Excel, or JSON file.");
      return;
    }

    setUploading(true);
    setError(null);
    setPreview(null);

    const form = new FormData();
    form.append("file", file);

    try {
      const res = await fetch(`${API}/api/import`, {
        method: "POST",
        body: form,
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || `Upload failed (${res.status})`);
      }

      const data: UploadPreview = await res.json();
      setPreview(data);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Upload failed";
      setError(msg);
    } finally {
      setUploading(false);
    }
  }, []);

  const handleFileChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) uploadFile(file);
      // Reset so the same file can be re-selected
      e.target.value = "";
    },
    [uploadFile],
  );

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      const file = e.dataTransfer.files?.[0];
      if (file) uploadFile(file);
    },
    [uploadFile],
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setDragOver(false);
  }, []);

  const handleConfirm = useCallback(() => {
    if (!preview) return;
    const imported: ImportedFile = {
      id: preview.id,
      name: preview.name,
      type: preview.type,
      columns: preview.columns,
      rowCount: preview.rowCount,
      preview: preview.preview,
      tableName: preview.tableName,
      uploadedAt: new Date(),
    };
    addImportedFile(imported);
    resetState();
    onClose();
  }, [preview, addImportedFile, resetState, onClose]);

  if (!open) return null;

  return (
    <div style={styles.backdrop} onClick={handleClose}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div style={styles.header}>
          <h2 style={styles.title}>Import Data</h2>
          <button style={styles.closeBtn} onClick={handleClose} aria-label="Close">
            &times;
          </button>
        </div>

        {/* Upload zone */}
        {!uploading && !preview && (
          <div
            style={{
              ...styles.dropZone,
              borderColor: dragOver ? C.brand : C.border,
              background: dragOver ? "#f0f5f0" : C.surface,
            }}
            onClick={() => fileInputRef.current?.click()}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
          >
            <div style={styles.dropIcon}>+</div>
            <div style={styles.dropText}>Drop a file here or click to browse</div>
            <div style={styles.dropSub}>CSV, Excel, JSON — up to 50MB</div>
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,.xlsx,.json"
              style={{ display: "none" }}
              onChange={handleFileChange}
            />
          </div>
        )}

        {/* Uploading spinner */}
        {uploading && (
          <div style={styles.spinnerWrap}>
            <div style={styles.spinner} />
            <div style={{ marginTop: 12, color: C.inkLight }}>Uploading...</div>
          </div>
        )}

        {/* Error message */}
        {error && (
          <div style={styles.error}>
            {error}
            <button
              style={styles.errorDismiss}
              onClick={() => setError(null)}
            >
              Dismiss
            </button>
          </div>
        )}

        {/* Validation preview */}
        {preview && (
          <div style={styles.previewWrap}>
            <div style={styles.previewMeta}>
              <span style={styles.previewName}>{preview.name}</span>
              <span style={styles.previewStat}>
                {preview.rowCount.toLocaleString()} rows
              </span>
              <span style={styles.previewStat}>
                {preview.columns.length} columns
              </span>
            </div>

            {/* Preview table */}
            <div style={styles.tableWrap}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    {preview.columns.map((col) => (
                      <th key={col} style={styles.th}>
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.preview.slice(0, 5).map((row, i) => (
                    <tr key={i}>
                      {preview.columns.map((col) => (
                        <td key={col} style={styles.td}>
                          {row[col] != null ? String(row[col]) : ""}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Action buttons */}
            <div style={styles.previewActions}>
              <button style={styles.cancelBtn} onClick={resetState}>
                Cancel
              </button>
              <button style={styles.confirmBtn} onClick={handleConfirm}>
                Make available to Aradune
              </button>
            </div>
          </div>
        )}

        {/* Active imports list */}
        {importedFiles.length > 0 && (
          <div style={styles.importsList}>
            <h3 style={styles.importsTitle}>Active Imports</h3>
            {importedFiles.map((f) => (
              <div key={f.id} style={styles.importRow}>
                <div style={styles.importInfo}>
                  <span style={styles.importName}>{f.name}</span>
                  <span style={styles.importDetail}>
                    {f.rowCount.toLocaleString()} rows &middot;{" "}
                    {f.columns.length} cols &middot;{" "}
                    <code style={styles.tableName}>{f.tableName}</code>
                  </span>
                </div>
                <button
                  style={styles.removeBtn}
                  onClick={() => removeImportedFile(f.id)}
                  aria-label={`Remove ${f.name}`}
                >
                  &times;
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Styles ─────────────────────────────────────────────────────────────── */

const styles: Record<string, React.CSSProperties> = {
  backdrop: {
    position: "fixed",
    inset: 0,
    background: "rgba(10,37,64,0.45)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 9000,
  },
  modal: {
    background: C.white,
    borderRadius: 12,
    width: "100%",
    maxWidth: 560,
    maxHeight: "90vh",
    overflowY: "auto",
    boxShadow: SHADOW_LG,
    padding: "24px 28px",
  },
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 20,
  },
  title: {
    margin: 0,
    fontSize: 18,
    fontFamily: FONT.body,
    fontWeight: 600,
    color: C.ink,
  },
  closeBtn: {
    background: "none",
    border: "none",
    fontSize: 24,
    color: C.inkLight,
    cursor: "pointer",
    lineHeight: 1,
    padding: "0 4px",
  },

  /* Drop zone */
  dropZone: {
    border: `2px dashed ${C.border}`,
    borderRadius: 10,
    padding: "40px 20px",
    textAlign: "center" as const,
    cursor: "pointer",
    transition: "border-color 0.15s, background 0.15s",
  },
  dropIcon: {
    fontSize: 32,
    fontWeight: 300,
    color: C.brand,
    marginBottom: 8,
  },
  dropText: {
    fontSize: 15,
    fontFamily: FONT.body,
    color: C.ink,
    fontWeight: 500,
  },
  dropSub: {
    fontSize: 13,
    fontFamily: FONT.body,
    color: C.inkLight,
    marginTop: 4,
  },

  /* Spinner */
  spinnerWrap: {
    display: "flex",
    flexDirection: "column" as const,
    alignItems: "center",
    padding: "40px 0",
  },
  spinner: {
    width: 32,
    height: 32,
    border: `3px solid ${C.border}`,
    borderTopColor: C.brand,
    borderRadius: "50%",
    animation: "importPanelSpin 0.8s linear infinite",
  },

  /* Error */
  error: {
    background: "#FDF2F2",
    border: `1px solid ${C.neg}`,
    borderRadius: 8,
    padding: "12px 16px",
    fontSize: 14,
    fontFamily: FONT.body,
    color: C.neg,
    marginTop: 16,
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 12,
  },
  errorDismiss: {
    background: "none",
    border: "none",
    color: C.neg,
    cursor: "pointer",
    fontWeight: 600,
    fontSize: 13,
    whiteSpace: "nowrap" as const,
  },

  /* Preview */
  previewWrap: {
    marginTop: 4,
  },
  previewMeta: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    marginBottom: 12,
    flexWrap: "wrap" as const,
  },
  previewName: {
    fontFamily: FONT.body,
    fontWeight: 600,
    fontSize: 15,
    color: C.ink,
  },
  previewStat: {
    fontFamily: FONT.mono,
    fontSize: 13,
    color: C.inkLight,
    background: C.surface,
    padding: "2px 8px",
    borderRadius: 4,
  },
  tableWrap: {
    overflowX: "auto" as const,
    border: `1px solid ${C.border}`,
    borderRadius: 8,
    marginBottom: 16,
  },
  table: {
    width: "100%",
    borderCollapse: "collapse" as const,
    fontSize: 13,
    fontFamily: FONT.mono,
  },
  th: {
    textAlign: "left" as const,
    padding: "8px 12px",
    borderBottom: `1px solid ${C.border}`,
    background: C.surface,
    fontWeight: 600,
    color: C.ink,
    whiteSpace: "nowrap" as const,
  },
  td: {
    padding: "6px 12px",
    borderBottom: `1px solid ${C.border}`,
    color: C.inkLight,
    whiteSpace: "nowrap" as const,
    maxWidth: 200,
    overflow: "hidden",
    textOverflow: "ellipsis",
  },
  previewActions: {
    display: "flex",
    justifyContent: "flex-end",
    gap: 10,
  },
  cancelBtn: {
    padding: "8px 18px",
    border: `1px solid ${C.border}`,
    borderRadius: 6,
    background: C.white,
    color: C.inkLight,
    fontSize: 14,
    fontFamily: FONT.body,
    fontWeight: 500,
    cursor: "pointer",
  },
  confirmBtn: {
    padding: "8px 18px",
    border: "none",
    borderRadius: 6,
    background: C.brand,
    color: C.white,
    fontSize: 14,
    fontFamily: FONT.body,
    fontWeight: 600,
    cursor: "pointer",
  },

  /* Active imports list */
  importsList: {
    marginTop: 24,
    borderTop: `1px solid ${C.border}`,
    paddingTop: 16,
  },
  importsTitle: {
    margin: "0 0 12px",
    fontSize: 14,
    fontFamily: FONT.body,
    fontWeight: 600,
    color: C.ink,
  },
  importRow: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "8px 12px",
    background: C.surface,
    borderRadius: 6,
    marginBottom: 6,
  },
  importInfo: {
    display: "flex",
    flexDirection: "column" as const,
    gap: 2,
  },
  importName: {
    fontFamily: FONT.body,
    fontWeight: 500,
    fontSize: 14,
    color: C.ink,
  },
  importDetail: {
    fontFamily: FONT.mono,
    fontSize: 12,
    color: C.inkLight,
  },
  tableName: {
    background: "#e8efe8",
    padding: "1px 5px",
    borderRadius: 3,
    fontSize: 11,
  },
  removeBtn: {
    background: "none",
    border: "none",
    fontSize: 20,
    color: C.inkLight,
    cursor: "pointer",
    lineHeight: 1,
    padding: "0 4px",
  },
};

/* Inject keyframes for spinner */
if (typeof document !== "undefined") {
  const styleId = "import-panel-spin";
  if (!document.getElementById(styleId)) {
    const sheet = document.createElement("style");
    sheet.id = styleId;
    sheet.textContent = `@keyframes importPanelSpin { to { transform: rotate(360deg); } }`;
    document.head.appendChild(sheet);
  }
}

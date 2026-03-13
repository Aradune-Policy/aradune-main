import { createContext, useContext, useState, useCallback } from "react";
import type { ReactNode } from "react";

// ── Types ─────────────────────────────────────────────────────────────────

export interface ImportedFile {
  id: string;
  name: string;
  type: "csv" | "xlsx" | "json";
  columns: string[];
  rowCount: number;
  preview: Record<string, unknown>[];
  tableName: string;
  uploadedAt: Date;
}

export interface ReportSection {
  id: string;
  prompt: string;
  response: string;
  queries: string[];
  createdAt: Date;
}

export interface IntelligenceContext {
  question?: string;
  table?: string;
  state?: string;
  summary?: string;
}

export interface AraduneState {
  // Navigation
  selectedState: string | null;
  setSelectedState: (s: string | null) => void;

  // Intelligence sidebar
  intelligenceOpen: boolean;
  intelligenceContext: IntelligenceContext | null;
  openIntelligence: (ctx?: IntelligenceContext) => void;
  closeIntelligence: () => void;

  // User-imported data (session-scoped)
  importedFiles: ImportedFile[];
  addImportedFile: (f: ImportedFile) => void;
  removeImportedFile: (id: string) => void;

  // Report builder
  reportSections: ReportSection[];
  addReportSection: (s: ReportSection) => void;
  removeReportSection: (id: string) => void;
  reorderReportSections: (fromIndex: number, toIndex: number) => void;
  clearReport: () => void;

  // Demo mode
  demoMode: boolean;
}

// ── Context ───────────────────────────────────────────────────────────────

const AraduneCtx = createContext<AraduneState | null>(null);

export function useAradune(): AraduneState {
  const ctx = useContext(AraduneCtx);
  if (!ctx) throw new Error("useAradune must be used within AraduneProvider");
  return ctx;
}

// ── Provider ──────────────────────────────────────────────────────────────

// Check for ?demo=true in URL
const _isDemoMode = (() => {
  try {
    const params = new URLSearchParams(window.location.search);
    return params.get("demo") === "true";
  } catch { return false; }
})();

export function AraduneProvider({ children }: { children: ReactNode }) {
  const [selectedState, setSelectedState] = useState<string | null>(null);
  const [intelligenceOpen, setIntelligenceOpen] = useState(false);
  const [intelligenceContext, setIntelligenceContext] = useState<IntelligenceContext | null>(null);
  const [importedFiles, setImportedFiles] = useState<ImportedFile[]>([]);
  const [reportSections, setReportSections] = useState<ReportSection[]>([]);
  const demoMode = _isDemoMode;

  const openIntelligence = useCallback((ctx?: IntelligenceContext) => {
    if (ctx) setIntelligenceContext(ctx);
    setIntelligenceOpen(true);
  }, []);

  const closeIntelligence = useCallback(() => {
    setIntelligenceOpen(false);
  }, []);

  const addImportedFile = useCallback((f: ImportedFile) => {
    setImportedFiles(prev => [...prev, f]);
  }, []);

  const removeImportedFile = useCallback((id: string) => {
    setImportedFiles(prev => prev.filter(f => f.id !== id));
  }, []);

  const addReportSection = useCallback((s: ReportSection) => {
    setReportSections(prev => [...prev, s]);
  }, []);

  const removeReportSection = useCallback((id: string) => {
    setReportSections(prev => prev.filter(s => s.id !== id));
  }, []);

  const reorderReportSections = useCallback((fromIndex: number, toIndex: number) => {
    setReportSections(prev => {
      const next = [...prev];
      const [moved] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, moved);
      return next;
    });
  }, []);

  const clearReport = useCallback(() => {
    setReportSections([]);
  }, []);

  return (
    <AraduneCtx.Provider value={{
      selectedState, setSelectedState,
      intelligenceOpen, intelligenceContext, openIntelligence, closeIntelligence,
      importedFiles, addImportedFile, removeImportedFile,
      reportSections, addReportSection, removeReportSection, reorderReportSections, clearReport,
      demoMode,
    }}>
      {children}
    </AraduneCtx.Provider>
  );
}

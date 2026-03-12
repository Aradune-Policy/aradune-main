/**
 * IntelligenceChat.tsx — Aradune Intelligence unified interface.
 *
 * Full-page chat with progress bar, smart routing (general knowledge vs data),
 * response caching, markdown tables, query trace, and CSV export.
 * Powered by /api/intelligence/stream (SSE).
 */

import { useState, useRef, useEffect, useCallback } from "react";
import { C, FONT, SHADOW } from "../design";
import { useAradune } from "../context/AraduneContext";
import { getAuthHeaders } from "../lib/api";

const API = import.meta.env.VITE_API_URL || "";

// ── Types ───────────────────────────────────────────────────────────────

interface ToolCall {
  name: string;
  input?: Record<string, unknown>;
  rows?: number | null;
  ms?: number | null;
}

interface MessageMeta {
  tool_calls: ToolCall[];
  queries: string[];
  model: string;
  rounds: number;
  cached?: boolean;
}

interface ProgressState {
  pct: number;
  label: string;
}

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  meta?: MessageMeta;
  streaming?: boolean;
  progress?: ProgressState;
  error?: boolean;
}

// ── Starter Prompts ─────────────────────────────────────────────────────

const STARTERS = [
  {
    label: "Policy",
    prompt: "What is the CPRA requirement under 42 CFR 447.203 and when is it due?",
  },
  {
    label: "Rate Adequacy",
    prompt: "Which states pay below 50% of Medicare for primary care E/M codes?",
  },
  {
    label: "Cross-Dataset",
    prompt: "Show me states with the longest HCBS waitlists and their FMAP rates",
  },
  {
    label: "Drug Spending",
    prompt: "What are the top 10 drugs by Medicaid spending in 2023?",
  },
  {
    label: "Enrollment",
    prompt: "Compare Florida's Medicaid enrollment trend to the national average",
  },
  {
    label: "Workforce",
    prompt: "Which states have the most severe primary care HPSA designations and how do their Medicaid rates compare?",
  },
];

// ── Progress Bar ────────────────────────────────────────────────────────

function ProgressBar({ pct, label }: ProgressState) {
  return (
    <div style={{ padding: "16px 18px 12px" }}>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        marginBottom: 7,
      }}>
        <span style={{ fontSize: 12, color: C.inkLight, fontFamily: FONT.body }}>
          {label}
        </span>
        <span style={{ fontSize: 11, color: C.inkLight, fontFamily: FONT.mono }}>
          {pct}%
        </span>
      </div>
      <div style={{
        height: 3, background: C.border, borderRadius: 2, overflow: "hidden",
      }}>
        <div style={{
          height: "100%",
          background: C.brand,
          borderRadius: 2,
          width: `${pct}%`,
          transition: "width 0.6s ease-out",
        }} />
      </div>
    </div>
  );
}

// ── Markdown-lite renderer ──────────────────────────────────────────────

function renderMarkdown(text: string) {
  const lines = text.split("\n");
  const elements: React.ReactElement[] = [];
  let inTable = false;
  let tableRows: string[][] = [];
  let inCode = false;
  let codeLines: string[] = [];

  const flushTable = () => {
    if (tableRows.length < 2) return;
    const headers = tableRows[0];
    const dataStart = tableRows[1]?.every(c => /^[\s\-:|]+$/.test(c)) ? 2 : 1;
    const data = tableRows.slice(dataStart);
    elements.push(
      <div key={`tbl-${elements.length}`} style={{ overflowX: "auto", margin: "12px 0" }}>
        <table style={{ borderCollapse: "collapse", fontSize: 11, fontFamily: FONT.mono, width: "100%", minWidth: 320 }}>
          <thead>
            <tr>
              {headers.map((h, i) => (
                <th key={i} style={{
                  padding: "6px 10px", borderBottom: `2px solid ${C.border}`,
                  textAlign: "left", fontWeight: 600, color: C.ink, whiteSpace: "nowrap",
                }}>
                  {formatInline(h.trim())}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, ri) => (
              <tr key={ri} style={{ background: ri % 2 === 0 ? "transparent" : C.bg }}>
                {row.map((cell, ci) => (
                  <td key={ci} style={{
                    padding: "5px 10px", borderBottom: `1px solid ${C.border}`, color: C.ink,
                  }}>
                    {formatInline(cell.trim())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
    tableRows = [];
  };

  const flushCode = () => {
    elements.push(
      <pre key={`code-${elements.length}`} style={{
        background: "#1a1a2e", color: "#e0e0e0", padding: "12px 16px", borderRadius: 6,
        fontSize: 11, fontFamily: FONT.mono, overflow: "auto", margin: "8px 0",
      }}>
        {codeLines.join("\n")}
      </pre>
    );
    codeLines = [];
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Code blocks
    if (line.startsWith("```")) {
      if (inCode) { inCode = false; flushCode(); }
      else { if (inTable) { inTable = false; flushTable(); } inCode = true; }
      continue;
    }
    if (inCode) { codeLines.push(line); continue; }

    // Tables
    if (line.includes("|") && line.trim().startsWith("|")) {
      if (!inTable) inTable = true;
      const cells = line.split("|").slice(1, -1);
      tableRows.push(cells.map(c => c.trim()));
      continue;
    } else if (inTable) {
      inTable = false;
      flushTable();
    }

    // Horizontal rule
    if (/^---+$/.test(line.trim())) {
      elements.push(<hr key={`hr-${i}`} style={{ border: "none", borderTop: `1px solid ${C.border}`, margin: "12px 0" }} />);
      continue;
    }

    // Headers
    if (line.startsWith("### ")) {
      elements.push(<h4 key={`h-${i}`} style={{ fontSize: 13, fontWeight: 700, color: C.ink, margin: "16px 0 6px" }}>{formatInline(line.slice(4))}</h4>);
      continue;
    }
    if (line.startsWith("## ")) {
      elements.push(<h3 key={`h-${i}`} style={{ fontSize: 14, fontWeight: 700, color: C.ink, margin: "18px 0 8px" }}>{formatInline(line.slice(3))}</h3>);
      continue;
    }

    // List items
    if (line.match(/^[-*] /)) {
      elements.push(
        <div key={`li-${i}`} style={{ paddingLeft: 16, position: "relative", margin: "2px 0", fontSize: 13, lineHeight: 1.6, color: C.ink }}>
          <span style={{ position: "absolute", left: 4, color: C.brand }}>&#183;</span>
          <span>{formatInline(line.slice(2))}</span>
        </div>
      );
      continue;
    }
    if (line.match(/^\d+\. /)) {
      const num = line.match(/^(\d+)\./)?.[1];
      elements.push(
        <div key={`ol-${i}`} style={{ paddingLeft: 20, position: "relative", margin: "2px 0", fontSize: 13, lineHeight: 1.6, color: C.ink }}>
          <span style={{ position: "absolute", left: 0, color: C.brand, fontWeight: 600, fontSize: 12 }}>{num}.</span>
          <span>{formatInline(line.replace(/^\d+\.\s*/, ""))}</span>
        </div>
      );
      continue;
    }

    // Empty line = paragraph break
    if (!line.trim()) {
      elements.push(<div key={`br-${i}`} style={{ height: 8 }} />);
      continue;
    }

    // Normal text
    elements.push(
      <p key={`p-${i}`} style={{ margin: "2px 0", fontSize: 13, lineHeight: 1.6, color: C.ink }}>
        {formatInline(line)}
      </p>
    );
  }

  if (inTable) flushTable();
  if (inCode) flushCode();

  return <>{elements}</>;
}

/**
 * Format inline markdown: **bold**, `code`, [links](url).
 * Returns React elements instead of dangerouslySetInnerHTML.
 */
function formatInline(text: string): React.ReactNode {
  // Split on bold, inline code, and links
  const parts: React.ReactNode[] = [];
  let remaining = text;
  let key = 0;

  while (remaining.length > 0) {
    // Find the earliest match
    const boldMatch = remaining.match(/\*\*(.+?)\*\*/);
    const codeMatch = remaining.match(/`([^`]+)`/);
    const linkMatch = remaining.match(/\[([^\]]+)\]\(([^)]+)\)/);

    const matches: { type: string; index: number; full: string; content: string; url?: string }[] = [];
    if (boldMatch?.index !== undefined) matches.push({ type: "bold", index: boldMatch.index, full: boldMatch[0], content: boldMatch[1] });
    if (codeMatch?.index !== undefined) matches.push({ type: "code", index: codeMatch.index, full: codeMatch[0], content: codeMatch[1] });
    if (linkMatch?.index !== undefined) matches.push({ type: "link", index: linkMatch.index, full: linkMatch[0], content: linkMatch[1], url: linkMatch[2] });

    if (matches.length === 0) {
      parts.push(remaining);
      break;
    }

    // Take the earliest match
    matches.sort((a, b) => a.index - b.index);
    const m = matches[0];

    // Add text before match
    if (m.index > 0) {
      parts.push(remaining.slice(0, m.index));
    }

    // Add formatted element
    if (m.type === "bold") {
      parts.push(<strong key={key++}>{m.content}</strong>);
    } else if (m.type === "code") {
      parts.push(
        <code key={key++} style={{
          background: "#f0f2f0", padding: "1px 5px", borderRadius: 3,
          fontSize: "0.9em", fontFamily: FONT.mono, color: C.ink,
        }}>
          {m.content}
        </code>
      );
    } else if (m.type === "link") {
      parts.push(
        <a key={key++} href={m.url} target="_blank" rel="noopener noreferrer" style={{
          color: C.brand, textDecoration: "underline",
        }}>
          {m.content}
        </a>
      );
    }

    remaining = remaining.slice(m.index + m.full.length);
  }

  return parts.length === 1 && typeof parts[0] === "string" ? parts[0] : <>{parts}</>;
}

// ── Query Trace ─────────────────────────────────────────────────────────

function QueryTrace({ meta }: { meta: MessageMeta }) {
  const [open, setOpen] = useState(false);
  const count = meta.queries.length;
  if (count === 0 && meta.tool_calls.length === 0) return null;

  return (
    <div style={{ marginTop: 8 }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
          padding: "3px 8px", fontSize: 10, color: C.inkLight, cursor: "pointer",
          fontFamily: FONT.mono,
        }}
      >
        {open ? "Hide" : "Show"} {count} quer{count === 1 ? "y" : "ies"} &middot; {meta.rounds} round{meta.rounds === 1 ? "" : "s"}
        {meta.cached && " (cached)"}
      </button>
      {open && (
        <div style={{ marginTop: 6, padding: "8px 12px", background: "#f8f9f8", borderRadius: 4, border: `1px solid ${C.border}` }}>
          {meta.tool_calls.map((tc, i) => (
            <div key={i} style={{ marginBottom: 6, fontSize: 11, fontFamily: FONT.mono }}>
              <span style={{ color: C.brand, fontWeight: 600 }}>{tc.name}</span>
              {tc.rows != null && <span style={{ color: C.inkLight }}> {tc.rows} rows</span>}
              {tc.ms != null && <span style={{ color: C.inkLight }}> ({tc.ms}ms)</span>}
            </div>
          ))}
          {meta.queries.map((q, i) => (
            <pre key={`q-${i}`} style={{
              background: "#1a1a2e", color: "#a0d0c0", padding: "8px 10px", borderRadius: 4,
              fontSize: 10, fontFamily: FONT.mono, overflow: "auto", marginTop: 4,
              whiteSpace: "pre-wrap", wordBreak: "break-all",
            }}>
              {q}
            </pre>
          ))}
        </div>
      )}
    </div>
  );
}

// ── CSV Export ───────────────────────────────────────────────────────────

function exportResponseCSV(content: string) {
  const lines = content.split("\n");
  const tableLines = lines.filter(l => l.includes("|") && l.trim().startsWith("|"));
  if (tableLines.length < 2) return;

  const rows = tableLines.map(l => l.split("|").slice(1, -1).map(c => c.trim()));
  const filtered = rows.filter(r => !r.every(c => /^[\s\-:|]+$/.test(c)));

  const csv = filtered.map(r => r.map(c => `"${c.replace(/"/g, '""')}"`).join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `aradune-export-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Error classification ─────────────────────────────────────────────────

function classifyError(status: number, body: string): { message: string; retryable: boolean } {
  if (status === 503 || body.includes("missing API key") || body.includes("not configured")) {
    return { message: "Aradune is not configured on the server. The API key may be missing.", retryable: false };
  }
  if (status === 429 || body.includes("rate limit") || body.includes("Rate limit")) {
    return { message: "Rate limit reached. Please wait a moment and try again.", retryable: true };
  }
  if (status === 502 || body.includes("Claude API error")) {
    return { message: "The AI service is temporarily unavailable. Please try again.", retryable: true };
  }
  if (status === 504 || body.includes("timeout") || body.includes("Timeout")) {
    return { message: "The request timed out. Try a simpler question or try again.", retryable: true };
  }
  if (status === 422) {
    return { message: "Could not process this query. Try rephrasing your question.", retryable: false };
  }
  if (status >= 500) {
    return { message: "Server error. Please try again in a moment.", retryable: true };
  }
  return { message: body || "An unexpected error occurred.", retryable: true };
}

// ── Main Component ──────────────────────────────────────────────────────

export default function IntelligenceChat() {
  const { importedFiles, addImportedFile, removeImportedFile, addReportSection } = useAradune();
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [autoSent, setAutoSent] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const scrollToBottom = useCallback(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  useEffect(() => { scrollToBottom(); }, [messages, scrollToBottom]);

  // Auto-resize textarea
  useEffect(() => {
    if (inputRef.current) {
      inputRef.current.style.height = "auto";
      inputRef.current.style.height = Math.min(inputRef.current.scrollHeight, 120) + "px";
    }
  }, [input]);

  // Auto-send query from URL param (homepage → Intelligence passthrough)
  useEffect(() => {
    if (autoSent) return;
    const hash = window.location.hash;
    const match = hash.match(/[?&]q=([^&]*)/);
    if (match) {
      const query = decodeURIComponent(match[1]);
      if (query) {
        setAutoSent(true);
        window.location.hash = "#/intelligence";
        setTimeout(() => sendMessage(query), 100);
      }
    }
  }, [autoSent]); // eslint-disable-line react-hooks/exhaustive-deps

  // Handle file upload for contextual data
  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const text = reader.result as string;
      const lines = text.split("\n").filter(l => l.trim());
      if (lines.length < 2) return;
      const columns = lines[0].split(",").map(c => c.trim().replace(/^"|"$/g, ""));
      const preview = lines.slice(1, 6).map(line => {
        const vals = line.split(",").map(v => v.trim().replace(/^"|"$/g, ""));
        const obj: Record<string, unknown> = {};
        columns.forEach((col, i) => { obj[col] = vals[i] || ""; });
        return obj;
      });
      const tableName = `user_${file.name.replace(/\.[^.]+$/, "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase()}`;
      addImportedFile({
        id: crypto.randomUUID(),
        name: file.name,
        type: file.name.endsWith(".csv") ? "csv" : file.name.endsWith(".json") ? "json" : "xlsx",
        columns,
        rowCount: lines.length - 1,
        preview,
        tableName,
        uploadedAt: new Date(),
      });
    };
    reader.readAsText(file);
    e.target.value = "";
  };

  const sendMessage = async (text: string) => {
    if (!text.trim() || loading) return;
    const userMsg: ChatMessage = { role: "user", content: text.trim() };
    const assistantMsg: ChatMessage = { role: "assistant", content: "", streaming: true };

    setMessages(prev => [...prev, userMsg, assistantMsg]);
    setInput("");
    setLoading(true);

    // Build history for API
    const history = messages.map(m => ({ role: m.role, content: m.content }));

    try {
      const authHeaders = await getAuthHeaders();
      const res = await fetch(`${API}/api/intelligence/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify({
          message: text.trim(),
          history,
          imported_files: importedFiles.map(f => ({
            table_name: f.tableName,
            filename: f.name,
            columns: f.columns,
            row_count: f.rowCount,
          })),
        }),
      });

      if (!res.ok) {
        const errBody = await res.text();
        const classified = classifyError(res.status, errBody);
        setMessages(prev => {
          const updated = [...prev];
          const last = updated[updated.length - 1];
          last.content = classified.message;
          last.streaming = false;
          last.error = true;
          last.progress = undefined;
          return [...updated];
        });
        setLoading(false);
        return;
      }

      const reader = res.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let fullText = "";
      let meta: MessageMeta | undefined;

      while (reader) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const events = buffer.split("\n\n");
        buffer = events.pop() || "";

        for (const raw of events) {
          const eventLines = raw.split("\n");
          let eventType = "";
          let data = "";

          for (const line of eventLines) {
            if (line.startsWith("event: ")) eventType = line.slice(7);
            if (line.startsWith("data: ")) data = line.slice(6);
          }

          if (!eventType || !data) continue;

          try {
            const parsed = JSON.parse(data);

            if (eventType === "progress") {
              setMessages(prev => {
                const updated = [...prev];
                const last = updated[updated.length - 1];
                last.progress = { pct: parsed.pct, label: parsed.label };
                return [...updated];
              });
            } else if (eventType === "token") {
              fullText += parsed.text;
              setMessages(prev => {
                const updated = [...prev];
                const last = updated[updated.length - 1];
                last.content = fullText;
                return [...updated];
              });
            } else if (eventType === "tool_result") {
              // Just let progress events handle the UI
            } else if (eventType === "metadata") {
              meta = parsed as MessageMeta;
            } else if (eventType === "error") {
              fullText += `\n\n${parsed.message}`;
              setMessages(prev => {
                const updated = [...prev];
                const last = updated[updated.length - 1];
                last.content = fullText;
                last.error = true;
                last.progress = undefined;
                return [...updated];
              });
            }
          } catch {
            // skip malformed events
          }
        }
      }

      // Finalize
      setMessages(prev => {
        const updated = [...prev];
        const last = updated[updated.length - 1];
        last.content = fullText || "I wasn't able to generate a response. Please try rephrasing your question.";
        last.streaming = false;
        last.progress = undefined;
        last.meta = meta;
        if (!fullText) last.error = true;
        return [...updated];
      });
    } catch (err) {
      const isTimeout = err instanceof Error && (err.message.includes("timeout") || err.message.includes("aborted"));
      setMessages(prev => {
        const updated = [...prev];
        const last = updated[updated.length - 1];
        last.content = isTimeout
          ? "The request timed out. This can happen with complex queries. Try again or simplify your question."
          : `Connection error: ${err instanceof Error ? err.message : "Unable to reach the server."}`;
        last.streaming = false;
        last.error = true;
        last.progress = undefined;
        return [...updated];
      });
    }

    setLoading(false);
  };

  const retryMessage = useCallback((messageIndex: number) => {
    const userMsg = messages[messageIndex - 1];
    if (!userMsg || userMsg.role !== "user") return;
    setMessages(prev => prev.slice(0, messageIndex));
    setTimeout(() => sendMessage(userMsg.content), 50);
  }, [messages]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  const showStarters = messages.length === 0;

  return (
    <div style={{
      display: "flex", flexDirection: "column",
      height: "calc(100dvh - 48px)", /* dvh for mobile browser chrome */
      maxWidth: 820, margin: "0 auto", padding: "0 16px",
    }}>
      {/* Messages area */}
      <div style={{ flex: 1, overflow: "auto", padding: "20px 0" }}>
        {showStarters && (
          <div style={{
            display: "flex", flexDirection: "column", alignItems: "center",
            justifyContent: "center", minHeight: "50vh", textAlign: "center",
            padding: "0 20px",
          }}>
            <img src="/assets/icon-bot.png" alt="" style={{ width: 36, height: 36, borderRadius: 8, marginBottom: 16, opacity: 0.8 }}
              onError={e => { (e.target as HTMLImageElement).style.display = "none"; }} />
            <h1 style={{
              fontSize: 22, fontWeight: 700, color: C.ink, margin: "0 0 6px",
              fontFamily: FONT.body,
            }}>
              Aradune
            </h1>
            <p style={{
              fontSize: 13, color: C.inkLight, margin: "0 0 32px", lineHeight: 1.5,
              maxWidth: 440,
            }}>
              Ask anything about Medicaid. Policy questions, data queries, state comparisons, or cross-dataset analysis, backed by 569+ tables and 305M+ rows.
            </p>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8, justifyContent: "center" }}>
              {STARTERS.slice(0, 3).map(s => (
                <button
                  key={s.label}
                  onClick={() => sendMessage(s.prompt)}
                  style={{
                    background: "none", border: `1px solid ${C.border}`, borderRadius: 20,
                    padding: "7px 16px", cursor: "pointer",
                    fontSize: 12, color: C.inkLight, fontFamily: FONT.body,
                    transition: "all .15s",
                  }}
                  onMouseEnter={e => {
                    e.currentTarget.style.borderColor = C.brand;
                    e.currentTarget.style.color = C.brand;
                  }}
                  onMouseLeave={e => {
                    e.currentTarget.style.borderColor = C.border;
                    e.currentTarget.style.color = C.inkLight;
                  }}
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} style={{
            marginBottom: 16, display: "flex", flexDirection: "column",
            alignItems: msg.role === "user" ? "flex-end" : "flex-start",
          }}>
            {msg.role === "user" ? (
              <div style={{
                background: C.brand, color: C.white, borderRadius: "16px 16px 4px 16px",
                padding: "10px 14px", maxWidth: "80%", fontSize: 13, lineHeight: 1.5,
              }}>
                {msg.content}
              </div>
            ) : (
              <div style={{ maxWidth: "100%", width: "100%" }}>
                {/* Progress bar (during streaming) */}
                {msg.streaming && msg.progress && msg.progress.pct < 100 && (
                  <div style={{
                    background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
                    marginBottom: msg.content ? 8 : 0,
                    overflow: "hidden",
                  }}>
                    <ProgressBar pct={msg.progress.pct} label={msg.progress.label} />
                  </div>
                )}

                {/* Error message */}
                {msg.error && msg.content && !msg.streaming ? (
                  <div style={{
                    background: "#FFF5F5", border: "1px solid #FED7D7", borderRadius: 8,
                    padding: "14px 18px",
                  }}>
                    <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
                      <span style={{ fontSize: 16, lineHeight: 1, flexShrink: 0, marginTop: 1, color: C.neg }}>!</span>
                      <div style={{ flex: 1 }}>
                        <p style={{ margin: 0, fontSize: 13, lineHeight: 1.5, color: C.ink }}>{msg.content}</p>
                        <button
                          onClick={() => retryMessage(i)}
                          style={{
                            marginTop: 10, background: C.brand, color: C.white, border: "none",
                            borderRadius: 6, padding: "6px 14px", fontSize: 12, fontWeight: 600,
                            cursor: "pointer", fontFamily: FONT.body, transition: "background .15s",
                          }}
                          onMouseEnter={e => { e.currentTarget.style.background = "#1e5a3a"; }}
                          onMouseLeave={e => { e.currentTarget.style.background = C.brand; }}
                        >
                          Retry
                        </button>
                      </div>
                    </div>
                  </div>
                ) : msg.content ? (
                  <div style={{
                    background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
                    padding: "14px 18px",
                  }}>
                    {renderMarkdown(msg.content)}

                    {/* Actions + query trace (after streaming completes) */}
                    {!msg.streaming && msg.content && (
                      <div style={{
                        marginTop: 12, display: "flex", gap: 8, flexWrap: "wrap",
                        alignItems: "flex-start",
                      }}>
                        <button
                          onClick={() => navigator.clipboard.writeText(msg.content)}
                          style={{
                            background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
                            padding: "3px 8px", fontSize: 10, color: C.inkLight, cursor: "pointer",
                            fontFamily: FONT.mono,
                          }}
                        >
                          Copy
                        </button>
                        {msg.content.includes("|") && (
                          <button
                            onClick={() => exportResponseCSV(msg.content)}
                            style={{
                              background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
                              padding: "3px 8px", fontSize: 10, color: C.inkLight, cursor: "pointer",
                              fontFamily: FONT.mono,
                            }}
                          >
                            Export CSV
                          </button>
                        )}
                        <button
                          onClick={() => {
                            const userMsgContent = messages[i - 1];
                            addReportSection({
                              id: crypto.randomUUID(),
                              prompt: userMsgContent?.content || "",
                              response: msg.content,
                              queries: msg.meta?.queries || [],
                              createdAt: new Date(),
                            });
                          }}
                          style={{
                            background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
                            padding: "3px 8px", fontSize: 10, color: C.inkLight, cursor: "pointer",
                            fontFamily: FONT.mono,
                          }}
                        >
                          + Report
                        </button>
                        {msg.meta?.cached && (
                          <span style={{
                            fontSize: 10, color: C.inkLight, fontFamily: FONT.mono,
                            padding: "3px 8px", border: `1px solid ${C.border}`, borderRadius: 4,
                            background: "#f0faf0",
                          }}>
                            instant (cached)
                          </span>
                        )}
                        {msg.meta && <QueryTrace meta={msg.meta} />}
                      </div>
                    )}
                  </div>
                ) : msg.streaming && !msg.progress ? (
                  /* Fallback: very brief loading before first progress event arrives */
                  <div style={{ fontSize: 12, color: C.inkLight, padding: "8px 0" }}>
                    Connecting...
                  </div>
                ) : null}
              </div>
            )}
          </div>
        ))}

        <div ref={bottomRef} />
      </div>

      {/* Input bar */}
      <div style={{
        borderTop: `1px solid ${C.border}`,
        padding: "12px 0 calc(16px + env(safe-area-inset-bottom, 0px))",
        background: C.bg,
      }}>
        {/* Imported files pills */}
        {importedFiles.length > 0 && (
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 8 }}>
            {importedFiles.map(f => (
              <span key={f.id} style={{
                display: "inline-flex", alignItems: "center", gap: 4,
                padding: "3px 10px", borderRadius: 12, fontSize: 10,
                background: `${C.brand}10`, border: `1px solid ${C.brand}30`,
                color: C.brand, fontFamily: FONT.mono,
              }}>
                {f.name} ({f.rowCount} rows)
                <button onClick={() => removeImportedFile(f.id)} style={{
                  background: "none", border: "none", cursor: "pointer",
                  color: C.inkLight, fontSize: 12, padding: 0, lineHeight: 1,
                }}>x</button>
              </span>
            ))}
          </div>
        )}
        <div style={{
          display: "flex", gap: 8, alignItems: "flex-end",
          background: C.white, border: `1px solid ${C.border}`, borderRadius: 12,
          padding: "8px 12px", boxShadow: SHADOW,
        }}>
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv,.xlsx,.json"
            onChange={handleFileUpload}
            style={{ display: "none" }}
          />
          <button
            onClick={() => fileInputRef.current?.click()}
            title="Upload data (CSV, Excel, JSON)"
            style={{
              background: "none", border: "none", cursor: "pointer",
              padding: "2px 4px", fontSize: 16, color: C.inkLight,
              transition: "color .15s", flexShrink: 0,
            }}
            onMouseEnter={e => { e.currentTarget.style.color = C.brand; }}
            onMouseLeave={e => { e.currentTarget.style.color = C.inkLight; }}
          >
            +
          </button>
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about Medicaid..."
            disabled={loading}
            rows={1}
            style={{
              flex: 1, border: "none", outline: "none", resize: "none",
              fontSize: 13, fontFamily: FONT.body, color: C.ink,
              background: "transparent", lineHeight: 1.5,
              minHeight: 20, maxHeight: 120,
            }}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={loading || !input.trim()}
            style={{
              background: loading || !input.trim() ? C.border : C.brand,
              color: C.white, border: "none", borderRadius: 8,
              padding: "6px 14px", fontSize: 12, fontWeight: 600,
              cursor: loading || !input.trim() ? "default" : "pointer",
              fontFamily: FONT.body, whiteSpace: "nowrap",
              transition: "background .15s",
            }}
          >
            {loading ? "..." : "Send"}
          </button>
        </div>
        <div style={{
          fontSize: 10, color: C.inkLight, textAlign: "center", marginTop: 6,
        }}>
          569+ tables &middot; 305M+ rows &middot; Shift+Enter for new line
        </div>
      </div>
    </div>
  );
}

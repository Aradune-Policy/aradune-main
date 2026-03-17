import { useState, useRef, useEffect, useCallback } from "react";
import { C, FONT, SHADOW_LG } from "../design";
import { useAradune } from "../context/AraduneContext";
import { getAuthHeaders, API_BASE } from "../lib/api";

const API = API_BASE;

interface ToolStatus {
  name: string;
  input: string;
  rowCount?: number;
  queryMs?: number;
  done: boolean;
}

interface PanelMessage {
  role: "user" | "assistant";
  content: string;
  toolStatuses?: ToolStatus[];
}

export default function IntelligencePanel() {
  const { intelligenceOpen, intelligenceContext, closeIntelligence, addReportSection } = useAradune();
  const [messages, setMessages] = useState<PanelMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [streaming, setStreaming] = useState("");
  const [toolStatuses, setToolStatuses] = useState<ToolStatus[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll on new content
  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [messages, streaming]);

  // Focus input when panel opens
  useEffect(() => {
    if (intelligenceOpen && inputRef.current) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [intelligenceOpen]);

  // Pre-fill context question
  useEffect(() => {
    if (intelligenceOpen && intelligenceContext?.question && messages.length === 0) {
      setInput(intelligenceContext.question);
    }
  }, [intelligenceOpen, intelligenceContext, messages.length]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || loading) return;
    const userMsg = text.trim();
    setInput("");
    setMessages(prev => [...prev, { role: "user", content: userMsg }]);
    setLoading(true);
    setStreaming("");
    setToolStatuses([]);

    try {
      const body: Record<string, unknown> = { message: userMsg };
      const context: Record<string, string> = {};
      if (intelligenceContext?.state) context.state = intelligenceContext.state;
      if (intelligenceContext?.table) context.table = intelligenceContext.table;
      if (intelligenceContext?.summary) context.summary = intelligenceContext.summary;
      if (Object.keys(context).length > 0) body.context = context;

      const authHeaders = await getAuthHeaders();
      const res = await fetch(`${API}/api/intelligence/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify(body),
      });

      if (!res.ok || !res.body) {
        setMessages(prev => [...prev, { role: "assistant", content: "Error: Could not reach Aradune." }]);
        setLoading(false);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      let fullText = "";
      const tools: ToolStatus[] = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        const lines = buf.split("\n");
        buf = lines.pop() || "";

        for (const line of lines) {
          if (line.startsWith("event: ")) {
            const evtType = line.slice(7).trim();
            const nextLine = lines[lines.indexOf(line) + 1];
            if (nextLine?.startsWith("data: ")) {
              try {
                const data = JSON.parse(nextLine.slice(6));
                if (evtType === "token") {
                  fullText += data.text || "";
                  setStreaming(fullText);
                } else if (evtType === "tool_call") {
                  tools.push({ name: data.name, input: data.input || "", done: false });
                  setToolStatuses([...tools]);
                } else if (evtType === "tool_result") {
                  const last = tools[tools.length - 1];
                  if (last) {
                    last.done = true;
                    last.rowCount = data.row_count;
                    last.queryMs = data.query_ms;
                    setToolStatuses([...tools]);
                  }
                } else if (evtType === "error") {
                  fullText += `\n\n**Error:** ${data.error || "Unknown error"}`;
                  setStreaming(fullText);
                }
              } catch { /* skip malformed */ }
            }
          }
        }
      }

      setMessages(prev => [...prev, { role: "assistant", content: fullText, toolStatuses: [...tools] }]);
      setStreaming("");
      setToolStatuses([]);
    } catch (err) {
      setMessages(prev => [...prev, { role: "assistant", content: `Error: ${err instanceof Error ? err.message : "Unknown"}` }]);
    }
    setLoading(false);
  }, [loading, intelligenceContext]);

  const handleAddToReport = useCallback((msg: PanelMessage) => {
    const userMsg = messages[messages.indexOf(msg) - 1];
    addReportSection({
      id: crypto.randomUUID(),
      prompt: userMsg?.content || "",
      response: msg.content,
      queries: msg.toolStatuses?.filter(t => t.name === "query_database").map(t => t.input) || [],
      createdAt: new Date(),
    });
  }, [messages, addReportSection]);

  if (!intelligenceOpen) return null;

  return (
    <div style={{
      position: "fixed", top: 0, right: 0, bottom: 0, width: 420,
      background: C.white, borderLeft: `1px solid ${C.border}`,
      boxShadow: SHADOW_LG, zIndex: 200,
      display: "flex", flexDirection: "column",
    }}>
      {/* Header */}
      <div style={{
        padding: "12px 16px", borderBottom: `1px solid ${C.border}`,
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: C.brand, fontFamily: FONT.body }}>Aradune</div>
          {intelligenceContext?.state && (
            <div style={{ fontSize: 10, color: C.inkLight, marginTop: 2 }}>Context: {intelligenceContext.state}</div>
          )}
        </div>
        <button onClick={closeIntelligence} style={{
          background: "none", border: "none", cursor: "pointer",
          fontSize: 18, color: C.inkLight, padding: "4px 8px",
        }}>×</button>
      </div>

      {/* Messages */}
      <div ref={scrollRef} style={{ flex: 1, overflowY: "auto", padding: "12px 16px" }}>
        {messages.length === 0 && !streaming && (
          <div style={{ fontSize: 12, color: C.inkLight, textAlign: "center", padding: "40px 0" }}>
            Ask Aradune about this data.
          </div>
        )}
        {messages.map((m, i) => (
          <div key={i} style={{ marginBottom: 16 }}>
            <div style={{
              fontSize: 9, fontWeight: 600, color: m.role === "user" ? C.accent : C.brand,
              textTransform: "uppercase", letterSpacing: 1, marginBottom: 4, fontFamily: FONT.mono,
            }}>
              {m.role === "user" ? "You" : "Aradune"}
            </div>
            <div style={{
              fontSize: 12, lineHeight: 1.7, color: C.ink,
              whiteSpace: "pre-wrap", fontFamily: FONT.body,
            }}>
              {m.content}
            </div>
            {m.role === "assistant" && m.content && (
              <div style={{ display: "flex", gap: 6, marginTop: 6, alignItems: "center" }}>
                <button onClick={() => handleAddToReport(m)} style={{
                  background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
                  padding: "3px 8px", fontSize: 10, color: C.inkLight, cursor: "pointer",
                  fontFamily: FONT.mono,
                }}>
                  + Add to Report
                </button>
                <button onClick={() => {
                  fetch(`${API_BASE}/api/intelligence/feedback`, {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ feedback: "positive", conversation_id: "" }),
                  }).catch(() => {});
                  (event?.target as HTMLElement)?.closest?.("button")?.setAttribute("style", "opacity:0.3");
                }} style={{ background: "none", border: "none", cursor: "pointer", fontSize: 14, opacity: 0.5, padding: "2px 4px" }} title="Helpful">
                  👍
                </button>
                <button onClick={() => {
                  fetch(`${API_BASE}/api/intelligence/feedback`, {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ feedback: "negative", conversation_id: "" }),
                  }).catch(() => {});
                  (event?.target as HTMLElement)?.closest?.("button")?.setAttribute("style", "opacity:0.3");
                }} style={{ background: "none", border: "none", cursor: "pointer", fontSize: 14, opacity: 0.5, padding: "2px 4px" }} title="Not helpful">
                  👎
                </button>
              </div>
            )}
          </div>
        ))}

        {/* Streaming state */}
        {(loading || streaming) && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 9, fontWeight: 600, color: C.accent, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4, fontFamily: FONT.mono }}>
              Aradune
            </div>
            {toolStatuses.map((t, i) => (
              <div key={i} style={{
                fontSize: 10, color: C.inkLight, padding: "2px 0",
                fontFamily: FONT.mono, display: "flex", alignItems: "center", gap: 6,
              }}>
                <span style={{ color: t.done ? C.pos : C.accent }}>{t.done ? "✓" : "⟳"}</span>
                {t.name}
                {t.done && t.rowCount != null && <span>({t.rowCount} rows, {t.queryMs}ms)</span>}
              </div>
            ))}
            {streaming && (
              <div style={{ fontSize: 12, lineHeight: 1.7, color: C.ink, whiteSpace: "pre-wrap", marginTop: 4 }}>
                {streaming}
              </div>
            )}
            {loading && !streaming && toolStatuses.length === 0 && (
              <div style={{ fontSize: 11, color: C.inkLight }}>Thinking...</div>
            )}
          </div>
        )}
      </div>

      {/* Input */}
      <div style={{ padding: "12px 16px", borderTop: `1px solid ${C.border}` }}>
        <div style={{ display: "flex", gap: 8 }}>
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                sendMessage(input);
              }
            }}
            placeholder="Ask about this data..."
            rows={1}
            style={{
              flex: 1, resize: "none", border: `1px solid ${C.border}`,
              borderRadius: 6, padding: "8px 10px", fontSize: 12,
              fontFamily: FONT.body, outline: "none",
              minHeight: 36, maxHeight: 100,
            }}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={loading || !input.trim()}
            style={{
              background: C.brand, color: C.white, border: "none",
              borderRadius: 6, padding: "0 14px", fontSize: 12,
              fontWeight: 600, cursor: loading ? "wait" : "pointer",
              opacity: loading || !input.trim() ? 0.5 : 1,
            }}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

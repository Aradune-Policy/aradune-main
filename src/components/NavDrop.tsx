import { useState, useEffect, useRef, useCallback } from "react";
import { C, FONT, SHADOW_LG } from "../design";
import type { NavGroup } from "../types";

interface NavDropProps {
  group: NavGroup;
  route: string;
}

export default function NavDrop({ group, route }: NavDropProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isActive = group.tools.some(t => route === `/${t.id}`);

  const clearTimer = useCallback(() => {
    if (timer.current) { clearTimeout(timer.current); timer.current = null; }
  }, []);

  const handleEnter = useCallback(() => {
    clearTimer();
    setOpen(true);
  }, [clearTimer]);

  const handleLeave = useCallback(() => {
    clearTimer();
    timer.current = setTimeout(() => setOpen(false), 150);
  }, [clearTimer]);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        clearTimer();
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => {
      document.removeEventListener("mousedown", handler);
      clearTimer();
    };
  }, [clearTimer]);

  return (
    <div
      ref={ref}
      style={{ position: "relative", alignSelf: "stretch", display: "flex", alignItems: "center" }}
      onMouseEnter={handleEnter}
      onMouseLeave={handleLeave}
    >
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: isActive ? "rgba(46,107,74,0.06)" : "none",
          border: "none",
          borderRadius: 6,
          color: isActive ? C.brand : C.inkLight,
          fontSize: 11,
          fontFamily: FONT.body,
          fontWeight: isActive ? 600 : 400,
          cursor: "pointer",
          padding: "4px 8px",
          display: "flex",
          alignItems: "center",
          gap: 4,
          transition: "all .15s",
        }}
      >
        {group.label}
        <svg width="8" height="5" viewBox="0 0 8 5" style={{ opacity: 0.4, transition: "transform .15s", transform: open ? "rotate(180deg)" : "none" }}>
          <path d="M1 1l3 3 3-3" stroke="currentColor" strokeWidth="1.2" fill="none" />
        </svg>
      </button>
      {open && (
        <div style={{
          position: "absolute",
          top: "100%",
          right: 0,
          paddingTop: 4,
          zIndex: 200,
        }}>
        <div style={{
          minWidth: 240,
          maxWidth: "calc(100vw - 24px)",
          background: C.white,
          border: `1px solid ${C.border}`,
          borderRadius: 10,
          boxShadow: SHADOW_LG,
          padding: "4px 0",
        }}>
          {group.tools.map(t => {
            const active = route === `/${t.id}`;
            return (
              <a
                key={t.id}
                href={`#/${t.id}`}
                onClick={() => setOpen(false)}
                style={{
                  display: "block",
                  padding: "10px 16px",
                  textDecoration: "none",
                  transition: "background .1s",
                  background: active ? "rgba(46,107,74,0.04)" : "transparent",
                }}
                onMouseEnter={e => { e.currentTarget.style.background = C.surface; }}
                onMouseLeave={e => { e.currentTarget.style.background = active ? "rgba(46,107,74,0.04)" : "transparent"; }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{
                    fontSize: 13,
                    width: 24,
                    height: 24,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    borderRadius: 6,
                    background: `${t.color}0D`,
                    color: t.color,
                    flexShrink: 0,
                  }}>
                    {t.icon}
                  </span>
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, fontFamily: FONT.body }}>{t.name}</div>
                    <div style={{ fontSize: 10, color: C.inkLight, marginTop: 1, fontFamily: FONT.body }}>{t.tagline.substring(0, 55)}</div>
                  </div>
                  {t.status === "coming" && (
                    <span style={{ fontSize: 8, fontFamily: FONT.mono, color: C.inkLight, marginLeft: "auto", flexShrink: 0 }}>SOON</span>
                  )}
                </div>
              </a>
            );
          })}
        </div>
        </div>
      )}
    </div>
  );
}

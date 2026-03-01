import { useState, useEffect, useRef } from "react";
import { C, FONT } from "../design";
import { GLOSSARY } from "../data/glossary";

interface TermProps {
  children: React.ReactNode;
  term?: string;
}

export default function Term({ children, term }: TermProps) {
  const label = typeof children === "string" ? children : (term ?? "");
  const key = (term ?? label).toLowerCase();
  const def = Object.entries(GLOSSARY).find(([k]) => k.toLowerCase() === key)?.[1];
  const [show, setShow] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const [above, setAbove] = useState(false);

  useEffect(() => {
    if (show && ref.current) {
      setAbove(ref.current.getBoundingClientRect().top > 240);
    }
  }, [show]);

  if (!def) return <>{children}</>;

  return (
    <span
      ref={ref}
      style={{ position: "relative", display: "inline" }}
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
      onClick={() => setShow(!show)}
    >
      <span style={{ borderBottom: "1.5px dotted rgba(66,90,112,0.4)", cursor: "help", paddingBottom: 1 }}>
        {children}
      </span>
      {show && (
        <span style={{
          position: "absolute",
          [above ? "bottom" : "top"]: "calc(100% + 8px)",
          left: "50%",
          transform: "translateX(-50%)",
          background: C.ink,
          color: "#fff",
          padding: "12px 16px",
          borderRadius: 10,
          fontSize: 12,
          lineHeight: 1.6,
          width: 290,
          maxWidth: "85vw",
          boxShadow: "0 8px 30px rgba(0,0,0,.25)",
          zIndex: 1000,
          fontFamily: FONT.body,
          pointerEvents: "none",
        }}>
          <span style={{
            fontWeight: 600,
            color: "#7FD4A0",
            fontSize: 10,
            fontFamily: FONT.mono,
            display: "block",
            marginBottom: 4,
            letterSpacing: 0.5,
            textTransform: "uppercase",
          }}>
            {label}
          </span>
          {def}
        </span>
      )}
    </span>
  );
}

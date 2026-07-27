/**
 * FloatingWindow.jsx — Fenêtre flottante réutilisable (non bloquante).
 *
 * • Déplaçable par l'en-tête.
 * • Redimensionnable sur les 8 bords/coins (n, s, e, w + diagonales).
 * • Pas de fond assombri : plusieurs fenêtres peuvent se superposer et rester
 *   utilisables simultanément.
 * • `onFocus` (mousedown n'importe où) → le parent remonte son z-index pour la
 *   passer au-dessus des autres.
 */
import { useState, useRef, useEffect } from "react";
import { useThemeContext } from "../theme";
import { IcX } from "../icons";

export default function FloatingWindow({
  title, icon, subtitle, headerRight = null,
  z = 9500, onFocus, onClose,
  initialPos = { x: 140, y: 100 },
  initialSize = { w: 380, h: null },   // h:null = hauteur auto tant qu'on ne redimensionne pas
  minW = 280, minH = 200,
  children, bodyStyle = {},
}) {
  const C = useThemeContext();
  const winRef = useRef(null);
  const [pos, setPos] = useState(initialPos);
  const [size, setSize] = useState(initialSize);
  const act = useRef(null);   // { mode, sx, sy, px, py, w, h }

  // Mobile : la fenêtre devient une feuille pleine largeur (pas de drag/resize).
  const [isMobile, setIsMobile] = useState(() => typeof window !== "undefined" && window.innerWidth < 640);
  useEffect(() => {
    const onR = () => setIsMobile(window.innerWidth < 640);
    window.addEventListener("resize", onR);
    return () => window.removeEventListener("resize", onR);
  }, []);

  // Au montage : borner la position initiale dans le viewport (après mesure réelle).
  useEffect(() => {
    if (isMobile) return;
    const el = winRef.current; if (!el) return;
    const w = el.offsetWidth, h = el.offsetHeight;
    setPos(p => ({
      x: Math.max(6, Math.min(p.x, Math.max(6, window.innerWidth - w - 6))),
      y: Math.max(6, Math.min(p.y, Math.max(6, window.innerHeight - h - 6))),
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isMobile]);

  // Borne une position pour garder la fenêtre entièrement dans le viewport.
  const clampPos = (x, y) => {
    const el = winRef.current;
    const w = el?.offsetWidth || size.w || minW;
    const h = el?.offsetHeight || minH;
    const maxX = Math.max(6, window.innerWidth - w - 6);
    const maxY = Math.max(6, window.innerHeight - h - 6);
    return { x: Math.max(6, Math.min(x, maxX)), y: Math.max(6, Math.min(y, maxY)) };
  };

  useEffect(() => {
    const move = (e) => {
      const a = act.current; if (!a) return;
      const dx = e.clientX - a.sx, dy = e.clientY - a.sy;
      if (a.mode === "drag") { setPos(clampPos(a.px + dx, a.py + dy)); return; }
      let x = a.px, y = a.py, w = a.w, h = a.h;
      const m = a.mode;
      if (m.includes("e")) w = Math.max(minW, a.w + dx);
      if (m.includes("s")) h = Math.max(minH, a.h + dy);
      if (m.includes("w")) { w = Math.max(minW, a.w - dx); x = a.px + (a.w - w); }
      if (m.includes("n")) { h = Math.max(minH, a.h - dy); y = a.py + (a.h - h); }
      setPos({ x, y }); setSize({ w, h });
    };
    const up = () => { if (act.current) { act.current = null; document.body.style.userSelect = ""; } };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
    return () => { window.removeEventListener("mousemove", move); window.removeEventListener("mouseup", up); };
  }, [minW, minH]);

  const begin = (mode) => (e) => {
    e.preventDefault();
    onFocus?.();
    const rect = winRef.current?.getBoundingClientRect();
    act.current = {
      mode, sx: e.clientX, sy: e.clientY, px: pos.x, py: pos.y,
      w: size.w ?? rect?.width ?? minW,
      h: size.h ?? rect?.height ?? minH,
    };
    document.body.style.userSelect = "none";
  };

  const H = { position: "absolute", zIndex: 12 };
  const handles = [
    { m: "n",  s: { top: -3, left: 14, right: 14, height: 8, cursor: "ns-resize" } },
    { m: "s",  s: { bottom: -3, left: 14, right: 14, height: 8, cursor: "ns-resize" } },
    { m: "w",  s: { left: -3, top: 14, bottom: 14, width: 8, cursor: "ew-resize" } },
    { m: "e",  s: { right: -3, top: 14, bottom: 14, width: 8, cursor: "ew-resize" } },
    { m: "nw", s: { top: -4, left: -4, width: 15, height: 15, cursor: "nwse-resize" } },
    { m: "ne", s: { top: -4, right: -4, width: 15, height: 15, cursor: "nesw-resize" } },
    { m: "sw", s: { bottom: -4, left: -4, width: 15, height: 15, cursor: "nesw-resize" } },
    { m: "se", s: { bottom: -4, right: -4, width: 15, height: 15, cursor: "nwse-resize" } },
  ];

  const frame = isMobile
    ? { position: "fixed", left: 8, right: 8, bottom: 8, top: "auto", width: "auto",
        height: "auto", maxHeight: "85vh", maxWidth: "none" }
    : { position: "fixed", left: pos.x, top: pos.y, width: size.w,
        height: size.h ?? "auto", maxHeight: "92vh", maxWidth: "96vw" };

  return (
    <div ref={winRef} onMouseDown={() => onFocus?.()} style={{
      ...frame,
      background: C.card, borderRadius: 14, border: `0.5px solid ${C.bdr}`,
      boxShadow: "0 16px 48px rgba(0,0,0,.4)", zIndex: z,
      display: "flex", flexDirection: "column", overflow: "hidden",
    }}>
      {!isMobile && handles.map(h => <div key={h.m} onMouseDown={begin(h.m)} style={{ ...H, ...h.s }} />)}

      {/* En-tête (déplaçable sur desktop ; fixe sur mobile) */}
      <div onMouseDown={isMobile ? undefined : begin("drag")} style={{ display: "flex", alignItems: "center", gap: 9, padding: "9px 11px", borderBottom: `0.5px solid ${C.bdr}`, cursor: isMobile ? "default" : "move", userSelect: "none", flexShrink: 0 }}>
        {icon && <span style={{ display: "flex", flexShrink: 0, color: C.acc }}>{icon}</span>}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 12.5, fontWeight: 600, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{title}</div>
          {subtitle && <div style={{ fontSize: 10, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{subtitle}</div>}
        </div>
        {headerRight}
        <button onMouseDown={e => e.stopPropagation()} onClick={onClose} title="Fermer" style={{ background: "transparent", border: "none", color: C.dim, cursor: "pointer", display: "flex", flexShrink: 0, padding: 2 }}><IcX size={16}/></button>
      </div>

      {/* Corps */}
      <div style={{ flex: 1, overflowY: "auto", padding: 13, ...bodyStyle }}>
        {children}
      </div>
    </div>
  );
}

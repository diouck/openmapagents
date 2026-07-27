/**
 * SearchPalette.jsx — Palette de recherche (style Spotlight) — Ctrl/⌘+K.
 *
 * Cherche parmi TOUS les indicateurs et outils (menuSearch). Navigable au
 * clavier (↑↓ Entrée, Échap). Un résultat → onSelect({kind,id}) : le parent
 * ouvre l'IndicatorModal (indicator) ou active l'outil (tool).
 */
import { useState, useMemo, useRef, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";
import { IcSearch, IcX } from "../icons";
import { buildSearchIndex, searchMenu } from "../utils/menuSearch";

export default function SearchPalette({ onSelect, onClose }) {
  const C = useThemeContext();
  const [q, setQ] = useState("");
  const [sel, setSel] = useState(0);
  const inputRef = useRef(null);
  const listRef = useRef(null);
  const index = useMemo(() => buildSearchIndex(), []);
  const results = useMemo(() => searchMenu(index, q).slice(0, 50), [index, q]);

  useEffect(() => { inputRef.current?.focus(); }, []);
  useEffect(() => { setSel(0); }, [q]);
  // Garder l'élément sélectionné visible
  useEffect(() => {
    const el = listRef.current?.querySelector(`[data-i="${sel}"]`);
    el?.scrollIntoView({ block: "nearest" });
  }, [sel]);

  const choose = (r) => { if (r) { onSelect(r); onClose(); } };

  const onKey = (e) => {
    if (e.key === "Escape") { e.preventDefault(); onClose(); }
    else if (e.key === "ArrowDown") { e.preventDefault(); setSel(s => Math.min(s + 1, results.length - 1)); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setSel(s => Math.max(s - 1, 0)); }
    else if (e.key === "Enter") { e.preventDefault(); choose(results[sel]); }
  };

  return (
    <div onMouseDown={onClose} style={{
      position: "fixed", inset: 0, background: "rgba(0,0,0,.5)", zIndex: 10050,
      display: "flex", alignItems: "flex-start", justifyContent: "center", padding: "9vh 12px 12px",
    }}>
      <div onMouseDown={e => e.stopPropagation()} style={{
        width: 560, maxWidth: "96vw", maxHeight: "78vh", background: C.bg,
        border: `0.5px solid ${C.bdr}`, borderRadius: 12, boxShadow: "0 24px 64px rgba(0,0,0,.5)",
        display: "flex", flexDirection: "column", overflow: "hidden",
      }}>
        {/* Champ */}
        <div style={{ display: "flex", alignItems: "center", gap: 9, padding: "11px 13px", borderBottom: `0.5px solid ${C.bdr}` }}>
          <IcSearch size={17} color={C.dim} />
          <input ref={inputRef} value={q} onChange={e => setQ(e.target.value)} onKeyDown={onKey}
            placeholder="Rechercher un indicateur ou un outil…"
            style={{ flex: 1, background: "transparent", border: "none", outline: "none", color: C.txt, fontFamily: F, fontSize: 14 }} />
          {results.length > 0 && <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>{results.length}</span>}
          <button onClick={onClose} title="Fermer (Échap)" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex", padding: 2 }}><IcX size={16} /></button>
        </div>

        {/* Résultats */}
        <div ref={listRef} style={{ overflowY: "auto" }}>
          {!q && (
            <div style={{ padding: 16, color: C.dim, fontSize: 11, lineHeight: 1.6 }}>
              Cherchez parmi les <strong style={{ color: C.mut }}>indicateurs</strong> (NDVI, LST, WorldCover, canopée…) et les <strong style={{ color: C.mut }}>outils</strong> (couches, bivariée, LiDAR, classification…).<br />
              <span style={{ opacity: .8 }}>↑↓ pour naviguer · Entrée pour ouvrir · Échap pour fermer</span>
            </div>
          )}
          {q && results.length === 0 && (
            <div style={{ padding: 22, textAlign: "center", color: C.dim, fontSize: 12 }}>Aucun résultat pour « {q} ».</div>
          )}
          {results.map((r, i) => {
            const Icon = r.icon;
            const on = i === sel;
            return (
              <button key={r.kind + ":" + r.id} data-i={i}
                onMouseMove={() => setSel(i)} onClick={() => choose(r)}
                style={{
                  width: "100%", display: "flex", alignItems: "center", gap: 11, padding: "8px 13px",
                  border: "none", cursor: "pointer", textAlign: "left",
                  background: on ? C.acc + "18" : "transparent",
                }}>
                <span style={{ display: "flex", color: on ? C.acc : C.mut, flexShrink: 0 }}>{Icon && <Icon size={17} />}</span>
                <span style={{ flex: 1, minWidth: 0 }}>
                  <span style={{ display: "block", fontSize: 12.5, fontWeight: on ? 600 : 400, color: on ? C.acc : C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.full}</span>
                  <span style={{ display: "block", fontSize: 9.5, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.sub}</span>
                </span>
                <span style={{ fontSize: 8.5, color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 4, padding: "1px 6px", flexShrink: 0, textTransform: "uppercase", letterSpacing: ".04em" }}>{r.kind === "tool" ? "Outil" : "Indice"}</span>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

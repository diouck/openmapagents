/**
 * AttributeTableModal.jsx — Table attributaire complète d'une couche vecteur.
 * Barre d'outils (recherche, statistiques, graphiques, export), sélection de
 * lignes, menu par colonne (trier, masquer, statistiques, graphique). Ouvert
 * depuis le menu « ⋯ » de la légende / du gestionnaire de couches.
 */
import { useState, useMemo, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX, IcSearch, IcZoomIn, IcBarChart, IcFileDown, IcChevronDown } from "../icons";

function toCsv(cols, rows) {
  const esc = (v) => {
    if (v == null) return "";
    const s = typeof v === "object" ? JSON.stringify(v) : String(v);
    return /[",\n;]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(","), ...rows.map(r => cols.map(c => esc(r.props[c])).join(","))].join("\n");
}
function download(name, text) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/csv;charset=utf-8" }));
  const a = document.createElement("a"); a.href = url; a.download = name;
  document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 1500);
}
function numStats(vals) {
  const n = vals.filter(v => typeof v === "number" && isFinite(v)).sort((a, b) => a - b);
  if (!n.length) return null;
  const sum = n.reduce((a, b) => a + b, 0);
  return { count: n.length, min: n[0], max: n[n.length - 1], mean: sum / n.length, median: n[Math.floor(n.length / 2)] };
}

export default function AttributeTableModal({ layer, onClose, onZoomFeature, onUpdateFeatures, onOpenDashboard }) {
  const C = useThemeContext();
  const [q, setQ] = useState("");
  const [sort, setSort] = useState(null);       // { col, dir }
  const [hidden, setHidden] = useState(new Set());
  const [selected, setSelected] = useState(new Set());
  const [colMenu, setColMenu] = useState(null);  // { col, x, y }
  const [panel, setPanel] = useState(null);      // { kind: "stats"|"chart"|"filter"|"calc", col }
  const [colFilters, setColFilters] = useState({}); // { col: Set(valeurs gardées) }

  // État local des entités : permet l'édition des champs sans attendre le parent.
  const [feats, setFeats] = useState(layer?.geojson?.features || []);
  useEffect(() => { setFeats(layer?.geojson?.features || []); }, [layer?.id]);
  const editable = typeof onUpdateFeatures === "function";
  // Applique une transformation aux entités puis remonte au parent.
  const commit = (nextFeats) => { setFeats(nextFeats); onUpdateFeatures?.(nextFeats); };
  const renameField = (col, name) => {
    const nm = (name || "").trim(); if (!nm || nm === col) return;
    commit(feats.map(f => { const p = { ...(f.properties || {}) }; p[nm] = p[col]; delete p[col]; return { ...f, properties: p }; }));
  };
  const deleteField = (col) => commit(feats.map(f => { const p = { ...(f.properties || {}) }; delete p[col]; return { ...f, properties: p }; }));
  const addField = (name, def = "") => {
    const nm = (name || "").trim(); if (!nm) return;
    commit(feats.map(f => ({ ...f, properties: { ...(f.properties || {}), [nm]: def } })));
  };
  // Calculatrice : expression type "[pop] / [area] * 100" évaluée par entité.
  const calcField = (target, expr) => {
    const nm = (target || "").trim(); if (!nm || !expr) return;
    commit(feats.map(f => {
      const p = { ...(f.properties || {}) };
      try {
        const filled = expr.replace(/\[([^\]]+)\]/g, (_, k) => { const v = Number(p[k]); return isFinite(v) ? v : 0; });
        // eslint-disable-next-line no-new-func
        const val = Function(`"use strict"; return (${filled});`)();
        p[nm] = (typeof val === "number" && isFinite(val)) ? val : "";
      } catch { p[nm] = ""; }
      return { ...f, properties: p };
    }));
  };
  const allCols = useMemo(() => {
    const seen = new Set(), out = [];
    feats.forEach(f => Object.keys(f.properties || {}).forEach(k => {
      if (!seen.has(k) && !["geom_json", "geom_wkt"].includes(k)) { seen.add(k); out.push(k); }
    }));
    return out;
  }, [feats]);
  const cols = allCols.filter(c => !hidden.has(c));

  const rows = useMemo(() => {
    const s = q.trim().toLowerCase();
    const fcols = Object.keys(colFilters).filter(c => colFilters[c]?.size);
    let r = feats.map((f, i) => ({ i, props: f.properties || {}, f }))
      .filter(x => !s || allCols.some(c => String(x.props[c] ?? "").toLowerCase().includes(s)))
      .filter(x => fcols.every(c => colFilters[c].has(String(x.props[c] ?? ""))));
    if (sort) {
      const { col, dir } = sort;
      r = [...r].sort((a, b) => {
        const va = a.props[col], vb = b.props[col];
        if (va == null) return 1; if (vb == null) return -1;
        const cmp = (typeof va === "number" && typeof vb === "number") ? va - vb : String(va).localeCompare(String(vb));
        return dir === "asc" ? cmp : -cmp;
      });
    }
    return r;
  }, [feats, allCols, q, sort, colFilters]);

  const exportRows = selected.size ? rows.filter(r => selected.has(r.i)) : rows;
  const allSel = rows.length > 0 && rows.every(r => selected.has(r.i));
  const toggleAll = () => setSelected(allSel ? new Set() : new Set(rows.map(r => r.i)));
  const toggleRow = (i) => setSelected(s => { const n = new Set(s); n.has(i) ? n.delete(i) : n.add(i); return n; });

  const cell = { padding: "5px 9px", fontSize: 11.5, fontFamily: M, borderBottom: `0.5px solid ${C.bdr}`, whiteSpace: "nowrap", maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis" };
  const th = { ...cell, position: "sticky", top: 0, background: C.card, fontWeight: 600, color: C.txt, zIndex: 1, fontFamily: F };
  const toolBtn = { display: "flex", alignItems: "center", gap: 5, padding: "5px 10px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: "transparent", color: C.txt, fontFamily: F, fontSize: 11.5, cursor: "pointer" };

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", zIndex: 1200, backdropFilter: "blur(2px)" }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1201, width: "min(940px, 96vw)", maxHeight: "86vh", display: "flex", flexDirection: "column", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", overflow: "hidden" }}>
        {/* En-tête + barre d'outils */}
        <div style={{ padding: "11px 14px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <div style={{ minWidth: 140 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>Table attributaire — {layer?.name}</div>
            <div style={{ fontSize: 10.5, color: C.dim, marginTop: 1 }}>{rows.length}/{feats.length} entités{selected.size ? ` · ${selected.size} sélectionnée${selected.size > 1 ? "s" : ""}` : ""} · {cols.length} champs</div>
          </div>
          <div style={{ flex: 1 }} />
          <div style={{ display: "flex", alignItems: "center", gap: 6, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "5px 9px", width: 180 }}>
            <IcSearch size={13} style={{ color: C.dim, flexShrink: 0 }} />
            <input value={q} onChange={e => setQ(e.target.value)} placeholder="Rechercher…" style={{ border: "none", background: "transparent", color: C.txt, fontFamily: F, fontSize: 12, width: "100%", outline: "none" }} />
          </div>
          {editable && <button style={toolBtn} onClick={() => { const n = window.prompt("Nom du nouveau champ :"); if (n) addField(n); }}>+ Champ</button>}
          {editable && <button style={toolBtn} onClick={() => setPanel({ kind: "calc", col: allCols[0] || "" })}>Calculer…</button>}
          <button style={toolBtn} onClick={() => setPanel({ kind: "stats", col: null })}><IcBarChart size={13} />Statistiques</button>
          <button style={toolBtn} onClick={() => setPanel({ kind: "chart", col: null })}><IcBarChart size={13} />Graphiques</button>
          {onOpenDashboard && <button style={toolBtn} onClick={onOpenDashboard}><IcBarChart size={13} />Tableau de bord</button>}
          <button style={toolBtn} onClick={() => download(`${(layer?.name || "table").replace(/[^\w.-]+/g, "_")}.csv`, toCsv(cols, exportRows))}>
            <IcFileDown size={13} />Exporter{selected.size ? ` (${selected.size})` : ""}
          </button>
          <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex", padding: 2 }}><IcX size={17} /></button>
        </div>

        {/* Tableau */}
        <div style={{ flex: 1, overflow: "auto" }}>
          {cols.length === 0 ? (
            <div style={{ padding: 24, textAlign: "center", color: C.dim, fontSize: 12 }}>Aucun attribut visible.</div>
          ) : (
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <thead>
                <tr>
                  <th style={{ ...th, width: 30, textAlign: "center" }}><input type="checkbox" checked={allSel} onChange={toggleAll} /></th>
                  <th style={{ ...th, width: 30, textAlign: "center" }}>#</th>
                  {onZoomFeature && <th style={{ ...th, width: 26 }}></th>}
                  {cols.map(c => (
                    <th key={c} style={th}>
                      <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                        <span onClick={() => setSort(s => s?.col === c ? { col: c, dir: s.dir === "asc" ? "desc" : "asc" } : { col: c, dir: "asc" })} style={{ cursor: "pointer", flex: 1 }}>
                          {c}{sort?.col === c ? (sort.dir === "asc" ? " ▲" : " ▼") : ""}
                        </span>
                        <span onClick={e => { const r = e.currentTarget.getBoundingClientRect(); setColMenu({ col: c, x: r.left, y: r.bottom + 2 }); }} style={{ cursor: "pointer", color: C.dim, fontWeight: 700 }}>⋯</span>
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.i} style={{ background: selected.has(r.i) ? C.acc + "14" : "transparent" }}>
                    <td style={{ ...cell, textAlign: "center" }}><input type="checkbox" checked={selected.has(r.i)} onChange={() => toggleRow(r.i)} /></td>
                    <td style={{ ...cell, color: C.dim, textAlign: "center" }}>{r.i + 1}</td>
                    {onZoomFeature && <td style={{ ...cell, textAlign: "center" }}><button onClick={() => onZoomFeature(r.f)} title="Zoomer" style={{ background: "none", border: "none", color: C.acc, cursor: "pointer", display: "flex", padding: 0 }}><IcZoomIn size={13} /></button></td>}
                    {cols.map(c => { const v = r.props[c]; const s = v == null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v); return <td key={c} style={{ ...cell, color: C.txt }} title={s}>{s}</td>; })}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Menu colonne « ⋯ » */}
      {colMenu && (
        <>
          <div onClick={() => setColMenu(null)} style={{ position: "fixed", inset: 0, zIndex: 1300 }} />
          <div style={{ position: "fixed", left: Math.min(colMenu.x, window.innerWidth - 210), top: Math.min(colMenu.y, window.innerHeight - 180), zIndex: 1301, minWidth: 190, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 8, boxShadow: "0 8px 24px rgba(0,0,0,.35)", overflow: "hidden" }}>
            <div style={{ padding: "7px 11px", fontSize: 9.5, letterSpacing: ".05em", textTransform: "uppercase", color: C.dim, borderBottom: `0.5px solid ${C.bdr}` }}>{colMenu.col}</div>
            {[
              ["Trier ▲", () => setSort({ col: colMenu.col, dir: "asc" })],
              ["Trier ▼", () => setSort({ col: colMenu.col, dir: "desc" })],
              ["Filtrer par valeurs…", () => setPanel({ kind: "filter", col: colMenu.col })],
              ["Masquer le champ", () => setHidden(h => new Set(h).add(colMenu.col))],
              ["Statistiques du champ", () => setPanel({ kind: "stats", col: colMenu.col })],
              ["Graphique du champ", () => setPanel({ kind: "chart", col: colMenu.col })],
              ...(editable ? [
                ["Renommer le champ…", () => { const n = window.prompt("Nouveau nom du champ :", colMenu.col); if (n) renameField(colMenu.col, n); }],
                ["Calculer dans ce champ…", () => setPanel({ kind: "calc", col: colMenu.col })],
                ["Supprimer le champ", () => { if (window.confirm(`Supprimer le champ « ${colMenu.col} » ?`)) deleteField(colMenu.col); }],
              ] : []),
            ].map(([lbl, fn]) => (
              <button key={lbl} onClick={() => { fn(); setColMenu(null); }} style={{ width: "100%", textAlign: "left", padding: "7px 11px", background: "transparent", border: "none", color: C.txt, cursor: "pointer", fontSize: 12, fontFamily: F }}
                onMouseEnter={e => e.currentTarget.style.background = C.hover} onMouseLeave={e => e.currentTarget.style.background = "transparent"}>{lbl}</button>
            ))}
            {hidden.size > 0 && (
              <button onClick={() => { setHidden(new Set()); setColMenu(null); }} style={{ width: "100%", textAlign: "left", padding: "7px 11px", background: "transparent", border: "none", color: C.acc, cursor: "pointer", fontSize: 12, fontFamily: F, borderTop: `0.5px solid ${C.bdr}` }}>Réafficher tous les champs</button>
            )}
          </div>
        </>
      )}

      {/* Sous-modal Statistiques / Graphiques / Filtre par valeurs */}
      {panel && panel.kind === "filter" && (
        <UniqueFilterPanel col={panel.col} feats={feats} current={colFilters[panel.col]} C={C}
          onApply={(set) => { setColFilters(f => ({ ...f, [panel.col]: set })); setPanel(null); }}
          onClear={() => { setColFilters(f => { const n = { ...f }; delete n[panel.col]; return n; }); setPanel(null); }}
          onClose={() => setPanel(null)} />
      )}
      {panel && panel.kind === "calc" && (
        <CalcPanel cols={allCols} col={panel.col} C={C}
          onApply={(target, expr) => { calcField(target, expr); setPanel(null); }}
          onClose={() => setPanel(null)} />
      )}
      {panel && panel.kind !== "filter" && panel.kind !== "calc" && (
        <StatsChartPanel kind={panel.kind} col={panel.col} cols={allCols} rows={rows} C={C} onClose={() => setPanel(null)} />
      )}
    </>
  );
}

// ── Sous-modal : calculatrice de champ (expression [a] / [b] * 100) ──
function CalcPanel({ cols, col, C, onApply, onClose }) {
  const [target, setTarget] = useState(col || cols[0] || "");
  const [expr, setExpr] = useState("");
  const sel = { padding: "7px 10px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: F, fontSize: 12, width: "100%" };
  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 1400 }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1401, width: "min(460px, 94vw)", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", padding: 18 }}>
        <div style={{ display: "flex", alignItems: "center", marginBottom: 12 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: C.txt, margin: 0, flex: 1 }}>Calculatrice de champ</h3>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={16} /></button>
        </div>
        <label style={{ fontSize: 11, color: C.dim, display: "block", marginBottom: 4 }}>Champ cible (existant ou nouveau nom)</label>
        <input list="calc-cols" value={target} onChange={e => setTarget(e.target.value)} placeholder="ex : densite" style={{ ...sel, marginBottom: 12 }} />
        <datalist id="calc-cols">{cols.map(c => <option key={c} value={c} />)}</datalist>
        <label style={{ fontSize: 11, color: C.dim, display: "block", marginBottom: 4 }}>Expression (champs entre crochets)</label>
        <input value={expr} onChange={e => setExpr(e.target.value)} placeholder="[pop] / [area] * 100" style={{ ...sel, marginBottom: 8, fontFamily: M }} />
        <div style={{ fontSize: 10.5, color: C.dim, marginBottom: 14 }}>
          Opérateurs : + − * / ( ). Champs disponibles : {cols.slice(0, 8).map(c => `[${c}]`).join(" ")}{cols.length > 8 ? "…" : ""}
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={onClose} style={{ flex: 1, padding: "9px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: "transparent", color: C.mut, fontFamily: F, fontSize: 12, cursor: "pointer" }}>Annuler</button>
          <button onClick={() => onApply(target, expr)} disabled={!target || !expr} style={{ flex: 1, padding: "9px", border: "none", borderRadius: 7, background: (target && expr) ? C.acc : C.bdr, color: "#04120a", fontFamily: F, fontSize: 12, fontWeight: 600, cursor: (target && expr) ? "pointer" : "default" }}>Calculer</button>
        </div>
      </div>
    </>
  );
}

// ── Sous-modal : filtre par valeurs uniques d'un champ ──
function UniqueFilterPanel({ col, feats, current, C, onApply, onClear, onClose }) {
  const uniques = useMemo(() => {
    const m = new Map();
    feats.forEach(f => { const v = String(f.properties?.[col] ?? ""); m.set(v, (m.get(v) || 0) + 1); });
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  }, [feats, col]);
  const [sel, setSel] = useState(() => new Set(current || uniques.map(u => u[0])));
  const [q, setQ] = useState("");
  const shown = uniques.filter(u => !q || u[0].toLowerCase().includes(q.toLowerCase()));
  const toggle = (v) => setSel(s => { const n = new Set(s); n.has(v) ? n.delete(v) : n.add(v); return n; });

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 1400 }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1401, width: "min(380px, 94vw)", maxHeight: "80vh", display: "flex", flexDirection: "column", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)" }}>
        <div style={{ padding: 14, borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: C.txt }}>Filtrer — {col}</div>
            <div style={{ fontSize: 10.5, color: C.dim, marginTop: 1 }}>{uniques.length} valeurs uniques</div>
          </div>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={16} /></button>
        </div>
        <div style={{ padding: "8px 12px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", gap: 8 }}>
          <input value={q} onChange={e => setQ(e.target.value)} placeholder="Rechercher une valeur…" style={{ flex: 1, padding: "5px 9px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: F, fontSize: 12, outline: "none" }} />
          <button onClick={() => setSel(new Set(uniques.map(u => u[0])))} style={{ fontSize: 10.5, color: C.acc, background: "none", border: "none", cursor: "pointer" }}>Tout</button>
          <button onClick={() => setSel(new Set())} style={{ fontSize: 10.5, color: C.dim, background: "none", border: "none", cursor: "pointer" }}>Aucun</button>
        </div>
        <div style={{ flex: 1, overflow: "auto", padding: "6px 12px" }}>
          {shown.map(([v, n]) => (
            <label key={v} style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 0", fontSize: 12, color: C.txt, cursor: "pointer" }}>
              <input type="checkbox" checked={sel.has(v)} onChange={() => toggle(v)} />
              <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{v || "(vide)"}</span>
              <span style={{ color: C.dim, fontFamily: M, fontSize: 10.5 }}>{n}</span>
            </label>
          ))}
        </div>
        <div style={{ padding: 12, borderTop: `0.5px solid ${C.bdr}`, display: "flex", gap: 8 }}>
          <button onClick={onClear} style={{ flex: 1, padding: "8px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: "transparent", color: C.mut, fontFamily: F, fontSize: 12, cursor: "pointer" }}>Réinitialiser</button>
          <button onClick={() => onApply(sel)} style={{ flex: 1, padding: "8px", border: "none", borderRadius: 7, background: C.acc, color: "#04120a", fontFamily: F, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>Appliquer ({sel.size})</button>
        </div>
      </div>
    </>
  );
}

// ── Palette catégorielle pour camembert / barres colorées ──
const PIE_COLORS = ["#1D9E75", "#378add", "#d85a30", "#d4537e", "#ba7517", "#7f77dd", "#0f6e56", "#185fa5", "#993c1d", "#72243e", "#854f0b", "#3c3489"];

const AGGS = { value: "Valeur", sum: "Somme", mean: "Moyenne", count: "Nombre" };
const fmtNum = (v) => (typeof v === "number" && isFinite(v)) ? (Math.abs(v) >= 1000 ? Math.round(v).toLocaleString("fr") : (Math.round(v * 100) / 100).toLocaleString("fr")) : v;

// Groupe les lignes par xField et agrège yField
function aggregate(rows, xField, yField, agg) {
  const g = new Map();
  rows.forEach(r => {
    const k = String(r.props[xField] ?? "(vide)");
    const y = Number(r.props[yField]);
    if (!g.has(k)) g.set(k, []);
    g.get(k).push(isFinite(y) ? y : null);
  });
  const out = [];
  for (const [k, arr] of g) {
    const nums = arr.filter(v => v != null);
    let val;
    if (agg === "count") val = arr.length;
    else if (agg === "sum") val = nums.reduce((a, b) => a + b, 0);
    else if (agg === "mean") val = nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : 0;
    else val = nums.length ? nums[0] : 0; // "value" : 1re valeur (X unique par entité)
    out.push([k, val]);
  }
  return out.sort((a, b) => b[1] - a[1]).slice(0, 40);
}

// ── Sous-modal : statistiques ou graphique (X/Y, agrégation, 5 types) ──
function StatsChartPanel({ kind, col, cols, rows, C, onClose }) {
  const numericCols = useMemo(() => cols.filter(c => rows.some(r => typeof r.props[c] === "number")), [cols, rows]);
  const textCols = useMemo(() => cols.filter(c => !numericCols.includes(c)), [cols, numericCols]);

  // stats : champ unique
  const [field, setField] = useState(col || numericCols[0] || cols[0]);
  // chart : X (étiquette), Y (valeur), agrégation, type
  const [ctype, setCtype] = useState("bar");   // bar | line | pie | histogram | scatter
  const [xField, setX] = useState(textCols[0] || cols[0]);
  const [yField, setY] = useState((col && numericCols.includes(col)) ? col : numericCols[0] || cols[0]);
  const [agg, setAgg] = useState("value");
  const [bins, setBins] = useState(10);

  const stats = numStats(rows.map(r => r.props[field]));
  const sel = { padding: "6px 8px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: F, fontSize: 11.5 };
  const lbl = { fontSize: 9.5, color: C.dim, textTransform: "uppercase", letterSpacing: ".04em", marginBottom: 3, display: "block" };

  // Séries agrégées (bar/line/pie)
  const series = useMemo(() => aggregate(rows, xField, yField, agg), [rows, xField, yField, agg]);
  const maxV = Math.max(1, ...series.map(s => Math.abs(s[1])));
  const totalV = series.reduce((a, s) => a + Math.abs(s[1]), 0) || 1;

  // Histogramme (bins sur yField numérique)
  const hist = useMemo(() => {
    const vals = rows.map(r => Number(r.props[yField])).filter(v => isFinite(v));
    if (!vals.length) return [];
    const mn = Math.min(...vals), mx = Math.max(...vals), w = (mx - mn) / bins || 1;
    const b = Array.from({ length: bins }, (_, i) => ({ lo: mn + i * w, hi: mn + (i + 1) * w, n: 0 }));
    vals.forEach(v => { let i = Math.floor((v - mn) / w); if (i >= bins) i = bins - 1; if (i < 0) i = 0; b[i].n++; });
    return b;
  }, [rows, yField, bins]);
  const maxH = Math.max(1, ...hist.map(b => b.n));

  // Nuage de points (X num, Y num)
  const scatter = useMemo(() => rows.map(r => [Number(r.props[xField]), Number(r.props[yField])]).filter(p => isFinite(p[0]) && isFinite(p[1])), [rows, xField, yField]);

  const renderPie = () => {
    const R = 70, cx = 90, cy = 90; let a0 = -Math.PI / 2;
    return (
      <div style={{ display: "flex", gap: 14, alignItems: "center", flexWrap: "wrap" }}>
        <svg width={180} height={180} viewBox="0 0 180 180">
          {series.map(([k, v], i) => {
            const frac = Math.abs(v) / totalV, a1 = a0 + frac * 2 * Math.PI;
            const x0 = cx + R * Math.cos(a0), y0 = cy + R * Math.sin(a0);
            const x1 = cx + R * Math.cos(a1), y1 = cy + R * Math.sin(a1);
            const d = `M${cx},${cy} L${x0},${y0} A${R},${R} 0 ${frac > 0.5 ? 1 : 0} 1 ${x1},${y1} Z`;
            a0 = a1;
            return <path key={k} d={d} fill={PIE_COLORS[i % PIE_COLORS.length]} stroke={C.bg} strokeWidth="1" />;
          })}
        </svg>
        <div style={{ flex: 1, minWidth: 120, maxHeight: 200, overflow: "auto" }}>
          {series.map(([k, v], i) => (
            <div key={k} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, marginBottom: 3 }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: PIE_COLORS[i % PIE_COLORS.length], flexShrink: 0 }} />
              <span style={{ flex: 1, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k}</span>
              <span style={{ color: C.dim, fontFamily: M }}>{Math.round(Math.abs(v) / totalV * 100)}%</span>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const renderLine = () => {
    const W = 400, H = 170, pad = 26;
    const pts = series.map(([, v], i) => [pad + (series.length === 1 ? 0 : i / (series.length - 1) * (W - 2 * pad)), H - pad - (Math.abs(v) / maxV) * (H - 2 * pad)]);
    return (
      <svg width="100%" viewBox={`0 0 ${W} ${H}`}>
        <polyline fill="none" stroke={C.acc} strokeWidth="2" points={pts.map(p => p.join(",")).join(" ")} />
        {pts.map(([x, y], i) => <circle key={i} cx={x} cy={y} r="3" fill={C.acc} />)}
        {series.map(([k], i) => i % Math.ceil(series.length / 8 || 1) === 0 && <text key={k} x={pts[i][0]} y={H - 8} fontSize="8" fill={C.dim} textAnchor="middle">{String(k).slice(0, 7)}</text>)}
      </svg>
    );
  };

  const renderBars = () => (
    <div style={{ maxHeight: 300, overflow: "auto" }}>
      {series.map(([k, v], i) => (
        <div key={k} style={{ marginBottom: 6 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10.5, color: C.mut, marginBottom: 2 }}>
            <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 260 }}>{k}</span><span style={{ fontFamily: M }}>{fmtNum(v)}</span>
          </div>
          <div style={{ height: 9, borderRadius: 4, background: C.bdr, overflow: "hidden" }}>
            <div style={{ width: `${(Math.abs(v) / maxV) * 100}%`, height: "100%", background: PIE_COLORS[i % PIE_COLORS.length] }} />
          </div>
        </div>
      ))}
    </div>
  );

  const renderHist = () => (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 11, color: C.dim }}>Classes</span>
        <input type="range" min="4" max="24" value={bins} onChange={e => setBins(+e.target.value)} style={{ flex: 1 }} />
        <span style={{ fontSize: 11, fontFamily: M, color: C.txt }}>{bins}</span>
      </div>
      <svg width="100%" viewBox={`0 0 400 170`}>
        {hist.map((b, i) => {
          const w = 400 / hist.length, h = (b.n / maxH) * 140;
          return <g key={i}><rect x={i * w + 1} y={150 - h} width={w - 2} height={h} fill={C.acc} rx="1" />
            {i % Math.ceil(hist.length / 6 || 1) === 0 && <text x={i * w + w / 2} y={164} fontSize="7.5" fill={C.dim} textAnchor="middle">{fmtNum(b.lo)}</text>}</g>;
        })}
      </svg>
    </div>
  );

  const renderScatter = () => {
    const W = 400, H = 240, pad = 34;
    const xs = scatter.map(p => p[0]), ys = scatter.map(p => p[1]);
    const xmin = Math.min(...xs), xmax = Math.max(...xs), ymin = Math.min(...ys), ymax = Math.max(...ys);
    const sx = v => pad + (xmax === xmin ? 0.5 : (v - xmin) / (xmax - xmin)) * (W - 2 * pad);
    const sy = v => H - pad - (ymax === ymin ? 0.5 : (v - ymin) / (ymax - ymin)) * (H - 2 * pad);
    return (
      <svg width="100%" viewBox={`0 0 ${W} ${H}`}>
        <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} stroke={C.bdr} />
        <line x1={pad} y1={pad} x2={pad} y2={H - pad} stroke={C.bdr} />
        {scatter.map((p, i) => <circle key={i} cx={sx(p[0])} cy={sy(p[1])} r="3" fill={C.acc} opacity="0.7" />)}
        <text x={W / 2} y={H - 4} fontSize="9" fill={C.dim} textAnchor="middle">{xField}</text>
        <text x={10} y={H / 2} fontSize="9" fill={C.dim} transform={`rotate(-90 10 ${H / 2})`} textAnchor="middle">{yField}</text>
      </svg>
    );
  };

  const usesAgg = ctype === "bar" || ctype === "line" || ctype === "pie";
  const usesY = usesAgg ? agg !== "count" : true;

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 1400 }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1401, width: "min(520px, 95vw)", maxHeight: "84vh", overflow: "auto", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", padding: 18 }}>
        <div style={{ display: "flex", alignItems: "center", marginBottom: 12 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: C.txt, margin: 0, flex: 1 }}>{kind === "stats" ? "Statistiques" : "Graphique"}</h3>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={16} /></button>
        </div>

        {kind === "stats" ? (
          <>
            <select value={field} onChange={e => setField(e.target.value)} style={{ ...sel, width: "100%", marginBottom: 12 }}>
              {cols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
            {stats ? (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {[["Nombre", stats.count], ["Minimum", stats.min], ["Maximum", stats.max], ["Moyenne", stats.mean], ["Médiane", stats.median]].map(([l, v]) => (
                  <div key={l} style={{ background: C.input, borderRadius: 8, padding: "10px 12px" }}>
                    <div style={{ fontSize: 10.5, color: C.dim }}>{l}</div>
                    <div style={{ fontSize: 17, fontWeight: 600, color: C.txt, fontFamily: M }}>{fmtNum(v)}</div>
                  </div>
                ))}
              </div>
            ) : <div style={{ fontSize: 12, color: C.dim }}>Champ non numérique — choisissez un champ numérique pour les statistiques.</div>}
          </>
        ) : (
          <>
            {/* Contrôles : type + X + Y + agrégation */}
            <div style={{ display: "grid", gridTemplateColumns: ctype === "scatter" ? "1fr 1fr" : "1fr 1fr 1fr", gap: 8, marginBottom: 14 }}>
              <div><span style={lbl}>Type</span>
                <select value={ctype} onChange={e => setCtype(e.target.value)} style={{ ...sel, width: "100%" }}>
                  <option value="bar">Barres</option><option value="line">Ligne</option><option value="pie">Camembert</option>
                  <option value="histogram">Histogramme</option><option value="scatter">Nuage de points</option>
                </select>
              </div>
              {ctype === "histogram" ? (
                <div style={{ gridColumn: "span 2" }}><span style={lbl}>Variable (numérique)</span>
                  <select value={yField} onChange={e => setY(e.target.value)} style={{ ...sel, width: "100%" }}>{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>
                </div>
              ) : ctype === "scatter" ? (
                <div><span style={lbl}>Axe X · Y (numériques)</span>
                  <div style={{ display: "flex", gap: 6 }}>
                    <select value={xField} onChange={e => setX(e.target.value)} style={{ ...sel, flex: 1 }}>{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>
                    <select value={yField} onChange={e => setY(e.target.value)} style={{ ...sel, flex: 1 }}>{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>
                  </div>
                </div>
              ) : (
                <>
                  <div><span style={lbl}>Étiquette (X)</span>
                    <select value={xField} onChange={e => setX(e.target.value)} style={{ ...sel, width: "100%" }}>{cols.map(c => <option key={c} value={c}>{c}</option>)}</select>
                  </div>
                  <div><span style={lbl}>Valeur (Y)</span>
                    <div style={{ display: "flex", gap: 6 }}>
                      <select value={agg} onChange={e => setAgg(e.target.value)} style={{ ...sel, width: 90 }}>{Object.entries(AGGS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select>
                      {usesY && <select value={yField} onChange={e => setY(e.target.value)} style={{ ...sel, flex: 1 }}>{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>}
                    </div>
                  </div>
                </>
              )}
            </div>

            {ctype === "pie" ? renderPie() : ctype === "line" ? renderLine() : ctype === "histogram" ? renderHist() : ctype === "scatter" ? renderScatter() : renderBars()}
          </>
        )}
      </div>
    </>
  );
}

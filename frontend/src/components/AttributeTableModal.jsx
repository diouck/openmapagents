/**
 * AttributeTableModal.jsx — Table attributaire complète d'une couche vecteur.
 * Barre d'outils (recherche, statistiques, graphiques, export), sélection de
 * lignes, menu par colonne (trier, masquer, statistiques, graphique). Ouvert
 * depuis le menu « ⋯ » de la légende / du gestionnaire de couches.
 */
import { useState, useMemo } from "react";
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

export default function AttributeTableModal({ layer, onClose, onZoomFeature }) {
  const C = useThemeContext();
  const [q, setQ] = useState("");
  const [sort, setSort] = useState(null);       // { col, dir }
  const [hidden, setHidden] = useState(new Set());
  const [selected, setSelected] = useState(new Set());
  const [colMenu, setColMenu] = useState(null);  // { col, x, y }
  const [panel, setPanel] = useState(null);      // { kind: "stats"|"chart", col }

  const feats = layer?.geojson?.features || [];
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
    let r = feats.map((f, i) => ({ i, props: f.properties || {}, f }))
      .filter(x => !s || allCols.some(c => String(x.props[c] ?? "").toLowerCase().includes(s)));
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
  }, [feats, allCols, q, sort]);

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
          <button style={toolBtn} onClick={() => setPanel({ kind: "stats", col: null })}><IcBarChart size={13} />Statistiques</button>
          <button style={toolBtn} onClick={() => setPanel({ kind: "chart", col: null })}><IcBarChart size={13} />Graphiques</button>
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
              ["Masquer le champ", () => setHidden(h => new Set(h).add(colMenu.col))],
              ["Statistiques du champ", () => setPanel({ kind: "stats", col: colMenu.col })],
              ["Graphique du champ", () => setPanel({ kind: "chart", col: colMenu.col })],
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

      {/* Sous-modal Statistiques / Graphiques */}
      {panel && (
        <StatsChartPanel kind={panel.kind} col={panel.col} cols={allCols} rows={rows} C={C} onClose={() => setPanel(null)} />
      )}
    </>
  );
}

// ── Palette catégorielle pour camembert / barres colorées ──
const PIE_COLORS = ["#1D9E75", "#378add", "#d85a30", "#d4537e", "#ba7517", "#7f77dd", "#0f6e56", "#185fa5", "#993c1d", "#72243e", "#854f0b", "#3c3489"];

// ── Sous-modal : statistiques ou graphique (barres / camembert / ligne) ──
function StatsChartPanel({ kind, col, cols, rows, C, onClose }) {
  const numericCols = cols.filter(c => rows.some(r => typeof r.props[c] === "number"));
  const [field, setField] = useState(col || (kind === "stats" ? (numericCols[0] || cols[0]) : (cols[0] || "")));
  const [ctype, setCtype] = useState("bar"); // bar | pie | line

  const vals = rows.map(r => r.props[field]);
  const stats = numStats(vals);
  const counts = useMemo(() => {
    const m = new Map();
    vals.forEach(v => { const k = v == null ? "(vide)" : String(v); m.set(k, (m.get(k) || 0) + 1); });
    return [...m.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12);
  }, [field, rows]);
  const maxC = Math.max(1, ...counts.map(c => c[1]));
  const total = counts.reduce((a, c) => a + c[1], 0) || 1;

  const sel = { padding: "6px 10px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: F, fontSize: 12 };

  // Rendu camembert (arcs SVG)
  const renderPie = () => {
    const R = 70, cx = 90, cy = 90; let a0 = -Math.PI / 2;
    return (
      <div style={{ display: "flex", gap: 14, alignItems: "center", flexWrap: "wrap" }}>
        <svg width={180} height={180} viewBox="0 0 180 180">
          {counts.map(([k, n], i) => {
            const frac = n / total, a1 = a0 + frac * 2 * Math.PI;
            const x0 = cx + R * Math.cos(a0), y0 = cy + R * Math.sin(a0);
            const x1 = cx + R * Math.cos(a1), y1 = cy + R * Math.sin(a1);
            const large = frac > 0.5 ? 1 : 0;
            const d = `M${cx},${cy} L${x0},${y0} A${R},${R} 0 ${large} 1 ${x1},${y1} Z`;
            a0 = a1;
            return <path key={k} d={d} fill={PIE_COLORS[i % PIE_COLORS.length]} stroke={C.bg} strokeWidth="1" />;
          })}
        </svg>
        <div style={{ flex: 1, minWidth: 120 }}>
          {counts.map(([k, n], i) => (
            <div key={k} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, marginBottom: 3 }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: PIE_COLORS[i % PIE_COLORS.length], flexShrink: 0 }} />
              <span style={{ flex: 1, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k}</span>
              <span style={{ color: C.dim, fontFamily: M }}>{Math.round(n / total * 100)}%</span>
            </div>
          ))}
        </div>
      </div>
    );
  };

  // Rendu ligne (polyline SVG)
  const renderLine = () => {
    const W = 380, H = 160, pad = 24;
    const pts = counts.map(([, n], i) => {
      const x = pad + (counts.length === 1 ? 0 : i / (counts.length - 1) * (W - 2 * pad));
      const y = H - pad - (n / maxC) * (H - 2 * pad);
      return [x, y];
    });
    return (
      <svg width="100%" viewBox={`0 0 ${W} ${H}`}>
        <polyline fill="none" stroke={C.acc} strokeWidth="2" points={pts.map(p => p.join(",")).join(" ")} />
        {pts.map(([x, y], i) => <circle key={i} cx={x} cy={y} r="3" fill={C.acc} />)}
        {counts.map(([k], i) => <text key={k} x={pts[i][0]} y={H - 6} fontSize="8" fill={C.dim} textAnchor="middle">{String(k).slice(0, 6)}</text>)}
      </svg>
    );
  };

  const renderBars = () => (
    <div>
      {counts.map(([k, n], i) => (
        <div key={k} style={{ marginBottom: 6 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10.5, color: C.mut, marginBottom: 2 }}>
            <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 240 }}>{k}</span><span style={{ fontFamily: M }}>{n}</span>
          </div>
          <div style={{ height: 8, borderRadius: 4, background: C.bdr, overflow: "hidden" }}>
            <div style={{ width: `${(n / maxC) * 100}%`, height: "100%", background: PIE_COLORS[i % PIE_COLORS.length] }} />
          </div>
        </div>
      ))}
    </div>
  );

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", zIndex: 1400 }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1401, width: "min(480px, 94vw)", maxHeight: "82vh", overflow: "auto", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", padding: 18 }}>
        <div style={{ display: "flex", alignItems: "center", marginBottom: 12 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: C.txt, margin: 0, flex: 1 }}>{kind === "stats" ? "Statistiques" : "Graphique"} — {field}</h3>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={16} /></button>
        </div>
        <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
          <select value={field} onChange={e => setField(e.target.value)} style={{ ...sel, flex: 1 }}>
            {cols.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
          {kind === "chart" && (
            <select value={ctype} onChange={e => setCtype(e.target.value)} style={sel}>
              <option value="bar">Barres</option>
              <option value="pie">Camembert</option>
              <option value="line">Ligne</option>
            </select>
          )}
        </div>

        {kind === "stats" ? (
          stats ? (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              {[["Nombre", stats.count], ["Minimum", stats.min], ["Maximum", stats.max], ["Moyenne", stats.mean.toFixed(2)], ["Médiane", stats.median]].map(([l, v]) => (
                <div key={l} style={{ background: C.input, borderRadius: 8, padding: "10px 12px" }}>
                  <div style={{ fontSize: 10.5, color: C.dim }}>{l}</div>
                  <div style={{ fontSize: 17, fontWeight: 600, color: C.txt, fontFamily: M }}>{typeof v === "number" ? v.toLocaleString("fr") : v}</div>
                </div>
              ))}
            </div>
          ) : (
            <div>
              <div style={{ fontSize: 11.5, color: C.dim, marginBottom: 8 }}>Champ texte — répartition des valeurs :</div>
              {counts.map(([k, n]) => (
                <div key={k} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4, fontSize: 11.5 }}>
                  <span style={{ width: 120, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k}</span>
                  <span style={{ color: C.dim, fontFamily: M }}>{n}</span>
                </div>
              ))}
            </div>
          )
        ) : (
          ctype === "pie" ? renderPie() : ctype === "line" ? renderLine() : renderBars()
        )}
      </div>
    </>
  );
}

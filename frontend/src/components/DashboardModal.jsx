/**
 * DashboardModal.jsx — Tableau de bord interactif d'une couche vecteur.
 * Fenêtre déplaçable/redimensionnable, KPI, cross-filtering (clic = filtre global),
 * graphiques SVG (barres/ligne/aire/camembert/anneau/nuage), export CSV + PNG.
 */
import { useState, useMemo, useEffect, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX, IcFileDown, IcBarChart } from "../icons";

const COLORS = ["#1D9E75", "#378add", "#d85a30", "#d4537e", "#ba7517", "#7f77dd", "#0f6e56", "#185fa5", "#993c1d", "#72243e", "#854f0b", "#3c3489"];
const AGGS = { value: "Valeur", sum: "Somme", mean: "Moyenne", count: "Nombre" };
const fmtNum = (v) => (typeof v === "number" && isFinite(v)) ? (Math.abs(v) >= 1000 ? Math.round(v).toLocaleString("fr") : (Math.round(v * 100) / 100).toLocaleString("fr")) : v;

function aggregate(feats, xField, yField, agg) {
  const g = new Map();
  feats.forEach(f => {
    const p = f.properties || {};
    const k = String(p[xField] ?? "(vide)");
    const y = Number(p[yField]);
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
    else val = nums.length ? nums[0] : 0;
    out.push([k, val]);
  }
  return out.sort((a, b) => b[1] - a[1]).slice(0, 24);
}

function svgToPng(svg, name) {
  try {
    const xml = new XMLSerializer().serializeToString(svg);
    const b64 = btoa(unescape(encodeURIComponent(xml)));
    const img = new Image();
    img.onload = () => {
      const W = svg.viewBox.baseVal.width || 400, H = svg.viewBox.baseVal.height || 260;
      const cv = document.createElement("canvas"); cv.width = W * 2; cv.height = H * 2;
      const ctx = cv.getContext("2d"); ctx.scale(2, 2); ctx.drawImage(img, 0, 0, W, H);
      cv.toBlob(bl => { const a = document.createElement("a"); a.href = URL.createObjectURL(bl); a.download = `${name}.png`; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1500); });
    };
    img.src = "data:image/svg+xml;base64," + b64;
  } catch (_) { /* ignore */ }
}

// ── Carte-graphique (SVG, interactive) ──────────────────────────
function ChartCard({ feats, cols, numericCols, C, cfg, onChange, onRemove, cross, onCross }) {
  const { type, x, y, agg } = cfg;
  const svgRef = useRef(null);
  const series = useMemo(() => aggregate(feats, x, y, agg), [feats, x, y, agg]);
  const maxV = Math.max(1, ...series.map(s => Math.abs(s[1])));
  const totalV = series.reduce((a, s) => a + Math.abs(s[1]), 0) || 1;
  const sel = { padding: "3px 6px", border: `0.5px solid ${C.bdr}`, borderRadius: 6, background: C.input, color: C.txt, fontFamily: F, fontSize: 10.5 };
  const active = (k) => cross && cross.field === x && cross.value === k;
  const clickCat = (k) => onCross(x, k);
  const W = 320, H = 190, pad = 30;

  const bars = () => (
    <svg ref={svgRef} width="100%" viewBox={`0 0 ${W} ${H}`} style={{ background: C.bg }}>
      {series.map(([k, v], i) => {
        const bw = (W - 2 * pad) / series.length, bh = (Math.abs(v) / maxV) * (H - 2 * pad);
        const on = active(k), dim = cross && !on;
        return <g key={k} onClick={() => clickCat(k)} style={{ cursor: "pointer" }}>
          <rect x={pad + i * bw + 1} y={H - pad - bh} width={bw - 2} height={bh} fill={COLORS[i % COLORS.length]} opacity={dim ? 0.35 : 1} rx="1" />
          {i % Math.ceil(series.length / 8 || 1) === 0 && <text x={pad + i * bw + bw / 2} y={H - pad + 10} fontSize="7.5" fill={C.dim} textAnchor="middle">{String(k).slice(0, 6)}</text>}
        </g>;
      })}
      <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} stroke={C.bdr} />
    </svg>
  );
  const lineArea = (fill) => {
    const pts = series.map(([, v], i) => [pad + (series.length === 1 ? 0 : i / (series.length - 1) * (W - 2 * pad)), H - pad - (Math.abs(v) / maxV) * (H - 2 * pad)]);
    const d = pts.map((p, i) => `${i ? "L" : "M"}${p[0]},${p[1]}`).join(" ");
    return (
      <svg ref={svgRef} width="100%" viewBox={`0 0 ${W} ${H}`} style={{ background: C.bg }}>
        {fill && <path d={`${d} L${pts[pts.length - 1][0]},${H - pad} L${pts[0][0]},${H - pad} Z`} fill={C.acc} opacity="0.18" />}
        <path d={d} fill="none" stroke={C.acc} strokeWidth="2" />
        {pts.map(([px, py], i) => <circle key={i} cx={px} cy={py} r="2.5" fill={C.acc} onClick={() => clickCat(series[i][0])} style={{ cursor: "pointer" }} />)}
        <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} stroke={C.bdr} />
      </svg>
    );
  };
  const pie = (donut) => {
    const R = 70, r0 = donut ? 34 : 0, cx = 90, cy = 95; let a0 = -Math.PI / 2;
    return (
      <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
        <svg ref={svgRef} width="180" height="190" viewBox="0 0 180 190" style={{ background: C.bg, flexShrink: 0 }}>
          {series.map(([k, v], i) => {
            const frac = Math.abs(v) / totalV, a1 = a0 + frac * 2 * Math.PI;
            const x1 = cx + R * Math.cos(a0), y1 = cy + R * Math.sin(a0), x2 = cx + R * Math.cos(a1), y2 = cy + R * Math.sin(a1);
            const large = frac > 0.5 ? 1 : 0;
            let d;
            if (donut) { const ix1 = cx + r0 * Math.cos(a0), iy1 = cy + r0 * Math.sin(a0), ix2 = cx + r0 * Math.cos(a1), iy2 = cy + r0 * Math.sin(a1); d = `M${x1},${y1} A${R},${R} 0 ${large} 1 ${x2},${y2} L${ix2},${iy2} A${r0},${r0} 0 ${large} 0 ${ix1},${iy1} Z`; }
            else d = `M${cx},${cy} L${x1},${y1} A${R},${R} 0 ${large} 1 ${x2},${y2} Z`;
            a0 = a1;
            const on = active(k), dim = cross && !on;
            return <path key={k} d={d} fill={COLORS[i % COLORS.length]} opacity={dim ? 0.35 : 1} stroke={C.bg} strokeWidth="1" onClick={() => clickCat(k)} style={{ cursor: "pointer" }} />;
          })}
        </svg>
        <div style={{ flex: 1, minWidth: 0, maxHeight: 180, overflow: "auto" }}>
          {series.map(([k, v], i) => (
            <div key={k} onClick={() => clickCat(k)} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10.5, marginBottom: 2, cursor: "pointer", opacity: cross && !active(k) ? 0.5 : 1 }}>
              <span style={{ width: 9, height: 9, borderRadius: 2, background: COLORS[i % COLORS.length], flexShrink: 0 }} />
              <span style={{ flex: 1, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k}</span>
              <span style={{ color: C.dim, fontFamily: M }}>{Math.round(Math.abs(v) / totalV * 100)}%</span>
            </div>
          ))}
        </div>
      </div>
    );
  };
  const scatter = () => {
    const pts = feats.map(f => [Number(f.properties?.[x]), Number(f.properties?.[y])]).filter(p => isFinite(p[0]) && isFinite(p[1]));
    const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]);
    const xmn = Math.min(...xs), xmx = Math.max(...xs), ymn = Math.min(...ys), ymx = Math.max(...ys);
    const sx = v => pad + (xmx === xmn ? 0.5 : (v - xmn) / (xmx - xmn)) * (W - 2 * pad);
    const sy = v => H - pad - (ymx === ymn ? 0.5 : (v - ymn) / (ymx - ymn)) * (H - 2 * pad);
    return (
      <svg ref={svgRef} width="100%" viewBox={`0 0 ${W} ${H}`} style={{ background: C.bg }}>
        <line x1={pad} y1={H - pad} x2={W - pad} y2={H - pad} stroke={C.bdr} />
        <line x1={pad} y1={pad} x2={pad} y2={H - pad} stroke={C.bdr} />
        {pts.map((p, i) => <circle key={i} cx={sx(p[0])} cy={sy(p[1])} r="2.5" fill={C.acc} opacity="0.6" />)}
      </svg>
    );
  };

  const render = () => type === "pie" ? pie(false) : type === "donut" ? pie(true) : type === "line" ? lineArea(false) : type === "area" ? lineArea(true) : type === "scatter" ? scatter() : bars();
  const usesX2 = type === "scatter";

  return (
    <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: 10, display: "flex", flexDirection: "column", gap: 7, minWidth: 0 }}>
      <div style={{ display: "flex", gap: 5, flexWrap: "wrap", alignItems: "center" }}>
        <select value={type} onChange={e => onChange({ ...cfg, type: e.target.value })} style={sel}>
          <option value="bar">Barres</option><option value="line">Ligne</option><option value="area">Aire</option>
          <option value="pie">Camembert</option><option value="donut">Anneau</option><option value="scatter">Nuage</option>
        </select>
        <select value={x} onChange={e => onChange({ ...cfg, x: e.target.value })} style={sel} title={usesX2 ? "Axe X" : "Étiquette"}>{(usesX2 ? numericCols : cols).map(c => <option key={c} value={c}>{c}</option>)}</select>
        {!usesX2 && <select value={agg} onChange={e => onChange({ ...cfg, agg: e.target.value })} style={sel}>{Object.entries(AGGS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select>}
        {(usesX2 || agg !== "count") && <select value={y} onChange={e => onChange({ ...cfg, y: e.target.value })} style={sel} title={usesX2 ? "Axe Y" : "Valeur"}>{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>}
        <div style={{ marginLeft: "auto", display: "flex", gap: 3 }}>
          <button onClick={() => svgRef.current && svgToPng(svgRef.current, `${type}_${x}`)} title="Exporter PNG" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcFileDown size={13} /></button>
          <button onClick={onRemove} title="Retirer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={14} /></button>
        </div>
      </div>
      {render()}
    </div>
  );
}

export default function DashboardModal({ layer, onClose }) {
  const C = useThemeContext();
  const allFeats = layer?.geojson?.features || [];
  const cols = useMemo(() => {
    const seen = new Set(), out = [];
    allFeats.forEach(f => Object.keys(f.properties || {}).forEach(k => { if (!seen.has(k) && !["geom_json", "geom_wkt"].includes(k)) { seen.add(k); out.push(k); } }));
    return out;
  }, [allFeats]);
  const numericCols = useMemo(() => cols.filter(c => allFeats.some(f => typeof f.properties?.[c] === "number")), [cols, allFeats]);

  const [cross, setCross] = useState(null);   // { field, value } — filtre global (cross-filtering)
  const onCross = (field, value) => setCross(c => (c && c.field === field && c.value === value) ? null : { field, value });
  const feats = useMemo(() => cross ? allFeats.filter(f => String(f.properties?.[cross.field] ?? "") === cross.value) : allFeats, [allFeats, cross]);

  const [metric, setMetric] = useState(numericCols[0] || "");
  const kpi = useMemo(() => {
    const vals = feats.map(f => Number(f.properties?.[metric])).filter(v => isFinite(v));
    const sum = vals.reduce((a, b) => a + b, 0);
    return { count: feats.length, sum, mean: vals.length ? sum / vals.length : 0, max: vals.length ? Math.max(...vals) : 0 };
  }, [feats, metric]);

  const mkDefault = () => ({ id: Math.random().toString(36).slice(2), type: "bar", x: cols[0] || "", y: numericCols[0] || cols[0] || "", agg: numericCols.length ? "sum" : "count" });
  const [charts, setCharts] = useState(() => [mkDefault(), { ...mkDefault(), type: "donut" }, { ...mkDefault(), type: "line" }]);
  const update = (id, cfg) => setCharts(cs => cs.map(c => c.id === id ? cfg : c));
  const remove = (id) => setCharts(cs => cs.filter(c => c.id !== id));

  const exportCsv = () => {
    const esc = (v) => { const s = v == null ? "" : String(v); return /[",\n;]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
    const csv = [cols.join(","), ...feats.map(f => cols.map(c => esc(f.properties?.[c])).join(","))].join("\n");
    const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
    a.download = `${(layer?.name || "dashboard").replace(/[^\w.-]+/g, "_")}.csv`; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1500);
  };

  // Fenêtre déplaçable + redimensionnable + responsive (comme la table).
  const [pos, setPos] = useState(null);
  const [size, setSize] = useState(() => ({ w: Math.min(1000, window.innerWidth - 40), h: Math.min(640, Math.round(window.innerHeight * 0.85)) }));
  const move = useRef(null);
  useEffect(() => {
    const onMove = (e) => {
      const m = move.current; if (!m) return;
      const dx = e.clientX - m.sx, dy = e.clientY - m.sy;
      if (m.mode === "drag") { setPos({ x: m.px + dx, y: m.py + dy }); return; }
      let x = m.px, y = m.py, w = m.pw, h = m.ph;
      if (m.dir.includes("e")) w = Math.max(340, m.pw + dx);
      if (m.dir.includes("s")) h = Math.max(240, m.ph + dy);
      if (m.dir.includes("w")) { w = Math.max(340, m.pw - dx); x = m.px + (m.pw - w); }
      if (m.dir.includes("n")) { h = Math.max(240, m.ph - dy); y = m.py + (m.ph - h); }
      setSize({ w, h }); setPos({ x, y });
    };
    const onUp = () => { move.current = null; };
    const onKey = (e) => { if (e.key === "Escape") onClose?.(); };
    window.addEventListener("mousemove", onMove); window.addEventListener("mouseup", onUp); window.addEventListener("keydown", onKey);
    return () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); window.removeEventListener("keydown", onKey); };
  }, [onClose]);
  const anchor = (el) => { const r = el.closest("[data-dashwin]").getBoundingClientRect(); const p = pos || { x: r.left, y: r.top }; if (!pos) setPos(p); return { p }; };
  const startDrag = (e) => { if (e.target.closest("button, input, select")) return; const { p } = anchor(e.currentTarget); move.current = { mode: "drag", sx: e.clientX, sy: e.clientY, px: p.x, py: p.y }; };
  const startResize = (e, dir) => { const { p } = anchor(e.currentTarget); move.current = { mode: "resize", dir, sx: e.clientX, sy: e.clientY, px: p.x, py: p.y, pw: size.w, ph: size.h }; e.preventDefault(); e.stopPropagation(); };
  const HANDLES = [["n", { top: -3, left: 10, right: 10, height: 6, cursor: "ns-resize" }], ["s", { bottom: -3, left: 10, right: 10, height: 6, cursor: "ns-resize" }], ["w", { left: -3, top: 10, bottom: 10, width: 6, cursor: "ew-resize" }], ["e", { right: -3, top: 10, bottom: 10, width: 6, cursor: "ew-resize" }], ["nw", { top: -3, left: -3, width: 12, height: 12, cursor: "nwse-resize" }], ["ne", { top: -3, right: -3, width: 12, height: 12, cursor: "nesw-resize" }], ["sw", { bottom: -3, left: -3, width: 12, height: 12, cursor: "nesw-resize" }], ["se", { bottom: -3, right: -3, width: 12, height: 12, cursor: "nwse-resize" }]];

  const toolBtn = { display: "flex", alignItems: "center", gap: 5, padding: "5px 10px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: F, fontSize: 11.5, cursor: "pointer", flexShrink: 0 };
  const kpiTile = (label, val) => (
    <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: "8px 12px", minWidth: 96, flexShrink: 0 }}>
      <div style={{ fontSize: 10, color: C.dim }}>{label}</div>
      <div style={{ fontSize: 17, fontWeight: 700, color: C.txt, fontFamily: M }}>{fmtNum(val)}</div>
    </div>
  );

  return (
    <div data-dashwin style={{ position: "fixed", ...(pos ? { top: pos.y, left: pos.x } : { top: "50%", left: "50%", transform: "translate(-50%, -50%)" }), zIndex: 1201, width: size.w, height: size.h, maxWidth: "98vw", maxHeight: "94vh", display: "flex", flexDirection: "column", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", overflow: "hidden" }}>
      {/* Barre de titre */}
      <div onMouseDown={startDrag} style={{ padding: "9px 12px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 8, cursor: "move", userSelect: "none", flexShrink: 0 }}>
        <IcBarChart size={15} style={{ color: C.acc, flexShrink: 0 }} />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>Tableau de bord — {layer?.name}</div>
          <div style={{ fontSize: 10.5, color: C.dim }}>{feats.length}/{allFeats.length} entités{cross ? ` · filtre : ${cross.field}=${cross.value}` : ""}</div>
        </div>
        {numericCols.length > 0 && <select value={metric} onChange={e => setMetric(e.target.value)} title="Mesure KPI" style={{ ...toolBtn, cursor: "pointer" }}>{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>}
        <button onClick={() => setCharts(cs => [...cs, mkDefault()])} style={toolBtn}>+ Graphique</button>
        <button onClick={exportCsv} style={toolBtn}><IcFileDown size={13} />CSV</button>
        <button onClick={onClose} title="Fermer (Échap)" style={{ ...toolBtn, fontWeight: 600 }}><IcX size={14} />Fermer</button>
      </div>

      {/* Bandeau KPI + filtre actif */}
      <div style={{ padding: "9px 12px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", gap: 8, overflowX: "auto", flexShrink: 0, alignItems: "center" }}>
        {kpiTile("Entités", kpi.count)}
        {numericCols.length > 0 && <>{kpiTile(`Somme ${metric}`, kpi.sum)}{kpiTile(`Moyenne ${metric}`, kpi.mean)}{kpiTile(`Max ${metric}`, kpi.max)}</>}
        {cross && <button onClick={() => setCross(null)} style={{ ...toolBtn, color: C.acc, borderColor: C.acc }}>✕ {cross.field}={cross.value}</button>}
        <div style={{ marginLeft: "auto", fontSize: 10, color: C.dim, flexShrink: 0 }}>Cliquez une barre/part pour filtrer</div>
      </div>

      {/* Grille de graphiques */}
      <div style={{ flex: 1, overflow: "auto", padding: 12 }}>
        {cols.length === 0 ? (
          <div style={{ padding: 24, textAlign: "center", color: C.dim, fontSize: 12 }}>Cette couche n'a pas d'attributs à visualiser.</div>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 12 }}>
            {charts.map(cfg => (
              <ChartCard key={cfg.id} feats={feats} cols={cols} numericCols={numericCols} C={C} cfg={cfg}
                cross={cross} onCross={onCross} onChange={(next) => update(cfg.id, next)} onRemove={() => remove(cfg.id)} />
            ))}
          </div>
        )}
      </div>

      {HANDLES.map(([dir, st]) => (
        <div key={dir} onMouseDown={e => startResize(e, dir)} style={{ position: "absolute", zIndex: 3, ...st }} />
      ))}
    </div>
  );
}

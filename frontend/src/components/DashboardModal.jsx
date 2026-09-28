/**
 * DashboardModal.jsx — Tableau de bord multi-graphiques d'une couche vecteur.
 * Grille de cartes-graphiques configurables (type, X, Y, agrégation) réutilisant
 * le même moteur d'agrégation que la table attributaire.
 */
import { useState, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX, IcBarChart } from "../icons";

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
  return out.sort((a, b) => b[1] - a[1]).slice(0, 20);
}

function ChartCard({ feats, cols, numericCols, C, cfg, onChange, onRemove }) {
  const { type, x, y, agg } = cfg;
  const series = useMemo(() => aggregate(feats, x, y, agg), [feats, x, y, agg]);
  const maxV = Math.max(1, ...series.map(s => Math.abs(s[1])));
  const totalV = series.reduce((a, s) => a + Math.abs(s[1]), 0) || 1;
  const sel = { padding: "3px 6px", border: `0.5px solid ${C.bdr}`, borderRadius: 6, background: C.input, color: C.txt, fontFamily: F, fontSize: 10.5 };

  const pie = () => {
    const R = 52, cx = 66, cy = 66; let a0 = -Math.PI / 2;
    return (
      <svg width="100%" viewBox="0 0 132 132" style={{ maxHeight: 150 }}>
        {series.map(([k, v], i) => {
          const frac = Math.abs(v) / totalV, a1 = a0 + frac * 2 * Math.PI;
          const d = `M${cx},${cy} L${cx + R * Math.cos(a0)},${cy + R * Math.sin(a0)} A${R},${R} 0 ${frac > 0.5 ? 1 : 0} 1 ${cx + R * Math.cos(a1)},${cy + R * Math.sin(a1)} Z`;
          a0 = a1;
          return <path key={k} d={d} fill={COLORS[i % COLORS.length]} stroke={C.bg} strokeWidth="1" />;
        })}
      </svg>
    );
  };
  const line = () => {
    const W = 260, H = 130, pad = 20;
    const pts = series.map(([, v], i) => [pad + (series.length === 1 ? 0 : i / (series.length - 1) * (W - 2 * pad)), H - pad - (Math.abs(v) / maxV) * (H - 2 * pad)]);
    return <svg width="100%" viewBox={`0 0 ${W} ${H}`}><polyline fill="none" stroke={C.acc} strokeWidth="2" points={pts.map(p => p.join(",")).join(" ")} />{pts.map(([px, py], i) => <circle key={i} cx={px} cy={py} r="2.5" fill={C.acc} />)}</svg>;
  };
  const bars = () => (
    <div style={{ maxHeight: 160, overflow: "auto" }}>
      {series.map(([k, v], i) => (
        <div key={k} style={{ marginBottom: 4 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: C.mut }}>
            <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 160 }}>{k}</span><span style={{ fontFamily: M }}>{fmtNum(v)}</span>
          </div>
          <div style={{ height: 7, borderRadius: 3, background: C.bdr, overflow: "hidden" }}><div style={{ width: `${(Math.abs(v) / maxV) * 100}%`, height: "100%", background: COLORS[i % COLORS.length] }} /></div>
        </div>
      ))}
    </div>
  );

  return (
    <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: 10, display: "flex", flexDirection: "column", gap: 8, minWidth: 0 }}>
      <div style={{ display: "flex", gap: 5, flexWrap: "wrap", alignItems: "center" }}>
        <select value={type} onChange={e => onChange({ ...cfg, type: e.target.value })} style={sel}>
          <option value="bar">Barres</option><option value="line">Ligne</option><option value="pie">Camembert</option>
        </select>
        <select value={x} onChange={e => onChange({ ...cfg, x: e.target.value })} style={sel} title="Étiquette (X)">{cols.map(c => <option key={c} value={c}>{c}</option>)}</select>
        <select value={agg} onChange={e => onChange({ ...cfg, agg: e.target.value })} style={sel}>{Object.entries(AGGS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select>
        {agg !== "count" && <select value={y} onChange={e => onChange({ ...cfg, y: e.target.value })} style={sel} title="Valeur (Y)">{numericCols.map(c => <option key={c} value={c}>{c}</option>)}</select>}
        <button onClick={onRemove} title="Retirer" style={{ marginLeft: "auto", background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={14} /></button>
      </div>
      {type === "pie" ? pie() : type === "line" ? line() : bars()}
    </div>
  );
}

export default function DashboardModal({ layer, onClose }) {
  const C = useThemeContext();
  const feats = layer?.geojson?.features || [];
  const cols = useMemo(() => {
    const seen = new Set(), out = [];
    feats.forEach(f => Object.keys(f.properties || {}).forEach(k => { if (!seen.has(k) && !["geom_json", "geom_wkt"].includes(k)) { seen.add(k); out.push(k); } }));
    return out;
  }, [feats]);
  const numericCols = useMemo(() => cols.filter(c => feats.some(f => typeof f.properties?.[c] === "number")), [cols, feats]);

  const mkDefault = () => ({ id: Math.random().toString(36).slice(2), type: "bar", x: cols[0] || "", y: numericCols[0] || cols[0] || "", agg: numericCols.length ? "sum" : "count" });
  const [charts, setCharts] = useState(() => [mkDefault(), { ...mkDefault(), type: "pie" }]);

  const update = (id, cfg) => setCharts(cs => cs.map(c => c.id === id ? cfg : c));
  const remove = (id) => setCharts(cs => cs.filter(c => c.id !== id));

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", zIndex: 1200, backdropFilter: "blur(2px)" }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1201, width: "min(1000px, 96vw)", maxHeight: "88vh", display: "flex", flexDirection: "column", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", overflow: "hidden" }}>
        <div style={{ padding: "12px 16px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 10 }}>
          <IcBarChart size={16} style={{ color: C.acc }} />
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 13.5, fontWeight: 600, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>Tableau de bord — {layer?.name}</div>
            <div style={{ fontSize: 11, color: C.dim, marginTop: 1 }}>{feats.length} entités · {charts.length} graphique{charts.length > 1 ? "s" : ""}</div>
          </div>
          <button onClick={() => setCharts(cs => [...cs, mkDefault()])} style={{ display: "flex", alignItems: "center", gap: 5, padding: "6px 11px", border: "none", borderRadius: 7, background: C.acc, color: "#04120a", fontFamily: F, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>+ Graphique</button>
          <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex", padding: 2 }}><IcX size={17} /></button>
        </div>
        <div style={{ flex: 1, overflow: "auto", padding: 14 }}>
          {cols.length === 0 ? (
            <div style={{ padding: 24, textAlign: "center", color: C.dim, fontSize: 12 }}>Cette couche n'a pas d'attributs à visualiser.</div>
          ) : (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 12 }}>
              {charts.map(cfg => (
                <ChartCard key={cfg.id} feats={feats} cols={cols} numericCols={numericCols} C={C} cfg={cfg}
                  onChange={(next) => update(cfg.id, next)} onRemove={() => remove(cfg.id)} />
              ))}
            </div>
          )}
        </div>
      </div>
    </>
  );
}

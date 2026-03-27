import { useState, useEffect, useMemo } from "react";
import { getTheme } from "../theme";
import { F, M, RAMPS } from "../config";
import { getLayerAttrs, getNumVals, getUniques } from "../utils/classification";
import { Btn } from "./ui";

// ═══════ STATS ═══════
function StatsWidget({ layer }) {
  const C = getTheme();
  const attrs = useMemo(() => getLayerAttrs(layer), [layer]);
  const stats = useMemo(() => {
    const r = {};
    attrs.num.forEach(a => {
      const v = getNumVals(layer, a); if (!v.length) return;
      const s = [...v].sort((a, b) => a - b), sum = v.reduce((s, x) => s + x, 0), mean = sum / v.length;
      r[a] = { count: v.length, min: s[0], max: s[s.length - 1], mean: Math.round(mean * 100) / 100, median: s[Math.floor(s.length / 2)], std: Math.round(Math.sqrt(v.reduce((s, x) => s + (x - mean) ** 2, 0) / v.length) * 100) / 100 };
    });
    return r;
  }, [layer, attrs]);

  const entries = Object.entries(stats);
  if (!entries.length) return <div style={{ padding: 10, fontSize: 11, color: C.dim }}>Aucun attribut numérique</div>;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, padding: 10 }}>
      {entries.map(([a, s]) => (
        <div key={a} style={{ background: C.hover, borderRadius: 6, padding: "6px 10px", border: `0.5px solid ${C.bdr}` }}>
          <div style={{ fontSize: 11, fontWeight: 500, color: C.acc, fontFamily: M, marginBottom: 4 }}>{a}</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 4, fontSize: 10 }}>
            {[["Min", s.min], ["Max", s.max], ["Moy", s.mean], ["Med", s.median], ["σ", s.std], ["Nb", s.count]].map(([l, v]) => (
              <div key={l}><div style={{ color: C.dim }}>{l}</div><div style={{ color: C.txt, fontFamily: M, fontWeight: 500 }}>{typeof v === "number" ? v.toLocaleString("fr", { maximumFractionDigits: 2 }) : v}</div></div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

// ═══════ HISTOGRAM ═══════
function Histogram({ layer, attribute }) {
  const C = getTheme();
  const vals = useMemo(() => getNumVals(layer, attribute), [layer, attribute]);
  const [br, setBr] = useState(null);
  const nB = 20;
  const bins = useMemo(() => {
    if (!vals.length) return [];
    const mn = Math.min(...vals), mx = Math.max(...vals), st = (mx - mn) / nB || 1;
    const b = Array.from({ length: nB }, (_, i) => ({ x0: Math.round((mn + st * i) * 100) / 100, x1: Math.round((mn + st * (i + 1)) * 100) / 100, count: 0 }));
    vals.forEach(v => { const i = Math.min(Math.floor((v - mn) / st), nB - 1); if (i >= 0) b[i].count++; });
    return b;
  }, [vals]);
  const mx = Math.max(...bins.map(b => b.count), 1);
  const W = 400, H = 90, bW = W / nB;
  if (!vals.length) return null;
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
        <span style={{ fontSize: 10, color: C.dim, fontFamily: M }}>{attribute}</span>
        {br && <Btn small color={C.red} onClick={() => setBr(null)}>Reset</Btn>}
      </div>
      <svg width="100%" viewBox={`0 0 ${W} ${H + 16}`} style={{ display: "block" }}>
        {bins.map((b, i) => {
          const h = (b.count / mx) * H, inB = br ? b.x0 >= br[0] && b.x1 <= br[1] : true;
          return <rect key={i} x={i * bW + 1} y={H - h} width={bW - 2} height={h} rx="1" fill={inB ? C.acc : C.dim} opacity={inB ? .8 : .2} style={{ cursor: "pointer" }}
            onClick={() => { const r = [b.x0, b.x1]; const same = br && br[0] === r[0] && br[1] === r[1]; setBr(same ? null : r); }} />;
        })}
        <text x="0" y={H + 12} fill={C.dim} fontSize="8" fontFamily={M}>{bins[0]?.x0}</text>
        <text x={W} y={H + 12} fill={C.dim} fontSize="8" fontFamily={M} textAnchor="end">{bins[bins.length - 1]?.x1}</text>
        <text x={W / 2} y={H + 12} fill={C.dim} fontSize="8" fontFamily={M} textAnchor="middle">{vals.length} val.</text>
      </svg>
      {br && <div style={{ fontSize: 10, color: C.acc, marginTop: 2 }}>Filtre: {br[0]} – {br[1]}</div>}
    </div>
  );
}

// ═══════ PIE CHART ═══════
function Pie({ layer, attribute }) {
  const C = getTheme();
  const u = useMemo(() => getUniques(layer, attribute).slice(0, 8), [layer, attribute]);
  const total = u.reduce((s, x) => s + x.count, 0), cols = RAMPS.categorial, R = 45, cx = 55, cy = 55;
  let a = -Math.PI / 2;
  if (!u.length) return null;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <svg width="110" height="110" viewBox="0 0 110 110">
        {u.map((v, i) => { const an = (v.count / total) * Math.PI * 2, x1 = cx + R * Math.cos(a), y1 = cy + R * Math.sin(a), x2 = cx + R * Math.cos(a + an), y2 = cy + R * Math.sin(a + an); const lg = an > Math.PI ? 1 : 0; const d = `M${cx} ${cy}L${x1} ${y1}A${R} ${R} 0 ${lg} 1 ${x2} ${y2}Z`; a += an; return <path key={i} d={d} fill={cols[i % cols.length]} opacity=".85" />; })}
        <circle cx={cx} cy={cy} r={R * .45} fill={C.card} />
        <text x={cx} y={cy + 3} textAnchor="middle" fill={C.txt} fontSize="12" fontWeight="600" fontFamily={M}>{total}</text>
      </svg>
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        {u.map((v, i) => (
          <div key={v.value} style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 10 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: cols[i % cols.length], flexShrink: 0 }} />
            <span style={{ color: C.mut, maxWidth: 90, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{v.value}</span>
            <span style={{ color: C.dim, fontFamily: M, marginLeft: "auto" }}>{v.count}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════ ATTRIBUTE TABLE ═══════
function AttrTable({ layers, onZoom }) {
  const C = getTheme();
  const [lid, setLid] = useState(null);
  const [sortC, setSortC] = useState(null);
  const [sortD, setSortD] = useState("asc");
  const [flt, setFlt] = useState("");
  const [pg, setPg] = useState(0);
  const PS = 50;

  const layer = layers.find(l => l.id === lid) || layers[0];
  const feats = layer?.geojson?.features || [];
  const cols = useMemo(() => {
    if (!feats.length) return [];
    const k = new Set();
    feats.slice(0, 20).forEach(f => Object.keys(f.properties || {}).forEach(c => k.add(c)));
    return [...k].filter(c => !["geom_json", "geom_wkt"].includes(c));
  }, [feats]);

  const rows = useMemo(() => {
    let r = feats.map((f, i) => ({ ...f.properties, _i: i, _g: f.geometry }));
    if (flt) { const q = flt.toLowerCase(); r = r.filter(x => cols.some(c => String(x[c] ?? "").toLowerCase().includes(q))); }
    if (sortC) r.sort((a, b) => { const va = a[sortC], vb = b[sortC]; if (va == null) return 1; if (vb == null) return -1; const na = Number(va), nb = Number(vb); const cmp = (!isNaN(na) && !isNaN(nb)) ? na - nb : String(va).localeCompare(String(vb)); return sortD === "asc" ? cmp : -cmp; });
    return r;
  }, [feats, cols, flt, sortC, sortD]);

  const paged = rows.slice(pg * PS, (pg + 1) * PS), tpg = Math.ceil(rows.length / PS);

  if (!layers.length) return <div style={{ padding: 10, fontSize: 11, color: C.dim }}>Chargez des données via le chat</div>;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 10px", borderBottom: `0.5px solid ${C.bdr}`, flexShrink: 0 }}>
        <select value={lid || layer?.id || ""} onChange={e => { setLid(e.target.value); setPg(0); }}
          style={{ fontFamily: F, fontSize: 11, padding: "3px 8px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }}>
          {layers.map(l => <option key={l.id} value={l.id}>{l.name} ({l.featureCount})</option>)}
        </select>
        <input value={flt} onChange={e => { setFlt(e.target.value); setPg(0); }} placeholder="Filtrer..."
          style={{ fontFamily: F, fontSize: 11, padding: "3px 8px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: 120 }} />
        <span style={{ fontSize: 10, color: C.dim, marginLeft: "auto" }}>{rows.length}/{feats.length}</span>
        <Btn small onClick={() => setPg(Math.max(0, pg - 1))} active={pg > 0}>◀</Btn>
        <span style={{ fontSize: 10, color: C.dim }}>{pg + 1}/{tpg || 1}</span>
        <Btn small onClick={() => setPg(Math.min(tpg - 1, pg + 1))} active={pg < tpg - 1}>▶</Btn>
      </div>
      <div style={{ flex: 1, overflowY: "auto", overflowX: "auto" }}>
        <table style={{ borderCollapse: "collapse", fontSize: 11, width: "100%", minWidth: cols.length * 100 }}>
          <thead>
            <tr style={{ position: "sticky", top: 0, background: C.card, zIndex: 1 }}>
              <th style={{ padding: "4px 8px", borderBottom: `0.5px solid ${C.bdr}`, color: C.dim, fontSize: 10, fontWeight: 400, textAlign: "left" }}>#</th>
              {cols.map(c => (
                <th key={c} onClick={() => { if (sortC === c) setSortD(d => d === "asc" ? "desc" : "asc"); else { setSortC(c); setSortD("asc"); } }}
                  style={{ padding: "4px 8px", borderBottom: `0.5px solid ${C.bdr}`, color: sortC === c ? C.acc : C.dim, fontSize: 10, fontWeight: sortC === c ? 500 : 400, textAlign: "left", cursor: "pointer", whiteSpace: "nowrap", userSelect: "none" }}>
                  {c}{sortC === c ? (sortD === "asc" ? " ↑" : " ↓") : ""}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paged.map((r, i) => (
              <tr key={r._i} style={{ borderBottom: `0.5px solid ${C.bdr}`, cursor: "pointer" }}
                onClick={() => {
                  const g = r._g; if (!g?.coordinates) return;
                  let ln, lt;
                  if (g.type === "Point") { ln = g.coordinates[0]; lt = g.coordinates[1]; }
                  else if (g.type === "Polygon") { const p = g.coordinates[0] || []; ln = p.reduce((s, c) => s + c[0], 0) / p.length; lt = p.reduce((s, c) => s + c[1], 0) / p.length; }
                  if (ln != null) onZoom?.(ln, lt);
                }}
                onMouseEnter={e => e.currentTarget.style.background = C.hover}
                onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                <td style={{ padding: "3px 8px", color: C.dim, fontFamily: M, fontSize: 9 }}>{pg * PS + i + 1}</td>
                {cols.map(c => (
                  <td key={c} style={{ padding: "3px 8px", color: C.txt, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {r[c] != null ? (typeof r[c] === "object" ? JSON.stringify(r[c]) : String(r[c])) : ""}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

import SpatialPanel from "./SpatialPanel";

// ═══════ BOTTOM PANEL (main export) ═══════
export default function BottomPanel({ layers, activeTab, onTab, onZoom, onAddLayer }) {
  const C = getTheme();
  const [selNum, setSelNum] = useState("");
  const [selCat, setSelCat] = useState("");
  const layer = layers.filter(l => l.visible)[0];
  const attrs = useMemo(() => layer ? getLayerAttrs(layer) : { num: [], cat: [] }, [layer]);

  useEffect(() => {
    if (attrs.num.length && !selNum) setSelNum(attrs.num[0]);
    if (attrs.cat.length && !selCat) setSelCat(attrs.cat[0]);
  }, [attrs]);

  const tabs = [
    { key: "table", label: "Table" },
    { key: "stats", label: "Stats" },
    { key: "histo", label: "Histogramme" },
    { key: "pie", label: "Répartition" },
    { key: "spatial", label: "Analyse spatiale" },
  ];

  return (
    <div style={{ borderTop: `0.5px solid ${C.bdr}`, background: C.card }}>
      <div style={{ display: "flex", alignItems: "center", padding: "0 10px", borderBottom: activeTab ? `0.5px solid ${C.bdr}` : "none", flexShrink: 0 }}>
        {tabs.map(t => (
          <button key={t.key} onClick={() => onTab(activeTab === t.key ? null : t.key)} style={{
            fontFamily: F, fontSize: 11, fontWeight: 500, padding: "7px 14px", border: "none", cursor: "pointer",
            background: activeTab === t.key ? C.acc + "15" : "transparent",
            color: activeTab === t.key ? C.acc : C.mut,
            borderBottom: activeTab === t.key ? `2px solid ${C.acc}` : "2px solid transparent",
          }}>{t.label}</button>
        ))}
        {activeTab && (activeTab === "histo" || activeTab === "pie") && (
          <select value={activeTab === "histo" ? selNum : selCat}
            onChange={e => activeTab === "histo" ? setSelNum(e.target.value) : setSelCat(e.target.value)}
            style={{ fontFamily: F, fontSize: 10, padding: "3px 6px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", marginLeft: 8 }}>
            {(activeTab === "histo" ? attrs.num : attrs.cat).map(a => <option key={a} value={a}>{a}</option>)}
          </select>
        )}
        {layers.length > 0 && <span style={{ fontSize: 10, color: C.dim, marginLeft: "auto", padding: "0 8px" }}>{layer?.featureCount || 0} features</span>}
      </div>
      {activeTab && (
        <div style={{ height: activeTab === "table" ? 220 : activeTab === "spatial" ? 260 : 150, overflowY: "auto", overflowX: "auto" }}>
          {activeTab === "table" && <AttrTable layers={layers.filter(l => l.visible)} onZoom={onZoom} />}
          {activeTab === "stats" && layer && <StatsWidget layer={layer} />}
          {activeTab === "histo" && layer && selNum && <div style={{ padding: 10 }}><Histogram layer={layer} attribute={selNum} /></div>}
          {activeTab === "pie" && layer && selCat && <div style={{ padding: 10 }}><Pie layer={layer} attribute={selCat} /></div>}
          {activeTab === "spatial" && <SpatialPanel layers={layers} onAddLayer={onAddLayer} />}
        </div>
      )}
    </div>
  );
}

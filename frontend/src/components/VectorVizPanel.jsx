/**
 * VectorVizPanel.jsx — Densité & flux : symbologie avancée pour couches vecteur.
 *
 *  • Chaleur (KDE) + Clusters : rendu natif MapLibre (drapeaux heatmap/cluster
 *    sur une couche point ; l'app sait déjà les rendre).
 *  • Hexagones : agrège les points dans une grille hexagonale (turf.hexGrid)
 *    colorée par le nombre de points.
 *  • Flux : arcs origine→destination (turf.greatCircle) depuis une couche ligne,
 *    largeur proportionnelle à une valeur.
 *
 * Sans backend : tout est calculé côté client (turf déjà en dépendance) puis
 * ajouté via onAdd (addLayerSilent) avec les bons drapeaux de rendu.
 */
import { useState, useMemo, useCallback } from "react";
import * as turf from "@turf/turf";
import { useThemeContext } from "../theme";
import { F, M, RAMPS } from "../config";

const hasGeom = (l, ...types) => l.geojson?.features?.some((f) => types.includes(f.geometry?.type));
function numericFields(feats) {
  const sample = (feats || []).slice(0, 40); const keys = new Set();
  sample.forEach((f) => Object.keys(f.properties || {}).forEach((k) => keys.add(k)));
  return [...keys].filter((k) => {
    let n = 0, t = 0;
    for (const f of sample) { const v = f.properties?.[k]; if (v == null || v === "") continue; t++; if (typeof v === "number" || (typeof v === "string" && v.trim() !== "" && isFinite(Number(v)))) n++; }
    return t > 0 && n / t >= 0.7;
  });
}

export default function VectorVizPanel({ layers = [], onAdd }) {
  const C = useThemeContext();
  const pts = useMemo(() => layers.filter((l) => l.geojson && hasGeom(l, "Point", "MultiPoint")), [layers]);
  const lines = useMemo(() => layers.filter((l) => l.geojson && hasGeom(l, "LineString", "MultiLineString")), [layers]);

  const [tab, setTab] = useState("heat");
  const [msg, setMsg] = useState(null);
  const [err, setErr] = useState(null);

  // ── Chaleur & clusters ──
  const [pid, setPid] = useState(pts[0]?.id || "");
  const pLayer = pts.find((l) => l.id === pid) || pts[0] || null;
  const [mode, setMode] = useState("heat");
  const [hRadius, setHRadius] = useState(30);
  const [hInt, setHInt] = useState(3);
  const [cRadius, setCRadius] = useState(50);

  const runHeatCluster = useCallback(() => {
    if (!pLayer) return; setErr(null); setMsg(null);
    if (mode === "heat") {
      onAdd?.(pLayer.geojson, `Chaleur · ${pLayer.name}`, "heatmap", { heatmap: true, heatmapRadius: Number(hRadius), heatmapIntensity: Number(hInt), opacity: 0.85 });
      setMsg(`Carte de chaleur créée depuis « ${pLayer.name} ».`);
    } else {
      onAdd?.(pLayer.geojson, `Clusters · ${pLayer.name}`, "cluster", { cluster: true, clusterRadius: Number(cRadius), color: pLayer.color, opacity: 1 });
      setMsg(`Regroupement (clusters) créé depuis « ${pLayer.name} ».`);
    }
  }, [pLayer, mode, hRadius, hInt, cRadius, onAdd]);

  // ── Hexagones ──
  const [hid, setHid] = useState(pts[0]?.id || "");
  const hLayer = pts.find((l) => l.id === hid) || pts[0] || null;
  const [cell, setCell] = useState(20);

  const runHex = useCallback(() => {
    if (!hLayer?.geojson?.features?.length) return; setErr(null); setMsg(null);
    const src = hLayer.geojson;
    try {
      const bb = turf.bbox(src);
      const areaKm2 = turf.area(turf.bboxPolygon(bb)) / 1e6;
      const est = areaKm2 / (cell * cell * 2.598);           // hexagones estimés
      if (est > 4000) { setErr(`Trop d'hexagones (~${Math.round(est)}). Augmentez la taille de cellule.`); return; }
      // Élargit l'emprise avant la grille : hexGrid coupe les hexagones de bord,
      // sinon les points près des bords ne tomberaient dans aucune cellule.
      const bb2 = turf.bbox(turf.buffer(turf.bboxPolygon(bb), cell * 1.2, { units: "kilometers" }));
      const grid = turf.hexGrid(bb2, cell, { units: "kilometers" });
      const P = src.features;
      grid.features.forEach((h) => {
        let c = 0;
        for (const p of P) { if (p.geometry?.type === "Point" && turf.booleanPointInPolygon(p, h)) c++; }
        h.properties = { count: c };
      });
      const filled = { type: "FeatureCollection", features: grid.features.filter((h) => h.properties.count > 0) };
      if (!filled.features.length) { setErr("Aucun point dans la grille."); return; }
      const counts = filled.features.map((h) => h.properties.count);
      const mn = Math.min(...counts), mx = Math.max(...counts);
      const ramp = [...RAMPS.reds].reverse();                 // clair (peu) → foncé (beaucoup)
      let expr;
      if (mx <= mn) { expr = ramp[ramp.length - 1]; }
      else { const stops = []; ramp.forEach((col, i) => stops.push(mn + (mx - mn) * i / (ramp.length - 1), col)); expr = ["interpolate", ["linear"], ["get", "count"], ...stops]; }
      onAdd?.(filled, `Hexagones · ${hLayer.name}`, "hexbin", { classResult: { type: "choropleth", expression: expr }, color: ramp[ramp.length - 1], opacity: 1, labels: false });
      setMsg(`${filled.features.length} hexagones · ${P.length} points (max ${mx}).`);
    } catch (e) { setErr("Agrégation impossible : " + (e.message || e)); }
  }, [hLayer, cell, onAdd]);

  // ── Flux ──
  const [lid, setLid] = useState(lines[0]?.id || "");
  const lLayer = lines.find((l) => l.id === lid) || lines[0] || null;
  const lFields = useMemo(() => numericFields(lLayer?.geojson?.features), [lLayer]);
  const [field, setField] = useState("");

  const runFlow = useCallback(() => {
    if (!lLayer?.geojson?.features?.length) return; setErr(null); setMsg(null);
    const feats = lLayer.geojson.features;
    if (feats.length > 5000) { setErr(`Trop de lignes (${feats.length}) — limite 5000.`); return; }
    const arcs = [];
    for (const f of feats) {
      const g = f.geometry; if (!g) continue;
      const cc = g.type === "LineString" ? g.coordinates : g.type === "MultiLineString" ? g.coordinates[0] : null;
      if (!cc || cc.length < 2) continue;
      try {
        const arc = turf.greatCircle(turf.point(cc[0]), turf.point(cc[cc.length - 1]), { properties: f.properties || {}, npoints: 48 });
        if (arc.geometry?.type === "LineString") arcs.push(arc);
      } catch (_) { /* paire dégénérée */ }
    }
    if (!arcs.length) { setErr("Aucun arc généré (lignes trop courtes ?)."); return; }
    let classResult = null;
    const fld = field && lFields.includes(field) ? field : null;
    if (fld) {
      const vals = feats.map((f) => Number(f.properties?.[fld])).filter(Number.isFinite);
      const mn = Math.min(...vals), mx = Math.max(...vals);
      if (mx > mn) classResult = { type: "proportional_line", widthExpression: ["interpolate", ["linear"], ["to-number", ["get", fld], mn], mn, 1, mx, 8] };
    }
    onAdd?.({ type: "FeatureCollection", features: arcs }, `Flux · ${lLayer.name}`, "flow", { color: "#378ADD", opacity: 0.8, classResult });
    setMsg(`${arcs.length} arcs de flux${fld ? ` (largeur ∝ ${fld})` : ""}.`);
  }, [lLayer, field, lFields, onAdd]);

  // ── UI ──
  const tabBtn = (id, label) => (
    <button key={id} onClick={() => { setTab(id); setErr(null); setMsg(null); }}
      style={{ fontFamily: F, fontSize: 11.5, fontWeight: tab === id ? 600 : 500, padding: "5px 9px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>
      {label}
    </button>
  );
  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };
  const Code = ({ children }) => (<code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>);
  const runBtn = (onClick, label, disabled) => (
    <button onClick={onClick} disabled={disabled}
      style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: disabled ? "not-allowed" : "pointer",
        background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: disabled ? 0.5 : 1 }}>
      {label}
    </button>
  );
  const layerSelect = (list, val, set, kind) => (
    <div>
      <div style={lbl}>Couche {kind}</div>
      {list.length === 0 ? (
        <div style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucune couche {kind} chargée.</div>
      ) : (
        <select value={val} onChange={(e) => set(e.target.value)} style={inp}>
          {list.map((l) => <option key={l.id} value={l.id}>{l.name} · {l.geojson.features.length}</option>)}
        </select>
      )}
    </div>
  );
  const feedback = (
    <>
      {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
      {msg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{msg}</div>}
    </>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}`, flexWrap: "wrap" }}>
        {tabBtn("heat", "Chaleur & clusters")}
        {tabBtn("hex", "Hexagones")}
        {tabBtn("flow", "Flux")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Symbologie avancée pour révéler la structure d'un semis de points ou de flux.</p>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Chaleur & clusters</div>
            <p style={{ margin: 0, color: C.mut }}>Sur une couche de <b>points</b> : une <b>carte de chaleur</b> (densité, KDE) montre les zones de concentration ; le <b>regroupement</b> (clusters) agrège les points proches en pastilles chiffrées qui se scindent au zoom.</p></div>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Hexagones</div>
            <p style={{ margin: 0, color: C.mut }}>Découpe l'emprise en <b>grille hexagonale</b> et compte les points par cellule (<Code>count</Code>), colorée du clair (peu) au foncé (beaucoup).</p></div>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Flux (origine→destination)</div>
            <p style={{ margin: 0, color: C.mut }}>Depuis une couche de <b>lignes</b>, trace des <b>arcs</b> (grands cercles) entre le début et la fin de chaque ligne ; leur <b>largeur</b> peut être proportionnelle à une valeur.</p></div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            Tout est calculé côté navigateur (sans backend). Les résultats s'ajoutent comme des couches ordinaires (gérables dans le gestionnaire de couches). Hexagones bornés (~4 000 cellules).
          </div>
        </div>
      ) : tab === "heat" ? (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {layerSelect(pts, pid, setPid, "de points")}
          <div style={{ display: "flex", gap: 6 }}>
            {[["heat", "Chaleur (densité)"], ["cluster", "Clusters"]].map(([m, label]) => (
              <button key={m} onClick={() => setMode(m)}
                style={{ flex: 1, fontFamily: F, fontSize: 12, padding: "7px 6px", borderRadius: 7, cursor: "pointer",
                  border: `0.5px solid ${mode === m ? C.acc + "55" : C.bdr}`, background: mode === m ? C.acc + "18" : "transparent", color: mode === m ? C.acc : C.mut }}>
                {label}
              </button>
            ))}
          </div>
          {mode === "heat" ? (
            <>
              <div><div style={lbl}>Rayon · {hRadius}px</div><input type="range" min={8} max={60} value={hRadius} onChange={(e) => setHRadius(e.target.value)} style={{ width: "100%" }} /></div>
              <div><div style={lbl}>Intensité · {hInt}</div><input type="range" min={1} max={6} step={0.5} value={hInt} onChange={(e) => setHInt(e.target.value)} style={{ width: "100%" }} /></div>
            </>
          ) : (
            <div><div style={lbl}>Rayon de regroupement · {cRadius}px</div><input type="range" min={20} max={100} value={cRadius} onChange={(e) => setCRadius(e.target.value)} style={{ width: "100%" }} /></div>
          )}
          {runBtn(runHeatCluster, mode === "heat" ? "Créer la carte de chaleur" : "Créer les clusters", !pLayer)}
          {feedback}
        </div>
      ) : tab === "hex" ? (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {layerSelect(pts, hid, setHid, "de points")}
          <div><div style={lbl}>Taille de cellule · {cell} km</div><input type="range" min={1} max={100} value={cell} onChange={(e) => setCell(e.target.value)} style={{ width: "100%" }} /></div>
          {runBtn(runHex, "Agréger en hexagones", !hLayer)}
          {feedback}
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {layerSelect(lines, lid, setLid, "de lignes")}
          <div>
            <div style={lbl}>Largeur ∝ (optionnel)</div>
            <select value={field} onChange={(e) => setField(e.target.value)} style={inp}>
              <option value="">— largeur fixe —</option>
              {lFields.map((k) => <option key={k} value={k}>{k}</option>)}
            </select>
          </div>
          {runBtn(runFlow, "Générer les arcs de flux", !lLayer)}
          {feedback}
        </div>
      )}
    </div>
  );
}

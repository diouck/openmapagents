/**
 * VectorVizPanel.jsx — Chaleur & clusters : symbologie de DENSITÉ pour points.
 *
 * Carte de chaleur (KDE) et regroupement (clusters), rendu NATIF MapLibre — deux
 * visualisations qui n'existaient pas ailleurs. L'agrégation hexagonale et les
 * flux O→D vivent déjà dans l'outil « Analyse spatiale » (hex_grid, density_grid,
 * desire_lines, od_flows…) → non dupliqués ici.
 *
 * 100% côté navigateur : pose les drapeaux `heatmap` / `cluster` sur une copie
 * de la couche via onAdd (addLayerSilent), l'app sait déjà les rendre.
 */
import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const hasGeom = (l, ...types) => l.geojson?.features?.some((f) => types.includes(f.geometry?.type));

export default function VectorVizPanel({ layers = [], onAdd }) {
  const C = useThemeContext();
  const pts = useMemo(() => layers.filter((l) => l.geojson && hasGeom(l, "Point", "MultiPoint")), [layers]);

  const [tab, setTab] = useState("go");
  const [pid, setPid] = useState(pts[0]?.id || "");
  const pLayer = pts.find((l) => l.id === pid) || pts[0] || null;
  const [mode, setMode] = useState("heat");
  const [hRadius, setHRadius] = useState(30);
  const [hInt, setHInt] = useState(3);
  const [cRadius, setCRadius] = useState(50);
  const [msg, setMsg] = useState(null);

  const run = useCallback(() => {
    if (!pLayer) return; setMsg(null);
    if (mode === "heat") {
      onAdd?.(pLayer.geojson, `Chaleur · ${pLayer.name}`, "heatmap", { heatmap: true, heatmapRadius: Number(hRadius), heatmapIntensity: Number(hInt), opacity: 0.85 });
      setMsg(`Carte de chaleur créée depuis « ${pLayer.name} ».`);
    } else {
      onAdd?.(pLayer.geojson, `Clusters · ${pLayer.name}`, "cluster", { cluster: true, clusterRadius: Number(cRadius), color: pLayer.color, opacity: 1 });
      setMsg(`Regroupement (clusters) créé depuis « ${pLayer.name} ».`);
    }
  }, [pLayer, mode, hRadius, hInt, cRadius, onAdd]);

  const tabBtn = (id, label) => (
    <button key={id} onClick={() => { setTab(id); setMsg(null); }}
      style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>
      {label}
    </button>
  );
  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("go", "Chaleur & clusters")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Deux façons de faire parler un semis de <b>points</b>.</p>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Carte de chaleur (densité, KDE)</div>
            <p style={{ margin: 0, color: C.mut }}>Un dégradé continu révèle les <b>zones de concentration</b> ; le rayon élargit ou resserre les foyers, l'intensité accentue les pics.</p></div>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Regroupement (clusters)</div>
            <p style={{ margin: 0, color: C.mut }}>Les points proches se condensent en <b>pastilles chiffrées</b> qui se scindent au zoom — idéal pour des milliers de points sans surcharge.</p></div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            Pour l'agrégation en <b>hexagones</b>, la <b>densité sur grille</b> ou les <b>flux origine→destination</b>, voir l'outil <b>Analyse spatiale</b> (groupes Géométrie, Statistiques et Mobilité/Flux). Ici, tout est calculé côté navigateur et rendu nativement par MapLibre.
          </div>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <div>
            <div style={lbl}>Couche de points</div>
            {pts.length === 0 ? (
              <div style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucune couche de points chargée.</div>
            ) : (
              <select value={pid} onChange={(e) => setPid(e.target.value)} style={inp}>
                {pts.map((l) => <option key={l.id} value={l.id}>{l.name} · {l.geojson.features.length}</option>)}
              </select>
            )}
          </div>

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

          <button onClick={run} disabled={!pLayer}
            style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: pLayer ? "pointer" : "not-allowed",
              background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: pLayer ? 1 : 0.5 }}>
            {mode === "heat" ? "Créer la carte de chaleur" : "Créer les clusters"}
          </button>

          {msg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{msg}</div>}
        </div>
      )}
    </div>
  );
}

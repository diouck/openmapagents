/**
 * FloodSimPanel.jsx — Module « Simulation d'inondation » (pluvial 2D, v1).
 *
 * Décisions v1 : solveur backend (numpy WCA2D) · pluvial urbain · MNT GEE (SRTM)
 * ou raster importé · sorties = profondeur dans le temps + profondeur max +
 * surface inondée. Le calcul est serveur (POST /api/flood/simulate) ; le panneau
 * anime les frames en remplaçant l'image d'une seule couche overlay.
 *
 * Le cadre/titre est dessiné par FloatingPanel : ce composant ne dessine pas de
 * cadre. Racine scrollable (responsive), comme les autres panneaux.
 */
import { useState, useEffect, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const API = import.meta.env.VITE_API_URL || (import.meta.env.DEV ? "http://localhost:8000" : "");

export default function FloodSimPanel({ mapRef, onAddImageLayer, onUpdateRasterLayer, onAddLayer, onAddLayerSilent, layers = [] }) {
  const C = useThemeContext();

  const [demSource, setDemSource] = useState("gee");   // gee | <rasterToken>
  const [rain, setRain]   = useState(50);              // mm/h
  const [dur, setDur]     = useState(60);              // min
  const [infil, setInfil] = useState(5);               // mm/h
  const [manning, setManning] = useState(0.05);
  const [buildings, setBuildings] = useState(true);    // obstacles OSM
  const [flowArrows, setFlowArrows] = useState(true);  // sens d'écoulement
  const [busy, setBusy]   = useState(false);
  const [err, setErr]     = useState(null);
  const [res, setRes]     = useState(null);            // payload simulate
  const [frameIdx, setFrameIdx] = useState(0);
  const [showMax, setShowMax] = useState(false);
  const [playing, setPlaying] = useState(false);
  const [opacity, setOpacity] = useState(0.8);

  const layerIdRef = useRef(null);
  const styleVRef = useRef(0);
  const mapObj = () => mapRef?.current?.getMap?.() || null;

  // Rasters importés utilisables comme MNT.
  const importedDems = layers.filter(l => l.kind === "image" && l.rasterToken);

  async function run() {
    const m = mapObj();
    if (!m) { setErr("Carte non prête."); return; }
    setBusy(true); setErr(null); setPlaying(false);
    try {
      const b = m.getBounds();
      const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
      const body = {
        bbox, rainfall_mm_h: Number(rain), duration_min: Number(dur),
        infiltration_mm_h: Number(infil), manning: Number(manning), n_frames: 12,
        buildings, flow_arrows: flowArrows,
      };
      if (demSource === "gee") { body.dem_source = "gee"; body.dem_asset = "SRTM_30m"; }
      else if (demSource === "ign") { body.dem_source = "ign"; }
      else { body.dem_source = "imported"; body.raster_token = demSource; }
      const r = await fetch(`${API}/api/flood/simulate`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      if (!d.frames?.length) throw new Error("Aucune frame produite.");
      setRes(d); setFrameIdx(d.frames.length - 1); setShowMax(false);
      // Couche flèches « sens de l'écoulement » (addLayerSilent applique les overrides couleur).
      if (d.flow_geojson?.features?.length) {
        (onAddLayerSilent || onAddLayer)?.(d.flow_geojson, "Écoulement (sens)", "analysis", { color: "#ffffff", opacity: 0.95 });
      }
      // Crée/replace la couche overlay.
      const id = layerIdRef.current || `flood_${Date.now()}`;
      layerIdRef.current = id;
      const last = d.frames[d.frames.length - 1];
      onAddImageLayer?.({
        id, name: "Inondation — profondeur", imageUrl: `data:image/png;base64,${last.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity, legend: d.legend, unit: "m", fit: true,
      });
    } catch (e) { setErr(String(e.message || e)); }
    finally { setBusy(false); }
  }

  // Applique la frame (ou la profondeur max) à la couche.
  function applyFrame(idx, max) {
    if (!res || !layerIdRef.current) return;
    const png = max ? res.max_depth_png_b64 : res.frames[idx]?.png_b64;
    if (!png) return;
    styleVRef.current += 1;
    onUpdateRasterLayer?.(layerIdRef.current, {
      imageUrl: `data:image/png;base64,${png}`, styleV: styleVRef.current,
      name: max ? "Inondation — profondeur max" : "Inondation — profondeur",
    });
  }

  useEffect(() => { if (res) applyFrame(frameIdx, showMax); /* eslint-disable-next-line */ }, [frameIdx, showMax]);

  // Lecture animée.
  useEffect(() => {
    if (!playing || !res) return;
    const n = res.frames.length;
    const t = setInterval(() => setFrameIdx(i => (i + 1) % n), 600);
    return () => clearInterval(t);
  }, [playing, res]);

  const cur = res?.frames?.[frameIdx];

  // ── styles ──
  const lab = { fontSize: 10, fontWeight: 600, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", margin: "0 0 4px" };
  const inp = { fontFamily: F, fontSize: 12, padding: "7px 9px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input, color: C.txt, width: "100%", outline: "none", boxSizing: "border-box" };
  const sec = { padding: "10px 12px 0" };
  const btn = (bg) => ({ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 12px", borderRadius: 8, border: "none", background: bg, color: "#fff", cursor: "pointer", width: "100%" });
  const kpi = (v, l) => (
    <div style={{ flex: 1, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "7px 8px" }}>
      <div style={{ fontSize: 15, fontWeight: 700, color: C.txt, fontFamily: M }}>{v}</div>
      <div style={{ fontSize: 9, color: C.mut, textTransform: "uppercase", letterSpacing: ".04em" }}>{l}</div>
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0, overflowY: "auto", overflowX: "hidden", paddingBottom: 12 }}>
      {/* Source MNT */}
      <div style={sec}>
        <p style={lab}>MNT (modèle de terrain)</p>
        <select style={inp} value={demSource} onChange={e => setDemSource(e.target.value)}>
          <option value="ign">IGN RGE ALTI — ~1–5 m (France, emprise de la carte)</option>
          <option value="gee">GEE — SRTM 30 m (mondial, emprise de la carte)</option>
          {importedDems.map(l => <option key={l.id} value={l.rasterToken}>Raster importé — {l.name}</option>)}
        </select>
        <p style={{ fontSize: 9.5, color: C.mut, marginTop: 4 }}>IGN RGE ALTI = fin (France). SRTM 30 m = mondial mais grossier. Tu peux aussi importer ton MNT .tif.</p>
      </div>

      {/* Pluie & durée */}
      <div style={sec}>
        <p style={lab}>Pluie & durée</p>
        <div style={{ display: "flex", gap: 6 }}>
          <div style={{ flex: 1 }}>
            <input type="number" min={0} step={5} value={rain} onChange={e => setRain(e.target.value)} style={inp} />
            <div style={{ fontSize: 9, color: C.mut, marginTop: 2 }}>Intensité (mm/h)</div>
          </div>
          <div style={{ flex: 1 }}>
            <input type="number" min={1} step={10} value={dur} onChange={e => setDur(e.target.value)} style={inp} />
            <div style={{ fontSize: 9, color: C.mut, marginTop: 2 }}>Durée (min)</div>
          </div>
        </div>
      </div>

      {/* Infiltration & rugosité */}
      <div style={sec}>
        <p style={lab}>Sol</p>
        <div style={{ display: "flex", gap: 6 }}>
          <div style={{ flex: 1 }}>
            <input type="number" min={0} step={1} value={infil} onChange={e => setInfil(e.target.value)} style={inp} />
            <div style={{ fontSize: 9, color: C.mut, marginTop: 2 }}>Infiltration (mm/h)</div>
          </div>
          <div style={{ flex: 1 }}>
            <input type="number" min={0.01} step={0.01} value={manning} onChange={e => setManning(e.target.value)} style={inp} />
            <div style={{ fontSize: 9, color: C.mut, marginTop: 2 }}>Rugosité (Manning n)</div>
          </div>
        </div>
      </div>

      {/* Options réalisme */}
      <div style={sec}>
        <p style={lab}>Réalisme</p>
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.txt, cursor: "pointer" }}>
          <input type="checkbox" checked={buildings} onChange={e => setBuildings(e.target.checked)} />
          Bâtiments OSM comme obstacles (canalise l'eau dans les rues)
        </label>
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.txt, cursor: "pointer", marginTop: 5 }}>
          <input type="checkbox" checked={flowArrows} onChange={e => setFlowArrows(e.target.checked)} />
          Afficher le sens de l'écoulement (flèches)
        </label>
      </div>

      {/* Opacité */}
      <div style={sec}>
        <p style={lab}>Opacité · <span style={{ color: C.acc }}>{Math.round(opacity * 100)} %</span></p>
        <input type="range" min={0.2} max={1} step={0.05} value={opacity}
               onChange={e => { setOpacity(+e.target.value); if (layerIdRef.current) onUpdateRasterLayer?.(layerIdRef.current, { opacity: +e.target.value }); }}
               style={{ width: "100%" }} />
      </div>

      {/* Lancer */}
      <div style={{ ...sec, marginTop: 4 }}>
        <button style={btn(busy ? C.mut : C.acc)} disabled={busy} onClick={run}>
          {busy ? "Simulation en cours…" : "Lancer la simulation"}
        </button>
      </div>

      {/* Résultats : lecture + KPI */}
      {res && (
        <>
          <div style={sec}>
            <p style={lab}>Lecture {showMax ? "· profondeur max" : cur ? `· t = ${cur.t_min} min` : ""}</p>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <button onClick={() => setPlaying(p => !p)} disabled={showMax}
                      style={{ ...btn(playing ? C.amb : "#555"), width: "auto", padding: "6px 12px" }}>
                {playing ? "❚❚" : "▶"}
              </button>
              <input type="range" min={0} max={res.frames.length - 1} step={1} value={frameIdx} disabled={showMax}
                     onChange={e => { setPlaying(false); setFrameIdx(+e.target.value); }} style={{ flex: 1 }} />
            </div>
            <label style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 6, fontSize: 11, color: C.txt, cursor: "pointer" }}>
              <input type="checkbox" checked={showMax} onChange={e => { setShowMax(e.target.checked); setPlaying(false); }} />
              Afficher la profondeur maximale (enveloppe)
            </label>
          </div>

          {/* Vue 3D : eau extrudée + bascule caméra */}
          {res.depth3d_geojson?.features?.length > 0 && (
            <div style={sec}>
              <button style={btn("#2a6fb0")} onClick={() => {
                (onAddLayerSilent || onAddLayer)?.(res.depth3d_geojson, "Inondation 3D (eau)", "analysis",
                  { extrude: true, extrudeAttr: "depth", extrudeScale: 8, color: "#2a6fb0", opacity: 0.8 });
                const m = mapObj();
                try { m?.easeTo({ pitch: 62, duration: 900 }); } catch { /* noop */ }
              }}>Vue 3D — eau extrudée</button>
              <p style={{ fontSize: 9, color: C.mut, marginTop: 4 }}>Active aussi <b>Relief 3D</b> (haut, moteur Mapbox) pour le terrain + bâtiments extrudés.</p>
            </div>
          )}

          {/* KPI */}
          {cur && !showMax && (
            <div style={{ ...sec, display: "flex", gap: 6 }}>
              {kpi(`${cur.flooded_ha} ha`, "Surface inondée")}
              {kpi(`${cur.max_depth_m} m`, "Profondeur max")}
              {kpi(`${Math.round(cur.volume_m3).toLocaleString("fr-FR")} m³`, "Volume")}
            </div>
          )}

          {/* Courbe surface inondée */}
          {res.kpi?.flooded_ha?.length > 1 && <AreaChart kpi={res.kpi} C={C} M={M} idx={frameIdx} />}

          {/* Légende */}
          {res.legend && (
            <div style={sec}>
              <p style={lab}>Profondeur (m)</p>
              <div style={{ display: "flex", height: 12, borderRadius: 3, overflow: "hidden", border: `0.5px solid ${C.bdr}` }}>
                {res.legend.map((it, i) => <div key={i} style={{ flex: 1, background: it.color }} title={it.label} />)}
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.mut, fontFamily: M, marginTop: 2 }}>
                <span>0</span><span>{res.vmax} m</span>
              </div>
            </div>
          )}
          <p style={{ ...sec, fontSize: 9, color: C.mut }}>Grille {res.grid?.[1]}×{res.grid?.[0]} · maille {res.cell_m} m · modèle pluvial WCA2D (v1).</p>
        </>
      )}

      {err && <p style={{ ...sec, color: "#e06", fontSize: 11 }}>⚠ {err}</p>}
    </div>
  );
}

// Courbe « surface inondée dans le temps » avec repère sur la frame courante.
function AreaChart({ kpi, C, M, idx }) {
  const ys = kpi.flooded_ha, xs = kpi.t_min;
  if (!ys?.length) return null;
  const W = 240, H = 70, pad = 4;
  const ymax = Math.max(...ys, 0.01);
  const sx = i => pad + (W - 2 * pad) * (i / Math.max(1, ys.length - 1));
  const sy = v => H - pad - (H - 2 * pad) * (v / ymax);
  const d = ys.map((v, i) => `${i ? "L" : "M"}${sx(i).toFixed(1)},${sy(v).toFixed(1)}`).join(" ");
  const area = `M${sx(0)},${H - pad} ${ys.map((v, i) => `L${sx(i).toFixed(1)},${sy(v).toFixed(1)}`).join(" ")} L${sx(ys.length - 1)},${H - pad} Z`;
  return (
    <div style={{ padding: "10px 12px 0" }}>
      <p style={{ fontSize: 10, fontWeight: 600, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", margin: "0 0 4px" }}>Surface inondée (ha)</p>
      <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ background: C.input, borderRadius: 7, border: `0.5px solid ${C.bdr}` }}>
        <path d={area} fill={C.acc + "33"} />
        <path d={d} fill="none" stroke={C.acc} strokeWidth="1.6" />
        <line x1={sx(idx)} y1={pad} x2={sx(idx)} y2={H - pad} stroke={C.amb} strokeWidth="1.2" />
      </svg>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.mut, fontFamily: M, marginTop: 2 }}>
        <span>{xs[0]} min</span><span>{xs[xs.length - 1]} min</span>
      </div>
    </div>
  );
}

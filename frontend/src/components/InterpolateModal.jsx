/**
 * InterpolateModal.jsx — Interpolation d'une couche de points en surface raster.
 * Krigeage (pykrige) ou IDW côté backend → overlay image ajouté à la carte.
 */
import { useState, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX } from "../icons";

const API = import.meta.env.VITE_API_URL || (import.meta.env.DEV ? "http://localhost:8000" : "");

function bboxOf(geojson) {
  let w = 180, s = 90, e = -180, n = -90;
  const walk = (c) => {
    if (typeof c[0] === "number") { w = Math.min(w, c[0]); e = Math.max(e, c[0]); s = Math.min(s, c[1]); n = Math.max(n, c[1]); }
    else c.forEach(walk);
  };
  (geojson?.features || []).forEach(f => f.geometry?.coordinates && walk(f.geometry.coordinates));
  return [w, s, e, n];
}

export default function InterpolateModal({ layer, onClose, onResult }) {
  const C = useThemeContext();
  const feats = layer?.geojson?.features || [];
  const numFields = useMemo(() => {
    const s = new Set();
    feats.forEach(f => Object.entries(f.properties || {}).forEach(([k, v]) => { if (typeof v === "number" && isFinite(v)) s.add(k); }));
    return [...s];
  }, [feats]);

  const [field, setField] = useState(numFields[0] || "");
  const [method, setMethod] = useState("kriging");
  const [resolution, setRes] = useState(120);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);

  const nPoints = feats.filter(f => f.geometry?.type === "Point").length;
  const sel = { width: "100%", padding: "8px 10px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: F, fontSize: 13, marginBottom: 12, boxSizing: "border-box" };

  const run = async () => {
    if (!field) { setError("Choisissez un champ numérique."); return; }
    setRunning(true); setError(null);
    try {
      const bbox = layer.bbox || bboxOf(layer.geojson);
      const res = await fetch(`${API}/api/whitebox/interpolate`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ points_geojson: layer.geojson, field, bbox, resolution: Number(resolution), method }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      onResult({
        id: `interp_${field}_${Date.now()}`,
        name: `${method === "kriging" ? "Krigeage" : "IDW"} — ${field}`,
        imageUrl: `data:image/png;base64,${data.png_b64}`,
        coordinates: data.image_coordinates, bbox: data.bbox, visParams: data.vis_params, opacity: 0.8,
      });
      onClose();
    } catch (e) { setError(e.message); setRunning(false); }
  };

  return (
    <>
      <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", zIndex: 1200, backdropFilter: "blur(2px)" }} />
      <div style={{ position: "fixed", top: "50%", left: "50%", transform: "translate(-50%, -50%)", zIndex: 1201, width: "min(420px, 94vw)", background: C.bg, borderRadius: 10, border: `0.5px solid ${C.bdr}`, boxShadow: "0 24px 64px rgba(0,0,0,0.4)", padding: 18 }}>
        <div style={{ display: "flex", alignItems: "center", marginBottom: 4 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: C.txt, margin: 0, flex: 1 }}>Interpolation — {layer?.name}</h3>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={16} /></button>
        </div>
        <div style={{ fontSize: 11, color: C.dim, marginBottom: 14 }}>{nPoints} points · {numFields.length} champs numériques</div>

        {numFields.length === 0 ? (
          <div style={{ fontSize: 12.5, color: C.dim }}>Cette couche n'a pas de champ numérique à interpoler (il faut une couche de points avec une valeur mesurée).</div>
        ) : (
          <>
            <label style={{ fontSize: 11, color: C.dim, display: "block", marginBottom: 4 }}>Champ à interpoler</label>
            <select value={field} onChange={e => setField(e.target.value)} style={sel}>{numFields.map(c => <option key={c} value={c}>{c}</option>)}</select>

            <label style={{ fontSize: 11, color: C.dim, display: "block", marginBottom: 4 }}>Méthode</label>
            <select value={method} onChange={e => setMethod(e.target.value)} style={sel}>
              <option value="kriging">Krigeage ordinaire</option>
              <option value="idw">IDW (distance inverse)</option>
            </select>

            <label style={{ fontSize: 11, color: C.dim, display: "block", marginBottom: 4 }}>Résolution (cellules/côté) : {resolution}</label>
            <input type="range" min="60" max="240" step="20" value={resolution} onChange={e => setRes(e.target.value)} style={{ width: "100%", marginBottom: 14 }} />

            {error && <div style={{ fontSize: 12, color: C.red, marginBottom: 12 }}>{error}</div>}
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={onClose} style={{ flex: 1, padding: "9px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: "transparent", color: C.mut, fontFamily: F, fontSize: 12, cursor: "pointer" }}>Annuler</button>
              <button onClick={run} disabled={running} style={{ flex: 2, padding: "9px", border: "none", borderRadius: 7, background: C.acc, color: "#04120a", fontFamily: F, fontSize: 12, fontWeight: 600, cursor: running ? "default" : "pointer", opacity: running ? 0.7 : 1 }}>{running ? "Calcul…" : "Interpoler"}</button>
            </div>
          </>
        )}
      </div>
    </>
  );
}

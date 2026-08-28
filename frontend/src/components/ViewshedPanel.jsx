/**
 * ViewshedPanel.jsx — Analyse de visibilité (viewshed) depuis un point.
 *
 * On place un observateur sur la carte (clic capté via `viewshedClickRef`, armé
 * par App), on règle sa hauteur, le rayon et la hauteur de cible, puis on lance
 * POST /api/viewshed/compute : le backend assemble un MNT (tuiles Terrarium) et
 * renvoie un overlay (vert = visible) ajouté via onAddImageLayer.
 */
import { useState, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

const HEIGHTS = [["Piéton", 2], ["Bâtiment", 10], ["Tour", 30], ["Drone", 100]];

export default function ViewshedPanel({ viewshedClickRef, onAddImageLayer }) {
  const C = useThemeContext();
  const [obs, setObs] = useState(null);          // {lng, lat}
  const [arming, setArming] = useState(false);
  const [height, setHeight] = useState(2);
  const [radius, setRadius] = useState(5);
  const [tgt, setTgt] = useState(0);
  const [curv, setCurv] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [info, setInfo] = useState(null);

  useEffect(() => () => { if (viewshedClickRef) viewshedClickRef.current = null; }, [viewshedClickRef]);

  const arm = useCallback(() => {
    if (!viewshedClickRef) return;
    setArming(true); setErr(null);
    viewshedClickRef.current = (lng, lat) => {
      setObs({ lng: +lng.toFixed(6), lat: +lat.toFixed(6) });
      setArming(false); viewshedClickRef.current = null;
    };
  }, [viewshedClickRef]);

  const run = useCallback(async () => {
    if (!obs) return;
    setBusy(true); setErr(null); setInfo(null);
    try {
      const r = await fetch(`${API}/viewshed/compute`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lng: obs.lng, lat: obs.lat, observer_height: Number(height), target_height: Number(tgt), radius_km: Number(radius), curvature: curv }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({
        name: d.name, imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.85,
        rasterToken: null, bands: d.bands, fit: true,
      });
      setInfo(`Visible : ${d.visible_km2} km² · observateur à ${d.observer_elev_m} m (MNT ~${d.resolution_m} m/px).`);
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [obs, height, radius, tgt, curv, onAddImageLayer]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
        Ce qui est visible depuis un point, sur le relief (MNT mondial). Placez l'observateur, réglez sa hauteur, puis calculez.
      </div>

      {/* Observateur */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, background: obs ? C.acc + "10" : "transparent", border: `0.5px solid ${obs ? C.acc + "44" : C.bdr}`, borderRadius: 8, padding: "8px 10px", flexWrap: "wrap" }}>
        {obs ? (
          <span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>📍 {obs.lat}, {obs.lng}</span>
        ) : (
          <span style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucun observateur placé.</span>
        )}
        {arming ? (
          <>
            <span style={{ fontFamily: F, fontSize: 11.5, color: C.acc, fontWeight: 600 }}>Cliquez sur la carte…</span>
            <button onClick={() => { if (viewshedClickRef) viewshedClickRef.current = null; setArming(false); }} style={{ marginLeft: "auto", fontFamily: F, fontSize: 11, padding: "3px 8px", cursor: "pointer", background: "transparent", color: C.mut, border: `1px solid ${C.bdr}`, borderRadius: 6 }}>Annuler</button>
          </>
        ) : (
          <button onClick={arm} style={{ marginLeft: "auto", fontFamily: F, fontSize: 12, fontWeight: 500, padding: "5px 10px", cursor: "pointer", background: C.acc, color: "#fff", border: "none", borderRadius: 7 }}>
            {obs ? "Déplacer" : "Placer l'observateur"}
          </button>
        )}
      </div>

      {/* Hauteur observateur */}
      <div>
        <div style={lbl}>Hauteur de l'observateur · {height} m</div>
        <div style={{ display: "flex", gap: 5, marginBottom: 6, flexWrap: "wrap" }}>
          {HEIGHTS.map(([label, h]) => (
            <button key={h} onClick={() => setHeight(h)}
              style={{ fontFamily: F, fontSize: 10.5, padding: "3px 8px", borderRadius: 6, cursor: "pointer",
                border: `0.5px solid ${height === h ? C.acc + "55" : C.bdr}`, background: height === h ? C.acc + "18" : "transparent", color: height === h ? C.acc : C.mut }}>
              {label} {h}m
            </button>
          ))}
        </div>
        <input type="range" min={1} max={150} value={height} onChange={(e) => setHeight(Number(e.target.value))} style={{ width: "100%" }} />
      </div>

      <div style={{ display: "flex", gap: 10 }}>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Rayon · {radius} km</div>
          <input type="range" min={1} max={20} value={radius} onChange={(e) => setRadius(Number(e.target.value))} style={{ width: "100%" }} />
        </div>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Hauteur cible · {tgt} m</div>
          <input type="range" min={0} max={30} value={tgt} onChange={(e) => setTgt(Number(e.target.value))} style={{ width: "100%" }} />
        </div>
      </div>

      <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.mut, cursor: "pointer" }}>
        <input type="checkbox" checked={curv} onChange={(e) => setCurv(e.target.checked)} />
        Tenir compte de la courbure terrestre
      </label>

      <button onClick={run} disabled={busy || !obs}
        style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: (busy || !obs) ? "not-allowed" : "pointer",
          background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: obs ? 1 : 0.5 }}>
        {busy ? "Calcul du MNT…" : "Calculer la visibilité"}
      </button>

      {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
      {info && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{info}</div>}

      <div style={{ fontFamily: F, fontSize: 10.5, color: C.dim, lineHeight: 1.5, marginTop: "auto" }}>
        MNT mondial ~30 m (Terrarium, sans clé). La résolution lisse les sommets pointus : sur un relief marqué, augmentez la hauteur de l'observateur pour « passer au-dessus » de la brisure de pente.
      </div>
    </div>
  );
}

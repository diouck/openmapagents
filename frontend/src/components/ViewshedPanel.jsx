/**
 * ViewshedPanel.jsx — Analyse de visibilité (viewshed) depuis un point.
 *
 * Observateur placé sur la carte (clic capté via `viewshedClickRef`, marqueur
 * affiché sur la carte via onObsChange), hauteur/rayon/cible réglables, source
 * de MNT au choix (mondial Terrarium / IGN RGE ALTI France / GeoTIFF importé),
 * puis POST /api/viewshed/compute → overlay (vert = visible) via onAddImageLayer.
 * Onglets : Outil / Définition.
 */
import { useState, useEffect, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

const HEIGHTS = [["Piéton", 2], ["Bâtiment", 10], ["Tour", 30], ["Drone", 100]];

export default function ViewshedPanel({ viewshedClickRef, onAddImageLayer, layers = [], onObsChange }) {
  const C = useThemeContext();
  const rasters = useMemo(() => layers.filter((l) => l.kind === "image" && l.rasterToken), [layers]);
  const [tab, setTab] = useState("go");
  const [obs, setObs] = useState(null);
  const [arming, setArming] = useState(false);
  const [source, setSource] = useState("world");   // world | ign | raster
  const [rid, setRid] = useState(rasters[0]?.id || "");
  const [height, setHeight] = useState(2);
  const [radius, setRadius] = useState(5);
  const [tgt, setTgt] = useState(0);
  const [curv, setCurv] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [info, setInfo] = useState(null);

  // Marqueur observateur sur la carte + nettoyage
  useEffect(() => {
    onObsChange?.(obs ? { type: "FeatureCollection", features: [{ type: "Feature", geometry: { type: "Point", coordinates: [obs.lng, obs.lat] }, properties: { label: "👁" } }] } : null);
  }, [obs, onObsChange]);
  useEffect(() => () => { if (viewshedClickRef) viewshedClickRef.current = null; onObsChange?.(null); }, [viewshedClickRef, onObsChange]);

  const arm = useCallback(() => {
    if (!viewshedClickRef) return;
    setArming(true); setErr(null);
    viewshedClickRef.current = (lng, lat) => { setObs({ lng: +lng.toFixed(6), lat: +lat.toFixed(6) }); setArming(false); viewshedClickRef.current = null; };
  }, [viewshedClickRef]);

  const run = useCallback(async () => {
    if (!obs) return;
    if (source === "raster" && !rid) { setErr("Choisissez un MNT importé."); return; }
    setBusy(true); setErr(null); setInfo(null);
    try {
      const raster = rasters.find((l) => l.id === rid);
      const body = { lng: obs.lng, lat: obs.lat, observer_height: Number(height), target_height: Number(tgt), radius_km: Number(radius), curvature: curv, dem_source: source === "world" ? "terrarium" : source };
      if (source === "raster") body.raster_token = raster?.rasterToken;
      const r = await fetch(`${API}/viewshed/compute`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({ name: d.name, imageUrl: `data:image/png;base64,${d.png_b64}`, coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.85, rasterToken: null, bands: d.bands, fit: true });
      setInfo(`Visible : ${d.visible_km2} km² · observateur à ${d.observer_elev_m} m · ${d.source} (~${d.resolution_m} m/px).`);
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [obs, source, rid, rasters, height, radius, tgt, curv, onAddImageLayer]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };
  const Code = ({ children }) => (<code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>);
  const tabBtn = (id, label) => (
    <button key={id} onClick={() => setTab(id)} style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer", background: "transparent", color: tab === id ? C.acc : C.mut, border: "none", borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>{label}</button>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("go", "Outil")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Répond à « <b>qu'est-ce qu'on voit d'ici ?</b> ». Depuis un point de vue, l'outil reconstruit le relief autour et calcule, par lancer de rayons, les zones <b>visibles</b> (en vert) en tenant compte des obstacles du terrain, de la hauteur de l'observateur et — au choix — de la courbure terrestre.</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Source du relief (MNT)</div>
            <ul style={{ margin: 0, paddingLeft: 18, color: C.mut, fontSize: 11.5, display: "flex", flexDirection: "column", gap: 3 }}>
              <li><b>Mondial (~30 m)</b> : tuiles Terrarium, partout, sans clé.</li>
              <li><b>IGN RGE ALTI</b> : haute résolution, <b>France métropolitaine</b> uniquement.</li>
              <li><b>MNT importé</b> : votre GeoTIFF (BD ALTI, RGE ALTI, SRTM…), importé via « Importer », puis choisi ici. Sous-échantillonné à l'import s'il est trop lourd.</li>
            </ul>
          </div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            La résolution du MNT lisse les sommets pointus : sur relief marqué, un observateur bas (2 m) peut « buter » sur la brisure de pente proche → montez sa hauteur (ex. Mont Ventoux : 2 % à 2 m, 52 % à 100 m). Ni la végétation ni le bâti ne sont modélisés.
          </div>
        </div>
      ) : (
        <>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
            Placez l'observateur (il apparaît sur la carte), réglez la hauteur et la source du relief, puis calculez.
          </div>

          {/* Observateur */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, background: obs ? C.acc + "10" : "transparent", border: `0.5px solid ${obs ? C.acc + "44" : C.bdr}`, borderRadius: 8, padding: "8px 10px", flexWrap: "wrap" }}>
            {obs ? (<span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>👁 {obs.lat}, {obs.lng}</span>) : (<span style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucun observateur placé.</span>)}
            {arming ? (
              <><span style={{ fontFamily: F, fontSize: 11.5, color: C.acc, fontWeight: 600 }}>Cliquez sur la carte…</span><button onClick={() => { if (viewshedClickRef) viewshedClickRef.current = null; setArming(false); }} style={{ marginLeft: "auto", fontFamily: F, fontSize: 11, padding: "3px 8px", cursor: "pointer", background: "transparent", color: C.mut, border: `1px solid ${C.bdr}`, borderRadius: 6 }}>Annuler</button></>
            ) : (
              <button onClick={arm} style={{ marginLeft: "auto", fontFamily: F, fontSize: 12, fontWeight: 500, padding: "5px 10px", cursor: "pointer", background: C.acc, color: "#fff", border: "none", borderRadius: 7 }}>{obs ? "Déplacer" : "Placer l'observateur"}</button>
            )}
          </div>

          {/* Source MNT */}
          <div>
            <div style={lbl}>Source du relief</div>
            <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
              {[["world", "Mondial ~30 m"], ["ign", "IGN (France)"], ["raster", "MNT importé"]].map(([s, label]) => (
                <button key={s} onClick={() => setSource(s)} disabled={s === "raster" && rasters.length === 0}
                  style={{ fontFamily: F, fontSize: 10.5, padding: "4px 9px", borderRadius: 6, cursor: (s === "raster" && rasters.length === 0) ? "not-allowed" : "pointer",
                    border: `0.5px solid ${source === s ? C.acc + "55" : C.bdr}`, background: source === s ? C.acc + "18" : "transparent", color: source === s ? C.acc : ((s === "raster" && rasters.length === 0) ? C.dim : C.mut) }}>
                  {label}
                </button>
              ))}
            </div>
            {source === "raster" && (
              rasters.length === 0 ? (
                <div style={{ fontFamily: F, fontSize: 11, color: C.dim, marginTop: 5 }}>Importez d'abord un GeoTIFF (MNT) mono-bande.</div>
              ) : (
                <select value={rid} onChange={(e) => setRid(e.target.value)} style={{ ...inp, marginTop: 5 }}>
                  {rasters.map((l) => <option key={l.id} value={l.id}>{l.name}</option>)}
                </select>
              )
            )}
          </div>

          {/* Hauteur observateur */}
          <div>
            <div style={lbl}>Hauteur de l'observateur · {height} m</div>
            <div style={{ display: "flex", gap: 5, marginBottom: 6, flexWrap: "wrap" }}>
              {HEIGHTS.map(([label, h]) => (
                <button key={h} onClick={() => setHeight(h)} style={{ fontFamily: F, fontSize: 10.5, padding: "3px 8px", borderRadius: 6, cursor: "pointer", border: `0.5px solid ${height === h ? C.acc + "55" : C.bdr}`, background: height === h ? C.acc + "18" : "transparent", color: height === h ? C.acc : C.mut }}>{label} {h}m</button>
              ))}
            </div>
            <input type="range" min={1} max={150} value={height} onChange={(e) => setHeight(Number(e.target.value))} style={{ width: "100%" }} />
          </div>

          <div style={{ display: "flex", gap: 10 }}>
            <div style={{ flex: 1 }}><div style={lbl}>Rayon · {radius} km</div><input type="range" min={1} max={20} value={radius} onChange={(e) => setRadius(Number(e.target.value))} style={{ width: "100%" }} /></div>
            <div style={{ flex: 1 }}><div style={lbl}>Hauteur cible · {tgt} m</div><input type="range" min={0} max={30} value={tgt} onChange={(e) => setTgt(Number(e.target.value))} style={{ width: "100%" }} /></div>
          </div>

          <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.mut, cursor: "pointer" }}>
            <input type="checkbox" checked={curv} onChange={(e) => setCurv(e.target.checked)} /> Tenir compte de la courbure terrestre
          </label>

          <button onClick={run} disabled={busy || !obs}
            style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: (busy || !obs) ? "not-allowed" : "pointer", background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: obs ? 1 : 0.5 }}>
            {busy ? "Calcul du MNT…" : "Calculer la visibilité"}
          </button>

          {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
          {info && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{info}</div>}
        </>
      )}
    </div>
  );
}

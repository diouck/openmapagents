/**
 * CanopyPanel.jsx — Foresterie LiDAR (utilisé dans le menu LiDAR).
 *
 * Appelle POST /api/lidar/canopy (via jeton du fichier déjà importé, ou upload)
 * et ajoute AUTOMATIQUEMENT les produits au menu Couches de l'appli :
 *   - rasters MNT / MNS / MNH  → couches image (onAddImageLayer)
 *   - houppiers + cimes        → couches GeoJSON (onAddLayer)
 * Comme tout passe par le gestionnaire de couches, l'affichage survit au
 * changement de fond de carte et se pilote depuis le menu Couches.
 * Fournit aussi statistiques + exports GeoJSON (4326) + GeoTIFF.
 */
import { useState, useCallback, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcTreePine, IcFileDown, IcCheck, IcLoader } from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Étapes affichées pendant le calcul (progression estimée, le calcul est un seul POST)
const STEPS = ["Lecture du nuage", "MNT (sol)", "MNS (surface)", "MNH (canopée)", "Détection des arbres", "Houppiers"];

// Upload + analyse avec progression (XHR : envoi vs calcul serveur)
function _postCanopy(fd, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API}/api/lidar/canopy`);
    xhr.upload.onprogress = (e) => { if (e.lengthComputable) onProgress(e.loaded / e.total); };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try { resolve(JSON.parse(xhr.responseText)); } catch (_) { reject(new Error("Réponse illisible.")); }
      } else {
        // On privilégie le message serveur (detail), qui est actionnable
        // (ex. dépendance manquante en 503).
        let m = "";
        try { m = JSON.parse(xhr.responseText).detail || ""; } catch (_) {}
        if (!m) {
          if (xhr.status === 413) m = "Fichier trop volumineux (limite nginx « client_max_body_size »).";
          else if (xhr.status === 502 || xhr.status === 504) m = `Backend injoignable (${xhr.status}) : redémarrez le serveur ou vérifiez qu'il a bien été redéployé.`;
          else m = `Erreur ${xhr.status}`;
        }
        reject(new Error(m));
      }
    };
    xhr.onerror = () => reject(new Error("Échec réseau pendant l'envoi."));
    xhr.send(fd);
  });
}

// Télécharge un objet en fichier GeoJSON (déjà en WGS84 / EPSG:4326)
function _downloadGeoJSON(obj, name) {
  const blob = new Blob([JSON.stringify(obj)], { type: "application/geo+json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = name; document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const _dlBtn = (C) => ({
  fontFamily: F, fontSize: 9, padding: "3px 8px", borderRadius: 4, cursor: "pointer",
  background: "transparent", border: `0.5px solid ${C.acc}66`, color: C.acc,
});

export default function CanopyPanel({ mapRef, file, fileToken, onAddLayer, onAddImageLayer }) {
  const C = useThemeContext();
  const getMap = useCallback(() => mapRef.current?.getMap?.() || null, [mapRef]);

  const [res, setRes] = useState(0.5);
  const [minH, setMinH] = useState(2);
  const [spacing, setSpacing] = useState(2.5);
  const [detect, setDetect] = useState(true);
  const [useView, setUseView] = useState(false);
  const [zFactor, setZFactor] = useState(1);   // exagération verticale (ombrage MNT/MNS/MNH)
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(null);
  const [data, setData] = useState(null);
  const [step, setStep] = useState(-1);       // étape courante (-1 = inactif)
  const [dem3D, setDem3D] = useState("");     // produit servant de terrain 3D ("" = aucun)

  // ── Terrain 3D depuis le LiDAR : le MNS/MNH devient la source `raster-dem`
  // MapLibre (tuiles Terrarium servies par /api/lidar/dem) → les bâtiments et la
  // canopée se lèvent RÉELLEMENT en volume (un overlay image reste plat).
  const applyDem3D = useCallback((product) => {
    const map = getMap(); if (!map || !data?.job) return;
    const srcId = `lidar-dem-${data.job}-${product}`;
    try {
      if (!product) { map.setTerrain(null); map.easeTo({ pitch: 0, duration: 600 }); setDem3D(""); return; }
      if (!map.getSource(srcId)) {
        map.addSource(srcId, {
          type: "raster-dem", encoding: "terrarium", tileSize: 256, maxzoom: 20,
          tiles: [`${API}/api/lidar/dem/${data.job}/{z}/{x}/{y}.png?product=${product}`],
        });
      }
      map.setTerrain({ source: srcId, exaggeration: Math.max(1, zFactor) });
      if (map.getPitch() < 20) map.easeTo({ pitch: 65, duration: 800 });
      setDem3D(product);
    } catch (e) { setStatus({ t: "error", m: "Terrain 3D : " + e.message }); }
  }, [getMap, data, zFactor]);
  const [upPct, setUpPct] = useState(null);   // % d'envoi (null = pas d'upload en cours)
  const stepTimer = useRef(null);

  const inp = {
    fontFamily: M, fontSize: 10, padding: "4px 6px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box",
  };

  const run = async () => {
    const map = getMap();
    if (!map) { setStatus({ t: "error", m: "Carte non prête" }); return; }
    if (!file && !fileToken) { setStatus({ t: "error", m: "Importez d'abord un LAS/LAZ." }); return; }
    // Démarre l'égreneur d'étapes (estimation ; le calcul est un seul POST)
    const startSteps = () => {
      if (stepTimer.current) return;
      setStatus(null); setStep(0);
      stepTimer.current = setInterval(() => setStep(s => Math.min(s + 1, STEPS.length - 1)), 2200);
    };
    const stopSteps = () => { if (stepTimer.current) { clearInterval(stepTimer.current); stepTimer.current = null; } setStep(-1); setUpPct(null); };

    try {
      setBusy(true); setUpPct(fileToken ? null : 0);
      const fd = new FormData();
      if (fileToken) fd.append("file_token", fileToken);   // réutilise le fichier déjà importé
      else fd.append("file", file);
      fd.append("resolution", String(res));
      fd.append("min_tree_height", String(minH));
      fd.append("detect_trees", detect ? "true" : "false");
      fd.append("tree_min_distance", String(spacing));
      fd.append("z_factor", String(zFactor));
      if (useView) {
        const b = map.getBounds();
        fd.append("aoi", `${b.getWest()},${b.getSouth()},${b.getEast()},${b.getNorth()}`);
      }
      if (fileToken) startSteps();   // pas d'upload → on passe direct au calcul
      const d = await _postCanopy(fd, (p) => {
        if (p < 1) setUpPct(Math.round(p * 100));
        else startSteps();           // upload fini → calcul serveur
      });
      stopSteps();

      // ── Ajout AUTOMATIQUE au menu Couches ──
      if (onAddImageLayer) {
        onAddImageLayer({ name: "MNH — canopée", imageUrl: `data:image/png;base64,${d.png.mnh}`, coordinates: d.image_coordinates, bbox: d.bbox_lonlat, opacity: 0.85 });
        onAddImageLayer({ name: "MNS — surface", imageUrl: `data:image/png;base64,${d.png.mns}`, coordinates: d.image_coordinates, bbox: d.bbox_lonlat, opacity: 0.85, fit: false });
        onAddImageLayer({ name: "MNT — sol", imageUrl: `data:image/png;base64,${d.png.mnt}`, coordinates: d.image_coordinates, bbox: d.bbox_lonlat, opacity: 0.85, fit: false });
      }
      if (onAddLayer) {
        if (d.crowns?.features?.length) onAddLayer(d.crowns, "Houppiers LiDAR", "analysis");
        if (d.treetops?.features?.length) onAddLayer(d.treetops, "Cimes LiDAR", "analysis");
      }

      setData(d);
      setStatus({ t: "ok", m: `${d.stats.n_trees} arbres · couches ajoutées au menu Couches` });
    } catch (e) {
      stopSteps();
      setStatus({ t: "error", m: e?.message || String(e) });
    }
    setBusy(false);
  };

  const statColor = { ok: C.acc, error: C.red, info: C.amb };
  const S = data?.stats;

  return (
    <div style={{ background: C.hover, borderRadius: 8, padding: "9px 10px", border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <IcTreePine size={14} color={C.acc}/>
        <span style={{ fontSize: 11, fontWeight: 600, color: C.txt, flex: 1 }}>Créer MNT / MNS / MNH + arbres</span>
      </div>

      {/* Paramètres */}
      <div style={{ display: "flex", gap: 5 }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 8, color: C.dim }}>Résolution (m)</div>
          <input type="number" min="0.25" max="5" step="0.25" value={res} onChange={e => setRes(parseFloat(e.target.value) || 0.5)} style={inp} />
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 8, color: C.dim }}>Hauteur mini (m)</div>
          <input type="number" min="0.5" max="20" step="0.5" value={minH} onChange={e => setMinH(parseFloat(e.target.value) || 2)} style={inp} />
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 8, color: C.dim }}>Espacement (m)</div>
          <input type="number" min="1" max="20" step="0.5" value={spacing} onChange={e => setSpacing(parseFloat(e.target.value) || 2.5)} style={inp} />
        </div>
      </div>
      <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 9, color: C.mut, cursor: "pointer" }}>
        <input type="checkbox" checked={detect} onChange={e => setDetect(e.target.checked)} style={{ accentColor: C.acc }} />
        Détecter les arbres + houppiers
      </label>
      {/* Zone d'analyse : boutons segmentés (pas de case/liste) */}
      <div>
        <div style={{ fontSize: 9, color: C.dim, marginBottom: 4, textTransform: "uppercase", letterSpacing: ".05em" }}>Zone d'analyse</div>
        <div style={{ display: "flex", gap: 5 }}>
          {[[false, "Fichier entier"], [true, "Vue carte"]].map(([v, l]) => (
            <button key={l} onClick={() => setUseView(v)} style={{
              flex: 1, fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, cursor: "pointer",
              background: useView === v ? C.acc + "18" : "transparent",
              border: `0.5px solid ${useView === v ? C.acc + "55" : C.bdr}`, color: useView === v ? C.acc : C.dim,
            }}>{l}</button>
          ))}
        </div>
        <div style={{ fontSize: 8, color: C.dim, marginTop: 3 }}>{useView ? "AOI limitée à l'emprise écran actuelle." : "Tout le nuage de points du fichier."}</div>
      </div>

      {/* Exagération verticale : ombrage des MNT / MNS / MNH */}
      <div>
        <div style={{ fontSize: 9, color: C.dim, marginBottom: 4, textTransform: "uppercase", letterSpacing: ".05em" }}>Relief (exagération)</div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <input type="range" min="1" max="8" step="0.5" value={zFactor} onChange={e => setZFactor(parseFloat(e.target.value))} style={{ flex: 1, height: 3 }} />
          <span style={{ fontFamily: M, fontSize: 10, color: C.txt, width: 30, textAlign: "right" }}>×{zFactor}</span>
        </div>
        <div style={{ fontSize: 8, color: C.dim, marginTop: 3 }}>
          {zFactor > 1 ? "MNT / MNS / MNH ombrés — monte le modelé." : "×1 = pas d'ombrage (couleurs seules)."}
        </div>
      </div>

      <button onClick={run} disabled={busy || (!file && !fileToken)} style={{
        fontFamily: F, fontSize: 12, fontWeight: 600, padding: "8px 0", borderRadius: 6, width: "100%",
        background: busy || (!file && !fileToken) ? C.hover : C.acc, color: busy || (!file && !fileToken) ? C.dim : "#fff", border: "none",
        cursor: busy || (!file && !fileToken) ? "default" : "pointer",
        display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
      }}><IcTreePine size={14}/> {busy ? "Analyse…" : "Calculer MNT/MNS/MNH + arbres"}</button>
      {fileToken && !busy && <div style={{ fontSize: 8, color: C.dim }}>Réutilise le fichier déjà importé (pas de nouvel envoi).</div>}

      {/* Progression : envoi puis étapes de calcul */}
      {busy && (
        <div style={{ background: C.input, borderRadius: 6, padding: "7px 9px", display: "flex", flexDirection: "column", gap: 4 }}>
          {upPct != null && upPct < 100 ? (
            <>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.dim }}>
                <span>Envoi du fichier…</span><span style={{ fontFamily: M, color: C.txt }}>{upPct}%</span>
              </div>
              <div style={{ height: 4, borderRadius: 2, background: C.bdr, overflow: "hidden" }}>
                <div style={{ height: "100%", width: `${upPct}%`, background: C.acc, transition: "width .2s" }} />
              </div>
            </>
          ) : (
            <>
              <div style={{ fontSize: 9, color: C.dim, marginBottom: 2 }}>Calcul en cours…</div>
              {STEPS.map((s, i) => (
                <div key={s} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 9,
                  color: i < step ? C.acc : i === step ? C.txt : C.dim }}>
                  <span style={{ width: 12, display: "flex", justifyContent: "center", alignItems: "center" }}>
                    {i < step ? <IcCheck size={11}/> : i === step ? <IcLoader size={11}/> : <span style={{ width: 6, height: 6, borderRadius: "50%", border: `1px solid ${C.dim}` }} />}
                  </span>
                  {s}
                </div>
              ))}
            </>
          )}
        </div>
      )}

      {status && (
        <div style={{ fontSize: 10, padding: "5px 8px", borderRadius: 5, lineHeight: 1.5,
          background: statColor[status.t] + "15", border: `0.5px solid ${statColor[status.t]}44`, color: statColor[status.t] }}>
          {status.m}
        </div>
      )}

      {data && (
        <>
          {/* Statistiques */}
          {S && (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4, fontSize: 9 }}>
              {[
                ["Arbres", S.n_trees], ["Densité/ha", S.trees_per_ha],
                ["Couvert", S.canopy_cover_pct != null ? S.canopy_cover_pct + " %" : "—"],
                ["H. moy.", S.height_mean_m != null ? S.height_mean_m + " m" : "—"],
                ["H. max", S.height_max_m != null ? S.height_max_m + " m" : "—"],
                ["H. P95", S.height_p95_m != null ? S.height_p95_m + " m" : "—"],
                ["Houppier moy.", S.crown_area_mean_m2 != null ? S.crown_area_mean_m2 + " m²" : "—"],
                ["Surface", S.area_ha + " ha"],
              ].map(([k, v]) => (
                <div key={k} style={{ background: C.input, borderRadius: 4, padding: "3px 6px", display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: C.dim }}>{k}</span><span style={{ color: C.txt, fontFamily: M }}>{v}</span>
                </div>
              ))}
            </div>
          )}
          {S?.ground_mode && <div style={{ fontSize: 8, color: C.dim }}>Sol : {S.ground_mode} · canopée : {S.canopy_from}</div>}
          {data.warnings?.map((w, i) => <div key={i} style={{ fontSize: 8, color: C.amb }}>{w}</div>)}

          <div style={{ fontSize: 8, color: C.dim }}>Couches ajoutées : <b>MNT, MNS, MNH</b>, <b>Cimes</b>, <b>Houppiers</b> (menu Couches).</div>

          {/* Exports GeoJSON (EPSG:4326) */}
          <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
            {data.treetops?.features?.length > 0 && (
              <button onClick={() => _downloadGeoJSON(data.treetops, "cimes_4326.geojson")} style={{ ..._dlBtn(C), display: "inline-flex", alignItems: "center", gap: 4 }}><IcFileDown size={11}/> Cimes .geojson</button>
            )}
            {data.crowns?.features?.length > 0 && (
              <button onClick={() => _downloadGeoJSON(data.crowns, "houppiers_4326.geojson")} style={{ ..._dlBtn(C), display: "inline-flex", alignItems: "center", gap: 4 }}><IcFileDown size={11}/> Houppiers .geojson</button>
            )}
          </div>

          {/* Relief 3D réel : le MNS/MNH devient le terrain MapLibre */}
          {data.job && (
            <div>
              <div style={{ fontSize: 9, color: C.dim, marginBottom: 4, textTransform: "uppercase", letterSpacing: ".05em" }}>Relief 3D (volume réel)</div>
              <div style={{ display: "flex", gap: 5 }}>
                {[["mns", "MNS (bâti+végé)"], ["mnh", "MNH (canopée)"], ["", "Aucun"]].map(([p, l]) => (
                  <button key={l} onClick={() => applyDem3D(p)} style={{
                    flex: 1, fontFamily: F, fontSize: 9.5, padding: "5px 0", borderRadius: 5, cursor: "pointer",
                    background: dem3D === p ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${dem3D === p ? C.acc + "55" : C.bdr}`, color: dem3D === p ? C.acc : C.dim,
                  }}>{l}</button>
                ))}
              </div>
              <div style={{ fontSize: 8, color: C.dim, marginTop: 3 }}>
                {dem3D ? `Les volumes se lèvent (exagération ×${Math.max(1, zFactor)}). Inclinez la carte à la souris.`
                       : "Fait monter réellement bâtiments / arbres en 3D (≠ ombrage)."}
              </div>
            </div>
          )}

          {/* Téléchargements GeoTIFF */}
          <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
            {Object.entries(data.downloads || {}).map(([k, url]) => (
              <a key={k} href={`${API}${url}`} download style={{
                fontFamily: F, fontSize: 9, padding: "3px 8px", borderRadius: 4, textDecoration: "none",
                background: "transparent", border: `0.5px solid ${C.acc}66`, color: C.acc,
                display: "inline-flex", alignItems: "center", gap: 4,
              }}><IcFileDown size={11}/> {k.toUpperCase()}.tif</a>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

/**
 * ShadowPanel.jsx — Ombres portées des bâtiments + canopée.
 *
 * • Bâtiments : 100 % client-side. Lus des tuiles MapLibre déjà chargées
 *   (couche `building`, `render_height`), projetés au sol selon le soleil
 *   (SunCalc porté & validé) → polygones d'ombre (enveloppe convexe balayée).
 * • Canopée : modèle WRI/Meta ~1 m via GEE (POST /api/shadow/canopy) rendu en
 *   APERÇU RASTER lissé (vraie emprise, pas de polygones carrés), posé en
 *   overlay vert ; son ombre = une copie sombre du même raster décalée selon la
 *   hauteur moyenne et le soleil.
 *
 * Curseur d'heure (+ « Journée ») → l'ombre défile. À l'ouverture : fond Liberty
 * (bâtiments), vue 3D et zoom bâtiments réglés automatiquement.
 */
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

/* ── Position du soleil (port SunCalc, validé en Python) → {alt, az} rad ; az
   mesuré depuis le SUD, positif vers l'ouest. */
const RAD = Math.PI / 180;
const dayMs = 86400000, J1970 = 2440588, J2000 = 2451545, OBL = RAD * 23.4397;
const toDays = (ms) => ms / dayMs - 0.5 + J1970 - J2000;
const solarMeanAnomaly = (d) => RAD * (357.5291 + 0.98560028 * d);
const eclipticLongitude = (M) => {
  const C = RAD * (1.9148 * Math.sin(M) + 0.02 * Math.sin(2 * M) + 0.0003 * Math.sin(3 * M));
  return M + C + RAD * 102.9372 + Math.PI;
};
const declination = (l) => Math.asin(Math.sin(OBL) * Math.sin(l));
const rightAscension = (l) => Math.atan2(Math.sin(l) * Math.cos(OBL), Math.cos(l));
const siderealTime = (d, lw) => RAD * (280.16 + 360.9856235 * d) - lw;
function sunPosition(ms, lat, lng) {
  const lw = RAD * -lng, phi = RAD * lat, d = toDays(ms);
  const L = eclipticLongitude(solarMeanAnomaly(d));
  const dec = declination(L), H = siderealTime(d, lw) - rightAscension(L);
  const alt = Math.asin(Math.sin(phi) * Math.sin(dec) + Math.cos(phi) * Math.cos(dec) * Math.cos(H));
  const az = Math.atan2(Math.sin(H), Math.cos(H) * Math.sin(phi) - Math.tan(dec) * Math.cos(phi));
  return { alt, az };
}
function utcMs(dateStr, hour, lng) {
  const [y, mo, d] = dateStr.split("-").map(Number);
  return Date.UTC(y, mo - 1, d, 0, 0, 0, 0) + (hour - lng / 15) * 3600 * 1000;
}

/* Enveloppe convexe (chaîne monotone d'Andrew) sur des [lng,lat]. */
function convexHull(pts) {
  if (pts.length < 3) return pts;
  const p = pts.slice().sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const cross = (o, a, b) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const lo = [];
  for (const q of p) { while (lo.length >= 2 && cross(lo[lo.length - 2], lo[lo.length - 1], q) <= 0) lo.pop(); lo.push(q); }
  const up = [];
  for (let i = p.length - 1; i >= 0; i--) { const q = p[i]; while (up.length >= 2 && cross(up[up.length - 2], up[up.length - 1], q) <= 0) up.pop(); up.push(q); }
  lo.pop(); up.pop(); return lo.concat(up);
}

/* Interroge une couche vectorielle des tuiles chargées (building…). */
function queryTiles(map, name) {
  const sl = map.getStyle().layers || [];
  const lyr = sl.find((l) => l["source-layer"] === name && l.source);
  try {
    let f = lyr ? map.querySourceFeatures(lyr.source, { sourceLayer: name }) : [];
    if (!f.length) f = map.querySourceFeatures("openmaptiles", { sourceLayer: name });
    return f || [];
  } catch (_) { return []; }
}

/* Emprise [w,s,e,n] d'une couche : bbox fournie, sinon calculée du GeoJSON. */
function layerBbox(l) {
  if (Array.isArray(l.bbox) && l.bbox.length === 4) return l.bbox;
  const gj = l.geojson; if (!gj?.features?.length) return null;
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  const scan = (c) => {
    if (typeof c[0] === "number") { w = Math.min(w, c[0]); e = Math.max(e, c[0]); s = Math.min(s, c[1]); n = Math.max(n, c[1]); }
    else for (const k of c) scan(k);
  };
  for (const f of gj.features) if (f.geometry?.coordinates) scan(f.geometry.coordinates);
  return isFinite(w) ? [w, s, e, n] : null;
}

const SRC = "oma-shadow-src", LYR = "oma-shadow-fill";        // ombres bâtiments (vecteur)
const IMG_DISP = "oma-canopy-img", IMG_SHAD = "oma-canopy-shad"; // canopée raster (vraie emprise) + son ombre
const MAX_BLD = 4000;
const BLD_ZOOM = 16;

export default function ShadowPanel({ mapRef, layers = [], basemap, setBasemap }) {
  const C = useThemeContext();
  const today = new Date().toISOString().slice(0, 10);
  const [tab, setTab] = useState("sim");
  const [date, setDate] = useState(today);
  const [hour, setHour] = useState(14);
  const [opacity, setOpacity] = useState(0.35);
  const [defH, setDefH] = useState(9);
  const [trees, setTrees] = useState(true);
  const [playing, setPlaying] = useState(false);
  const [scope, setScope] = useState("view");
  const [info, setInfo] = useState(null);
  const [canopyMsg, setCanopyMsg] = useState(null);

  const bldRef = useRef([]);
  const canopyRef = useRef(null);       // { url, corners:[[lng,lat]×4], meanH }
  const treesRef = useRef(true);
  const canopyTimer = useRef(null);
  const scopeBboxRef = useRef(null);
  const prevBaseRef = useRef(null);
  const prevPitchRef = useRef(null);
  const playRef = useRef(null);

  // ── À l'ouverture : Liberty + vue 3D + zoom bâtiments ; restauré à la sortie.
  useEffect(() => {
    prevBaseRef.current = basemap;
    if (basemap !== "liberty") setBasemap?.("liberty");
    const map = mapRef?.current?.getMap?.();
    if (map) {
      prevPitchRef.current = map.getPitch();
      const opts = { pitch: 55, duration: 900 };
      if (map.getZoom() < 15) opts.zoom = BLD_ZOOM;
      try { map.easeTo(opts); } catch (_) {}
    }
    return () => {
      if (prevBaseRef.current && prevBaseRef.current !== "liberty") setBasemap?.(prevBaseRef.current);
      const m = mapRef?.current?.getMap?.();
      try { if (m && prevPitchRef.current != null) m.easeTo({ pitch: prevPitchRef.current, duration: 600 }); } catch (_) {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const layerOptions = useMemo(() =>
    layers.filter((l) => l && (Array.isArray(l.bbox) || l.geojson?.features?.length))
      .map((l) => ({ id: l.id, name: l.name, bbox: layerBbox(l) }))
      .filter((o) => o.bbox), [layers]);

  const beforeId = (map) => {
    const sl = map.getStyle().layers || [];
    return (sl.find((l) => l.type === "fill-extrusion") || sl.find((l) => l.type === "symbol"))?.id;
  };
  const setVis = (map, id, on) => { try { if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", on ? "visible" : "none"); } catch (_) {} };

  // ── couche d'ombre bâtiments (fill vecteur), sous les bâtiments 3D ─────────
  const ensureShadowLayer = useCallback((map) => {
    if (!map.getSource(SRC)) map.addSource(SRC, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    if (!map.getLayer(LYR)) {
      map.addLayer({ id: LYR, type: "fill", source: SRC,
        paint: { "fill-color": "#0e1630", "fill-opacity": opacity, "fill-antialias": false } }, beforeId(map));
    }
  }, [opacity]);

  // ── couches raster canopée : ombre (sombre, décalée) + affichage (vert) ────
  const ensureCanopyLayers = useCallback((map, url, corners) => {
    const before = beforeId(map);
    // ombre de canopée (dessous)
    if (!map.getSource(IMG_SHAD)) map.addSource(IMG_SHAD, { type: "image", url, coordinates: corners });
    else map.getSource(IMG_SHAD).updateImage({ url, coordinates: corners });
    if (!map.getLayer(IMG_SHAD)) {
      map.addLayer({ id: IMG_SHAD, type: "raster", source: IMG_SHAD,
        paint: { "raster-opacity": Math.min(0.85, opacity + 0.15), "raster-brightness-max": 0.22, "raster-saturation": -0.4, "raster-fade-duration": 0 } }, before);
    }
    // canopée réelle (dessus) — vraie emprise, vert
    if (!map.getSource(IMG_DISP)) map.addSource(IMG_DISP, { type: "image", url, coordinates: corners });
    else map.getSource(IMG_DISP).updateImage({ url, coordinates: corners });
    if (!map.getLayer(IMG_DISP)) {
      map.addLayer({ id: IMG_DISP, type: "raster", source: IMG_DISP,
        paint: { "raster-opacity": 0.8, "raster-fade-duration": 0 } }, before);
    }
  }, [opacity]);

  // ── lecture des bâtiments dans la vue (bbox optionnel = emprise couche) ────
  const refreshBuildings = useCallback((map, bbox) => {
    const inBbox = bbox ? (lng, lat) => lng >= bbox[0] && lng <= bbox[2] && lat >= bbox[1] && lat <= bbox[3] : null;
    const seen = new Set();
    const out = [];
    for (const f of queryTiles(map, "building")) {
      if (out.length >= MAX_BLD) break;
      const id = f.id != null ? f.id : null;
      if (id != null) { if (seen.has(id)) continue; seen.add(id); }
      const p = f.properties || {};
      const g = f.geometry; if (!g) continue;
      let hh = Number(p.render_height ?? p.height);
      if (!isFinite(hh) || hh <= 0) hh = NaN;
      const polys = g.type === "Polygon" ? [g.coordinates] : g.type === "MultiPolygon" ? g.coordinates : [];
      for (const poly of polys) {
        const ring = poly[0]; if (!ring || ring.length < 4) continue;
        if (inBbox && !inBbox(ring[0][0], ring[0][1])) continue;
        out.push({ ring, h: hh, lat: ring[0][1] });
      }
    }
    bldRef.current = out;
    return out;
  }, []);

  // ── calcul + rendu des ombres pour l'instant courant ──────────────────────
  const compute = useCallback(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    ensureShadowLayer(map);
    let blds = bldRef.current;
    if (!blds.length) blds = refreshBuildings(map, scopeBboxRef.current);

    const c = map.getCenter();
    const { alt, az } = sunPosition(utcMs(date, Number(hour), c.lng), c.lat, c.lng);
    const altDeg = alt / RAD;
    const src = map.getSource(SRC);
    const can = canopyRef.current;
    const night = alt <= 0.02;

    // canopée : visible tant que « trees » ; son OMBRE seulement de jour.
    setVis(map, IMG_DISP, trees && !!can);
    if (night || !trees || !can) {
      src && src.setData({ type: "FeatureCollection", features: [] });
      setVis(map, IMG_SHAD, false);
      setInfo({ night, alt: altDeg, count: 0 });
      if (night) return;
    }

    if (!night) {
      const bearing = ((az / RAD) % 360 + 360) % 360;
      const th = bearing * RAD;
      const factor = 1 / Math.tan(alt);
      const cosN = Math.cos(th), sinE = Math.sin(th);

      // ── ombres bâtiments (vecteur, enveloppe convexe balayée) ──
      const feats = [];
      for (const b of blds) {
        const H = isFinite(b.h) ? b.h : Number(defH);
        if (!(H > 0)) continue;
        const d = H * factor;
        const dLat = (d * cosN) / 111320;
        const dLng = (d * sinE) / (111320 * Math.cos(b.lat * RAD));
        const nn = b.ring.length, pts = new Array(nn * 2);
        for (let i = 0; i < nn; i++) { const q = b.ring[i]; pts[i] = q; pts[nn + i] = [q[0] + dLng, q[1] + dLat]; }
        const hull = convexHull(pts);
        if (hull.length < 3) continue;
        hull.push(hull[0]);
        feats.push({ type: "Feature", properties: null, geometry: { type: "Polygon", coordinates: [hull] } });
      }
      src && src.setData({ type: "FeatureCollection", features: feats });

      // ── ombre de canopée : copie sombre du raster décalée (hauteur moyenne) ──
      if (trees && can && can.meanH > 0) {
        const d = can.meanH * factor;
        const dLat = (d * cosN) / 111320;
        const shad = can.corners.map(([lng, lat]) => [lng + (d * sinE) / (111320 * Math.cos(lat * RAD)), lat + dLat]);
        const ss = map.getSource(IMG_SHAD);
        if (ss) { ss.updateImage({ url: can.url, coordinates: shad }); setVis(map, IMG_SHAD, true); }
      } else setVis(map, IMG_SHAD, false);

      setInfo({ night: false, alt: altDeg, factor, count: feats.length });
    }
  }, [mapRef, date, hour, defH, trees, ensureShadowLayer, refreshBuildings]);

  useEffect(() => { const t = requestAnimationFrame(compute); return () => cancelAnimationFrame(t); }, [compute]);

  // ── Canopée Meta (raster lissé) sur l'emprise ─────────────────────────────
  const fetchCanopy = useCallback(async () => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    let bbox = scopeBboxRef.current;
    if (!bbox) { const b = map.getBounds(); if (!b) return; bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
    if ((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) > 0.2) {
      canopyRef.current = null; setVis(map, IMG_DISP, false); setVis(map, IMG_SHAD, false);
      setCanopyMsg({ err: "Zoomez pour charger la canopée (emprise trop grande)." }); return;
    }
    setCanopyMsg({ busy: true });
    try {
      const r = await fetch(`${API}/shadow/canopy`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ bbox, min_height: 3 }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      const url = `data:image/png;base64,${d.canopy_b64}`;
      canopyRef.current = { url, corners: d.image_coordinates, meanH: d.mean_height };
      ensureCanopyLayers(map, url, d.image_coordinates);
      setCanopyMsg({ ok: true, dataset: d.dataset, meanH: d.mean_height });
      compute();
    } catch (e) {
      canopyRef.current = null; setVis(map, IMG_DISP, false); setVis(map, IMG_SHAD, false);
      setCanopyMsg({ err: e.message || String(e) }); compute();
    }
  }, [mapRef, compute, ensureCanopyLayers]);

  const scheduleCanopy = useCallback(() => {
    clearTimeout(canopyTimer.current);
    canopyTimer.current = setTimeout(() => fetchCanopy(), 400);
  }, [fetchCanopy]);

  useEffect(() => {
    treesRef.current = trees;
    if (trees) scheduleCanopy();
    else {
      const map = mapRef?.current?.getMap?.();
      if (map) { setVis(map, IMG_DISP, false); setVis(map, IMG_SHAD, false); }
      setCanopyMsg(null); compute();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trees]);

  // opacité → ombres (bâtiments + canopée)
  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    try {
      if (map.getLayer(LYR)) map.setPaintProperty(LYR, "fill-opacity", Number(opacity));
      if (map.getLayer(IMG_SHAD)) map.setPaintProperty(IMG_SHAD, "raster-opacity", Math.min(0.85, Number(opacity) + 0.15));
    } catch (_) {}
  }, [opacity, mapRef]);

  const changeScope = useCallback((val) => {
    setScope(val);
    const map = mapRef?.current?.getMap?.();
    if (val === "view") {
      scopeBboxRef.current = null;
      if (map) { refreshBuildings(map, null); compute(); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    const opt = layerOptions.find((o) => o.id === val);
    scopeBboxRef.current = opt?.bbox || null;
    if (map && opt?.bbox) { const [w, s, e, n] = opt.bbox; map.fitBounds([[w, s], [e, n]], { padding: 40, duration: 800 }); }
  }, [mapRef, layerOptions, refreshBuildings, compute, scheduleCanopy]);

  // ré-échantillonne bâtiments + canopée quand la carte bouge / le fond change
  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const onMove = () => { refreshBuildings(map, scopeBboxRef.current); compute(); if (treesRef.current) scheduleCanopy(); };
    const onStyle = () => { setTimeout(() => { refreshBuildings(map, scopeBboxRef.current); compute(); if (treesRef.current) scheduleCanopy(); }, 300); };
    map.on("moveend", onMove);
    map.on("styledata", onStyle);
    return () => { map.off("moveend", onMove); map.off("styledata", onStyle); };
  }, [mapRef, refreshBuildings, compute, scheduleCanopy]);

  // lecture « toute la journée »
  useEffect(() => {
    if (!playing) { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } return; }
    playRef.current = setInterval(() => { setHour((h) => { const nx = h + 0.25; return nx > 21 ? 6 : nx; }); }, 220);
    return () => { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } };
  }, [playing]);

  // nettoyage à la fermeture
  useEffect(() => {
    return () => {
      const map = mapRef?.current?.getMap?.();
      if (playRef.current) clearInterval(playRef.current);
      clearTimeout(canopyTimer.current);
      try {
        if (map) {
          [LYR, IMG_SHAD, IMG_DISP].forEach((id) => { if (map.getLayer(id)) map.removeLayer(id); });
          [SRC, IMG_SHAD, IMG_DISP].forEach((id) => { if (map.getSource(id)) map.removeSource(id); });
        }
      } catch (_) {}
    };
  }, [mapRef]);

  const hh = Math.floor(hour), mm = Math.round((hour - hh) * 60);
  const clock = `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", boxSizing: "border-box" };
  const Code = ({ children }) => (<code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>);
  const tabBtn = (id, label) => (
    <button key={id} onClick={() => setTab(id)}
      style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>{label}</button>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("sim", "Ombrage")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Simule l'<b>ombre portée au sol</b> des <b>bâtiments</b> et de la <b>canopée</b>, à une date et une heure, et la fait défiler tout au long de la journée.</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Bâtiments — sans téléchargement</div>
            <p style={{ margin: 0, color: C.mut }}>Lus des <b>tuiles vectorielles déjà chargées</b> (couche <Code>building</Code>, hauteur <Code>render_height</Code>). Ombre = <i>H / tan(hauteur solaire)</i>, direction opposée au soleil (position SunCalc). Sol plat supposé.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Canopée — modèle Meta ~1 m</div>
            <p style={{ margin: 0, color: C.mut }}>Modèle <b>WRI/Meta High Resolution Canopy Height 2020</b> (via Earth Engine), rendu en <b>aperçu raster lissé</b> (vraie emprise des arbres, en vert) ; son ombre est une copie sombre du raster décalée selon la hauteur moyenne et le soleil.</p>
          </div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            À venir — relief, et itinéraires « plus ou moins ombragés ». Hypothèses : sol plat, ombre au sol seulement ; l'ombre de canopée utilise la hauteur MOYENNE (décalage uniforme).
          </div>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1, minHeight: 0, overflowY: "auto", paddingRight: 4 }}>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
            Ombre des <b>bâtiments</b> + <b>canopée réelle</b> (Meta ~1 m). Fond <b>Liberty</b>, <b>vue 3D</b> et zoom réglés automatiquement — déplacez-vous sur une ville et faites défiler l'heure.
          </div>

          <div>
            <div style={lbl}>Emprise du calcul</div>
            <select value={scope} onChange={(e) => changeScope(e.target.value)} style={{ ...inp, width: "100%" }}>
              <option value="view">Vue courante de la carte</option>
              {layerOptions.map((o) => <option key={o.id} value={o.id}>Emprise de la couche : {o.name}</option>)}
            </select>
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Date</div>
              <input type="date" value={date} onChange={(e) => setDate(e.target.value)} style={{ ...inp, width: "100%" }} />
            </div>
            <div style={{ width: 96 }}>
              <div style={lbl}>Heure</div>
              <div style={{ fontFamily: M, fontSize: 15, fontWeight: 600, color: C.txt, padding: "5px 0", textAlign: "center" }}>{clock}</div>
            </div>
          </div>

          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <button onClick={() => setPlaying((p) => !p)}
                style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "5px 12px", cursor: "pointer",
                  background: playing ? C.acc : "transparent", color: playing ? "#fff" : C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7, flexShrink: 0 }}>
                {playing ? "❚❚ Pause" : "▶ Journée"}
              </button>
              <input type="range" min={0} max={24} step={0.25} value={hour}
                onChange={(e) => { setPlaying(false); setHour(Number(e.target.value)); }} style={{ flex: 1 }} />
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", fontFamily: M, fontSize: 9, color: C.dim, marginTop: 2 }}>
              <span>0h</span><span>6h</span><span>12h</span><span>18h</span><span>24h</span>
            </div>
          </div>

          <div style={{ background: C.bg2 || C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", fontFamily: F, fontSize: 11.5, color: C.txt }}>
            {!info ? "Calcul…" : info.night ? (
              <span>🌙 Soleil sous l'horizon ({info.alt.toFixed(0)}°) — nuit, pas d'ombre portée.</span>
            ) : (
              <span>☀️ Soleil à <b>{info.alt.toFixed(0)}°</b> · ombre ≈ <b>{info.factor.toFixed(1)}×</b> la hauteur · <b>{info.count}</b> bâtiment(s).</span>
            )}
            {info && !info.night && info.count === 0 && !canopyRef.current && (
              <div style={{ color: C.dim, marginTop: 4 }}>Aucun bâtiment ici : déplacez la carte sur une ville (le fond Liberty et le zoom sont déjà réglés).</div>
            )}
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Opacité ombre · {Math.round(opacity * 100)}%</div>
              <input type="range" min={0.1} max={0.8} step={0.05} value={opacity} onChange={(e) => setOpacity(Number(e.target.value))} style={{ width: "100%" }} />
            </div>
            <div style={{ width: 110 }}>
              <div style={lbl}>Haut. défaut · m</div>
              <input type="number" min={2} max={200} value={defH} onChange={(e) => setDefH(Number(e.target.value))} style={{ ...inp, width: "100%" }} />
            </div>
          </div>
          <div style={{ fontFamily: F, fontSize: 10, color: C.dim }}>« Hauteur défaut » : bâtiments sans hauteur renseignée dans les tuiles.</div>

          <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.txt, cursor: "pointer" }}>
              <input type="checkbox" checked={trees} onChange={(e) => setTrees(e.target.checked)} />
              🌳 Canopée <span style={{ color: C.dim }}>(Meta ~1 m, vraie emprise)</span>
            </label>
            {trees && (
              <div style={{ fontFamily: F, fontSize: 10.5, marginTop: 4 }}>
                {canopyMsg?.busy ? <span style={{ color: C.mut }}>⏳ Chargement de la canopée Meta…</span>
                  : canopyMsg?.ok ? <span style={{ color: "#2e7d4f" }}>Canopée {String(canopyMsg.dataset || "").includes("Meta") ? "Meta ~1 m" : "ETH 10 m"} affichée{canopyMsg.meanH ? ` · hauteur moy. ${canopyMsg.meanH} m` : ""}.</span>
                  : canopyMsg?.err ? <span style={{ color: C.dim }}>Canopée indisponible — {canopyMsg.err}</span>
                  : <span style={{ color: C.dim }}>Vraie emprise des arbres (raster lissé, vert) + son ombre.</span>}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

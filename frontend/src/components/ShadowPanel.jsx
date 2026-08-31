/**
 * ShadowPanel.jsx — Ombres portées des bâtiments + canopée.
 *
 * • Bâtiments : 100 % client-side, lus des tuiles MapLibre (couche `building`,
 *   `render_height`), projetés au sol (SunCalc validé) → polygones d'ombre.
 * • Canopée : modèle WRI/Meta ~1 m via GEE (/api/shadow/canopy), rendu en
 *   aperçu raster lissé (vraie emprise, vert) ; son ombre = plusieurs copies
 *   sombres du raster empilées de la BASE jusqu'au décalage plein (pas de trou).
 * • Emprise du calcul : vue courante, emprise d'une couche, ou ROI dessiné.
 * • Statistiques des surfaces ombragées sur la zone (bouton, actif une fois la
 *   canopée chargée).
 */
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";
import ShadowDashboard from "./ShadowDashboard";
import { importFile } from "../utils/helpers";

/* ── Position du soleil (port SunCalc, validé) → {alt, az} rad ; az depuis le
   SUD, positif vers l'ouest. */
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

function queryTiles(map, name) {
  const sl = map.getStyle().layers || [];
  const lyr = sl.find((l) => l["source-layer"] === name && l.source);
  try {
    let f = lyr ? map.querySourceFeatures(lyr.source, { sourceLayer: name }) : [];
    if (!f.length) f = map.querySourceFeatures("openmaptiles", { sourceLayer: name });
    return f || [];
  } catch (_) { return []; }
}

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


const SRC = "oma-shadow-src", LYR = "oma-shadow-fill";
const IMG_DISP = "oma-canopy-img";
const SHAD_K = 6;                              // copies d'ombre canopée (base→plein)
const shadId = (i) => `oma-canopy-shad-${i}`;
const ROI_SRC = "oma-roi-src", ROI_LYR = "oma-roi-line";
const ZONE_SRC = "oma-zone-src", ZONE_FILL = "oma-zone-fill", ZONE_LINE = "oma-zone-line";
const MAX_BLD = 4000, BLD_ZOOM = 16;

/* Polygones [ [ [lng,lat]… ] …] extraits d'un GeoJSON (anneaux extérieurs). */
function extractPolys(gj) {
  const out = [];
  for (const f of gj.features || []) {
    const g = f.geometry; if (!g) continue;
    if (g.type === "Polygon") out.push(g.coordinates[0]);
    else if (g.type === "MultiPolygon") for (const p of g.coordinates) out.push(p[0]);
  }
  return out;
}
const loadImage = (url) => new Promise((res, rej) => { const im = new Image(); im.onload = () => res(im); im.onerror = rej; im.src = url; });

/* Point dans l'un des anneaux (ray casting) — filtre à l'emprise exacte. */
function pointInPolys(lng, lat, polys) {
  for (const ring of polys) {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const xi = ring[i][0], yi = ring[i][1], xj = ring[j][0], yj = ring[j][1];
      if (((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
    }
    if (inside) return true;
  }
  return false;
}
/* Géométrie MultiPolygon (pour le clip GEE de la canopée). */
function zoneGeometry(gj) {
  const polys = [];
  for (const f of gj.features || []) { const g = f.geometry; if (!g) continue; if (g.type === "Polygon") polys.push(g.coordinates); else if (g.type === "MultiPolygon") for (const p of g.coordinates) polys.push(p); }
  return polys.length ? { type: "MultiPolygon", coordinates: polys } : null;
}

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
  const [roiReady, setRoiReady] = useState(false);
  const [roiDrawing, setRoiDrawing] = useState(false);
  const [info, setInfo] = useState(null);
  const [canopyMsg, setCanopyMsg] = useState(null);
  const [zoneName, setZoneName] = useState(null);   // nom du GeoJSON importé
  const [dashData, setDashData] = useState(null);   // {data, meta} du tableau de bord
  const [dashBusy, setDashBusy] = useState(false);

  const bldRef = useRef([]);
  const featsRef = useRef([]);
  const canopyRef = useRef(null);       // { url, corners, meanH, areaM2 }
  const zoneRef = useRef(null);         // { geojson, bbox, name } (GeoJSON importé)
  const zonePolysRef = useRef(null);    // anneaux du GeoJSON (filtre exact) ou null
  const fileRef = useRef(null);
  const treesRef = useRef(true);
  const canopyTimer = useRef(null);
  const scopeBboxRef = useRef(null);
  const roiBboxRef = useRef(null);
  const roiHandlerRef = useRef(null);
  const roiPtsRef = useRef([]);
  const prevBaseRef = useRef(null);
  const prevPitchRef = useRef(null);
  const playRef = useRef(null);

  // ── ouverture : Liberty + 3D + zoom ; restauré à la sortie ────────────────
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

  const ensureShadowLayer = useCallback((map) => {
    if (!map.getSource(SRC)) map.addSource(SRC, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    if (!map.getLayer(LYR)) {
      map.addLayer({ id: LYR, type: "fill", source: SRC,
        paint: { "fill-color": "#0e1630", "fill-opacity": opacity, "fill-antialias": false } }, beforeId(map));
    }
  }, [opacity]);

  // canopée : K copies d'ombre (base→plein) + affichage vert par-dessus
  const ensureCanopyLayers = useCallback((map, url, corners) => {
    const before = beforeId(map);
    for (let i = 0; i < SHAD_K; i++) {
      const id = shadId(i);
      if (!map.getSource(id)) map.addSource(id, { type: "image", url, coordinates: corners });
      else map.getSource(id).updateImage({ url, coordinates: corners });
      if (!map.getLayer(id)) map.addLayer({ id, type: "raster", source: id,
        paint: { "raster-opacity": 0.17, "raster-brightness-max": 0.2, "raster-saturation": -0.4, "raster-fade-duration": 0 } }, before);
    }
    if (!map.getSource(IMG_DISP)) map.addSource(IMG_DISP, { type: "image", url, coordinates: corners });
    else map.getSource(IMG_DISP).updateImage({ url, coordinates: corners });
    if (!map.getLayer(IMG_DISP)) map.addLayer({ id: IMG_DISP, type: "raster", source: IMG_DISP,
      paint: { "raster-opacity": 0.82, "raster-fade-duration": 0 } }, before);
  }, []);

  const refreshBuildings = useCallback((map, bbox) => {
    const inB = bbox ? (lng, lat) => lng >= bbox[0] && lng <= bbox[2] && lat >= bbox[1] && lat <= bbox[3] : null;
    const zp = zonePolysRef.current;   // filtre à l'emprise exacte (GeoJSON importé)
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
        if (inB && !inB(ring[0][0], ring[0][1])) continue;
        if (zp && !pointInPolys(ring[0][0], ring[0][1], zp)) continue;
        // hf = enveloppe convexe de l'emprise, précalculée → ombre projetée plus légère
        out.push({ hf: convexHull(ring), h: hh, lat: ring[0][1] });
      }
    }
    bldRef.current = out;
    return out;
  }, []);

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

    setVis(map, IMG_DISP, trees && !!can);
    for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false);

    if (night) {
      src && src.setData({ type: "FeatureCollection", features: [] });
      featsRef.current = [];
      setInfo({ night: true, alt: altDeg, count: 0 });
      return;
    }

    const bearing = ((az / RAD) % 360 + 360) % 360;
    const th = bearing * RAD;
    const factor = 1 / Math.tan(alt);
    const cosN = Math.cos(th), sinE = Math.sin(th);

    // ombres bâtiments (vecteur)
    const feats = [];
    for (const b of blds) {
      const H = isFinite(b.h) ? b.h : Number(defH);
      if (!(H > 0)) continue;
      const d = H * factor;
      const dLat = (d * cosN) / 111320, dLng = (d * sinE) / (111320 * Math.cos(b.lat * RAD));
      const hf = b.hf, m = hf.length, pts = new Array(m * 2);
      for (let i = 0; i < m; i++) { const q = hf[i]; pts[i] = q; pts[m + i] = [q[0] + dLng, q[1] + dLat]; }
      const hull = convexHull(pts);
      if (hull.length < 3) continue;
      hull.push(hull[0]);
      feats.push({ type: "Feature", properties: null, geometry: { type: "Polygon", coordinates: [hull] } });
    }
    src && src.setData({ type: "FeatureCollection", features: feats });
    featsRef.current = feats;

    // ombre de canopée : copies empilées de la base (0) au décalage plein
    if (trees && can && can.meanH > 0) {
      const full = can.meanH * factor;
      for (let i = 0; i < SHAD_K; i++) {
        const frac = SHAD_K > 1 ? i / (SHAD_K - 1) : 1;
        const d = full * frac;
        const dLat = (d * cosN) / 111320;
        const shad = can.corners.map(([lng, lat]) => [lng + (d * sinE) / (111320 * Math.cos(lat * RAD)), lat + dLat]);
        const ss = map.getSource(shadId(i));
        if (ss) { try { ss.setCoordinates(shad); } catch (_) {} setVis(map, shadId(i), true); }
      }
    }
    setInfo({ night: false, alt: altDeg, factor, count: feats.length });
  }, [mapRef, date, hour, defH, trees, ensureShadowLayer, refreshBuildings]);

  useEffect(() => { const t = requestAnimationFrame(compute); return () => cancelAnimationFrame(t); }, [compute]);

  // ── Canopée Meta (raster lissé) ───────────────────────────────────────────
  const fetchCanopy = useCallback(async () => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    let bbox = scopeBboxRef.current;
    if (!bbox) { const b = map.getBounds(); if (!b) return; bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
    if ((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) > 0.2) {
      canopyRef.current = null; setVis(map, IMG_DISP, false); for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false);
      setCanopyMsg({ err: "Zoomez pour charger la canopée (emprise trop grande)." }); return;
    }
    setCanopyMsg({ busy: true });
    try {
      const body = { bbox, min_height: 3 };
      if (zonePolysRef.current && zoneRef.current?.geojson) { const gm = zoneGeometry(zoneRef.current.geojson); if (gm) body.geometry = gm; }
      const r = await fetch(`${API}/shadow/canopy`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      const url = `data:image/png;base64,${d.canopy_b64}`;
      canopyRef.current = { url, corners: d.image_coordinates, meanH: d.mean_height, areaM2: d.canopy_area_m2 };
      ensureCanopyLayers(map, url, d.image_coordinates);
      setCanopyMsg({ ok: true, dataset: d.dataset, meanH: d.mean_height, areaM2: d.canopy_area_m2 });
      compute();
    } catch (e) {
      canopyRef.current = null; setVis(map, IMG_DISP, false); for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false);
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
      if (map) { setVis(map, IMG_DISP, false); for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false); }
      setCanopyMsg(null); compute();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trees]);

  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (map && map.getLayer(LYR)) { try { map.setPaintProperty(LYR, "fill-opacity", Number(opacity)); } catch (_) {} }
  }, [opacity, mapRef]);

  // ── Emprise de calcul : vue / couche / ROI ────────────────────────────────
  const clearRoi = useCallback((map) => {
    roiBboxRef.current = null; setRoiReady(false);
    const cs = map?.getSource?.(ROI_SRC); if (cs) cs.setData({ type: "FeatureCollection", features: [] });
  }, []);

  const changeScope = useCallback((val) => {
    setScope(val);
    const map = mapRef?.current?.getMap?.();
    if (val === "view") {
      scopeBboxRef.current = null; zonePolysRef.current = null; if (map) clearRoi(map);
      if (map) { refreshBuildings(map, null); compute(); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    if (val === "roi") {
      scopeBboxRef.current = roiBboxRef.current; zonePolysRef.current = null;
      if (map && roiBboxRef.current) { const [w, s, e, n] = roiBboxRef.current; map.fitBounds([[w, s], [e, n]], { padding: 60, duration: 700 }); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    if (val === "import") {
      scopeBboxRef.current = zoneRef.current?.bbox || null;
      zonePolysRef.current = zoneRef.current?.geojson ? extractPolys(zoneRef.current.geojson) : null;
      if (map && zoneRef.current?.bbox) { const [w, s, e, n] = zoneRef.current.bbox; map.fitBounds([[w, s], [e, n]], { padding: 50, duration: 700 }); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    const opt = layerOptions.find((o) => o.id === val);
    scopeBboxRef.current = opt?.bbox || null; zonePolysRef.current = null; if (map) clearRoi(map);
    if (map && opt?.bbox) { const [w, s, e, n] = opt.bbox; map.fitBounds([[w, s], [e, n]], { padding: 40, duration: 800 }); if (treesRef.current) scheduleCanopy(); }
  }, [mapRef, layerOptions, refreshBuildings, compute, scheduleCanopy, clearRoi]);

  // dessine la zone GeoJSON importée (contour + léger remplissage)
  const drawZone = useCallback((map, gj) => {
    if (!map.getSource(ZONE_SRC)) map.addSource(ZONE_SRC, { type: "geojson", data: gj });
    else map.getSource(ZONE_SRC).setData(gj);
    const before = beforeId(map);
    if (!map.getLayer(ZONE_FILL)) map.addLayer({ id: ZONE_FILL, type: "fill", source: ZONE_SRC, paint: { "fill-color": "#e8590c", "fill-opacity": 0.06 } }, before);
    if (!map.getLayer(ZONE_LINE)) map.addLayer({ id: ZONE_LINE, type: "line", source: ZONE_SRC, paint: { "line-color": "#e8590c", "line-width": 2 } }, before);
  }, []);

  const onImportZone = useCallback(async (file) => {
    if (!file) return;
    try {
      const gj = await importFile(file);
      const bbox = layerBbox({ geojson: gj });
      if (!bbox) throw new Error("aucune géométrie exploitable");
      zoneRef.current = { geojson: gj, bbox, name: file.name };
      zonePolysRef.current = extractPolys(gj);   // filtre + clip à l'emprise exacte
      setZoneName(file.name);
      const map = mapRef?.current?.getMap?.();
      scopeBboxRef.current = bbox; setScope("import");
      if (map) {
        drawZone(map, gj); clearRoi(map);
        const [w, s, e, n] = bbox; map.fitBounds([[w, s], [e, n]], { padding: 50, duration: 700 });
        refreshBuildings(map, bbox); compute(); if (treesRef.current) scheduleCanopy();
      }
    } catch (e) { setCanopyMsg({ err: `Import zone : ${e.message || e}` }); }
  }, [mapRef, drawZone, clearRoi, refreshBuildings, compute, scheduleCanopy]);

  // ── Tableau de bord : ombrage de la zone sur toute la journée ─────────────
  const computeDaily = useCallback(async () => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    let bbox = scopeBboxRef.current;
    if (!bbox) { const b = map.getBounds(); bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
    setDashBusy(true);
    try {
      const blds = refreshBuildings(map, bbox);
      const [w, s, e, n] = bbox;
      const midlat = ((s + n) / 2) * RAD;
      const bboxArea = Math.abs((e - w) * 111320 * Math.cos(midlat) * (n - s) * 111320);
      const W = 440, Hh = Math.max(1, Math.min(1000, Math.round(W * (n - s) / (e - w))));
      const X = (lng) => (lng - w) / (e - w) * W, Y = (lat) => (n - lat) / (n - s) * Hh;
      // masque de zone
      const cvZ = document.createElement("canvas"); cvZ.width = W; cvZ.height = Hh;
      const zx = cvZ.getContext("2d"); zx.fillStyle = "#fff";
      const zonePolys = (scope === "import" && zoneRef.current?.geojson) ? extractPolys(zoneRef.current.geojson) : [[[w, s], [e, s], [e, n], [w, n], [w, s]]];
      for (const ring of zonePolys) { zx.beginPath(); ring.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? zx.lineTo(x, y) : zx.moveTo(x, y); }); zx.closePath(); zx.fill(); }
      const zoneA = zx.getImageData(0, 0, W, Hh).data;
      let zonePix = 0; for (let i = 3; i < zoneA.length; i += 4) if (zoneA[i] > 0) zonePix++;
      const zoneArea = (zonePix / (W * Hh)) * bboxArea;

      let canImg = null, meanH = 0, canPctStatic = 0;
      if (trees && canopyRef.current) {
        try { canImg = await loadImage(canopyRef.current.url); } catch (_) { canImg = null; }
        meanH = canopyRef.current.meanH || 0;
        canPctStatic = zoneArea ? (canopyRef.current.areaM2 || 0) / zoneArea * 100 : 0;
      }
      const cvB = document.createElement("canvas"); cvB.width = W; cvB.height = Hh;
      const cvC = document.createElement("canvas"); cvC.width = W; cvC.height = Hh;
      const bx = cvB.getContext("2d"), cx = cvC.getContext("2d");
      const cLng = (w + e) / 2, cLat = (s + n) / 2;

      const out = [];
      for (let hr = 6; hr <= 20; hr++) {
        const { alt, az } = sunPosition(utcMs(date, hr, cLng), cLat, cLng);
        if (alt <= 0.02) { out.push({ hour: hr, alt: alt / RAD, night: true, bldPct: 0, canPct: 0, totalPct: 0 }); continue; }
        const bearing = ((az / RAD) % 360 + 360) % 360, th = bearing * RAD, factor = 1 / Math.tan(alt);
        const cosN = Math.cos(th), sinE = Math.sin(th);
        // bâtiments
        bx.clearRect(0, 0, W, Hh); bx.fillStyle = "#fff";
        for (const b of blds) {
          const H = isFinite(b.h) ? b.h : Number(defH); if (!(H > 0)) continue;
          const d = H * factor, dLat = (d * cosN) / 111320, dLng = (d * sinE) / (111320 * Math.cos(b.lat * RAD));
          const hf = b.hf, m = hf.length, pts = new Array(m * 2);
          for (let i = 0; i < m; i++) { const q = hf[i]; pts[i] = q; pts[m + i] = [q[0] + dLng, q[1] + dLat]; }
          const hull = convexHull(pts); if (hull.length < 3) continue;
          bx.beginPath(); hull.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? bx.lineTo(x, y) : bx.moveTo(x, y); }); bx.closePath(); bx.fill();
        }
        const bldA = bx.getImageData(0, 0, W, Hh).data;
        // canopée
        let canA = null;
        if (canImg) {
          cx.clearRect(0, 0, W, Hh);
          const full = meanH * factor;
          for (let k = 0; k < SHAD_K; k++) {
            const frac = SHAD_K > 1 ? k / (SHAD_K - 1) : 1, d = full * frac;
            const px = ((d * sinE) / (111320 * Math.cos(cLat * RAD))) / (e - w) * W;
            const py = -((d * cosN) / 111320) / (n - s) * Hh;
            cx.drawImage(canImg, px, py, W, Hh);
          }
          canA = cx.getImageData(0, 0, W, Hh).data;
        }
        let bP = 0, cP = 0, tP = 0;
        for (let i = 3; i < zoneA.length; i += 4) { if (zoneA[i] <= 0) continue; const bb = bldA[i] > 0, cc = canA ? canA[i] > 0 : false; if (bb) bP++; if (cc) cP++; if (bb || cc) tP++; }
        out.push({ hour: hr, alt: alt / RAD, night: false, bldPct: zonePix ? 100 * bP / zonePix : 0, canPct: zonePix ? 100 * cP / zonePix : 0, totalPct: zonePix ? 100 * tP / zonePix : 0 });
      }
      const zn = scope === "import" ? (zoneRef.current?.name || "Zone importée") : scope === "roi" ? "ROI dessiné" : scope === "view" ? "Vue courante" : "Couche";
      setDashData({ data: out, meta: { zoneName: zn, date, zoneArea, dataset: canopyRef.current ? (canopyMsg?.dataset || "") : "", canopy: !!canopyRef.current, canPctStatic } });
    } finally { setDashBusy(false); }
  }, [mapRef, date, defH, trees, scope, refreshBuildings, canopyMsg]);

  const drawRoi = useCallback((map, bbox) => {
    const [w, s, e, n] = bbox;
    const fc = { type: "FeatureCollection", features: [{ type: "Feature", properties: {}, geometry: { type: "Polygon", coordinates: [[[w, s], [e, s], [e, n], [w, n], [w, s]]] } }] };
    if (!map.getSource(ROI_SRC)) map.addSource(ROI_SRC, { type: "geojson", data: fc });
    else map.getSource(ROI_SRC).setData(fc);
    if (!map.getLayer(ROI_LYR)) {
      const before = beforeId(map);
      map.addLayer({ id: ROI_LYR, type: "line", source: ROI_SRC, paint: { "line-color": "#e8590c", "line-width": 2, "line-dasharray": [2, 1] } }, before);
    }
  }, []);

  const startRoi = useCallback(() => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    if (roiDrawing) { // annuler
      if (roiHandlerRef.current) map.off("click", roiHandlerRef.current);
      roiHandlerRef.current = null; setRoiDrawing(false); map.getCanvas().style.cursor = ""; return;
    }
    roiPtsRef.current = []; setRoiDrawing(true); map.getCanvas().style.cursor = "crosshair";
    const h = (ev) => {
      roiPtsRef.current.push([ev.lngLat.lng, ev.lngLat.lat]);
      if (roiPtsRef.current.length >= 2) {
        const [a, b] = roiPtsRef.current;
        const bbox = [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[0], b[0]), Math.max(a[1], b[1])];
        roiBboxRef.current = bbox; scopeBboxRef.current = bbox; setScope("roi"); setRoiReady(true);
        drawRoi(map, bbox);
        map.off("click", h); roiHandlerRef.current = null; map.getCanvas().style.cursor = ""; setRoiDrawing(false);
        refreshBuildings(map, bbox); compute(); if (treesRef.current) scheduleCanopy();
      }
    };
    roiHandlerRef.current = h; map.on("click", h);
  }, [mapRef, roiDrawing, drawRoi, refreshBuildings, compute, scheduleCanopy]);

  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const onMove = () => { refreshBuildings(map, scopeBboxRef.current); compute(); if (treesRef.current) scheduleCanopy(); };
    const onStyle = () => { setTimeout(() => { refreshBuildings(map, scopeBboxRef.current); compute(); if (treesRef.current) scheduleCanopy(); }, 300); };
    map.on("moveend", onMove); map.on("styledata", onStyle);
    return () => { map.off("moveend", onMove); map.off("styledata", onStyle); };
  }, [mapRef, refreshBuildings, compute, scheduleCanopy]);

  useEffect(() => {
    if (!playing) { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } return; }
    // pas fin + cadence élevée → défilement fluide (l'enveloppe convexe précalculée
    // et le filtre d'emprise gardent chaque image légère).
    playRef.current = setInterval(() => { setHour((h) => { const nx = h + 0.12; return nx > 21 ? 6 : nx; }); }, 80);
    return () => { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } };
  }, [playing]);

  useEffect(() => {
    return () => {
      const map = mapRef?.current?.getMap?.();
      if (playRef.current) clearInterval(playRef.current);
      clearTimeout(canopyTimer.current);
      if (map && roiHandlerRef.current) { try { map.off("click", roiHandlerRef.current); map.getCanvas().style.cursor = ""; } catch (_) {} }
      try {
        if (map) {
          const ids = [LYR, IMG_DISP, ROI_LYR, ZONE_FILL, ZONE_LINE, ...Array.from({ length: SHAD_K }, (_, i) => shadId(i))];
          ids.forEach((id) => { if (map.getLayer(id)) map.removeLayer(id); });
          const srcs = [SRC, IMG_DISP, ROI_SRC, ZONE_SRC, ...Array.from({ length: SHAD_K }, (_, i) => shadId(i))];
          srcs.forEach((id) => { if (map.getSource(id)) map.removeSource(id); });
        }
      } catch (_) {}
    };
  }, [mapRef]);

  const hh = Math.floor(hour), mm = Math.round((hour - hh) * 60);
  const clock = `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
  const statsLocked = trees && (canopyMsg?.busy || !canopyRef.current);

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
          <p style={{ margin: 0 }}>Simule l'<b>ombre portée au sol</b> des <b>bâtiments</b> et de la <b>canopée</b>, à une date et une heure, et la fait défiler sur la journée.</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Bâtiments — sans téléchargement</div>
            <p style={{ margin: 0, color: C.mut }}>Lus des <b>tuiles</b> (couche <Code>building</Code>, <Code>render_height</Code>). Ombre = <i>H / tan(soleil)</i>, direction opposée au soleil (SunCalc). Sol plat.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Canopée — Meta ~1 m</div>
            <p style={{ margin: 0, color: C.mut }}>Modèle <b>WRI/Meta 2020</b> (Earth Engine) en aperçu raster lissé (vraie emprise, vert) ; son ombre = plusieurs copies sombres empilées de la base au décalage plein (sans trou).</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Emprise & statistiques</div>
            <p style={{ margin: 0, color: C.mut }}>Calcul sur la vue, l'emprise d'une couche, ou un <b>ROI</b> dessiné (2 clics). Le bouton <b>Statistiques</b> (actif une fois la canopée chargée) donne les surfaces ombragées de la zone.</p>
          </div>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1, minHeight: 0, overflowY: "auto", paddingRight: 4 }}>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
            Ombre des <b>bâtiments</b> + <b>canopée réelle</b> (Meta). Fond <b>Liberty</b>, <b>3D</b> et zoom réglés automatiquement — déplacez-vous sur une ville.
          </div>

          <div>
            <div style={lbl}>Emprise du calcul</div>
            <div style={{ display: "flex", gap: 6 }}>
              <select value={scope} onChange={(e) => changeScope(e.target.value)} style={{ ...inp, flex: 1 }}>
                <option value="view">Vue courante de la carte</option>
                {zoneName && <option value="import">Zone importée : {zoneName}</option>}
                {roiReady && <option value="roi">ROI dessiné</option>}
                {layerOptions.map((o) => <option key={o.id} value={o.id}>Couche : {o.name}</option>)}
              </select>
              <button onClick={startRoi}
                style={{ fontFamily: F, fontSize: 11, fontWeight: 500, padding: "0 10px", cursor: "pointer",
                  background: roiDrawing ? C.acc : "transparent", color: roiDrawing ? "#fff" : C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7, whiteSpace: "nowrap" }}>
                {roiDrawing ? "Annuler" : "ROI"}
              </button>
            </div>
            <input ref={fileRef} type="file" accept=".geojson,.json" style={{ display: "none" }}
              onChange={(e) => { const f = e.target.files?.[0]; e.target.value = ""; onImportZone(f); }} />
            <button onClick={() => fileRef.current?.click()}
              style={{ fontFamily: F, fontSize: 11, fontWeight: 500, padding: "6px 10px", marginTop: 6, width: "100%", cursor: "pointer",
                background: "transparent", color: C.acc, border: `1px dashed ${C.acc}66`, borderRadius: 7 }}>
              ⭱ Importer une zone (GeoJSON)
            </button>
            {roiDrawing && <div style={{ fontFamily: F, fontSize: 10.5, color: C.acc, marginTop: 3 }}>Cliquez 2 coins sur la carte pour définir le ROI.</div>}
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
              <span>🌙 Soleil sous l'horizon ({info.alt.toFixed(0)}°) — nuit, pas d'ombre.</span>
            ) : (
              <span>☀️ Soleil à <b>{info.alt.toFixed(0)}°</b> · ombre ≈ <b>{info.factor.toFixed(1)}×</b> la hauteur · <b>{info.count}</b> bâtiment(s).</span>
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

          <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.txt, cursor: "pointer" }}>
              <input type="checkbox" checked={trees} onChange={(e) => setTrees(e.target.checked)} />
              🌳 Canopée <span style={{ color: C.dim }}>(Meta ~1 m, vraie emprise)</span>
            </label>
            {trees && (
              <div style={{ fontFamily: F, fontSize: 10.5, marginTop: 4 }}>
                {canopyMsg?.busy ? <span style={{ color: C.mut }}>⏳ Chargement de la canopée Meta…</span>
                  : canopyMsg?.ok ? <span style={{ color: "#2e7d4f" }}>Canopée {String(canopyMsg.dataset || "").includes("Meta") ? "Meta ~1 m" : "ETH 10 m"} affichée{canopyMsg.meanH ? ` · h. moy. ${canopyMsg.meanH} m` : ""}.</span>
                  : canopyMsg?.err ? <span style={{ color: C.dim }}>Canopée indisponible — {canopyMsg.err}</span>
                  : <span style={{ color: C.dim }}>Vraie emprise des arbres (raster) + son ombre.</span>}
              </div>
            )}
          </div>

          <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10 }}>
            <button onClick={computeDaily} disabled={statsLocked || dashBusy}
              style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "9px 14px", width: "100%",
                cursor: (statsLocked || dashBusy) ? "not-allowed" : "pointer",
                background: (statsLocked || dashBusy) ? C.bg2 || C.bg : C.acc, color: (statsLocked || dashBusy) ? C.dim : "#fff",
                border: `1px solid ${statsLocked ? C.bdr : C.acc}`, borderRadius: 8 }}>
              {dashBusy ? "Calcul de la journée…" : statsLocked ? "Tableau de bord (attente canopée…)" : "📊 Tableau de bord — ombrage sur la journée"}
            </button>
            <div style={{ fontFamily: F, fontSize: 10, color: C.dim, marginTop: 4 }}>
              Ouvre un tableau de bord (graphiques + tableau) : % de la zone à l'ombre heure par heure, sur l'emprise choisie.
            </div>
          </div>
        </div>
      )}

      {dashData && <ShadowDashboard data={dashData.data} meta={dashData.meta} onClose={() => setDashData(null)} />}
    </div>
  );
}

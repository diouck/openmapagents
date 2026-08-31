/**
 * ShadowPanel.jsx — Ombres portées des bâtiments (étape 1, 100 % client-side).
 *
 * Utilise les bâtiments DÉJÀ chargés par MapLibre (source vectorielle
 * OpenMapTiles, couche `building` avec `render_height`) — AUCUN téléchargement.
 * À partir de la date + l'heure (curseur), calcule la position du soleil
 * (algo SunCalc porté et validé) et projette l'ombre au sol de chaque bâtiment
 * (longueur = H / tan(hauteur solaire), direction opposée au soleil). Le curseur
 * d'heure (+ lecture « toute la journée ») fait défiler l'ombre au fil du jour.
 *
 * Rien n'est envoyé au serveur : lecture des tuiles + géométrie en JS.
 */
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

/* ── Position du soleil (port SunCalc, validé en Python) ────────────────────
   Renvoie {alt, az} en radians. az mesuré depuis le SUD, positif vers l'ouest. */
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
/* Heure solaire locale (au méridien du lieu) → ms UTC. Simple et physiquement
   juste pour la DIRECTION de l'ombre (pas de base de fuseaux nécessaire). */
function utcMs(dateStr, hour, lng) {
  const [y, mo, d] = dateStr.split("-").map(Number);
  return Date.UTC(y, mo - 1, d, 0, 0, 0, 0) + (hour - lng / 15) * 3600 * 1000;
}

/* ── Enveloppe convexe (chaîne monotone d'Andrew) sur des [lng,lat] ───────── */
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

/* Interroge une couche vectorielle des tuiles chargées (building, landcover…). */
function queryTiles(map, name) {
  const sl = map.getStyle().layers || [];
  const lyr = sl.find((l) => l["source-layer"] === name && l.source);
  try {
    let f = lyr ? map.querySourceFeatures(lyr.source, { sourceLayer: name }) : [];
    if (!f.length) f = map.querySourceFeatures("openmaptiles", { sourceLayer: name });
    return f || [];
  } catch (_) { return []; }
}

const SRC = "oma-shadow-src", LYR = "oma-shadow-fill";       // ombres portées
const CAN_SRC = "oma-canopy-src", CAN_LYR = "oma-canopy-fill"; // canopée réelle (vert)
const MAX_BLD = 4000;          // plafond de bâtiments (perf)
const MAX_TREE = 2500;         // plafond de surfaces arborées
const WOOD = new Set(["wood", "forest", "tree", "trees"]);   // classes arborées OMT (landcover/landuse)

const BLD_ZOOM = 16;           // niveau où les bâtiments sont chargés/visibles

export default function ShadowPanel({ mapRef, layers = [], basemap, setBasemap }) {
  const C = useThemeContext();
  const today = new Date().toISOString().slice(0, 10);
  const [tab, setTab] = useState("sim");
  const [date, setDate] = useState(today);
  const [hour, setHour] = useState(14);          // heure solaire locale
  const [opacity, setOpacity] = useState(0.35);
  const [defH, setDefH] = useState(9);           // hauteur si absente (m)
  const [trees, setTrees] = useState(true);      // inclure les surfaces arborées
  const [canopyH, setCanopyH] = useState(12);    // hauteur de canopée (m)
  const [playing, setPlaying] = useState(false);
  const [scope, setScope] = useState("view");    // "view" (carte) | id de couche
  const [info, setInfo] = useState(null);        // {alt, az, shadowBearing, count, night}
  const [canopyMsg, setCanopyMsg] = useState(null);   // état chargement canopée Meta

  const bldRef = useRef([]);       // footprints cache : [{ring:[[lng,lat]..], h, lat}]
  const treeRef = useRef([]);      // arbres des tuiles (fallback) : [{ring, lat}]
  const metaTreeRef = useRef([]);  // canopée Meta (GEE) : [{ring, lat, h}] hauteurs réelles
  const treesRef = useRef(true);   // miroir de `trees` pour les handlers
  const canopyTimer = useRef(null);
  const playRef = useRef(null);
  const scopeBboxRef = useRef(null);   // emprise du calcul (null = vue courante)
  const prevBaseRef = useRef(null);    // fond de carte avant ouverture (pour restaurer)
  const prevPitchRef = useRef(null);   // inclinaison avant ouverture (pour restaurer)

  // ── À l'ouverture : fond LIBERTY (porte/affiche les bâtiments) + zoom au
  //    niveau bâtiments + VUE 3D (inclinaison) pour voir les hauteurs → l'outil
  //    est utilisable d'emblée. On restaure fond + inclinaison en quittant.
  useEffect(() => {
    prevBaseRef.current = basemap;
    if (basemap !== "liberty") setBasemap?.("liberty");
    const map = mapRef?.current?.getMap?.();
    if (map) {
      prevPitchRef.current = map.getPitch();
      const opts = { pitch: 55, duration: 900 };        // bascule en 3D (relief bâti)
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

  // Couches ayant une emprise exploitable (pour restreindre le calcul).
  const layerOptions = useMemo(() =>
    layers.filter((l) => l && (Array.isArray(l.bbox) || l.geojson?.features?.length))
      .map((l) => ({ id: l.id, name: l.name, bbox: layerBbox(l) }))
      .filter((o) => o.bbox), [layers]);

  // ── couche MapLibre (ajout paresseux, ré-ajout après changement de fond) ──
  const ensureLayer = useCallback((map) => {
    // Insertion sous les bâtiments 3D (fill-extrusion) → ils s'élèvent au-dessus
    // de l'ombre ; à défaut, sous les labels.
    const sl = map.getStyle().layers || [];
    const before = (sl.find((l) => l.type === "fill-extrusion") || sl.find((l) => l.type === "symbol"))?.id;
    if (!map.getSource(SRC)) map.addSource(SRC, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    if (!map.getLayer(LYR)) {
      map.addLayer({ id: LYR, type: "fill", source: SRC,
        paint: { "fill-color": ["case", ["==", ["get", "kind"], "tree"], "#1b6b3a", "#0e1630"],
                 "fill-opacity": opacity, "fill-antialias": false } }, before);
    }
    // Canopée réelle (polygones Meta) en vert, au-dessus de l'ombre.
    if (!map.getSource(CAN_SRC)) map.addSource(CAN_SRC, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    if (!map.getLayer(CAN_LYR)) {
      map.addLayer({ id: CAN_LYR, type: "fill", source: CAN_SRC,
        paint: { "fill-color": "#2f9e57", "fill-opacity": 0.55, "fill-outline-color": "#14532d" } }, before);
    }
  }, [opacity]);

  // ── (re)lecture des bâtiments dans la vue (tuiles déjà chargées) ──────────
  // bbox optionnel [w,s,e,n] : restreint le calcul à l'emprise d'une couche.
  const refreshBuildings = useCallback((map, bbox) => {
    const inBbox = bbox ? (lng, lat) => lng >= bbox[0] && lng <= bbox[2] && lat >= bbox[1] && lat <= bbox[3] : null;

    // 1) Bâtiments (couche `building`, hauteur render_height).
    const seen = new Set();
    const out = [];
    for (const f of queryTiles(map, "building")) {
      if (out.length >= MAX_BLD) break;
      const id = f.id != null ? f.id : null;
      if (id != null) { if (seen.has(id)) continue; seen.add(id); }
      const p = f.properties || {};
      const g = f.geometry; if (!g) continue;
      let hh = Number(p.render_height ?? p.height);
      if (!isFinite(hh) || hh <= 0) hh = NaN;    // hauteur manquante → défaut au calcul
      const polys = g.type === "Polygon" ? [g.coordinates] : g.type === "MultiPolygon" ? g.coordinates : [];
      for (const poly of polys) {
        const ring = poly[0]; if (!ring || ring.length < 4) continue;
        if (inBbox && !inBbox(ring[0][0], ring[0][1])) continue;   // filtre emprise couche
        out.push({ ring, h: hh, lat: ring[0][1] });
      }
    }
    bldRef.current = out;

    // 2) Surfaces arborées (landcover/landuse classe wood/forest) → hauteur canopée.
    const tOut = [];
    const tseen = new Set();
    for (const name of ["landcover", "landuse"]) {
      for (const f of queryTiles(map, name)) {
        if (tOut.length >= MAX_TREE) break;
        const p = f.properties || {};
        const cls = String(p.class ?? p.subclass ?? "").toLowerCase();
        if (!WOOD.has(cls)) continue;
        const id = f.id != null ? `${name}:${f.id}` : null;
        if (id) { if (tseen.has(id)) continue; tseen.add(id); }
        const g = f.geometry; if (!g) continue;
        const polys = g.type === "Polygon" ? [g.coordinates] : g.type === "MultiPolygon" ? g.coordinates : [];
        for (const poly of polys) {
          const ring = poly[0]; if (!ring || ring.length < 4) continue;
          if (inBbox && !inBbox(ring[0][0], ring[0][1])) continue;
          tOut.push({ ring, lat: ring[0][1] });
        }
      }
    }
    treeRef.current = tOut;
    return out;
  }, []);

  // ── calcul + rendu des ombres pour l'instant courant ──────────────────────
  const compute = useCallback(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    ensureLayer(map);
    let blds = bldRef.current;
    if (!blds.length) blds = refreshBuildings(map, scopeBboxRef.current);

    const c = map.getCenter();
    const ms = utcMs(date, Number(hour), c.lng);
    const { alt, az } = sunPosition(ms, c.lat, c.lng);
    const altDeg = alt / RAD;
    const src = map.getSource(SRC);

    if (alt <= 0.02) {   // soleil ~sous l'horizon → nuit, pas d'ombre nette
      src && src.setData({ type: "FeatureCollection", features: [] });
      setInfo({ night: true, alt: altDeg, count: blds.length });
      return;
    }
    const bearing = ((az / RAD) % 360 + 360) % 360;        // direction de l'ombre (depuis N)
    const th = bearing * RAD;
    const factor = 1 / Math.tan(alt);                       // ombre = factor × hauteur
    const cosN = Math.cos(th), sinE = Math.sin(th);
    const feats = [];
    // Projette une emprise (bâtiment ou surface arborée) de hauteur H au sol.
    const project = (ring, latc, H, kind) => {
      if (!(H > 0)) return;
      const d = H * factor;                                 // longueur d'ombre (m)
      const dLat = (d * cosN) / 111320;
      const dLng = (d * sinE) / (111320 * Math.cos(latc * RAD));
      const n = ring.length, pts = new Array(n * 2);
      for (let i = 0; i < n; i++) { const q = ring[i]; pts[i] = q; pts[n + i] = [q[0] + dLng, q[1] + dLat]; }
      const hull = convexHull(pts);
      if (hull.length < 3) return;
      hull.push(hull[0]);
      feats.push({ type: "Feature", properties: { kind }, geometry: { type: "Polygon", coordinates: [hull] } });
    };
    for (const b of blds) project(b.ring, b.lat, isFinite(b.h) ? b.h : Number(defH), "bld");
    const nBld = feats.length;
    if (trees) {
      // canopée Meta (hauteurs réelles) si chargée, sinon arbres des tuiles (canopyH).
      const tsrc = metaTreeRef.current.length ? metaTreeRef.current : treeRef.current;
      for (const t of tsrc) project(t.ring, t.lat, t.h != null ? t.h : Number(canopyH), "tree");
    }
    src && src.setData({ type: "FeatureCollection", features: feats });
    setInfo({ night: false, alt: altDeg, az: (az / RAD % 360 + 540) % 360, bearing, factor,
              count: feats.length, nBld, nTree: feats.length - nBld });
  }, [mapRef, date, hour, defH, trees, canopyH, ensureLayer, refreshBuildings]);

  // recalcul quand date / heure / hauteur changent
  useEffect(() => { const t = requestAnimationFrame(compute); return () => cancelAnimationFrame(t); }, [compute]);

  // ── Canopée Meta (GEE) : polygones + hauteurs réelles sur l'emprise ───────
  const fetchCanopy = useCallback(async () => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    let bbox = scopeBboxRef.current;
    if (!bbox) { const b = map.getBounds(); if (!b) return; bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
    if ((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) > 0.25) {
      metaTreeRef.current = []; setCanopyMsg({ err: "Zoomez pour charger la canopée (emprise trop grande)." }); compute(); return;
    }
    setCanopyMsg({ busy: true });
    try {
      const r = await fetch(`${API}/shadow/canopy`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ bbox, scale: 10, min_height: 3 }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const gj = await r.json();
      const out = [];
      for (const f of gj.features || []) {
        const g = f.geometry; if (!g) continue;
        const h = f.properties?.height;
        const polys = g.type === "Polygon" ? [g.coordinates] : g.type === "MultiPolygon" ? g.coordinates : [];
        for (const poly of polys) { const ring = poly[0]; if (!ring || ring.length < 4) continue; out.push({ ring, lat: ring[0][1], h }); }
      }
      metaTreeRef.current = out;
      ensureLayer(map);
      const cs = map.getSource(CAN_SRC);          // canopée réelle en vert (vraie emprise)
      if (cs) cs.setData({ type: "FeatureCollection", features: gj.features || [] });
      setCanopyMsg({ ok: true, n: out.length, dataset: gj.dataset });
      compute();
    } catch (e) {
      metaTreeRef.current = [];
      const cs = map.getSource?.(CAN_SRC); if (cs) cs.setData({ type: "FeatureCollection", features: [] });
      setCanopyMsg({ err: e.message || String(e) }); compute();
    }
  }, [mapRef, compute, ensureLayer]);

  const scheduleCanopy = useCallback(() => {
    clearTimeout(canopyTimer.current);
    canopyTimer.current = setTimeout(() => fetchCanopy(), 400);
  }, [fetchCanopy]);

  // charge/relâche la canopée Meta quand on (dés)active les arbres
  useEffect(() => {
    treesRef.current = trees;
    if (trees) scheduleCanopy();
    else {
      metaTreeRef.current = []; setCanopyMsg(null);
      const cs = mapRef?.current?.getMap?.()?.getSource?.(CAN_SRC);
      if (cs) cs.setData({ type: "FeatureCollection", features: [] });
      compute();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trees]);

  // ── change l'emprise du calcul : vue courante ou emprise d'une couche ─────
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
    if (map && opt?.bbox) {
      const [w, s, e, n] = opt.bbox;
      // recadre sur la couche → charge les tuiles bâtiments là-bas, puis
      // « moveend » relit et recalcule sur cette emprise.
      map.fitBounds([[w, s], [e, n]], { padding: 40, duration: 800 });
    }
  }, [mapRef, layerOptions, refreshBuildings, compute, scheduleCanopy]);

  // opacité → simple maj de peinture
  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (map && map.getLayer(LYR)) map.setPaintProperty(LYR, "fill-opacity", Number(opacity));
  }, [opacity, mapRef]);

  // ré-échantillonne les bâtiments quand la carte bouge / le fond change
  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const onMove = () => { refreshBuildings(map, scopeBboxRef.current); compute(); if (treesRef.current) scheduleCanopy(); };
    const onStyle = () => { setTimeout(() => { refreshBuildings(map, scopeBboxRef.current); compute(); }, 300); };
    map.on("moveend", onMove);
    map.on("styledata", onStyle);
    return () => { map.off("moveend", onMove); map.off("styledata", onStyle); };
  }, [mapRef, refreshBuildings, compute, scheduleCanopy]);

  // lecture « toute la journée »
  useEffect(() => {
    if (!playing) { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } return; }
    playRef.current = setInterval(() => {
      setHour((h) => { const n = h + 0.25; return n > 21 ? 6 : n; });
    }, 220);
    return () => { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } };
  }, [playing]);

  // nettoyage à la fermeture du panneau
  useEffect(() => {
    return () => {
      const map = mapRef?.current?.getMap?.();
      if (playRef.current) clearInterval(playRef.current);
      clearTimeout(canopyTimer.current);
      try {
        if (map) {
          [LYR, CAN_LYR].forEach((id) => { if (map.getLayer(id)) map.removeLayer(id); });
          [SRC, CAN_SRC].forEach((id) => { if (map.getSource(id)) map.removeSource(id); });
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
          <p style={{ margin: 0 }}>Simule l'<b>ombre portée des bâtiments</b> au sol, à une <b>date</b> et une <b>heure</b> données, et la fait défiler <b>tout au long de la journée</b>.</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Sans téléchargement</div>
            <p style={{ margin: 0, color: C.mut }}>Les bâtiments proviennent des <b>tuiles vectorielles déjà chargées</b> par la carte (couche <Code>building</Code>, hauteur <Code>render_height</Code>) — rien n'est demandé au serveur. Zoomez sur une ville (niveau ~15+) pour qu'ils soient disponibles.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Modèle</div>
            <p style={{ margin: 0, color: C.mut }}>Position du soleil calculée pour le centre de la carte (algorithme SunCalc). L'ombre d'un bâtiment de hauteur <i>H</i> a pour longueur <i>H / tan(hauteur solaire)</i>, dans la direction opposée au soleil. Sol plat supposé.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Canopée (Meta ~1 m)</div>
            <p style={{ margin: 0, color: C.mut }}>Les arbres proviennent du modèle <b>WRI/Meta High Resolution Canopy Height 2020</b> (~1 m, via Earth Engine) : le serveur seuille la canopée (≥ 3 m) et la vectorise en zones portant leur <b>hauteur réelle moyenne</b>, projetées en ombre comme les bâtiments (teinte verte). Si Earth Engine est indisponible, repli sur les zones boisées des tuiles avec une hauteur de secours. Modèle simplifié : canopée opaque, sol plat.</p>
          </div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            À venir — relief, et itinéraires « plus ou moins ombragés ». Hypothèses : sol plat, hauteurs de bâtiment parfois manquantes (repli sur une hauteur par défaut), ombre au sol seulement, canopée opaque et non poreuse.
          </div>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1, minHeight: 0, overflowY: "auto", paddingRight: 4 }}>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
            Ombre portée des <b>bâtiments</b> et des <b>surfaces arborées</b> de la carte (aucun téléchargement). Fond <b>Liberty</b>, <b>vue 3D</b> et zoom bâtiments réglés automatiquement — <b>déplacez-vous sur une ville</b> et faites défiler l'heure.
          </div>

          <div>
            <div style={lbl}>Emprise du calcul</div>
            <select value={scope} onChange={(e) => changeScope(e.target.value)} style={{ ...inp, width: "100%" }}>
              <option value="view">Vue courante de la carte</option>
              {layerOptions.map((o) => <option key={o.id} value={o.id}>Emprise de la couche : {o.name}</option>)}
            </select>
            {scope !== "view" && <div style={{ fontFamily: F, fontSize: 10, color: C.dim, marginTop: 3 }}>Calcul restreint aux bâtiments dans l'emprise de la couche (la carte est recadrée dessus).</div>}
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
              <span>☀️ Soleil à <b>{info.alt.toFixed(0)}°</b> · ombre ≈ <b>{info.factor.toFixed(1)}×</b> la hauteur · <b>{info.nBld}</b> bâtiment(s){trees && info.nTree ? <> · <b style={{ color: "#2e7d4f" }}>{info.nTree}</b> arborée(s)</> : null}.</span>
            )}
            {info && !info.night && info.count === 0 && (
              <div style={{ color: C.dim, marginTop: 4 }}>Aucun bâtiment ici : déplacez la carte sur une ville et zoomez un peu (le fond Liberty et le zoom bâtiments sont déjà réglés).</div>
            )}
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Opacité · {Math.round(opacity * 100)}%</div>
              <input type="range" min={0.1} max={0.8} step={0.05} value={opacity} onChange={(e) => setOpacity(Number(e.target.value))} style={{ width: "100%" }} />
            </div>
            <div style={{ width: 110 }}>
              <div style={lbl}>Haut. défaut · m</div>
              <input type="number" min={2} max={200} value={defH} onChange={(e) => setDefH(Number(e.target.value))} style={{ ...inp, width: "100%" }} />
            </div>
          </div>
          <div style={{ fontFamily: F, fontSize: 10, color: C.dim }}>
            « Hauteur défaut » : appliquée aux bâtiments sans hauteur renseignée dans les tuiles.
          </div>

          <div style={{ display: "flex", alignItems: "flex-end", gap: 8, borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.txt, cursor: "pointer", flex: 1 }}>
              <input type="checkbox" checked={trees} onChange={(e) => setTrees(e.target.checked)} />
              🌳 Canopée <span style={{ color: C.dim }}>(Meta ~1 m)</span>
            </label>
            <div style={{ width: 120 }}>
              <div style={lbl}>Haut. si indispo · m</div>
              <input type="number" min={2} max={60} value={canopyH} disabled={!trees}
                onChange={(e) => setCanopyH(Number(e.target.value))} style={{ ...inp, width: "100%", opacity: trees ? 1 : 0.5 }} />
            </div>
          </div>
          {trees && (
            <div style={{ fontFamily: F, fontSize: 10.5 }}>
              {canopyMsg?.busy ? <span style={{ color: C.mut }}>⏳ Chargement de la canopée Meta…</span>
                : canopyMsg?.ok ? <span style={{ color: "#2e7d4f" }}>Canopée {String(canopyMsg.dataset || "").includes("Meta") ? "Meta ~1 m" : "ETH 10 m"} : <b>{canopyMsg.n}</b> zone(s), hauteurs réelles.</span>
                : canopyMsg?.err ? <span style={{ color: C.dim }}>Canopée Meta indisponible — repli sur les tuiles ({Number(canopyH)} m). {canopyMsg.err}</span>
                : <span style={{ color: C.dim }}>Hauteurs réelles du modèle WRI/Meta 2020 (~1 m). Ombre teintée en vert.</span>}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * IndicatorModal.jsx — Fenêtre simplifiée pour UN indicateur (piloté par menuTree).
 *
 * Réutilise /api/gee/tiles verrouillé sur 1 indice. Emprise au choix : vue carte,
 * échelle mondiale (globe entier, sans clip) OU une couche vectorielle importée
 * (ROI, ex. GeoJSON de Dakar) → clip GEE.
 * « Générer » ajoute la couche + la légende dans Couches. Rendu dans une
 * FloatingWindow : déplaçable, redimensionnable sur tous les bords, superposable
 * (plusieurs indicateurs ouverts en même temps, click = passe au-dessus).
 */
import { useState, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { INDICATORS } from "../utils/menuTree";
import { IcSatellite, IcTrendingUp, IcTriangle, IcCalendar, IcMap, IcGlobe, IcHexagon, IcInfo } from "../icons";
import { canAnimate } from "../utils/timelapse";
import FloatingWindow from "./FloatingWindow";
import TimeSeriesModal from "./TimeSeriesModal";
import TimeAnimTab from "./TimeAnimTab";
import IndicatorDoc from "./IndicatorDoc";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
// Emprise « monde entier » : couvre tout le globe pour que GEE garde TOUTES les
// images (filterBounds), pas seulement la partie visible. Lat plafonnée à ±85°
// (limite Web Mercator des tuiles raster ; au-delà les pôles ne se tuilent pas).
const WORLD_BBOX = [-179.9, -85, 179.9, 85];
const _iso = (d) => d.toISOString().slice(0, 10);
const _today = () => _iso(new Date());
const _ago = (m) => { const d = new Date(); d.setMonth(d.getMonth() - m); return _iso(d); };
const _thisMonth = () => _iso(new Date()).slice(0, 7);                 // "YYYY-MM"
const _thisYear = () => String(new Date().getFullYear());             // "YYYY"
// Bornes d'un mois "YYYY-MM" ou d'une année "YYYY" (dernier jour INCLUSIF —
// évite le point fantôme d'une fin exclusive ; GEE ne perd qu'1 jour, négligeable).
const _monthSpan = (ym) => { const [y, m] = ym.split("-").map(Number); const last = new Date(y, m, 0).getDate(); return [`${ym}-01`, `${ym}-${String(last).padStart(2, "0")}`]; };
const _yearSpan = (y) => [`${y}-01-01`, `${y}-12-31`];
// Libellé lisible de la période réellement envoyée à GEE.
const _periodLabel = (mode, ym, y, ds, de) => {
  if (mode === "month" && ym) {
    try { return new Date(ym + "-02").toLocaleDateString("fr-FR", { month: "long", year: "numeric" }) + " (mois entier)"; }
    catch { return ym + " (mois entier)"; }
  }
  if (mode === "year" && y) return `année ${y} (janv → déc)`;
  return `${ds} → ${de}`;
};
// Nb de points d'une série (mensuel/annuel) sur [ds, de[.
const _countSteps = (ds, de, agg) => {
  try {
    const a = new Date(ds), b = new Date(de);
    if (agg === "yearly") return Math.max(1, b.getFullYear() - a.getFullYear() + 1);
    return Math.max(1, (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth()) + 1);
  } catch { return 1; }
};

// bbox [w,s,e,n] à partir d'un GeoJSON (FeatureCollection / Feature / geometry)
function _bboxOf(gj) {
  let w = 180, s = 90, e = -180, n = -90, seen = false;
  const eat = (c) => { const x = c[0], y = c[1]; if (typeof x === "number") { seen = true; if (x < w) w = x; if (x > e) e = x; if (y < s) s = y; if (y > n) n = y; } };
  const walk = (a) => { if (!Array.isArray(a)) return; if (typeof a[0] === "number") eat(a); else a.forEach(walk); };
  const feats = gj?.features || (gj?.type === "Feature" ? [gj] : (gj?.type ? [{ geometry: gj }] : []));
  feats.forEach(f => walk(f?.geometry?.coordinates));
  return seen ? [w, s, e, n] : null;
}

// Géométrie polygonale exacte d'une couche → clip GEE sur le contour réel (pas
// la bbox). Fusionne les features Polygon/MultiPolygon en une GeometryCollection.
function _roiGeometry(gj) {
  if (!gj) return null;
  if (gj.type === "Polygon" || gj.type === "MultiPolygon" || gj.type === "GeometryCollection") return gj;
  const feats = gj.features || (gj.type === "Feature" ? [gj] : []);
  const geoms = feats.map(f => f?.geometry).filter(g => g && (g.type === "Polygon" || g.type === "MultiPolygon"));
  if (!geoms.length) return null;
  return geoms.length === 1 ? geoms[0] : { type: "GeometryCollection", geometries: geoms };
}

export default function IndicatorModal({ indKey, mapRef, layers = [], onAddRasterLayer, onClose, z, onFocus, initialPos, onAnimate }) {
  const C = useThemeContext();
  const ind = INDICATORS[indKey];
  const [optIdx, setOptIdx] = useState(0);
  const [dateStart, setDateStart] = useState(_ago(3));
  const [dateEnd, setDateEnd] = useState(_today());
  const [periodMode, setPeriodMode] = useState("range"); // range | month | year
  const [pMonth, setPMonth] = useState(_thisMonth());
  const [pYear, setPYear] = useState(_thisYear());
  const [cloud, setCloud] = useState(20);
  const [composite, setComposite] = useState("least_cloudy"); // moins nuageux | médiane | mosaïque
  const [agg, setAgg] = useState("monthly");  // série temporelle : mensuel | annuel
  const [roi, setRoi] = useState("map");     // "map" | "world" | id de couche
  const [tab, setTab] = useState("map");     // onglet : "map" (générer) | "analysis" (série/anomalie)
  const [classifyMode, setClassifyMode] = useState("quantile");  // quantile par défaut
  const [nClasses, setNClasses] = useState(5);
  const [zFactor, setZFactor] = useState(1);   // exagération verticale du relief (×1 = neutre)
  const [sunAz, setSunAz] = useState(315);     // azimut du soleil (ombrage)
  const [sunAlt, setSunAlt] = useState(45);    // hauteur du soleil (ombrage)
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(null);
  // Identifiant de la dernière couche générée : sert à la masquer pendant
  // l'animation. Une ref suffit — aucun rendu n'en dépend, et elle est lue
  // depuis un callback figé (onActiveChange de l'onglet Animation).
  const lastIdRef = useRef(null);
  const [ts, setTs] = useState(null);
  const [dates, setDates] = useState(null);   // null=non chargées ; [] ou liste des dates dispo
  const [datesBusy, setDatesBusy] = useState(false);

  const roiLayers = layers.filter(l => l.geojson?.features?.length);

  if (!ind) return null;
  const opt = ind.options[optIdx];
  const isStatic = !!opt.static;
  // Onglet Animation : seulement si le jeu a une dimension temporelle
  const animOk = !isStatic && canAnimate(opt.dataset);
  // Indices NON classifiables : RGB (multi-bandes) et couches catégorielles
  // (classes fixes → une classification quantile n'aurait aucun sens).
  const NO_CLASSIFY = ["RGB", "False Color (NIR)", "SWIR (feux)", "Occupation du sol", "Occupation du sol (DW)",
                       "Degré d'urbanisation", "Ombrage", "Couverture forêt 2000", "Perte forêt", "Gain forêt"];
  const canClassify = !NO_CLASSIFY.includes(opt.index);
  // Composite coloré multi-bandes : le masque nuages efface fumées et panaches,
  // soit l'information recherchée. Décoché possible, mais actif par défaut ici.
  const isComposite = ["RGB", "False Color (NIR)", "SWIR (feux)"].includes(opt.index);
  const [rawOverride, setRawOverride] = useState(null);   // null = suit l'indicateur
  const noCloudMask = rawOverride ?? (!!ind.rawClouds || isComposite);
  const isRelief = !!opt.relief;                          // élévation / ombrage / pente
  // Le soleil n'a de sens que si un ombrage est calculé (Ombrage, ou Élévation exagérée = relief ombré)
  const isHillshade = isRelief && (opt.index === "Ombrage" || opt.index === "Élévation");
  const isOptical = ["sentinel2", "landsat", "landsat8", "landsat9"].includes(opt.dataset);
  // Le contrôle « Nuages max » s'affiche aussi pour le MODIS vraie couleur (MCD43A4).
  // MODIS n'a pas de % de nuages par scène : le curseur reste à 100 (aucune image
  // écartée, même à >99 % de nuages), et sert de repère plutôt que de filtre.
  const showClouds = isOptical || ["modis_rgb", "modis_daily"].includes(opt.dataset);
  // Composite pertinent seulement pour les collections optiques/temporelles classiques
  // (5P/CHIRPS/ET/SMAP ont leur propre agrégation côté serveur).
  const showComposite = !isStatic && !["sentinel5p", "chirps", "modis_et", "smap"].includes(opt.dataset);
  const showDates = !isStatic && opt.dataset !== "sentinel5p"; // 5P n'a pas de collection unique côté /dates

  // bbox de la vue carte, bornée à des valeurs GEE valides (accepte l'échelle
  // mondiale : si dézoomé au-delà du monde, on retombe sur [-179.9,179.9]).
  const getMapBbox = () => {
    const map = mapRef?.current?.getMap?.(); if (!map) return null;
    try {
      const b = map.getBounds();
      let w = b.getWest(), s = b.getSouth(), e = b.getEast(), n = b.getNorth();
      w = Math.max(-180, Math.min(180, w)); e = Math.max(-180, Math.min(180, e));
      s = Math.max(-85, Math.min(85, s));   n = Math.max(-85, Math.min(85, n));
      if (e <= w) { w = -179.9; e = 179.9; }   // vue mondiale / franchit l'antiméridien
      if (n <= s) { s = -85; n = 85; }
      return [w, s, e, n];
    } catch (_) { return null; }
  };
  const body = () => {
    // dateStart/dateEnd sont la SOURCE UNIQUE : le mois/l'année les mettent à jour.
    const b = { dataset: opt.dataset, index: opt.index, date_start: dateStart, date_end: dateEnd,
                cloud_max: (showClouds && !noCloudMask) ? cloud : 100, composite, agg,
                no_cloud_mask: noCloudMask };
    if (isRelief) { b.z_factor = zFactor; if (isHillshade) { b.sun_azimuth = sunAz; b.sun_altitude = sunAlt; } }
    if (roi === "world") { b.bbox = WORLD_BBOX; return b; }  // globe entier, sans clip
    if (roi !== "map") {
      const layer = roiLayers.find(l => l.id === roi);
      const geom = layer && _roiGeometry(layer.geojson);
      if (geom) { b.roi_geojson = geom; b.bbox = layer.bbox || _bboxOf(layer.geojson); return b; }
    }
    b.bbox = getMapBbox();   // vue carte (accepte l'échelle mondiale si dézoomé)
    return b;
  };

  const _post = async (url, okMsg, onData, extra) => {
    const b = { ...body(), ...(extra || {}) };
    if (!b.bbox && !b.roi_geojson) { setStatus({ t: "error", m: "Emprise introuvable (cadrez la carte ou choisissez une couche)." }); return; }
    setBusy(true); setStatus({ t: "info", m: "Calcul GEE en cours…" });
    try {
      const res = await fetch(`${API}${url}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b) });
      const d = await res.json();
      if (!res.ok) throw new Error(d.detail || `Erreur ${res.status}`);
      onData(d, b);
      if (okMsg) setStatus({ t: "ok", m: okMsg });
    } catch (e) { setStatus({ t: "error", m: e.message }); }
    setBusy(false);
  };

  const generate = () => _post("/api/gee/tiles", "✓ Couche et légende ajoutées au menu Couches.", (d, b) => {
    const id = `gee_${opt.dataset}_${Date.now()}`;
    onAddRasterLayer?.({
      id,
      name: `${ind.title.split(" — ")[0].split(" (")[0]} · ${opt.label}`,
      tileUrl: d.tile_url, type: "wms", opacity: 0.85, visParams: d.vis_params,
      legend: d.legend || null, geeParams: b, bbox: d.clip_bbox || b.bbox,
    });
    // Mémorisée pour que l'onglet Animation puisse masquer cette tuile fixe
    // pendant la lecture, sinon elle transparaît sous les frames.
    lastIdRef.current = canAnimate(opt.dataset) ? id : null;
  }, canClassify
      ? (classifyMode === "none"
          ? { auto_stretch: true }                                  // rampe continue, min/max auto
          : { classify: classifyMode, n_classes: nClasses })        // classes + bornes min/max
      : undefined);                                                 // catégoriel / RGB : tel quel
  // Liste les dates d'images réellement disponibles (aide à cadrer la période)
  const fetchDates = async () => {
    const b = body();
    if (!b.bbox && !b.roi_geojson) { setStatus({ t: "error", m: "Emprise introuvable (cadrez la carte ou choisissez une couche)." }); return; }
    setDatesBusy(true); setDates(null);
    try {
      const res = await fetch(`${API}/api/gee/dates`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b) });
      const d = await res.json();
      if (!res.ok) throw new Error(d.detail || `Erreur ${res.status}`);
      setDates(d.dates || []);
      if (d.static) setStatus({ t: "info", m: "Dataset statique — aucune date à choisir." });
      else if (!d.dates?.length) setStatus({ t: "info", m: "Aucune image sur cette période/zone (élargissez ou augmentez les nuages)." });
    } catch (e) { setStatus({ t: "error", m: e.message }); }
    setDatesBusy(false);
  };
  const pickDate = (d) => {
    // Fenêtre d'1 jour : fin exclusive = lendemain (sinon filterDate(d,d) est vide).
    const nx = new Date(d); nx.setDate(nx.getDate() + 1);
    setPeriodMode("range"); setDateStart(d); setDateEnd(_iso(nx));
    setStatus({ t: "info", m: `Période cadrée sur le ${d}.` });
  };

  const runTimeseries = () => _post("/api/gee/index/timeseries", null, (d) => { setTs({ dataset: opt.dataset, index: opt.index, series: d.series || [], agg: d.agg || agg }); setStatus(null); });
  const runAnomaly = () => _post("/api/gee/index/anomaly", `✓ Anomalie ajoutée.`, (d, b) => {
    onAddRasterLayer?.({ id: `gee_anom_${Date.now()}`, name: `Anomalie ${opt.index}`, tileUrl: d.tile_url, type: "wms", opacity: 0.85, visParams: d.vis_params, bbox: b.bbox });
  });

  const statColor = { ok: C.acc, error: C.red, info: C.amb };
  const inp = { fontFamily: M, fontSize: 11, padding: "6px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" };

  return (
    <>
      <FloatingWindow
        title={ind.title} subtitle={ind.desc} icon={ind.icon ? <ind.icon size={18}/> : null}
        z={z} onFocus={onFocus} onClose={onClose}
        initialPos={initialPos} initialSize={{ w: 360, h: null }} minW={300} minH={220}
      >
          {/* Onglets. « Définition » est toujours présent, y compris sur un jeu
              statique où les autres onglets n'ont pas lieu d'être : c'est là qu'on
              lit ce que mesure l'indicateur, comment l'interpréter et ses limites.
              Le terme « métadonnées » est réservé au tableau technique interne —
              résolution, couverture, licence — dont c'est le sens exact. */}
          <div style={{ display: "flex", gap: 4, marginBottom: 12, borderBottom: `0.5px solid ${C.bdr}` }}>
            {[
              ["map", "Carte"],
              ...(animOk    ? [["anim", "Animation"]]  : []),
              ...(!isStatic ? [["analysis", "Analyse"]] : []),
              ["info", "Définition"],
            ].map(([k, l]) => (
              <button key={k} onClick={() => setTab(k)} title={l} style={{
                flex: 1, minWidth: 0,   // sans ça, 4 onglets débordent sous 340 px
                fontFamily: F, fontSize: 11.5, fontWeight: tab === k ? 600 : 400, padding: "6px 2px", cursor: "pointer",
                background: "transparent", border: "none", borderBottom: `2px solid ${tab === k ? C.acc : "transparent"}`,
                color: tab === k ? C.acc : C.dim, marginBottom: -0.5,
                display: "flex", alignItems: "center", justifyContent: "center", gap: 4,
              }}>
                {k === "info" && <IcInfo size={12} style={{ flexShrink: 0 }}/>}
                <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{l}</span>
              </button>
            ))}
          </div>

          {tab === "info" && (
            <IndicatorDoc indKey={indKey} dataset={opt.dataset} title={ind.title} />
          )}

          {tab === "map" && (<>
          {/* Satellite */}
          {ind.options.length > 1 && <div style={{ fontSize: 11, color: C.dim, marginBottom: 5 }}>Satellite</div>}
          <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap" }}>
            {ind.options.map((o, i) => (
              <button key={i} onClick={() => setOptIdx(i)} style={{
                flex: "1 1 30%", minWidth: 90, textAlign: "left", cursor: "pointer", borderRadius: 7, padding: "7px 9px",
                background: optIdx === i ? C.acc + "14" : "transparent", border: optIdx === i ? `2px solid ${C.acc}` : `0.5px solid ${C.bdr}`, position: "relative",
              }}>
                <div style={{ fontSize: 12, fontWeight: 500, color: C.txt }}>{o.label}</div>
                <div style={{ fontSize: 10, color: C.dim }}>{o.res}</div>
                {i === 0 && ind.options.length > 1 && <span style={{ position: "absolute", top: -8, right: 6, background: C.acc + "22", color: C.acc, fontSize: 9, padding: "1px 6px", borderRadius: 4 }}>conseillé</span>}
              </button>
            ))}
          </div>

          {/* Période : plage libre / 1 mois / 1 année (règle dateStart/dateEnd) */}
          {!isStatic && (() => {
            const applyMonth = (ym) => { if (!ym) return; setPMonth(ym); const [s, e] = _monthSpan(ym); setDateStart(s); setDateEnd(e); };
            const applyYear  = (y)  => { const yy = String(y); setPYear(yy); const [s, e] = _yearSpan(yy); setDateStart(s); setDateEnd(e); };
            const pickMode   = (k) => { setPeriodMode(k); if (k === "month") applyMonth(pMonth); else if (k === "year") applyYear(pYear); };
            return (
            <div style={{ marginBottom: 12 }}>
              <div style={{ display: "flex", gap: 4, marginBottom: 6 }}>
                {[["range", "Plage"], ["month", "Un mois"], ["year", "Une année"]].map(([k, l]) => (
                  <button key={k} onClick={() => pickMode(k)} title={k === "range" ? "Composite sur une plage de dates libre" : k === "month" ? "Carte d'un mois précis" : "Carte d'une année précise"} style={{
                    flex: 1, fontFamily: F, fontSize: 10.5, padding: "5px 0", borderRadius: 6, cursor: "pointer",
                    background: periodMode === k ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${periodMode === k ? C.acc + "66" : C.bdr}`,
                    color: periodMode === k ? C.acc : C.dim,
                  }}>{l}</button>
                ))}
              </div>
              {periodMode === "range" && (
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <input type="date" value={dateStart} max={dateEnd} onChange={e => setDateStart(e.target.value)} style={{ ...inp, flex: 1 }} />
                  <span style={{ color: C.dim }}>→</span>
                  <input type="date" value={dateEnd} min={dateStart} max={_today()} onChange={e => setDateEnd(e.target.value)} style={{ ...inp, flex: 1 }} />
                </div>
              )}
              {periodMode === "month" && (
                <input type="month" value={pMonth} max={_thisMonth()} onChange={e => applyMonth(e.target.value)} style={{ ...inp, width: "100%" }} />
              )}
              {periodMode === "year" && (
                <input type="number" value={pYear} min={2000} max={_thisYear()} onChange={e => applyYear(e.target.value)} style={{ ...inp, width: "100%" }} />
              )}
              {/* Période EFFECTIVE réellement envoyée à GEE */}
              <div style={{ fontSize: 9.5, color: C.acc, marginTop: 5, background: C.acc + "10", border: `0.5px solid ${C.acc}33`, borderRadius: 5, padding: "3px 7px" }}>
                Période analysée : <strong>{_periodLabel(periodMode, pMonth, pYear, dateStart, dateEnd)}</strong>
              </div>
            </div>
          ); })()}

          {/* Nuages */}
          {showClouds && (<>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8, opacity: noCloudMask ? 0.4 : 1 }}>
              <span style={{ fontSize: 11, color: C.dim, flexShrink: 0 }}>Nuages max</span>
              <input type="range" min="0" max="100" step="5" value={cloud} disabled={noCloudMask}
                onChange={e => setCloud(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
              <span style={{ fontFamily: M, fontSize: 11, color: C.txt, width: 34, textAlign: "right" }}>{noCloudMask ? "100" : cloud} %</span>
            </div>
            <label style={{ display: "flex", alignItems: "flex-start", gap: 7, marginBottom: 12, cursor: "pointer" }}>
              <input type="checkbox" checked={noCloudMask} onChange={e => setRawOverride(e.target.checked)}
                style={{ accentColor: C.acc, marginTop: 2, cursor: "pointer" }} />
              <span>
                <span style={{ fontSize: 11, color: C.txt }}>Image brute — sans masque nuages</span>
                <span style={{ display: "block", fontSize: 9.5, color: C.dim, lineHeight: 1.45, marginTop: 1 }}>
                  Conserve fumées, panaches et scènes couvertes. Indispensable pour observer
                  un incendie ou un changement soudain, que le masque effacerait.
                </span>
              </span>
            </label>
          </>)}

          {/* Composite sur la période : moins nuageux / médiane / mosaïque */}
          {showComposite && (
            <div style={{ marginBottom: 12 }}>
              <div style={{ fontSize: 11, color: C.dim, marginBottom: 5 }}>Composite sur la période</div>
              <div style={{ display: "flex", gap: 4 }}>
                {[["least_cloudy", "Moins nuageux"], ["median", "Médiane"], ["mosaic", "Mosaïque"]].map(([k, l]) => (
                  <button key={k} onClick={() => setComposite(k)} title={
                    k === "least_cloudy" ? "Image la moins nuageuse de la période"
                    : k === "median" ? "Médiane pixel par pixel (lisse nuages/artefacts)"
                    : "Assemblage des images les plus récentes"
                  } style={{
                    flex: 1, fontFamily: F, fontSize: 10.5, padding: "6px 4px", borderRadius: 6, cursor: "pointer",
                    background: composite === k ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${composite === k ? C.acc + "66" : C.bdr}`,
                    color: composite === k ? C.acc : C.dim,
                  }}>{l}</button>
                ))}
              </div>
            </div>
          )}

          {/* Dates réellement disponibles */}
          {showDates && (
            <div style={{ marginBottom: 14 }}>
              <button onClick={fetchDates} disabled={datesBusy || busy} style={{
                width: "100%", fontFamily: F, fontSize: 11, padding: "7px 0", borderRadius: 6, cursor: datesBusy ? "default" : "pointer",
                background: "transparent", border: `0.5px dashed ${C.bdr}`, color: C.dim,
                display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
              }}><IcCalendar size={13}/> {datesBusy ? "Recherche des dates…" : "Voir les dates disponibles"}</button>
              {Array.isArray(dates) && dates.length > 0 && (
                <div style={{ marginTop: 6 }}>
                  <div style={{ fontSize: 9, color: C.dim, marginBottom: 4 }}>{dates.length} date(s) — cliquez pour cadrer la période dessus :</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4, maxHeight: 92, overflowY: "auto" }}>
                    {dates.map(d => (
                      <button key={d} onClick={() => pickDate(d)} style={{
                        fontFamily: M, fontSize: 9.5, padding: "2px 6px", borderRadius: 4, cursor: "pointer",
                        background: (dateStart === d && dateEnd === d) ? C.acc + "22" : C.input,
                        border: `0.5px solid ${(dateStart === d && dateEnd === d) ? C.acc : C.bdr}`,
                        color: (dateStart === d && dateEnd === d) ? C.acc : C.txt,
                      }}>{d}</button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Relief : exagération verticale + position du soleil (ombrage/pente) */}
          {isRelief && (
            <div style={{ marginBottom: 14, background: C.acc + "0c", border: `0.5px solid ${C.acc}22`, borderRadius: 8, padding: "9px 10px" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: isHillshade ? 8 : 0 }}>
                <span style={{ fontSize: 11, color: C.dim, flexShrink: 0, width: 82 }}>Exagération</span>
                <input type="range" min="1" max="8" step="0.5" value={zFactor} onChange={e => setZFactor(parseFloat(e.target.value))} style={{ flex: 1, height: 3 }} />
                <span style={{ fontFamily: M, fontSize: 11, color: C.txt, width: 32, textAlign: "right" }}>×{zFactor}</span>
              </div>
              {isHillshade && (
                <>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                    <span style={{ fontSize: 11, color: C.dim, flexShrink: 0, width: 82 }}>Azimut soleil</span>
                    <input type="range" min="0" max="360" step="5" value={sunAz} onChange={e => setSunAz(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
                    <span style={{ fontFamily: M, fontSize: 11, color: C.txt, width: 32, textAlign: "right" }}>{sunAz}°</span>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <span style={{ fontSize: 11, color: C.dim, flexShrink: 0, width: 82 }}>Hauteur soleil</span>
                    <input type="range" min="5" max="90" step="5" value={sunAlt} onChange={e => setSunAlt(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
                    <span style={{ fontFamily: M, fontSize: 11, color: C.txt, width: 32, textAlign: "right" }}>{sunAlt}°</span>
                  </div>
                </>
              )}
              <div style={{ fontSize: 9, color: C.dim, marginTop: 6, lineHeight: 1.4 }}>
                {opt.index === "Élévation"
                  ? <>×1 = altitude simple. <strong style={{ color: C.mut }}>Au-delà de ×1</strong> : relief ombré (couleurs d'altitude + ombres) — monte / baisse le modelé.</>
                  : <>Exagération = monter / baisser le relief modélisé.</>}
                {isHillshade ? " Soleil = direction et hauteur de la lumière." : ""}
              </div>
            </div>
          )}

          {/* Rendu : classification (quantiles par défaut) + nb de classes */}
          {canClassify && (
            <div style={{ marginBottom: 14 }}>
              <div style={{ fontSize: 11, color: C.dim, marginBottom: 5 }}>Rendu</div>
              <div style={{ display: "flex", gap: 4 }}>
                {[["quantile", "Quantiles"], ["jenks", "Jenks"], ["equal", "Ég. int."], ["none", "Continu"]].map(([k, l]) => (
                  <button key={k} onClick={() => setClassifyMode(k)} title={
                    k === "quantile" ? "Effectifs égaux par classe (défaut)"
                    : k === "jenks" ? "Ruptures naturelles (Fisher-Jenks)"
                    : k === "equal" ? "Intervalles de largeur égale"
                    : "Rampe continue (min/max automatiques)"
                  } style={{
                    flex: 1, fontFamily: F, fontSize: 9.5, padding: "5px 0", borderRadius: 5, cursor: "pointer",
                    background: classifyMode === k ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${classifyMode === k ? C.acc + "55" : C.bdr}`,
                    color: classifyMode === k ? C.acc : C.dim,
                  }}>{l}</button>
                ))}
              </div>
              {classifyMode !== "none" && (
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
                  <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>Classes</span>
                  <input type="range" min="3" max="9" step="1" value={nClasses}
                    onChange={e => setNClasses(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
                  <span style={{ fontFamily: M, fontSize: 11, color: C.txt, width: 16, textAlign: "right" }}>{nClasses}</span>
                </div>
              )}
              <div style={{ fontSize: 9, color: C.dim, marginTop: 4 }}>
                {classifyMode === "none"
                  ? "Rampe continue, bornes min/max calculées sur l'emprise."
                  : "Légende en classes avec les valeurs min/max de chaque intervalle."}
              </div>
            </div>
          )}

          {/* Emprise : boutons segmentés (Vue carte / Zone mondiale / Couche) */}
          {(() => {
            const isLayerRoi = roi !== "map" && roi !== "world";
            const embBtn = (active, onClick, Ic, label) => (
              <button onClick={onClick} style={{
                flex: 1, fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, cursor: "pointer",
                background: active ? C.acc + "18" : "transparent",
                border: `0.5px solid ${active ? C.acc + "55" : C.bdr}`, color: active ? C.acc : C.dim,
                display: "flex", alignItems: "center", justifyContent: "center", gap: 5, minWidth: 0,
              }}><Ic size={12} /> <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{label}</span></button>
            );
            return (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 11, color: C.dim, marginBottom: 5 }}>Emprise</div>
                <div style={{ display: "flex", gap: 5 }}>
                  {embBtn(roi === "map", () => setRoi("map"), IcMap, "Vue carte")}
                  {embBtn(roi === "world", () => setRoi("world"), IcGlobe, "Zone mondiale")}
                  {roiLayers.length > 0 && embBtn(isLayerRoi, () => setRoi(roiLayers.find(l => l.id === roi)?.id || roiLayers[0].id), IcHexagon, "Couche")}
                </div>
                {isLayerRoi && roiLayers.length > 1 && (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 5 }}>
                    {roiLayers.map(l => (
                      <button key={l.id} onClick={() => setRoi(l.id)} style={{
                        fontFamily: F, fontSize: 9.5, padding: "3px 8px", borderRadius: 5, cursor: "pointer",
                        background: roi === l.id ? C.acc + "1e" : C.input,
                        border: `0.5px solid ${roi === l.id ? C.acc : C.bdr}`, color: roi === l.id ? C.acc : C.txt,
                        maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                      }}>{l.name}</button>
                    ))}
                  </div>
                )}
                {roi === "world" && <div style={{ fontSize: 9, color: C.dim, marginTop: 4 }}>Tout le globe est calculé, y compris la partie non visible. Tuiles raster limitées à ±85° (Web Mercator) : les pôles ne s'affichent pas.</div>}
                {isLayerRoi && <div style={{ fontSize: 9, color: C.dim, marginTop: 4 }}>L'indice sera découpé sur l'emprise de la couche.</div>}
                {roi === "map" && <div style={{ fontSize: 9, color: C.dim, marginTop: 4 }}>Zoomez sur la zone souhaitée avant de générer.</div>}
              </div>
            );
          })()}

          {/* Générer */}
          <button onClick={generate} disabled={busy} style={{
            width: "100%", fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "9px 0", borderRadius: 8, border: "none",
            background: busy ? C.hover : C.acc, color: busy ? C.dim : "#fff", cursor: busy ? "default" : "pointer",
            display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
          }}><IcSatellite size={15}/> {busy ? "Calcul…" : `Générer ${ind.title.split(" — ")[0].split(" (")[0]}`}</button>
          </>)}

          {/* ── Animation temporelle : les tuiles défilent sur la carte ── */}
          {animOk && tab === "anim" && (
            <TimeAnimTab
              geeParams={body()}
              animId={`anim_${indKey}`}   // stable : ne doit pas changer en cours d'animation
              opacity={0.85}
              mapRef={mapRef}
              // Masque la tuile fixe générée pendant l'animation, sinon elle
              // transparaît sous les frames et brouille les couleurs.
              onActiveChange={(on) => onAnimate?.(on ? lastIdRef.current : null)}
              isOptical={isOptical} showComposite={showComposite}
              sourceLabel={opt.label}
            />
          )}

          {!isStatic && tab === "analysis" && (
            <>
              <div style={{ fontSize: 9.5, color: C.dim, marginBottom: 10, background: C.acc + "0c", border: `0.5px solid ${C.acc}22`, borderRadius: 6, padding: "6px 8px" }}>
                Analyse sur la <strong style={{ color: C.mut }}>période</strong> et l'<strong style={{ color: C.mut }}>emprise</strong> réglées dans l'onglet <em>Carte</em> ({_periodLabel(periodMode, pMonth, pYear, dateStart, dateEnd)}).
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 2 }}>
                <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>Série par</span>
                {[["monthly", "Mois"], ["yearly", "Année"]].map(([k, l]) => (
                  <button key={k} onClick={() => setAgg(k)} style={{
                    flex: 1, fontFamily: F, fontSize: 10.5, padding: "5px 0", borderRadius: 6, cursor: "pointer",
                    background: agg === k ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${agg === k ? C.acc + "66" : C.bdr}`,
                    color: agg === k ? C.acc : C.dim,
                  }}>{l}</button>
                ))}
              </div>
              <div style={{ fontSize: 9, color: C.dim, marginTop: 4 }}>
                La série découpe la période analysée en <strong style={{ color: C.mut }}>≈ {_countSteps(dateStart, dateEnd, agg)}</strong> point{_countSteps(dateStart, dateEnd, agg) > 1 ? "s" : ""} ({agg === "yearly" ? "annuels" : "mensuels"}). Pour une tendance, choisissez une <em>plage</em> large.
              </div>
              <div style={{ display: "flex", gap: 6, marginTop: 6 }}>
                <button onClick={runTimeseries} disabled={busy} style={{ flex: 1, fontFamily: F, fontSize: 11, padding: "7px 0", borderRadius: 6, background: "transparent", border: `0.5px solid ${C.acc}55`, color: C.acc, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}><IcTrendingUp size={13}/> Série temporelle</button>
                <button onClick={runAnomaly} disabled={busy} style={{ flex: 1, fontFamily: F, fontSize: 11, padding: "7px 0", borderRadius: 6, background: "transparent", border: `0.5px solid ${C.acc}55`, color: C.acc, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}><IcTriangle size={13}/> Anomalie</button>
              </div>
            </>
          )}

          {status && (
            <div style={{ marginTop: 10, fontSize: 11, padding: "6px 9px", borderRadius: 6, lineHeight: 1.5, background: statColor[status.t] + "15", border: `0.5px solid ${statColor[status.t]}44`, color: statColor[status.t] }}>
              {status.m}
            </div>
          )}
      </FloatingWindow>

      {ts && <TimeSeriesModal dataset={ts.dataset} index={ts.index} series={ts.series} agg={ts.agg} onClose={() => setTs(null)} />}
    </>
  );
}

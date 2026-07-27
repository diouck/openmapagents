/**
 * BurnSeverityPanel.jsx — Sévérité d'incendie par dNBR.
 *
 * Méthodologie UN-SPIDER / USGS, vérifiée contre l'implémentation de référence
 * (github.com/UN-SPIDER/burn-severity-mapping-EO) :
 *   NBR  = (B8A − B12) / (B8A + B12)      [B8A, la bande PIR étroite à 20 m]
 *   dNBR = NBR_avant − NBR_après          [positif = brûlé]
 *   seuils 0,1 / 0,27 / 0,44 / 0,66 appliqués au ratio brut, sans ×1000.
 *
 * Onglets : « Calcul » porte TOUTE la configuration — emprise (vue, vecteur ou
 * couche) et les deux fenêtres de dates ; « SWIR avant » / « SWIR après »
 * affichent les composites d'inspection encadrant l'incendie ; « Définition »
 * suit la charte des fiches indicateurs. La saisie des dates reste sur le seul
 * onglet Calcul : dispersée, on ne savait plus laquelle on réglait.
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { Sel, Lbl } from "./ui";
import {
  IcFlame, IcAlert, IcCheck, IcChevronDown, IcChevronRight,
  IcImage, IcInfo, IcExternalLink, IcCalendar, IcRefreshCw,
} from "../icons";
import { SOURCE_META, geeCatalog } from "../utils/datasetMeta";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const DEFAULT_THR   = [-0.1, 0.1, 0.27, 0.44, 0.66];          // 6 classes
const DEFAULT_THR_7 = [-0.251, -0.101, 0.1, 0.27, 0.44, 0.66]; // table USGS intégrale

// Miroir des classes du backend, pour montrer la grille de lecture AVANT calcul.
const CLASSES = [
  ["Repousse / rehaussement", "#7a8b3c"], ["Non brûlé", "#4bd44b"],
  ["Sévérité faible", "#ffff3f"], ["Faible à modérée", "#ff9a1f"],
  ["Modérée à forte", "#f01e1e"], ["Sévérité forte", "#e01ee0"],
];
const CLASSES_7 = [
  ["Repousse forte", "#6e7b36"], ["Repousse faible", "#a5c249"], ["Non brûlé", "#4bd44b"],
  ["Sévérité faible", "#ffff3f"], ["Faible à modérée", "#ff9a1f"],
  ["Modérée à forte", "#f01e1e"], ["Sévérité forte", "#e01ee0"],
];
const rangeOf = (thr, i, n) =>
  i === 0 ? `< ${thr[0].toFixed(3)}`
  : i === n - 1 ? `≥ ${thr[n - 2].toFixed(3)}`
  : `${thr[i - 1].toFixed(3)} – ${thr[i].toFixed(3)}`;

const iso = d => d.toISOString().slice(0, 10);
const shift = (d, n) => { const x = new Date(d); x.setDate(x.getDate() + n); return x; };
const WIN = { pre: "avant", post: "après" };

// Emprise d'un GeoJSON, quand la couche n'a pas de bbox mémorisée.
const geoBbox = (gj) => {
  let minX = 180, minY = 90, maxX = -180, maxY = -90, seen = false;
  const walk = (a) => {
    if (typeof a[0] === "number") {
      const [x, y] = a;
      if (x < minX) minX = x; if (x > maxX) maxX = x;
      if (y < minY) minY = y; if (y > maxY) maxY = y; seen = true;
    } else a.forEach(walk);
  };
  (gj?.features || []).forEach(f => f?.geometry?.coordinates && walk(f.geometry.coordinates));
  return seen ? [minX, minY, maxX, maxY] : null;
};
const isPolygonal = (gj) => (gj?.features || []).some(f => /Polygon/.test(f?.geometry?.type || ""));

export default function BurnSeverityPanel({ layers, mapRef, onAddRasterLayer, onAddLayer }) {
  const C = useThemeContext();
  const today = new Date();

  const [tab, setTab] = useState("params");   // params | pre | post | info
  const [dataset, setDataset] = useState("sentinel2");
  const [preStart,  setPreStart]  = useState(iso(shift(today, -45)));
  const [preEnd,    setPreEnd]    = useState(iso(shift(today, -30)));
  const [postStart, setPostStart] = useState(iso(shift(today, -15)));
  const [postEnd,   setPostEnd]   = useState(iso(today));
  const [cloud, setCloud] = useState(40);
  const [roi, setRoi] = useState("map");
  const [split, setSplit] = useState(false);
  const [thr, setThr] = useState(null);   // null = seuils de référence du mode courant
  const [openThr, setOpenThr] = useState(false);
  const [vectorize, setVectorize] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [res, setRes] = useState(null);
  const [avail, setAvail] = useState({ pre: null, post: null });  // {busy|dates|err}

  const refThr = split ? DEFAULT_THR_7 : DEFAULT_THR;
  const curThr = thr || refThr;
  const classes = split ? CLASSES_7 : CLASSES;

  const allLayers = layers || [];
  const vectorLayers = allLayers.filter(l => !l.isRaster && l.geojson);
  const rasterLayers = allLayers.filter(l => l.isRaster && l.bbox);
  const win = k => k === "pre" ? [preStart, preEnd] : [postStart, postEnd];

  // Un « après » qui commence avant la fin de l'« avant » mélange les deux états.
  const overlap = postStart < preEnd;
  const badRange = preEnd <= preStart || postEnd <= postStart;

  const bbox = () => {
    const m = mapRef?.current?.getMap?.(); if (!m) return null;
    try {
      const b = m.getBounds();
      let w = Math.max(-180, b.getWest()), e = Math.min(180, b.getEast());
      let s = Math.max(-85, b.getSouth()), n = Math.min(85, b.getNorth());
      if (e <= w) { w = -179.9; e = 179.9; }
      return [w, s, e, n];
    } catch (_) { return null; }
  };

  /** Emprise choisie → { bbox, roi_geojson? }. Vecteur polygonal : clip exact sur
      TOUTES ses entités. Autre couche : sa bbox. Vue carte : cadrage courant. */
  const resolveRoi = () => {
    if (roi !== "map") {
      const l = allLayers.find(x => x.id === roi);
      if (l && !l.isRaster && l.geojson) {
        const bb = l.bbox || geoBbox(l.geojson) || bbox();
        return isPolygonal(l.geojson) ? { bbox: bb, roi_geojson: l.geojson } : { bbox: bb };
      }
      if (l?.bbox) return { bbox: l.bbox };
    }
    return { bbox: bbox() };
  };

  const body = () => {
    const { bbox: bb, roi_geojson } = resolveRoi();
    const b = { dataset, pre_start: preStart, pre_end: preEnd,
                post_start: postStart, post_end: postEnd,
                cloud_max: cloud, thresholds: curThr, vectorize, split_regrowth: split, bbox: bb };
    if (roi_geojson) b.roi_geojson = roi_geojson;
    return b;
  };

  /** Dates réellement disponibles sur une fenêtre, à l'emprise et au filtre
      nuages courants — la réponse à « pourquoi ça ne marche pas ». */
  const checkDates = async (which) => {
    const [d0, d1] = win(which);
    const { bbox: bb, roi_geojson } = resolveRoi();
    setAvail(a => ({ ...a, [which]: { busy: true } }));
    try {
      const r = await fetch(`${API}/api/gee/dates`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataset, date_start: d0, date_end: d1,
                               cloud_max: cloud, bbox: bb, roi_geojson }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `Erreur ${r.status}`);
      const dates = d.dates || [];
      setAvail(a => ({ ...a, [which]: { dates } }));
      return dates;
    } catch (e) {
      setAvail(a => ({ ...a, [which]: { err: e.message } }));
      return null;
    }
  };

  const run = async () => {
    setErr(null); setRes(null);
    const b = body();
    if (!b.bbox && !b.roi_geojson) return setErr("Emprise introuvable — cadrez la carte ou choisissez une couche.");
    if (badRange) return setErr("Chaque fenêtre doit se terminer après sa date de début.");
    setBusy(true);
    try {
      const r = await fetch(`${API}/api/gee/burn-severity`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `Erreur ${r.status}`);
      setRes(d);

      onAddRasterLayer?.({
        id: `burn_${Date.now()}`, name: `Sévérité incendie · dNBR`,
        tileUrl: d.tile_url, type: "wms", opacity: 0.85,
        visParams: d.vis_params, legend: d.legend, bbox: b.bbox,
      });
      if (d.perimeter) {
        onAddLayer?.({ type: "FeatureCollection",
          features: [{ type: "Feature", properties: { surface_ha: d.burned_ha }, geometry: d.perimeter }] },
          "Périmètre incendie", "analysis");
      }
    } catch (e) {
      setErr(e.message);
      // Échec faute d'images : identifier tout de suite LAQUELLE des deux
      // fenêtres est vide et l'annoncer, plutôt que de laisser tâtonner.
      if (/image/i.test(e.message)) {
        const [p, q] = await Promise.all([checkDates("pre"), checkDates("post")]);
        const vide = [!p?.length && "avant", !q?.length && "après"].filter(Boolean);
        if (vide.length) setErr(`Aucune image disponible ${vide.join(" ni ")} sur cette zone. ` +
                                `Élargissez la fenêtre concernée ou relevez le filtre nuages.`);
      }
    }
    setBusy(false);
  };

  /** Composites SWIR encadrant l'incendie — masque nuages DÉSACTIVÉ ici : la
      fumée et les panaches sont l'information, pas du bruit. */
  const addSwir = async (which) => {
    setErr(null); setBusy(true);
    const { bbox: bb, roi_geojson } = resolveRoi();
    const [d0, d1] = win(which);
    try {
      const r = await fetch(`${API}/api/gee/tiles`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataset, index: "SWIR (feux)", date_start: d0, date_end: d1,
                               cloud_max: 100, no_cloud_mask: true, composite: "median",
                               bbox: bb, roi_geojson }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `Erreur ${r.status}`);
      onAddRasterLayer?.({ id: `swir_${which}_${Date.now()}`,
        name: `SWIR ${WIN[which]} · ${d0}`,
        tileUrl: d.tile_url, type: "wms", opacity: 1, visParams: d.vis_params, bbox: bb });
    } catch (e) {
      setErr(`Composite SWIR ${WIN[which]} indisponible — ${e.message}`);
      if (/image|aucune/i.test(e.message)) checkDates(which);
    }
    setBusy(false);
  };

  const addDnbr = () => res?.dnbr_tile_url && onAddRasterLayer?.({
    id: `dnbr_${Date.now()}`, name: "dNBR (continu)", tileUrl: res.dnbr_tile_url,
    type: "wms", opacity: 0.85,
    visParams: { min: -0.5, max: 1, palette: ["#2b83ba", "#ffffbf", "#fdae61", "#d7191c"] },
  });

  // ── Styles partagés ──────────────────────────────────────────
  const box = { background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`,
                display: "flex", flexDirection: "column", gap: 8 };
  const dat = { fontFamily: M, fontSize: 10.5, padding: "5px 7px", borderRadius: 5, width: "100%",
                background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none",
                boxSizing: "border-box" };
  const ghost = { fontFamily: F, fontSize: 10, padding: "6px 0", borderRadius: 6, cursor: "pointer",
                  background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.mut,
                  display: "flex", alignItems: "center", justifyContent: "center", gap: 5 };
  const h = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 4 };
  const p = { fontSize: 11, color: C.mut, lineHeight: 1.6, margin: 0 };

  /** Pastille d'état d'une fenêtre : muette tant qu'aucune vérification n'a eu lieu. */
  const dot = (k) => {
    const a = avail[k];
    if (!a || a.busy) return null;
    const ko = a.err || !a.dates?.length;
    return <span style={{ width: 5, height: 5, borderRadius: 5, flexShrink: 0,
                          background: ko ? C.red : C.acc }} />;
  };

  // ── Bloc d'une fenêtre de dates (onglet Calcul) ──────────────
  // Fonction et non composant : un composant redéfini ici serait remonté à
  // chaque frappe et les champs date perdraient le focus.
  const dateWindow = (k) => {
    const [d0, d1] = win(k);
    const set0 = k === "pre" ? setPreStart : setPostStart;
    const set1 = k === "pre" ? setPreEnd   : setPostEnd;
    const a = avail[k];
    const empty = a && !a.busy && !a.err && !a.dates?.length;

    return (
      <div style={box} key={k}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <IcCalendar size={12} style={{ color: C.dim, flexShrink: 0 }} />
          <span style={{ fontSize: 10.5, fontWeight: 500, color: C.txt, flex: 1 }}>
            {k === "pre" ? "Avant l'incendie" : "Après l'incendie"}
          </span>
          {dot(k)}
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          <input type="date" value={d0} onChange={e => set0(e.target.value)} style={dat} />
          <input type="date" value={d1} onChange={e => set1(e.target.value)} style={dat} />
        </div>
        {d1 <= d0 && (
          <div style={{ fontSize: 9.5, color: C.red }}>La fin doit être postérieure au début.</div>
        )}

        <button onClick={() => checkDates(k)} disabled={a?.busy || d1 <= d0}
          style={{ ...ghost, cursor: a?.busy ? "default" : "pointer" }}>
          <IcRefreshCw size={11} /> {a?.busy ? "Interrogation…" : "Images disponibles ?"}
        </button>

        {a?.err && (
          <div style={{ display: "flex", gap: 6, fontSize: 9.5, color: C.red }}>
            <IcAlert size={11} style={{ flexShrink: 0, marginTop: 1 }} /> {a.err}
          </div>
        )}
        {empty && (
          <div style={{ display: "flex", gap: 6, fontSize: 10, color: C.red,
                        background: C.red + "12", borderRadius: 6, padding: "6px 8px" }}>
            <IcAlert size={11} style={{ flexShrink: 0, marginTop: 1 }} />
            <span>Aucune image {WIN[k]} à {cloud} % de nuages sur cette zone. Élargissez la fenêtre ou relevez le filtre.</span>
          </div>
        )}
        {a?.dates?.length > 0 && (
          <>
            <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: C.acc }}>
              <IcCheck size={11} /> {a.dates.length} date{a.dates.length > 1 ? "s" : ""} exploitable{a.dates.length > 1 ? "s" : ""}
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
              {a.dates.map(d => (
                <button key={d} onClick={() => { set0(d); set1(iso(shift(new Date(d), 1))); }}
                  title={`Cadrer la fenêtre sur le ${d}`} style={{
                    fontFamily: M, fontSize: 9, padding: "2px 5px", borderRadius: 4, cursor: "pointer",
                    background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.mut,
                  }}>{d}</button>
              ))}
            </div>
          </>
        )}
        <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
          {k === "pre"
            ? <>Une fenêtre de quelques jours, non une date unique. Gardez la <b style={{ color: C.mut }}>même
                saison</b> que la fenêtre « après ».</>
            : <>Au plus près de l'extinction : plus on attend, plus la repousse atténue le signal.</>}
        </div>
      </div>
    );
  };

  // ── Onglet SWIR (avant / après) ──────────────────────────────
  const swirView = (k) => {
    const [d0, d1] = win(k);
    const a = avail[k];
    return (
      <>
        <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
          Composite <b style={{ color: C.mut }}>SWIR / PIR / rouge</b> {WIN[k]} l'incendie,
          <b style={{ color: C.mut }}> sans masque nuages</b> : à l'inverse du calcul dNBR, on veut
          voir la fumée, les fronts actifs et les cicatrices. Vue d'inspection, pas une mesure.
        </div>

        <div style={box}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <IcCalendar size={12} style={{ color: C.dim, flexShrink: 0 }} />
            <span style={{ fontSize: 10.5, fontWeight: 500, color: C.txt, flex: 1 }}>Fenêtre {WIN[k]}</span>
            {dot(k)}
          </div>
          <div style={{ fontFamily: M, fontSize: 11, color: C.txt }}>{d0} → {d1}</div>
          <div style={{ fontSize: 9.5, color: C.dim }}>
            Les dates et l'emprise se règlent dans l'onglet <b style={{ color: C.mut }}>Calcul</b>.
          </div>

          <button onClick={() => checkDates(k)} disabled={a?.busy || d1 <= d0}
            style={{ ...ghost, cursor: a?.busy ? "default" : "pointer" }}>
            <IcRefreshCw size={11} /> {a?.busy ? "Interrogation…" : "Images disponibles ?"}
          </button>
          {a?.err && <div style={{ fontSize: 9.5, color: C.red }}>{a.err}</div>}
          {a && !a.busy && !a.err && (
            a.dates?.length
              ? <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: C.acc }}>
                  <IcCheck size={11} /> {a.dates.length} date{a.dates.length > 1 ? "s" : ""} sur la période
                </div>
              : <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10, color: C.red }}>
                  <IcAlert size={11} /> aucune image {WIN[k]} à {cloud} % de nuages
                </div>
          )}
        </div>

        <button onClick={() => addSwir(k)} disabled={busy || d1 <= d0} style={{
          fontFamily: F, fontSize: 11, fontWeight: 600, padding: "9px 0", borderRadius: 7,
          background: busy ? C.hover : C.acc, color: busy ? C.dim : "#fff", border: "none",
          cursor: busy ? "default" : "pointer",
          display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
        }}>
          <IcImage size={13} /> {busy ? "…" : `Afficher le composite SWIR ${WIN[k]}`}
        </button>

        {err && (
          <div style={{ display: "flex", gap: 6, fontSize: 10, color: C.red, background: C.red + "12",
                        borderRadius: 6, padding: "6px 8px" }}>
            <IcAlert size={11} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
          </div>
        )}
      </>
    );
  };

  // ── Rendu ────────────────────────────────────────────────────
  const src = SOURCE_META[dataset];
  const cat = src && geeCatalog(src.asset);

  return (
    <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto" }}>

      {/* Onglets — « Définition » en dernier, précédé des deux composites SWIR. */}
      <div style={{ display: "flex", gap: 4, borderBottom: `0.5px solid ${C.bdr}` }}>
        {[["params", "Calcul"], ["pre", "SWIR avant"], ["post", "SWIR après"], ["info", "Définition"]]
          .map(([k, l]) => (
            <button key={k} onClick={() => setTab(k)} title={l} style={{
              flex: 1, minWidth: 0,
              fontFamily: F, fontSize: 11, fontWeight: tab === k ? 600 : 400,
              padding: "6px 2px", cursor: "pointer", background: "transparent", border: "none",
              borderBottom: `2px solid ${tab === k ? C.acc : "transparent"}`,
              color: tab === k ? C.acc : C.dim, marginBottom: -0.5,
              display: "flex", alignItems: "center", justifyContent: "center", gap: 3,
            }}>
              {k === "info" && <IcInfo size={11} style={{ flexShrink: 0 }} />}
              {(k === "pre" || k === "post") && <IcImage size={10} style={{ flexShrink: 0 }} />}
              <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{l}</span>
              {(k === "pre" || k === "post") && dot(k)}
            </button>
          ))}
      </div>

      {/* ══ Onglet Calcul ══ */}
      {tab === "params" && (<>
        <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
          Compare le <b style={{ color: C.mut }}>NBR</b> avant et après un incendie pour estimer
          la sévérité des dégâts. Méthode UN-SPIDER / USGS.
        </div>

        <div style={box}>
          <Lbl>Satellite</Lbl>
          <Sel value={dataset} onChange={v => { setDataset(v); setAvail({ pre: null, post: null }); }}
            options={[
              { value: "sentinel2", label: "Sentinel-2 (20 m) — recommandé" },
              { value: "landsat",   label: "Landsat (30 m)" },
            ]} />
        </div>

        <div style={box}>
          <Lbl>Emprise</Lbl>
          <Sel value={roi} onChange={v => { setRoi(v); setAvail({ pre: null, post: null }); }}
            options={[
              { value: "map", label: "Vue carte actuelle" },
              ...vectorLayers.map(l => ({ value: l.id, label: `Vecteur · ${l.name}` })),
              ...rasterLayers.map(l => ({ value: l.id, label: `Emprise couche · ${l.name}` })),
            ]} />
          <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.4 }}>
            Un <b style={{ color: C.mut }}>vecteur</b> polygonal découpe le calcul sur son contour
            exact ; une autre couche ou la vue carte servent d'emprise rectangulaire.
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 2 }}>
            <span style={{ fontSize: 10.5, color: C.dim, flexShrink: 0 }}>Nuages max</span>
            <input type="range" min="0" max="100" step="5" value={cloud}
              onChange={e => setCloud(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
            <span style={{ fontFamily: M, fontSize: 10.5, color: C.txt, width: 32, textAlign: "right" }}>{cloud} %</span>
          </div>
          <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
            Le masque nuages reste <b style={{ color: C.mut }}>actif</b> pour le calcul : un nuage
            sur l'image d'après ferait chuter le NBR et produirait une fausse sévérité forte.
          </div>
        </div>

        {/* Les deux fenêtres de dates, sur ce seul onglet */}
        {dateWindow("pre")}
        {dateWindow("post")}

        {overlap && (
          <div style={{ display: "flex", gap: 6, fontSize: 9.5, color: C.amb, lineHeight: 1.45,
                        background: C.amb + "10", borderRadius: 6, padding: "6px 8px" }}>
            <IcAlert size={11} style={{ flexShrink: 0, marginTop: 1 }} />
            La fenêtre « après » commence avant la fin de la fenêtre « avant » : les deux états
            se mélangent et le dNBR perd son sens.
          </div>
        )}

        {/* Seuils — repliés : la référence USGS doit rester le comportement normal */}
        <div style={box}>
          <button onClick={() => setOpenThr(o => !o)} style={{
            width: "100%", display: "flex", alignItems: "center", gap: 6, background: "transparent",
            border: "none", cursor: "pointer", padding: 0, color: C.txt,
          }}>
            {openThr ? <IcChevronDown size={13} /> : <IcChevronRight size={13} />}
            <span style={{ fontSize: 11, fontWeight: 500, flex: 1, textAlign: "left" }}>Seuils de sévérité</span>
            <span style={{ fontSize: 8.5, color: thr ? C.amb : C.dim,
                           border: `0.5px solid ${C.bdr}`, borderRadius: 3, padding: "0 4px" }}>
              {thr ? "modifiés" : "USGS"}
            </span>
          </button>
          {openThr && (
            <>
              <div style={{ display: "flex", gap: 4 }}>
                {curThr.map((t, i) => (
                  <input key={i} type="number" step="0.01" value={t}
                    onChange={e => setThr(curThr.map((x, j) => j === i ? parseFloat(e.target.value) : x))}
                    style={{ ...dat, fontSize: 10 }} />
                ))}
              </div>
              <button onClick={() => setThr(null)} style={{ ...ghost, padding: "4px 0" }}>
                Rétablir les valeurs USGS
              </button>
              <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.4 }}>
                Modifier les seuils permet un calage sur des relevés de terrain, mais
                le résultat n'est plus comparable aux analyses publiées.
              </div>
            </>
          )}
        </div>

        <label style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 10.5, color: C.mut, cursor: "pointer" }}>
          <input type="checkbox" checked={vectorize} onChange={e => setVectorize(e.target.checked)}
            style={{ accentColor: C.acc, cursor: "pointer" }} />
          Extraire le périmètre de l'incendie en vecteur
        </label>

        <label style={{ display: "flex", alignItems: "flex-start", gap: 7, cursor: "pointer" }}>
          <input type="checkbox" checked={split} onChange={e => { setSplit(e.target.checked); setThr(null); }}
            style={{ accentColor: C.acc, marginTop: 2, cursor: "pointer" }} />
          <span>
            <span style={{ fontSize: 10.5, color: C.txt }}>Table USGS intégrale (7 classes)</span>
            <span style={{ display: "block", fontSize: 9.5, color: C.dim, lineHeight: 1.45, marginTop: 1 }}>
              Distingue les deux niveaux de repousse (borne à −0,251). Décoché, ils
              sont fusionnés en une seule classe et l'on obtient 6 niveaux.
            </span>
          </span>
        </label>

        {err && (
          <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.red, background: C.red + "12",
                        borderRadius: 6, padding: "7px 9px" }}>
            <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
          </div>
        )}

        <button onClick={run} disabled={busy} style={{
          fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "9px 0", borderRadius: 7,
          background: busy ? C.hover : C.acc, color: busy ? C.dim : "#fff",
          border: "none", cursor: busy ? "default" : "pointer",
          display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
        }}>
          <IcFlame size={14} /> {busy ? "Calcul GEE en cours…" : "Calculer la sévérité"}
        </button>

        {/* ── Résultats ── */}
        {res && (
          <div style={{ ...box, borderColor: C.acc + "44", background: C.acc + "0a" }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 7 }}>
              <IcCheck size={13} style={{ color: C.acc }} />
              <span style={{ fontFamily: M, fontSize: 17, fontWeight: 700, color: C.txt }}>
                {res.burned_ha?.toLocaleString("fr")} ha
              </span>
              <span style={{ fontSize: 10.5, color: C.mut }}>brûlés</span>
            </div>
            <div style={{ fontSize: 9.5, color: C.dim, fontFamily: M }}>
              {res.images?.pre} image(s) avant · {res.images?.post} après · pas {res.scale} m
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: 3, marginTop: 2 }}>
              {res.legend?.map(e => (
                <div key={e.class_id} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 10 }}>
                  <span style={{ width: 12, height: 12, borderRadius: 2, flexShrink: 0, background: e.color }} />
                  <span style={{ flex: 1, minWidth: 0, color: C.mut, overflow: "hidden",
                                 textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{e.label}</span>
                  <span style={{ fontFamily: M, fontSize: 9, color: C.dim, flexShrink: 0 }}>{e.range}</span>
                  <span style={{ fontFamily: M, fontSize: 9.5, color: C.txt, width: 62,
                                 textAlign: "right", flexShrink: 0 }}>
                    {e.area_ha?.toLocaleString("fr")} ha
                  </span>
                </div>
              ))}
            </div>

            <button onClick={addDnbr} style={{ ...ghost, padding: "5px 0" }}>
              Ajouter le dNBR continu
            </button>
          </div>
        )}
      </>)}

      {/* ══ Onglets SWIR avant / après ══ */}
      {tab === "pre"  && swirView("pre")}
      {tab === "post" && swirView("post")}

      {/* ══ Onglet Définition ══ */}
      {tab === "info" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <div style={h}>Sévérité d'incendie (dNBR)</div>
            <p style={p}>
              Le <b style={{ color: C.mut }}>Normalized Burn Ratio</b> oppose le proche infrarouge,
              que la végétation vivante réfléchit fortement, au moyen infrarouge, que les sols nus
              et les cendres réfléchissent. Sa chute entre une image d'avant et une image d'après
              l'incendie mesure la perte de biomasse : c'est le dNBR.
            </p>
          </div>

          <div>
            <div style={h}>Calcul</div>
            <div style={{
              fontFamily: M, fontSize: 11, color: C.txt, background: C.hover,
              border: `0.5px solid ${C.bdr}`, borderRadius: 6, padding: "8px 10px",
              overflowX: "auto", whiteSpace: "nowrap", lineHeight: 1.7,
            }}>
              NBR = (B8A − B12) / (B8A + B12)<br />
              dNBR = NBR<sub>avant</sub> − NBR<sub>après</sub>
            </div>
            <p style={{ ...p, fontSize: 10, marginTop: 6 }}>
              B8A et non B8 : la bande PIR étroite est à 20 m, la résolution native du moyen
              infrarouge B12. Les seuils s'appliquent au ratio brut ; la table USGS souvent
              citée en 100 / 270 / 440 / 660 est la même, multipliée par 1000.
            </p>
          </div>

          <div>
            <div style={h}>Lecture des valeurs · {classes.length} classes</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {classes.map(([lab, col], i) => (
                <div key={lab} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 10 }}>
                  <span style={{ width: 12, height: 12, borderRadius: 2, flexShrink: 0, background: col }} />
                  <span style={{ flex: 1, minWidth: 0, color: C.mut }}>{lab}</span>
                  <span style={{ fontFamily: M, fontSize: 9, color: C.dim, flexShrink: 0 }}>
                    {rangeOf(curThr, i, classes.length)}
                  </span>
                </div>
              ))}
            </div>
            <p style={{ ...p, fontSize: 10, marginTop: 6 }}>
              Un dNBR négatif signale une végétation plus vigoureuse après qu'avant : repousse,
              ou simplement décalage de saison entre les deux images.
            </p>
          </div>

          <div style={{
            display: "flex", gap: 8, background: C.amb + "12",
            border: `0.5px solid ${C.amb}33`, borderRadius: 7, padding: "8px 10px",
          }}>
            <span style={{ color: C.amb, flexShrink: 0, marginTop: 1 }}><IcAlert size={13} /></span>
            <div>
              <div style={{ fontSize: 9.5, color: C.amb, fontWeight: 600, marginBottom: 2 }}>À SAVOIR</div>
              <p style={{ ...p, fontSize: 10.5 }}>
                Le dNBR mesure une perte de couvert, pas une température : un défrichement ou une
                coupe rase produisent la même signature qu'un feu. Comparez toujours des images de
                même saison, et vérifiez le périmètre obtenu sur le composite SWIR.
              </p>
            </div>
          </div>

          {src && (
            <div>
              <div style={h}>Métadonnées</div>
              <div style={{ fontSize: 12, fontWeight: 600, color: C.txt, marginBottom: 2 }}>{src.label}</div>
              <div style={{ fontSize: 10.5, color: C.dim, marginBottom: 7 }}>{src.provider}</div>
              <div style={{ background: C.hover, borderRadius: 7, padding: "7px 10px", border: `0.5px solid ${C.bdr}` }}>
                {[["Résolution", src.res], ["Fréquence", src.revisit], ["Couverture", src.coverage],
                  ["Asset GEE", src.asset]].map(([k, v]) => v ? (
                    <div key={k} style={{ display: "flex", gap: 8, fontSize: 10.5, padding: "2px 0" }}>
                      <span style={{ color: C.dim, width: 84, flexShrink: 0 }}>{k}</span>
                      <span style={{ color: C.mut, fontFamily: M, flex: 1, minWidth: 0, wordBreak: "break-all" }}>{v}</span>
                    </div>
                  ) : null)}
              </div>
            </div>
          )}

          <div>
            <div style={h}>Références</div>
            {[
              ["Méthodologie NBR — UN-SPIDER",
               "https://un-spider.org/advisory-support/recommended-practices/recommended-practice-burn-severity/in-detail/normalized-burn-ratio"],
              ["Implémentation de référence (code)",
               "https://github.com/UN-SPIDER/burn-severity-mapping-EO"],
              ...(cat ? [["Catalogue Earth Engine — citation et licence", cat]] : []),
            ].map(([label, href]) => (
              <a key={href} href={href} target="_blank" rel="noopener noreferrer" style={{
                display: "flex", alignItems: "center", gap: 6, marginTop: 6,
                fontFamily: F, fontSize: 10.5, color: C.acc, textDecoration: "none",
                border: `0.5px solid ${C.acc}66`, borderRadius: 6, padding: "6px 9px",
              }}>
                <IcExternalLink size={12} style={{ flexShrink: 0 }} /> {label}
              </a>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

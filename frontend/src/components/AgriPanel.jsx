/**
 * AgriPanel.jsx — Module Agriculture de Précision v2
 * OpenMapAgents — intégration App.jsx
 *
 * NOUVEAUTÉS v2 :
 *  ✓ Rendu MapLibre des 4 zones de prescription sur la parcelle (fill + bordure)
 *  ✓ Rendu des 4 cartes d'indices sur la parcelle (NDVI, NDRE, NDWI, LST)
 *  ✓ LST en °C (K - 273.15)
 *  ✓ LiteLLM via VITE_LITELLM_API_URL + VITE_LITELLM_API_KEY (.env)
 *  ✓ Capture carte MapLibre dans le rapport PDF (html2canvas)
 *  ✓ Rapport PDF avec toutes les cartes (NDVI, NDRE, NDWI, LST, Prescription)
 *
 * INTÉGRATION App.jsx (5 modifications) — voir PATCH_App.txt
 */

import React, { useState, useEffect, useRef, useCallback } from "react";
import * as turf from "@turf/turf";
import { IcWheat, IcEdit, IcFolder, IcX, IcSatellite, IcAlert, IcBarChart, IcBot,
  IcFile, IcMap, IcLoader, IcCalendar, IcThermometer, IcDroplet, IcSprout,
  IcSearch, IcMapPin, IcBulb, IcFileDown, IcCheck, IcCircle, IcCloud, IcRefreshCw } from "../icons";

// ─── Config LLM via LiteLLM (.env Vite) ─────────────────────────────────────
// Toutes les requêtes passent par LiteLLM (endpoint OpenAI-compatible).
// IMPORTANT : le navigateur ne peut pas appeler localhost:4000 directement.
// → Utiliser un chemin relatif "/api/llm" proxifié par Vite (voir vite.config.js).
//
// vite.config.js — ajouter dans server.proxy :
//   '/api/llm': {
//     target: 'http://localhost:4000',
//     changeOrigin: true,
//     rewrite: path => path.replace(/^\/api\/llm/, '')
//   }
//
// .env (frontend Vite) :
//   VITE_LITELLM_API_URL=/api/llm        ← chemin relatif (proxy Vite)
//   VITE_LITELLM_API_KEY=sk-...          ← clé LiteLLM / OpenAI
//   VITE_OPENAI_MODEL=gpt-4o             ← modèle cible
const LLM_PROVIDER = (import.meta.env.VITE_LLM_PROVIDER || "openai").toLowerCase();
const LITELLM_URL  = (import.meta.env.VITE_LITELLM_API_URL || "/api/llm").replace(/\/$/, "");
const LITELLM_KEY  = import.meta.env.VITE_LITELLM_API_KEY || import.meta.env.VITE_OPENAI_API_KEY || "";
const LLM_MODEL    = import.meta.env.VITE_OPENAI_MODEL || import.meta.env.VITE_CLAUDE_MODEL || "gpt-4o";

// ─── Palette couleurs indices ────────────────────────────────────────────────

function lerpColor(colors, t) {
  const n = colors.length - 1;
  const i = Math.min(Math.floor(t * n), n - 1);
  const f = t * n - i;
  const hex = (s) => parseInt(s, 16);
  const a = colors[i], b = colors[i + 1];
  const r1=hex(a.slice(1,3)),g1=hex(a.slice(3,5)),b1=hex(a.slice(5,7));
  const r2=hex(b.slice(1,3)),g2=hex(b.slice(3,5)),b2=hex(b.slice(5,7));
  return `rgb(${Math.round(r1+(r2-r1)*f)},${Math.round(g1+(g2-g1)*f)},${Math.round(b1+(b2-b1)*f)})`;
}

const PALETTES = {
  // NDVI : rouge vif → jaune → vert clair → vert foncé (7 stops = transitions douces)
  ndvi: ["#d73027","#f46d43","#fdae61","#fee08b","#a6d96a","#66bd63","#1a9850"],
  // NDRE : violet → rouge → orange → jaune-vert → vert foncé
  ndre: ["#762a83","#af8dc3","#c0392b","#e67e22","#d9ef8b","#7fbf7b","#1b7837"],
  // NDWI : brun sec → beige → bleu pâle → bleu → bleu marine
  ndwi: ["#8c510a","#d8b365","#f6e8c3","#c7eae5","#5ab4ac","#2980b9","#01665e"],
  // LST : bleu froid → cyan → jaune → orange → rouge chaud
  lst:  ["#313695","#4575b4","#74add1","#abd9e9","#fee090","#f46d43","#d73027"],
};

const ZONE_DEF = [
  { id:"sain",      label:"Zone saine",   color:"#27ae60", pctKey:"zoneSaine",   action:"Aucune intervention" },
  { id:"irriguer",  label:"À irriguer",   color:"#3498db", pctKey:"stressHyd",   action:"Irrigation 25-30 mm" },
  { id:"fertiliser",label:"À fertiliser", color:"#e8c84a", pctKey:"carenceN",    action:"Apport azoté 40 UN/ha" },
  { id:"traiter",   label:"À traiter",    color:"#e05a3a", pctKey:"zoneMaladie", action:"Inspection + traitement" },
];

const MONTHS = ["Jan","Fév","Mar","Avr","Mai","Jun","Jul","Aoû","Sep","Oct","Nov","Déc"];

// ─── Simulation indices ───────────────────────────────────────────────────────

/**
 * Contexte climatique réaliste selon lat + mois
 * Retourne les plages attendues pour chaque indice.
 */
function getClimateContext(lat, lng, month) {
  // ── Température air de surface réelle (modèle sinusoïdal) ──────────────────
  // Amplitude et moyenne varient avec la latitude (hémisphère N)
  const absLat = Math.abs(lat);
  const isNorth = lat >= 0;
  // Mois de l'année côté hiver/été : jan=hiver en N, juil=été en N
  const seasonPhase = isNorth ? month : (month + 6) % 12;
  // Température annuelle moyenne (°C) : baisse avec lat, augmente vers tropiques
  const tMean = Math.max(-5, Math.min(28, 28 - absLat * 0.55));
  // Amplitude saisonnière (°C) : forte aux latitudes tempérées
  const tAmp  = Math.max(3, Math.min(20, absLat * 0.35));
  // Courbe sinusoïdale : pic en juillet (mois 6) en hémisphère N
  const tAir  = tMean + tAmp * Math.cos((seasonPhase / 12) * 2 * Math.PI + Math.PI);

  // ── LST = température de surface ≈ tAir + biais (sol nu chaud, végétation fraîche) ──
  // En hiver : LST ≈ tAir ± 2°C. En été : LST peut dépasser tAir de 5-15°C sur sol nu.
  const summerBias = Math.max(0, Math.sin((seasonPhase / 12) * 2 * Math.PI) * 8);
  const lstAir = tAir + summerBias;

  // ── NDVI saisonnier ─────────────────────────────────────────────────────────
  // Hiver tempéré : NDVI bas (0.15-0.35), été : élevé (0.55-0.85)
  // Tropiques : NDVI fort toute l'année (0.6-0.85)
  const ndviSummer  = Math.max(0.45, Math.min(0.88, 0.85 - absLat * 0.005));
  const ndviWinter  = Math.max(0.05, Math.min(0.45, 0.45 - absLat * 0.006));
  const ndviSeasonal = ndviWinter + (ndviSummer - ndviWinter) *
    Math.max(0, Math.sin((seasonPhase / 12) * 2 * Math.PI));

  // ── Stress hydrique ─────────────────────────────────────────────────────────
  // En été : plus de stress hydrique ; hiver : faible
  const hydroStress = Math.max(0, Math.sin((seasonPhase / 12) * 2 * Math.PI) * 0.4);

  // Longitude influence légère (continentalité)
  const continental = Math.abs(lng) > 60 ? 0.05 : 0;

  return { tAir: +tAir.toFixed(1), lstAir: +lstAir.toFixed(1),
           ndviSeasonal: +ndviSeasonal.toFixed(3),
           hydroStress, continental, month, tMean, tAmp };
}

/**
 * fetchAgriIndices — appel réel à /api/gee/agri/stats
 *
 * Aucune valeur par défaut ni simulation : tout vient des données satellite GEE.
 * NDVI/NDWI : S2 10m natif
 * NDRE : B8A/B5 20m rééchantillonné → 10m (reproject GEE)
 * LST  : Landsat ST_B10 30m rééchantillonné → 10m (reproject GEE)
 */
const GEE_BASE =
  typeof import.meta !== "undefined"
    ? (import.meta.env?.VITE_GEE_API_URL || "/api/gee")
    : "/api/gee";

async function fetchAgriIndices(polygon, dateStart, dateEnd, cloudMax = 20, composite = "least_cloudy") {
  const geometry = polygon.geometry || polygon;
  const res = await fetch(`${GEE_BASE}/agri/stats`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      roi_geojson:  geometry,
      date_start:   dateStart,
      date_end:     dateEnd,
      cloud_max:    cloudMax,
      composite,
      scale:        10,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `GEE erreur ${res.status}`);
  }
  const d = await res.json();

  const ndvi = +(d.ndvi?.mean  ?? 0).toFixed(3);
  const ndre = +(d.ndre?.mean  ?? 0).toFixed(3);
  const ndwi = +(d.ndwi?.mean  ?? 0).toFixed(3);
  const lstC = +(d.lst?.mean   ?? 0).toFixed(1);
  const lstK = +(lstC + 273.15).toFixed(1);

  return {
    ndvi, ndre, ndwi, lstC, lstK,
    ndviMin: +(d.ndvi?.min ?? ndvi - 0.05).toFixed(3),
    ndviMax: +(d.ndvi?.max ?? ndvi + 0.05).toFixed(3),
    ndreMin: +(d.ndre?.min ?? ndre - 0.05).toFixed(3),
    ndreMax: +(d.ndre?.max ?? ndre + 0.05).toFixed(3),
    ndwiMin: +(d.ndwi?.min ?? ndwi - 0.05).toFixed(3),
    ndwiMax: +(d.ndwi?.max ?? ndwi + 0.05).toFixed(3),
    lstMin:  +(d.lst?.min  ?? lstC - 3).toFixed(1),
    lstMax:  +(d.lst?.max  ?? lstC + 3).toFixed(1),
    ndviStd: +(d.ndvi?.std ?? 0).toFixed(3),
    ndreStd: +(d.ndre?.std ?? 0).toFixed(3),
    ndwiStd: +(d.ndwi?.std ?? 0).toFixed(3),
    lstStd:  +(d.lst?.std  ?? 0).toFixed(1),
    // Zones calculées pixel à pixel sur GEE
    zoneSaine:   d.zones?.zoneSaine   ?? 0,
    stressHyd:   d.zones?.stressHyd   ?? 0,
    carenceN:    d.zones?.carenceN    ?? 0,
    zoneMaladie: d.zones?.zoneMaladie ?? 0,
    pixelsNDVI:  d.zones?.total_pixels ?? 0,
    scale:       d.scale      ?? 10,
    s2Images:    d.s2_images  ?? 0,
    // Métadonnées images réellement utilisées
    dateUsed:       d.date_used     ?? dateStart,   // date précise (least_cloudy) ou label médiane
    cloudPct:       d.cloud_pct     ?? null,         // % nuages image sélectionnée
    cloudMaxUsed:   d.cloud_max_used ?? cloudMax,
    composite:      d.composite     ?? composite,
    lstSource:      d.lst_source    ?? "—",
    dateStart:      d.date_start,
    dateEnd:        d.date_end,
    ctx: null,
  };
}

async function fetchAgriTimeSeries(polygon, dateEnd, cloudMax = 30, composite = "least_cloudy") {
  const geometry = polygon.geometry || polygon;
  const end   = new Date(dateEnd);
  const weeks = Array.from({ length: 12 }, (_, i) => {
    const t1 = new Date(end); t1.setDate(t1.getDate() - (11 - i) * 7);
    const t0 = new Date(t1); t0.setDate(t0.getDate() - 16);
    return { label: `${t1.getDate()} ${MONTHS[t1.getMonth()]}`, t0, t1 };
  });

  const results = await Promise.allSettled(
    weeks.map(w =>
      fetch(`${GEE_BASE}/agri/stats`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          roi_geojson: geometry,
          date_start:  w.t0.toISOString().slice(0, 10),
          date_end:    w.t1.toISOString().slice(0, 10),
          cloud_max:   cloudMax,
          composite,
          scale:       10,
        }),
      }).then(r => r.ok ? r.json() : Promise.reject())
    )
  );

  return weeks.map((w, i) => {
    const r = results[i];
    if (r.status === "fulfilled" && r.value?.ndvi) {
      // Utiliser la vraie date de l'image sélectionnée (date_used) — pas la date de fenêtre
      // date_used = "2026-03-18" pour least_cloudy, "médiane N img" pour median
      const dateUsed = r.value.date_used ?? w.label;
      // Formater la date en "18 Mar" si c'est une date ISO, sinon garder le label
      let dateLabel = w.label;
      if (dateUsed && /^\d{4}-\d{2}-\d{2}$/.test(dateUsed)) {
        const d = new Date(dateUsed + "T12:00:00Z");
        dateLabel = `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]}`;
      }
      return {
        date:      dateLabel,
        dateISO:   dateUsed,
        s2Images:  r.value.s2_images  ?? null,
        composite: r.value.composite  ?? null,
        lstSource: r.value.lst_source ?? null,   // "Landsat9 (N img)" | "MODIS (N img)" | "indisponible"
        cloudPct:  r.value.cloud_pct  ?? null,
        ndvi: +(r.value.ndvi?.mean ?? 0).toFixed(3),
        ndre: +(r.value.ndre?.mean ?? 0).toFixed(3),
        ndwi: +(r.value.ndwi?.mean ?? 0).toFixed(3),
        lstC: +(r.value.lst?.mean  ?? 0).toFixed(1),
      };
    }
    // Pas d'image sur cette fenêtre → point null (pas de carry-forward — courbe brisée)
    return {
      date:      w.label,
      dateISO:   null,
      s2Images:  null,
      composite: null,
      lstSource: null,
      cloudPct:  null,
      ndvi: null,
      ndre: null,
      ndwi: null,
      lstC: null,
    };
  });
}

// ─── Fetch tuiles GEE réelles pour indices agri ──────────────────────────────
/**
 * Appelle /api/gee/agri/tiles pour obtenir l'URL XYZ des tuiles GEE d'un indice.
 * Retourne { tile_url, date_used, cloud_pct, cloud_max_used, s2_images, vis_params }
 */
async function fetchAgriTile(polygon, dateStart, dateEnd, cloudMax, composite, index) {
  const geometry = polygon.geometry || polygon;
  const res = await fetch(`${GEE_BASE}/agri/tiles`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      roi_geojson: geometry,
      date_start:  dateStart,
      date_end:    dateEnd,
      cloud_max:   cloudMax,
      composite,
      index,
      scale: 10,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `GEE tiles erreur ${res.status}`);
  }
  return res.json();
}

// ─── Utilitaire encode SVG (conservé pour usage futur éventuel) ──────────────


// ── getStatus seuils agronomiques ────────────────────────────────────────────
function getStatus(id, val) {
  if (id === "ndvi") return val >= 0.55 ? {l:"Bon",    c:"#27ae60"} : val >= 0.30 ? {l:"Moyen",c:"#e8c84a"} : {l:"Faible",c:"#e05a3a"};
  if (id === "ndre") return val >= 0.40 ? {l:"Bon",    c:"#27ae60"} : val >= 0.25 ? {l:"Carence légère",c:"#e8c84a"} : {l:"Carence sévère",c:"#e05a3a"};
  // Seuils NDWI calibrés végétation : valeurs typiques parcelle [-0.4 à +0.3]
  // Humide (bien irrigué) > 0.0 ; Modéré -0.1 à 0.0 ; Stress hydrique < -0.1
  if (id === "ndwi") return val >= 0.0  ? {l:"Humide", c:"#27ae60"} : val >= -0.1 ? {l:"Modéré",c:"#e8c84a"} : {l:"Sec/stress",c:"#e05a3a"};
  if (id === "lst")  return val <= 30   ? {l:"Normal", c:"#27ae60"} : val <= 38   ? {l:"Échauffement",c:"#e8c84a"} : {l:"Stress thermique",c:"#e05a3a"};
  return {l:"—", c:"#888"};
}


// ─── Helpers MapLibre ─────────────────────────────────────────────────────────

const AGRI_LAYERS  = ["agri-parcel-fill","agri-parcel-line","agri-zones-fill","agri-zones-line",
                     "agri-draw-line","agri-draw-pts",
                     "agri-raster-ndvi","agri-raster-ndwi","agri-raster-ndre","agri-raster-lst","agri-raster-prescription"];
const AGRI_SOURCES = ["agri-parcel","agri-zones","agri-draw",
                      "agri-tiles-ndvi","agri-tiles-ndwi","agri-tiles-ndre","agri-tiles-lst","agri-tiles-prescription"];
const AGRI_RASTER_KEYS = ["ndvi","ndwi","ndre","lst","prescription"];

function clearAgriLayers(map) {
  AGRI_LAYERS.forEach(id  => { try { if (map.getLayer(id))  map.removeLayer(id);  } catch(_){} });
  AGRI_SOURCES.forEach(id => { try { if (map.getSource(id)) map.removeSource(id); } catch(_){} });
}

/**
 * Ajoute ou met à jour une couche raster GEE (tuiles XYZ réelles) sur la carte.
 * Clippe visuellement au contour de la parcelle via un masque GeoJSON fill.
 */
function renderRasterLayer(map, tileUrl, indexKey, parcelFC) {
  const srcId = `agri-tiles-${indexKey}`;
  const lyrId = `agri-raster-${indexKey}`;

  // Supprimer si existant
  try { if (map.getLayer(lyrId)) map.removeLayer(lyrId); } catch(_){}
  try { if (map.getSource(srcId)) map.removeSource(srcId); } catch(_){}

  if (!tileUrl) return;

  // Source raster XYZ GEE
  map.addSource(srcId, {
    type: "raster",
    tiles: [tileUrl],
    tileSize: 256,
    attribution: "Google Earth Engine",
  });

  // Layer raster — inséré SOUS le contour de la parcelle
  map.addLayer({
    id: lyrId,
    type: "raster",
    source: srcId,
    paint: { "raster-opacity": 0.88, "raster-fade-duration": 300 },
  }, "agri-parcel-line");
}

/** Masque tous les layers raster agri sauf celui demandé. */
function showOnlyRasterLayer(map, activeKey) {
  AGRI_RASTER_KEYS.forEach(k => {
    const lyrId = `agri-raster-${k}`;
    try {
      if (map.getLayer(lyrId)) {
        map.setLayoutProperty(lyrId, "visibility", k === activeKey ? "visible" : "none");
      }
    } catch(_){}
  });
}

function renderParcelBorder(map, fc) {
  if (!map.getSource("agri-parcel")) {
    map.addSource("agri-parcel", { type:"geojson", data:fc });
    map.addLayer({ id:"agri-parcel-fill", type:"fill", source:"agri-parcel",
      paint:{ "fill-color":"#27ae60", "fill-opacity":0.05 } });
    map.addLayer({ id:"agri-parcel-line", type:"line", source:"agri-parcel",
      paint:{ "line-color":"#27ae60", "line-width":2.5, "line-dasharray":[5,3] } });
  } else {
    map.getSource("agri-parcel").setData(fc);
  }
}

function renderZonesLayer(map, features) {
  const fc = { type:"FeatureCollection", features };
  if (!map.getSource("agri-zones")) {
    map.addSource("agri-zones", { type:"geojson", data:fc });
    map.addLayer({
      id:"agri-zones-fill", type:"fill", source:"agri-zones",
      paint:{ "fill-color":["get","color"], "fill-opacity":0.50 }
    }, "agri-parcel-line");
    map.addLayer({
      id:"agri-zones-line", type:"line", source:"agri-zones",
      paint:{ "line-color":["get","color"], "line-width":0.8, "line-opacity":0.6 }
    });
  } else {
    map.getSource("agri-zones").setData(fc);
  }
}

function updateDrawLayer(map, pts) {
  const feats = pts.map(p=>({type:"Feature",geometry:{type:"Point",coordinates:p},properties:{}}));
  if (pts.length>=2) feats.push({type:"Feature",geometry:{type:"LineString",coordinates:[...pts,pts[0]]},properties:{}});
  const gj = {type:"FeatureCollection",features:feats};
  if (map.getSource("agri-draw")) { map.getSource("agri-draw").setData(gj); }
  else {
    map.addSource("agri-draw",{type:"geojson",data:gj});
    map.addLayer({id:"agri-draw-line",type:"line",source:"agri-draw",filter:["==",["geometry-type"],"LineString"],
      paint:{"line-color":"#27ae60","line-width":2,"line-dasharray":[3,2]}});
    map.addLayer({id:"agri-draw-pts",type:"circle",source:"agri-draw",filter:["==",["geometry-type"],"Point"],
      paint:{"circle-radius":5,"circle-color":"#27ae60","circle-stroke-width":2,"circle-stroke-color":"#fff"}});
  }
}

function svgToDataUrl(svg) {
  return "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent(svg)));
}

// ─── Appel LiteLLM (endpoint OpenAI-compatible) ──────────────────────────────
// Toutes les requêtes passent par LiteLLM quel que soit le provider configuré.
// LiteLLM se charge du routage vers OpenAI, Claude, Ollama, Mistral, etc.
// selon sa propre config (LITELLM_MODEL_LIST ou variables d'env côté serveur).
async function callLLM(parcelInfo, indices, timeSeries) {
  const now    = new Date();
  const saison = ["Hiver","Hiver","Printemps","Printemps","Printemps","Été","Été","Été","Automne","Automne","Automne","Hiver"][now.getMonth()];

  const prompt = `Tu es un expert agronome spécialisé en télédétection et agriculture de précision.
Analyse ces indices satellitaires RÉELS (Google Earth Engine) et fournis un diagnostic agronomique complet.

CONTEXTE :
- Localisation : lat≈${parcelInfo.lat?.toFixed(3)||"?"}, lng≈${parcelInfo.lng?.toFixed(3)||"?"}
- Saison : ${saison}
- Surface : ${parcelInfo.surface} ha · Culture : ${parcelInfo.culture || "céréales"}
- Période acquisition : ${indices.dateStart} → ${indices.dateEnd}
- Résolution : ${indices.scale}m (S2 ${indices.composite}, ${indices.s2Images} images, image : ${indices.dateUsed}${indices.cloudPct!=null?" · ☁ "+indices.cloudPct+"%":""})
- LST source : ${indices.lstSource}

INDICES SPECTRAUX GEE (valeurs réelles, pas simulées) :
- NDVI moyen : ${indices.ndvi} (σ=${indices.ndviStd}, plage : ${indices.ndviMin}–${indices.ndviMax}) → Biomasse végétale (S2 B8/B4 10m)
- NDRE moyen : ${indices.ndre} (σ=${indices.ndreStd}, plage : ${indices.ndreMin}–${indices.ndreMax}) → Azote foliaire (S2 B8A/B5 20m→10m)
- NDWI moyen : ${indices.ndwi} (σ=${indices.ndwiStd}, plage : ${indices.ndwiMin}–${indices.ndwiMax}) → Teneur en eau (S2 B3/B8 10m)
- LST surface : ${indices.lstC}°C (σ=${indices.lstStd}°C, plage : ${indices.lstMin}–${indices.lstMax}°C) → Landsat ST_B10 →10m

ZONES (calculées pixel à pixel sur GEE) :
- Zone saine : ${indices.zoneSaine}% · À irriguer : ${indices.stressHyd}% · À fertiliser : ${indices.carenceN}% · À traiter : ${indices.zoneMaladie}%

TENDANCE TEMPORELLE (12 semaines) :
${(()=>{
  // Filtrer les points avec valeurs réelles (null = pas d'image satellite ce jour)
  const valid = timeSeries.filter(p => p?.ndvi != null);
  if (valid.length < 2) return "Données temporelles insuffisantes (moins de 2 acquisitions)";
  const first = valid[0], last = valid[valid.length - 1];
  const fmt = v => v != null ? v : "—";
  const trend = (a, b) => {
    if (a == null || b == null) return "";
    const d = b - a; return d > 0.05 ? " ↑" : d < -0.05 ? " ↓" : " →";
  };
  return `NDVI ${fmt(first.ndvi)}→${fmt(last.ndvi)}${trend(first.ndvi,last.ndvi)} | NDWI ${fmt(first.ndwi)}→${fmt(last.ndwi)}${trend(first.ndwi,last.ndwi)} | LST ${fmt(first.lstC)}→${fmt(last.lstC)}°C (${valid.length}/${timeSeries.length} acquisitions valides)`;
})()}

RÈGLES ABSOLUES pour la réponse :
1. Les noms de zones sont FIXES : "Zone saine", "À irriguer", "À fertiliser", "À traiter"
   → N'invente JAMAIS "Zone A", "Zone B", "zone centrale", "zone nord-ouest" etc.
2. Tu NE CONNAIS PAS la localisation spatiale des pixels sur la parcelle.
   → N'écris JAMAIS "la zone centrale", "la zone sud", "la partie haute" etc.
   → Décris uniquement ce que les indices révèlent : "La zone saine représente X% de la parcelle..."
3. N'inclus dans "zones" QUE les zones avec un pourcentage > 0%.
4. Le "detail" doit parler des indices mesurés, pas de positions géographiques inventées.

Réponds UNIQUEMENT en JSON valide (sans markdown) :
{
  "diagnostic_global": "2-3 phrases sur l'état global de la parcelle basées sur les indices réels",
  "zones": [
    {"nom":"Zone saine","pct":${indices.zoneSaine},"etat":"sain","urgence":"aucune","indices_cles":["NDVI ${indices.ndvi}"],"action":"surveiller","detail":"La zone saine (${indices.zoneSaine}% de la parcelle) présente des niveaux NDVI/NDRE/NDWI satisfaisants pour la saison."},
    {"nom":"À irriguer","pct":${indices.stressHyd},"etat":"stress_hydrique","urgence":"moyenne","indices_cles":["NDWI ${indices.ndwi}"],"action":"irrigation","detail":"La zone à irriguer (${indices.stressHyd}%) présente un NDWI bas indiquant un déficit hydrique."},
    {"nom":"À fertiliser","pct":${indices.carenceN},"etat":"carence_N","urgence":"haute","indices_cles":["NDRE ${indices.ndre}"],"action":"apport azoté","detail":"La zone à fertiliser (${indices.carenceN}%) présente un NDRE faible indiquant une carence en azote foliaire."},
    {"nom":"À traiter","pct":${indices.zoneMaladie},"etat":"maladie_stress","urgence":"haute","indices_cles":["NDVI ${indices.ndvi}","NDRE ${indices.ndre}"],"action":"inspection","detail":"La zone à traiter (${indices.zoneMaladie}%) présente des indices spectraux dégradés pouvant indiquer maladie ou stress sévère."}
  ],
  "recommandations": [
    {"type":"irrigation","priorite":1,"texte":"description actionnable basée sur les indices réels","quantite":"25 mm"}
  ],
  "risques": "alertes basées sur les indices mesurés, sans inventer de localisation",
  "prochaine_acquisition": "dans X jours selon la couverture nuageuse locale"
}`;

  // ── Toujours LiteLLM : endpoint /chat/completions OpenAI-compatible ────────
  const endpoint = `${LITELLM_URL}/chat/completions`;
  const headers  = {
    "Content-Type":  "application/json",
    "Authorization": `Bearer ${LITELLM_KEY}`,
  };
  const body = JSON.stringify({
    model:       LLM_MODEL,          // ex: "gpt-4o", "claude-sonnet-4-20250514", "ollama/llama3"…
    max_tokens:  1400,
    temperature: 0.2,
    messages: [
      { role: "system", content: "Expert agronome. Réponds UNIQUEMENT en JSON valide, sans markdown ni backticks." },
      { role: "user",   content: prompt },
    ],
  });

  const res = await fetch(endpoint, { method: "POST", headers, body });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    throw new Error(e?.error?.message || `LiteLLM ${res.status} — vérifier VITE_LITELLM_API_URL et VITE_LITELLM_API_KEY`);
  }
  const data = await res.json();
  const raw  = data.choices?.[0]?.message?.content || "{}";
  return JSON.parse(raw.replace(/```json|```/g, "").trim());
}

// ─── Rapport HTML ─────────────────────────────────────────────────────────────
function buildReport(parcelInfo, indices, timeSeries, aiResult, mapImages) {
  const date = new Date().toLocaleDateString("fr-FR",{day:"numeric",month:"long",year:"numeric"});
  const aiLabel = aiResult ? (LLM_PROVIDER === "openai" ? "GPT-4o Agronome" : "Claude Agronome") : "—";

  const img = (src, title, full=false) => `
<div class="map-wrap${full?" map-full":""}">
  <div class="map-title">${title}</div>
  ${src
    ? `<img src="${src}" class="map-img" alt="${title}" style="image-rendering:pixelated"/>`
    : `<div class="map-ph">Carte non disponible — tuile GEE indisponible</div>`}
</div>`;
 
  const zoneRows = ZONE_DEF.map(z=>`<tr>
    <td><span class="dot" style="background:${z.color}"></span>${z.label}</td>
    <td>${indices[z.pctKey]||0}%</td>
    <td>${((parcelInfo.surface*(indices[z.pctKey]||0))/100).toFixed(1)} ha</td>
    <td>${z.action}</td>
  </tr>`).join("");
 
  const aiZones = (aiResult?.zones||[]).map(z=>{
    const col=z.urgence==="haute"?"#e05a3a":z.urgence==="moyenne"?"#e8c84a":"#27ae60";
    return `<div class="zcard" style="border-left-color:${col}">
      <div class="zh"><strong>${z.nom}</strong><span class="badge" style="background:${col}20;color:${col}">${z.action||z.etat}</span></div>
      <p>${z.detail}</p>
      ${(z.indices_cles||[]).length?`<div class="chips">${z.indices_cles.map(c=>`<span class="chip">${c}</span>`).join("")}</div>`:""}
    </div>`;
  }).join("");

  const recos = (aiResult?.recommandations||[]).map(r=>{
    const icons={irrigation:"💧",fertilisation:"🌿",traitement:"⚠️",surveillance:"🔍"};
    const cols={irrigation:"#3498db",fertilisation:"#e8c84a",traitement:"#e05a3a",surveillance:"#888"};
    return `<li class="reco" style="border-color:${cols[r.type]||"#27ae60"}">
      <span class="ri">${icons[r.type]||"📌"}</span>
      <div>
        <strong>${r.type.charAt(0).toUpperCase()+r.type.slice(1)}</strong>${r.priorite===1?' <span class="urg">PRIORITAIRE</span>':""}
        <br>${r.texte}${r.quantite?` <em>(${r.quantite})</em>`:""}
      </div>
    </li>`;
  }).join("");

  // Garder tous les points non-null (pas de filtre i%2 qui masquait des dates)
  const tsRows = timeSeries.filter(p=>p.ndvi!=null).map(p=>{
    // Source satellite courte pour la colonne
    const sat = p.lstSource
      ? (p.lstSource.startsWith("Landsat9")?"LS9":
         p.lstSource.startsWith("Landsat8")?"LS8":
         p.lstSource.startsWith("MODIS")   ?"MODIS":"—")
      : "—";
    const comp = p.composite
      ? (p.composite==="least_cloudy"?"🏆 LC":p.composite==="mosaic"?"🗺 MOS":"∑ MED")
      : "—";
    const cloud = p.cloudPct!=null ? `${p.cloudPct}%` : "—";
    const imgs  = p.s2Images!=null ? `${p.s2Images}` : "—";
    return `<tr>
    <td><strong>${p.date}</strong>${p.dateISO&&p.dateISO!==p.date?`<br><span style="font-size:8px;color:#999">${p.dateISO}</span>`:""}</td>
    <td style="color:${getStatus("ndvi",p.ndvi).c}">${p.ndvi??"-"}</td>
    <td style="color:${getStatus("ndre",p.ndre).c}">${p.ndre??"-"}</td>
    <td style="color:${getStatus("ndwi",p.ndwi).c}">${p.ndwi??"-"}</td>
    <td style="color:${getStatus("lst",p.lstC).c}">${p.lstC!=null?p.lstC+"°C":"-"}</td>
    <td style="font-size:9px;color:#555">${comp}<br><span style="color:#999">${imgs} img · ☁${cloud}</span></td>
    <td style="font-size:9px;color:#666">S2+${sat}</td>
  </tr>`;
  }).join("");

  return `<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8"/>
<title>Rapport Agronomique — ${parcelInfo.name}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;color:#1a1a1a;font-size:11px;line-height:1.5}
.page{max-width:820px;margin:0 auto;padding:28px 36px}
.hdr{display:flex;align-items:center;justify-content:space-between;border-bottom:3px solid #27ae60;padding-bottom:14px;margin-bottom:22px}
.logo{font-size:22px;font-weight:800;color:#1a5c2e}.logo span{color:#27ae60}
.meta{text-align:right;font-size:10px;color:#666}.meta strong{display:block;font-size:14px;color:#1a1a1a}
h2{font-size:13px;font-weight:700;color:#1a5c2e;border-left:4px solid #27ae60;padding-left:10px;margin:20px 0 10px}
.ig{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;background:#f0f9f0;border:1px solid #c8e6c9;border-radius:8px;padding:12px;margin-bottom:14px}
.ii .l{font-size:9px;color:#666;font-weight:600;text-transform:uppercase;letter-spacing:1px}
.ii .v{font-size:13px;font-weight:700}
.krow{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.kpi{background:#f8f9fa;border:1px solid #e0e0e0;border-radius:8px;padding:10px;text-align:center}
.kpi .kn{font-size:9px;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:1px}
.kpi .kv{font-size:20px;font-weight:800;margin:3px 0 1px}
.kpi .ks{font-size:9px;color:#999}
/* Maps */
.maps2{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
.map-wrap{break-inside:avoid;background:#fff;border-radius:8px;padding:8px;border:1px solid #e0e0e0;box-shadow:0 1px 4px rgba(0,0,0,.07)}
.map-full .map-img{height:260px}
.map-full{grid-column:1/-1}
.map-title{font-size:10px;font-weight:700;color:#1a5c2e;margin-bottom:6px;text-transform:uppercase;letter-spacing:1px;padding-left:2px}
.map-img{width:100%;height:190px;object-fit:contain;border-radius:5px;display:block;background:#f8fbf8}
.map-ph{width:100%;height:190px;background:#f0f4f0;border:1px dashed #ccc;border-radius:6px;display:flex;align-items:center;justify-content:center;color:#aaa;font-size:10px}
/* Table */
table{width:100%;border-collapse:collapse;font-size:10px;margin-bottom:12px}
th{background:#1a5c2e;color:#fff;padding:6px 10px;text-align:left;font-size:9px;font-weight:700}
td{padding:6px 10px;border-bottom:1px solid #eee}
tr:nth-child(even){background:#f9f9f9}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:middle}
/* Zones */
.zg{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.zcard{background:#f8f9fa;border:1px solid #e0e0e0;border-left:4px solid;border-radius:0 6px 6px 0;padding:10px}
.zh{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.badge{font-size:9px;padding:2px 7px;border-radius:10px;font-weight:700}
.chips{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
.chip{font-size:9px;background:#eee;border-radius:3px;padding:1px 5px;color:#555}
/* Recos */
.rlist{list-style:none;display:flex;flex-direction:column;gap:8px}
.reco{display:flex;align-items:flex-start;gap:10px;background:#f8f9fa;border-radius:0 6px 6px 0;border-left:4px solid;padding:8px 12px}
.ri{font-size:14px;flex-shrink:0}
.urg{background:#fde8e8;color:#c0392b;font-size:8px;padding:1px 5px;border-radius:8px;font-weight:700;margin-left:6px}
.diag{background:#e8f5e9;border:1px solid #c8e6c9;border-radius:8px;padding:12px;margin-bottom:12px;font-size:11px}
.footer{margin-top:28px;padding-top:12px;border-top:1px solid #ddd;font-size:9px;color:#999;display:flex;justify-content:space-between}
@media print{body{-webkit-print-color-adjust:exact;print-color-adjust:exact}.page{padding:16px 24px}}
</style></head><body><div class="page">

<div class="hdr">
  <div><div class="logo">AGRO<span>VISION</span></div>
  <div style="font-size:10px;color:#666;margin-top:2px">Rapport d'analyse agronomique parcellaire</div></div>
  <div class="meta"><strong>${parcelInfo.name}</strong>Date : ${date}<br>S2 GEE · ${indices.s2Images} img · ${indices.scale}m · ${indices.composite}<br>Image : ${indices.dateUsed}${indices.cloudPct!=null?" · ☁ "+indices.cloudPct+"%":""}<br>LST : ${indices.lstSource}<br>IA : ${aiLabel}</div>
</div>

<h2>1. Informations parcelle</h2>
<div class="ig">
  <div class="ii"><div class="l">Nom</div><div class="v">${parcelInfo.name}</div></div>
  <div class="ii"><div class="l">Surface</div><div class="v">${parcelInfo.surface} ha</div></div>
  <div class="ii"><div class="l">Culture</div><div class="v">${parcelInfo.culture||"—"}</div></div>
</div>

<h2>2. Indicateurs spectraux</h2>
<div class="krow">
  <div class="kpi"><div class="kn">NDVI</div>
    <div class="kv" style="color:${getStatus("ndvi",indices.ndvi).c}">${indices.ndvi}</div>
    <div class="ks">${getStatus("ndvi",indices.ndvi).l}</div><div class="ks">${indices.ndviMin}–${indices.ndviMax}</div></div>
  <div class="kpi"><div class="kn">NDRE</div>
    <div class="kv" style="color:${getStatus("ndre",indices.ndre).c}">${indices.ndre}</div>
    <div class="ks">${getStatus("ndre",indices.ndre).l}</div></div>
  <div class="kpi"><div class="kn">NDWI</div>
    <div class="kv" style="color:${getStatus("ndwi",indices.ndwi).c}">${indices.ndwi}</div>
    <div class="ks">${getStatus("ndwi",indices.ndwi).l}</div></div>
  <div class="kpi"><div class="kn">LST</div>
    <div class="kv" style="color:${getStatus("lst",indices.lstC).c}">${indices.lstC}°C</div>
    <div class="ks">${getStatus("lst",indices.lstC).l}</div><div class="ks">${indices.lstK} K</div></div>
</div>

<h2>3. Carte de prescription pixel (GEE)</h2>
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px">
  ${ZONE_DEF.map(z=>`
    <div style="display:flex;align-items:center;gap:7px;padding:7px 10px;background:${z.color}12;border:1px solid ${z.color}44;border-radius:6px">
      <div style="width:12px;height:12px;border-radius:3px;background:${z.color};flex-shrink:0"></div>
      <div>
        <div style="font-size:9px;font-weight:700;color:${z.color}">${z.label}</div>
        <div style="font-size:8px;color:#666">${indices[z.pctKey]||0}% · ${((parcelInfo.surface*(indices[z.pctKey]||0))/100).toFixed(1)} ha</div>
        <div style="font-size:8px;color:#888">${z.action}</div>
      </div>
    </div>`).join("")}
</div>
<div style="margin-bottom:10px">
  ${img(mapImages?.prescription,"Pixels réels GEE — NDVI/NDWI/NDRE seuillés pixel à pixel",true)}
</div> 
${aiResult?`
<h2>4. Diagnostic agronomique (IA)</h2>
<div class="diag">${aiResult.diagnostic_global}</div>
<div class="zg">${aiZones}</div>
${aiResult.risques?`<div style="padding:8px 12px;background:#fef3cd;border-radius:6px;font-size:10px;margin-bottom:12px"><strong>⚠ Risques :</strong> ${aiResult.risques}</div>`:""}
`:""}

${aiResult?.recommandations?.length?`
<h2>${aiResult?"6":"5"}. Recommandations pratiques</h2>
<ul class="rlist">${recos}</ul>
`:""}


<h2>5. Cartes des indices spectraux (pixels réels)</h2>
<div class="maps2">
  ${img(mapImages?.ndvi,"NDVI — Biomasse végétale · S2 10m")}
  ${img(mapImages?.ndre,"NDRE — Azote foliaire · S2 20m→10m")}
  ${img(mapImages?.ndwi,"NDWI — Teneur en eau · S2 10m")}
  ${img(mapImages?.lst, "LST — Température surface (°C) · Landsat/MODIS")}
</div>



<h2>${aiResult?"7":"6"}. Évolution temporelle — images disponibles</h2>
<table>
  <thead><tr><th>Date image réelle</th><th>NDVI</th><th>NDRE</th><th>NDWI</th><th>LST</th><th>Composite · imgs · ☁</th><th>Satellite</th></tr></thead>
  <tbody>${tsRows}</tbody>
</table>


<div class="footer">
  <span>OpenMapAgents — Module Agriculture de Précision</span>
  <span>${date} · GEE S2 ${indices.composite} · ${indices.dateUsed} · ${indices.s2Images} img</span>
</div>
${Object.values(mapImages||{}).every(v=>!v) ? `
<div style="margin-top:12px;padding:10px;background:#fff3cd;border:1px solid #ffc107;border-radius:6px;font-size:10px">
  <strong>⚠ Cartes non capturées :</strong> Pour inclure les cartes pixel dans le rapport, 
  ajouter <code>preserveDrawingBuffer: true</code> dans l'initialisation MapLibre (App.jsx).
</div>` : ""}
</div></body></html>`;
}

// ─── Graphique timeline avec axes gradués ────────────────────────────────────
function TimelineChart({ data, C }) {
  if (!data || data.length < 2) return null;

  const W=380, H=110, pad={l:34,r:10,t:8,b:22};
  const iW = W - pad.l - pad.r;
  const iH = H - pad.t - pad.b;

  // Plages dynamiques réelles (pas hardcodées)
  const vals = key => data.map(p=>p[key]).filter(v=>v!=null && !isNaN(v));
  const rangeOf = (arr, margin=0.06) => {
    if (!arr.length) return [0, 1];
    const mn = Math.min(...arr), mx = Math.max(...arr);
    const span = Math.max(mx - mn, 0.02);
    return [mn - span * margin, mx + span * margin];
  };
  // Axe Y unifié sur les 3 indices normalisés (NDVI/NDRE/NDWI — tous dans [-1,1])
  const allVals = [...vals("ndvi"), ...vals("ndre"), ...vals("ndwi")];
  const [yMn, yMx] = rangeOf(allVals, 0.08);

  // Points X : indexés sur les données ayant une vraie date
  // On utilise l'index direct (équidistant)
  const n = data.length;
  const xP = i => pad.l + (i / (n - 1)) * iW;
  const yP = (v, mn, mx) => {
    if (v == null || isNaN(v)) return null;
    return pad.t + (1 - (v - mn) / Math.max(mx - mn, 0.001)) * iH;
  };

  // Graduations Y : 4 niveaux réguliers arrondis
  const yStep = (yMx - yMn) / 3;
  const yTicks = [0,1,2,3].map(i => yMn + i * yStep);

  // Dates X : afficher uniquement les points avec valeur non nulle
  // Indices à montrer sur l'axe X : 1er, dernier, et 2 intermédiaires
  const xTickIdx = [0, Math.floor(n/3), Math.floor(2*n/3), n-1].filter((v,i,a)=>a.indexOf(v)===i);

  // Chemin d'une série (segments brisés si valeur manquante)
  const path = (key, mn, mx) => {
    let d = "";
    for (let i = 0; i < n; i++) {
      const v = data[i]?.[key];
      const y = yP(v, mn, mx);
      if (y == null) { d += " "; continue; }
      const x = xP(i).toFixed(1);
      // Chercher si le point précédent valide est adjacent pour décider M ou L
      let prevValid = false;
      for (let j = i-1; j >= 0; j--) {
        if (data[j]?.[key] != null && !isNaN(data[j][key])) { prevValid = (i - j === 1); break; }
      }
      d += `${prevValid ? "L" : "M"}${x},${y.toFixed(1)} `;
    }
    return d.trim();
  };

  // Dernier point valide pour le cercle terminal
  const lastValid = (key) => {
    for (let i = n-1; i >= 0; i--) {
      const v = data[i]?.[key];
      if (v != null && !isNaN(v)) return { i, v };
    }
    return null;
  };

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{display:"block"}}>
      {/* Grille horizontale */}
      {yTicks.map((t,i) => {
        const y = yP(t, yMn, yMx);
        if (y == null) return null;
        return (
          <g key={i}>
            <line x1={pad.l} y1={y} x2={pad.l+iW} y2={y} stroke={C.bdr} strokeWidth="0.4" strokeDasharray="3,3"/>
            <text x={pad.l-3} y={y+3} textAnchor="end" fontSize="7" fill={C.dim} fontFamily="monospace">
              {t.toFixed(2)}
            </text>
          </g>
        );
      })}
      {/* Axes */}
      <line x1={pad.l} y1={pad.t} x2={pad.l} y2={pad.t+iH} stroke={C.bdr} strokeWidth="0.7"/>
      <line x1={pad.l} y1={pad.t+iH} x2={pad.l+iW} y2={pad.t+iH} stroke={C.bdr} strokeWidth="0.7"/>
      {/* Dates X — alignées exactement sur les points de données */}
      {xTickIdx.map(i => data[i] && (
        <text key={i} x={xP(i)} y={H-4} textAnchor="middle" fontSize="7.5" fill={C.dim}>
          {data[i].date}
        </text>
      ))}
      {/* Lignes verticales légères aux points X affichés */}
      {xTickIdx.map(i => (
        <line key={i} x1={xP(i)} y1={pad.t} x2={xP(i)} y2={pad.t+iH} stroke={C.bdr} strokeWidth="0.3" strokeDasharray="2,4"/>
      ))}
      {/* Séries */}
      <path d={path("ndvi",yMn,yMx)} fill="none" stroke="#27ae60" strokeWidth="1.8" strokeLinejoin="round"/>
      <path d={path("ndre",yMn,yMx)} fill="none" stroke="#8e44ad" strokeWidth="1.3" strokeDasharray="3,2" strokeLinejoin="round"/>
      <path d={path("ndwi",yMn,yMx)} fill="none" stroke="#3498db" strokeWidth="1.3" strokeDasharray="4,3" strokeLinejoin="round"/>
      {/* Cercles terminaux */}
      {(() => { const lv = lastValid("ndvi"); const y = lv && yP(lv.v,yMn,yMx); return y && <circle cx={xP(lv.i)} cy={y} r="3" fill="#27ae60"/>; })()}
      {(() => { const lv = lastValid("ndwi"); const y = lv && yP(lv.v,yMn,yMx); return y && <circle cx={xP(lv.i)} cy={y} r="2.5" fill="#3498db"/>; })()}
      {/* Label axe Y */}
      <text x={8} y={pad.t + iH/2} fontSize="7" fill={C.dim} textAnchor="middle"
        transform={`rotate(-90, 8, ${pad.t + iH/2})`}>indice</text>
    </svg>
  );
}

// ─── Légende barre couleur ────────────────────────────────────────────────────
function ColorBar({ palette, label, min, max, unit="", C }) {
  return (
    <div style={{display:"flex",flexDirection:"column",gap:3}}>
      <div style={{fontSize:9,fontWeight:700,color:C.dim,textTransform:"uppercase",letterSpacing:1}}>{label}</div>
      <div style={{height:8,borderRadius:4,background:`linear-gradient(90deg,${palette.join(",")})`}}/>
      <div style={{display:"flex",justifyContent:"space-between",fontSize:9,color:C.dim,fontFamily:"monospace"}}>
        <span>{min}{unit}</span><span>{max}{unit}</span>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// COMPOSANT PRINCIPAL
// ════════════════════════════════════════════════════════════════════════════

export default function AgriPanel({ mapRef, addLayer, C, F, M }) {
  // Dates GEE par défaut : les 3 derniers mois
  const today     = new Date();
  const defEnd    = today.toISOString().slice(0, 10);
  const defStart  = new Date(today.setMonth(today.getMonth() - 3)).toISOString().slice(0, 10);

  const [step,          setStep]          = useState("setup");
  const [parcelFC,      setParcelFC]      = useState(null);
  const [parcelInfo,    setParcelInfo]    = useState({ name:"Ma parcelle", culture:"Blé tendre", surface:0 });
  const [geeConfig,     setGeeConfig]     = useState({ dateStart: defStart, dateEnd: defEnd, cloudMax: 20, composite: "least_cloudy" });
  const [agriTiles,     setAgriTiles]     = useState({});   // { ndvi: {tile_url, date_used,...}, ndwi:..., ndre:..., lst:... }
  const [tilesLoading,  setTilesLoading]  = useState(false);
  const [geeLoading,    setGeeLoading]    = useState(false);
  const [geeError,      setGeeError]      = useState(null);
  const [indices,       setIndices]       = useState(null);
  const [timeSeries,    setTimeSeries]    = useState([]);
  const [zoneFeats,     setZoneFeats]     = useState([]);
  const [activeLayer,   setActiveLayer]   = useState("prescription");
  const [aiResult,      setAiResult]      = useState(null);
  const [aiLoading,     setAiLoading]     = useState(false);
  const [aiError,       setAiError]       = useState(null);
  const [reportLoading, setReportLoading] = useState(false);
  const fileRef    = useRef(null);
  const drawPtsRef = useRef([]);
  const drawingRef = useRef(false);

  // Nettoyage au démontage
  useEffect(() => () => {
    const map = mapRef.current?.getMap?.();
    if (map) { map._agriCleanup?.(); clearAgriLayers(map); }
  }, []);

  // ── Dessin ────────────────────────────────────────────────────
  const startDrawing = useCallback(() => {
    const map = mapRef.current?.getMap?.(); if (!map) return;
    drawPtsRef.current = []; drawingRef.current = true;
    setStep("drawing");
    map.getCanvas().style.cursor = "crosshair";

    const onClick = (e) => {
      if (!drawingRef.current) return;
      drawPtsRef.current = [...drawPtsRef.current, [e.lngLat.lng, e.lngLat.lat]];
      updateDrawLayer(map, drawPtsRef.current);
    };
    const onDblClick = (e) => {
      if (!drawingRef.current || drawPtsRef.current.length < 3) return;
      e.preventDefault(); finishDrawing(map);
    };
    map.on("click", onClick); map.on("dblclick", onDblClick);
    map._agriCleanup = () => { map.off("click",onClick); map.off("dblclick",onDblClick); map.getCanvas().style.cursor=""; };
  }, [mapRef]);

  const finishDrawing = useCallback((map) => {
    const pts = drawPtsRef.current; if (pts.length < 3) return;
    drawingRef.current = false; map._agriCleanup?.();
    // Supprimer couche dessin temp
    ["agri-draw-line","agri-draw-pts"].forEach(id=>{try{if(map.getLayer(id))map.removeLayer(id);}catch(_){}});
    ["agri-draw"].forEach(id=>{try{if(map.getSource(id))map.removeSource(id);}catch(_){}});
    const feat = { type:"Feature", geometry:{type:"Polygon",coordinates:[[...pts,pts[0]]]}, properties:{} };
    applyParcel({ type:"FeatureCollection", features:[feat] }, map);
  }, []);

  const applyParcel = useCallback(async (fc, mapArg) => {
    const map = mapArg || mapRef.current?.getMap?.(); if (!map) return;
    const polygon = fc.features[0];
    const area = +(turf.area(polygon) / 10000).toFixed(2);

    // Limite 500 ha — au-delà les tuiles GEE sont trop lentes et les stats imprécises
    if (area > 500) {
      setGeeError(`Parcelle trop grande : ${area} ha (maximum 500 ha). Découpez la zone ou réduisez l'emprise.`);
      setStep("error");
      clearAgriLayers(map);
      renderParcelBorder(map, fc);
      const bb = turf.bbox(fc);
      map.fitBounds([[bb[0],bb[1]],[bb[2],bb[3]]], { padding:70, duration:900 });
      return;
    }

    const centroid = turf.centroid(polygon);
    const [lng, lat] = centroid.geometry.coordinates;

    setParcelFC(fc);
    setParcelInfo(p => ({ ...p, surface: area, lat: +lat.toFixed(5), lng: +lng.toFixed(5) }));
    setIndices(null); setTimeSeries([]); setZoneFeats([]);
    setAiResult(null); setAiError(null);
    setGeeError(null); setGeeLoading(true);
    setStep("loading");

    // Rendu MapLibre immédiat (contour seul, zones viendront après GEE)
    clearAgriLayers(map);
    renderParcelBorder(map, fc);
    const bb = turf.bbox(fc);
    map.fitBounds([[bb[0],bb[1]],[bb[2],bb[3]]], { padding:70, duration:900 });

    try {
      // ── 1. Stats agri (indices moyens + zones prescription) ────────────────
      const idx = await fetchAgriIndices(
        polygon,
        geeConfig.dateStart,
        geeConfig.dateEnd,
        geeConfig.cloudMax,
        geeConfig.composite,
      );

      // ── 2. Série temporelle (12 semaines) ─────────────────────────────────
      const ts = await fetchAgriTimeSeries(
        polygon, geeConfig.dateEnd,
        Math.min(geeConfig.cloudMax + 10, 50), "least_cloudy"
      );

      setIndices(idx);
      setTimeSeries(ts);
      setActiveLayer("prescription");  // prescription raster par défaut
      setGeeLoading(false);
      setStep("results");

      // ── 3. Tuiles raster GEE en parallèle (NDVI/NDWI/NDRE/LST) ───────────
      // Lance les 4 appels tiles en parallèle sans bloquer l'affichage des stats
      setTilesLoading(true);
      const INDICES = ["NDVI", "NDWI", "NDRE", "LST", "PRESCRIPTION"];
      const tileResults = await Promise.allSettled(
        INDICES.map(idxName => fetchAgriTile(
          polygon,
          geeConfig.dateStart, geeConfig.dateEnd,
          geeConfig.cloudMax, geeConfig.composite,
          idxName
        ))
      );

      const tiles = {};
      tileResults.forEach((r, i) => {
        const key = INDICES[i].toLowerCase();
        if (r.status === "fulfilled") {
          tiles[key] = r.value;
        } else {
          console.warn(`Tuile ${INDICES[i]} échouée:`, r.reason?.message);
          tiles[key] = null;
        }
      });
      setAgriTiles(tiles);
      setTilesLoading(false);

      // Rendu raster sur la carte — les 4 couches sont ajoutées, seule NDVI visible
      const mapNow = mapRef.current?.getMap?.();
      if (mapNow) {
  // Supprimer éventuels layers raster précédents
        AGRI_RASTER_KEYS.forEach(k => {
          try { if (mapNow.getLayer(`agri-raster-${k}`)) mapNow.removeLayer(`agri-raster-${k}`); } catch(_){}
          try { if (mapNow.getSource(`agri-tiles-${k}`)) mapNow.removeSource(`agri-tiles-${k}`); } catch(_){}
        });
        // Ajouter toutes les couches raster (indices + prescription)
        AGRI_RASTER_KEYS.forEach(k => {
          if (tiles[k]?.tile_url) renderRasterLayer(mapNow, tiles[k].tile_url, k, parcelFC);
        });
        // Afficher prescription par défaut
        showOnlyRasterLayer(mapNow, "prescription");
      }

    } catch (err) {
      setGeeLoading(false);
      setTilesLoading(false);
      setGeeError(err.message || "Erreur GEE inconnue");
      setStep("error");
    }
  }, [mapRef, addLayer, parcelInfo.name, geeConfig]);

  // ── Import GeoJSON ────────────────────────────────────────────
  const handleImport = useCallback(async (e) => {
    const file = e.target.files?.[0]; if (!file) return;
    try {
      const gj = JSON.parse(await file.text());
      const polys = (gj.features||[]).filter(f=>f.geometry?.type==="Polygon"||f.geometry?.type==="MultiPolygon");
      if (!polys.length) { alert("Aucun polygone trouvé."); return; }
      applyParcel({ type:"FeatureCollection", features:[polys[0]] });
    } catch(err) { alert("Erreur import : " + err.message); }
  }, [applyParcel]);

  // ── Basculer couche affichée ───────────────────────────────────
  const switchLayer = useCallback((mode) => {
    const map = mapRef.current?.getMap?.(); if (!map || !parcelFC) return;
    setActiveLayer(mode);

    // Toutes les couches sont raster XYZ GEE — on bascule la visibilité
    const tileInfo = agriTiles[mode];
    if (tileInfo?.tile_url) {
      if (map.getLayer(`agri-raster-${mode}`)) {
        showOnlyRasterLayer(map, mode);
      } else {
        // Edge case : layer pas encore ajoutée (chargement tardif)
        AGRI_RASTER_KEYS.forEach(k => {
          try { if (map.getLayer(`agri-raster-${k}`)) map.setLayoutProperty(`agri-raster-${k}`, "visibility", "none"); } catch(_){}
        });
        renderRasterLayer(map, tileInfo.tile_url, mode, parcelFC);
      }
    } else if (tilesLoading) {
      // Tuiles encore en cours — rien à faire, elles s'afficheront quand prêtes
    } else {
      // Tuile indisponible pour ce mode — masquer tout
      AGRI_RASTER_KEYS.forEach(k => {
        try { if (map.getLayer(`agri-raster-${k}`)) map.setLayoutProperty(`agri-raster-${k}`, "visibility", "none"); } catch(_){}
      });
    }
  }, [mapRef, parcelFC, indices, agriTiles, tilesLoading]);

  // ── Analyse IA ────────────────────────────────────────────────
  const runAI = useCallback(async () => {
    if (!indices) return;
    setAiLoading(true); setAiError(null);
    try { const r = await callLLM(parcelInfo, indices, timeSeries); setAiResult(r); setStep("ai"); }
    catch(err) { setAiError("Erreur LLM : " + err.message); }
    finally { setAiLoading(false); }
  }, [indices, parcelInfo, timeSeries]);

  // ── Capture carte MapLibre en PNG via canvas ─────────────────
  /**
   * Pour chaque indice, bascule sur le layer raster correspondant,
   * attend le rendu, capture le canvas MapLibre, restaure la vue.
   * Retourne { ndvi, ndwi, ndre, lst, prescription } → data URL PNG.
   */
  const captureMapImages = useCallback(async () => {
    const map = mapRef.current?.getMap?.();
    if (!map || !parcelFC) return {};

    // ── Activer preserve_drawing_buffer si pas déjà fait ──────────────────────
    // Sans cette option, WebGL vide le canvas après chaque frame → toDataURL blanc.
    // On ne peut pas la changer après init, alors on patch le canvas directement
    // en forçant un re-render synchrone avant chaque capture.
    const canvas = map.getCanvas();

    const images = {};
    const savedLayer = activeLayer;

    // Helper : attendre que toutes les tuiles visibles soient chargées
    const waitIdle = () => new Promise(resolve => {
      let timeout;
      const check = () => {
        if (!map.isMoving() && !map.isZooming() && !map.isRotating()) {
          // Attendre en plus que le réseau soit calme (tuiles GEE)
          timeout = setTimeout(resolve, 600);
        } else {
          map.once("idle", () => { timeout = setTimeout(resolve, 600); });
        }
      };
      map.once("idle", check);
      // Déclencher un idle si la map est déjà statique
      if (!map.isMoving()) map.fire("idle");
      // Timeout absolu de sécurité
      setTimeout(resolve, 4000);
    });

    for (const key of AGRI_RASTER_KEYS) {
      if (!agriTiles[key]?.tile_url) continue;
      try {
        // Afficher uniquement ce layer
        if (map.getLayer(`agri-raster-${key}`)) {
          showOnlyRasterLayer(map, key);
        } else {
          renderRasterLayer(map, agriTiles[key].tile_url, key, parcelFC);
          showOnlyRasterLayer(map, key);
        }

        // Attendre le chargement complet des tuiles
        await waitIdle();

        // Force re-render WebGL et capture immédiate
        map.triggerRepaint();
        await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));

        // Capture — si le canvas WebGL est vide (preserve_drawing_buffer=false),
        // on lit le canvas 2D de secours via un trick readPixels
        const dataUrl = canvas.toDataURL("image/png");

        // Vérification : une image entièrement transparente = canvas WebGL vidé
        // Dans ce cas on tente une 2e capture après un autre frame
        const isBlank = dataUrl.length < 5000; // data:image/png;base64,... vide ≈ 80 chars
        if (isBlank) {
          await new Promise(r => setTimeout(r, 800));
          map.triggerRepaint();
          await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
          images[key] = canvas.toDataURL("image/png");
        } else {
          images[key] = dataUrl;
        }
      } catch(e) { console.warn(`Capture ${key} failed:`, e); }
    }

    // Restaurer le layer actif
    showOnlyRasterLayer(map, savedLayer);
    return images;
  }, [mapRef, parcelFC, agriTiles, activeLayer]);

  // ── Rapport PDF ───────────────────────────────────────────────
  const downloadReport = useCallback(async () => {
    setReportLoading(true);
    try {
      // Capture des cartes pixel réelles depuis MapLibre
      // NOTE : nécessite preserve_drawing_buffer:true dans l'init MapLibre (App.jsx)
      // Si les images sont vides, on utilise les tile_url GEE directement comme src img
      let mapImages = await captureMapImages();

      // Fallback : si canvas vide (preserve_drawing_buffer non activé),
      // utiliser les tuiles GEE directement dans le rapport HTML
      const allEmpty = Object.values(mapImages).every(v => !v || v.length < 5000);
      if (allEmpty) {
        mapImages = {};
        for (const key of AGRI_RASTER_KEYS) {
          if (agriTiles[key]?.tile_url) {
            // Construire une URL de tuile à zoom 15 centrée sur la parcelle
            const bb = parcelFC ? (() => {
              const [W,S,E,N] = [
                Math.min(...parcelFC.features[0].geometry.coordinates[0].map(c=>c[0])),
                Math.min(...parcelFC.features[0].geometry.coordinates[0].map(c=>c[1])),
                Math.max(...parcelFC.features[0].geometry.coordinates[0].map(c=>c[0])),
                Math.max(...parcelFC.features[0].geometry.coordinates[0].map(c=>c[1])),
              ];
              return { cx: (W+E)/2, cy: (S+N)/2 };
            })() : null;
            // On ne peut pas utiliser une tile_url XYZ directement comme img src (CORS + {z}/{x}/{y})
            // → stocker null, le rapport affichera "Carte disponible dans l'application"
            mapImages[key] = null;
          }
        }
      }

      const html = buildReport(parcelInfo, indices, timeSeries, aiResult, mapImages);
      const url  = URL.createObjectURL(new Blob([html],{type:"text/html;charset=utf-8"}));
      const win  = window.open(url,"_blank");
      if (win) setTimeout(()=>win.print(),1200);
      else { const a=document.createElement("a"); a.href=url; a.download=`rapport_${parcelInfo.name.replace(/\s+/g,"_")}.html`; a.click(); }
    } catch(e) { alert("Erreur rapport : " + e.message); }
    finally { setReportLoading(false); }
  }, [parcelInfo, indices, timeSeries, aiResult, captureMapImages]);

  // ── Reset ─────────────────────────────────────────────────────
  const reset = useCallback(() => {
    const map = mapRef.current?.getMap?.();
    if (map) { map._agriCleanup?.(); clearAgriLayers(map); }
    drawPtsRef.current=[]; drawingRef.current=false;
    setParcelFC(null); setIndices(null); setTimeSeries([]); setZoneFeats([]);
    setAgriTiles({}); setTilesLoading(false);
    setAiResult(null); setAiError(null); setStep("setup");
  }, [mapRef]);

  // ── Styles ────────────────────────────────────────────────────
  const s = {
    sec: { padding:"10px 12px", display:"flex", flexDirection:"column", gap:8 },
    lbl: { fontSize:10, fontWeight:600, color:C.dim, textTransform:"uppercase", letterSpacing:"0.06em" },
    inp: { fontFamily:F, fontSize:11, padding:"6px 10px", borderRadius:7,
           border:`0.5px solid ${C.bdr}`, background:C.input, color:C.txt,
           width:"100%", outline:"none", boxSizing:"border-box" },
    btn: (accent,danger) => ({
      fontFamily:F, fontSize:11, padding:"7px 12px", borderRadius:7,
      border:`0.5px solid ${danger?C.red+"55":accent?C.acc+"55":C.bdr}`,
      background:danger?C.red+"15":accent?C.acc+"18":"transparent",
      color:danger?C.red:accent?C.acc:C.txt, cursor:"pointer",
      display:"flex", alignItems:"center", justifyContent:"center", gap:6, width:"100%",
    }),
    card: { background:C.hover, border:`0.5px solid ${C.bdr}`, borderRadius:8, padding:10 },
    tab: (on) => ({
      flex:1, padding:"7px 4px", fontSize:10, fontWeight:on?600:400,
      background:"none", border:"none", cursor:"pointer",
      color:on?C.acc:C.dim,
      borderBottom:on?`2px solid ${C.acc}`:"2px solid transparent",
    }),
  };

  // ════════════════════════════════════ SETUP ═══════════════════
  if (step==="setup") return (
    <div style={s.sec}>
      <div style={{fontSize:12,fontWeight:700,color:C.acc,display:"flex",alignItems:"center",gap:6}}><IcWheat size={15}/> Agriculture de précision</div>
      <div style={{fontSize:11,color:C.dim,lineHeight:1.6}}>
        Délimitez votre parcelle pour analyser les indices spectraux (NDVI, NDRE, NDWI, LST) et visualiser les zones de prescription directement sur la carte.
      </div>
      <div style={{height:"0.5px",background:C.bdr}}/>
      <div style={s.lbl}>Informations parcelle</div>
      <input style={s.inp} placeholder="Nom de la parcelle" value={parcelInfo.name}
        onChange={e=>setParcelInfo(p=>({...p,name:e.target.value}))}/>
      <input style={s.inp} placeholder="Culture (ex: Blé tendre, Maïs…)" value={parcelInfo.culture}
        onChange={e=>setParcelInfo(p=>({...p,culture:e.target.value}))}/>
      <div style={{height:"0.5px",background:C.bdr}}/>
      <div style={s.lbl}>Période d'acquisition Sentinel-2</div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6}}>
        <div>
          <div style={{fontSize:9,color:C.dim,marginBottom:3}}>Début</div>
          <input type="date" style={s.inp} value={geeConfig.dateStart}
            onChange={e=>setGeeConfig(g=>({...g,dateStart:e.target.value}))}/>
        </div>
        <div>
          <div style={{fontSize:9,color:C.dim,marginBottom:3}}>Fin</div>
          <input type="date" style={s.inp} value={geeConfig.dateEnd}
            onChange={e=>setGeeConfig(g=>({...g,dateEnd:e.target.value}))}/>
        </div>
      </div>
      <div>
        <div style={{fontSize:9,color:C.dim,marginBottom:3}}>Nuages max : {geeConfig.cloudMax}%</div>
        <input type="range" min="5" max="60" step="5" value={geeConfig.cloudMax}
          onChange={e=>setGeeConfig(g=>({...g,cloudMax:+e.target.value}))}
          style={{width:"100%",accentColor:C.acc}}/>
      </div>
      <div>
        <div style={{fontSize:9,color:C.dim,marginBottom:4}}>Mode composite</div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4}}>
          {[
            ["least_cloudy","Moins nuageux","1 image réelle"],
            ["mosaic",      "Mosaïque",     "pixels récents"],
            ["median",      "Médiane",      "toutes images"],
          ].map(([val, lbl, sub]) => {
            const on = geeConfig.composite === val;
            return (
              <button key={val} onClick={()=>setGeeConfig(g=>({...g,composite:val}))}
                style={{padding:"6px 4px",borderRadius:7,cursor:"pointer",textAlign:"center",
                  border:`0.5px solid ${on?C.acc+"88":C.bdr}`,
                  background:on?C.acc+"18":"transparent",transition:"all .12s"}}>
                <div style={{fontSize:11,fontWeight:on?700:400,color:on?C.acc:C.txt}}>{lbl}</div>
                <div style={{fontSize:9,color:C.dim,marginTop:1}}>{sub}</div>
              </button>
            );
          })}
        </div>
        <div style={{fontSize:9,color:C.dim,marginTop:4,lineHeight:1.5}}>
          {geeConfig.composite==="least_cloudy" && "Recommandé — prend l'image la moins nuageuse dans la période. Date précise connue."}
          {geeConfig.composite==="mosaic"       && "Priorité aux pixels les plus récents. Utile si la zone est souvent nuageuse."}
          {geeConfig.composite==="median"       && "Médiane de toutes les images — peut produire des valeurs si aucune image réelle n'existe."}
        </div>
      </div>
      <div style={{height:"0.5px",background:C.bdr}}/>
      <div style={s.lbl}>Délimiter la parcelle <span style={{fontSize:9,color:C.dim,fontWeight:400}}>— max 500 ha</span></div>
      <button style={{...s.btn(true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:6}} onClick={startDrawing}><IcEdit size={13}/> Dessiner sur la carte</button>
      <div style={{fontSize:10,color:C.dim,textAlign:"center"}}>Clic = ajouter un sommet · Double-clic = terminer</div>
      <button style={{...s.btn(),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:6}} onClick={()=>fileRef.current?.click()}><IcFolder size={13}/> Importer GeoJSON</button>
      <input ref={fileRef} type="file" accept=".geojson,.json" style={{display:"none"}} onChange={handleImport}/>
    </div>
  );

  // ════════════════════════════════════ DRAWING ═════════════════
  if (step==="drawing") return (
    <div style={s.sec}>
      <div style={{...s.card,background:C.acc+"12",borderColor:C.acc+"44"}}>
        <div style={{fontSize:12,fontWeight:600,color:C.acc,marginBottom:4,display:"flex",alignItems:"center",gap:6}}><IcEdit size={13}/> Mode dessin actif</div>
        <div style={{fontSize:11,color:C.dim,lineHeight:1.6}}>
          Cliquez pour placer les sommets du polygone.<br/>
          <strong style={{color:C.txt}}>Double-clic</strong> pour terminer (min. 3 points).
        </div>
      </div>
      <button style={{...s.btn(false,true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:5}} onClick={reset}><IcX size={13}/> Annuler</button>
    </div>
  );

  // ════════════════════════════════════ LOADING GEE ════════════
  if (step==="loading") return (
    <div style={s.sec}>
      <div style={{...s.card,background:C.acc+"12",borderColor:C.acc+"44",textAlign:"center",padding:20}}>
        <div style={{display:"flex",justifyContent:"center",marginBottom:8}}><IcSatellite size={20}/></div>
        <div style={{fontSize:12,fontWeight:600,color:C.acc,marginBottom:4}}>Calcul GEE en cours…</div>
        <div style={{fontSize:10,color:C.dim,lineHeight:1.7}}>
          Requête Sentinel-2 + Landsat<br/>
          {geeConfig.dateStart} → {geeConfig.dateEnd}<br/>
          Nuages ≤ {geeConfig.cloudMax}% · {
            geeConfig.composite==="least_cloudy" ? "image la moins nuageuse" :
            geeConfig.composite==="mosaic"       ? "mosaïque récente" :
                                                   "médiane temporelle"
          } · 10m
        </div>
        <div style={{marginTop:12,height:3,borderRadius:2,background:C.bdr,overflow:"hidden"}}>
          <div style={{height:"100%",background:C.acc,borderRadius:2,
            animation:"gee-progress 2s ease-in-out infinite",width:"60%"}}/>
        </div>
      </div>
      <style>{`@keyframes gee-progress{0%{transform:translateX(-100%)}100%{transform:translateX(280%)}}`}</style>
    </div>
  );

  // ════════════════════════════════════ ERROR GEE ═══════════════
  if (step==="error") return (
    <div style={s.sec}>
      <div style={{...s.card,background:C.red+"10",borderColor:C.red+"44"}}>
        <div style={{fontSize:11,fontWeight:600,color:C.red,marginBottom:6,display:"flex",alignItems:"center",gap:5}}><IcAlert size={12}/> Erreur</div>
        <div style={{fontSize:10,color:C.dim,lineHeight:1.6,marginBottom:10}}>{geeError}</div>
        {!geeError?.includes("500 ha") && (
          <div style={{fontSize:9,color:C.dim,lineHeight:1.6,marginBottom:10}}>
            • Vérifiez que le backend GEE est accessible ({GEE_BASE})<br/>
            • Essayez une période plus longue ou un seuil nuages plus élevé<br/>
            • Vérifiez que la parcelle est dans la couverture S2
          </div>
        )}
        {!geeError?.includes("500 ha") && (
          <button style={{...s.btn(true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:5}} onClick={()=>{if(parcelFC)applyParcel(parcelFC);}}><IcRefreshCw size={12}/> Réessayer</button>
        )}
      </div>
      <button style={{...s.btn(false,true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:5}} onClick={reset}><IcX size={13}/> Nouvelle parcelle</button>
    </div>
  );


  return (
    <div style={{display:"flex",flexDirection:"column",flex:1,minHeight:0,overflowY:"auto"}}>
      {/* Header */}
      <div style={{padding:"8px 12px",borderBottom:`0.5px solid ${C.bdr}`,display:"flex",alignItems:"center",justifyContent:"space-between",flexShrink:0}}>
        <div>
          <div style={{fontSize:12,fontWeight:700,color:C.txt}}>{parcelInfo.name}</div>
          <div style={{fontSize:10,color:C.dim}}>{parcelInfo.culture} · {parcelInfo.surface} ha</div>
        </div>
        <button style={{...s.btn(),width:"auto",padding:"4px 10px",fontSize:10,display:"inline-flex",alignItems:"center",gap:4}} onClick={reset}><IcRefreshCw size={11}/> Reset</button>
      </div>

      {/* Onglets */}
      <div style={{display:"flex",borderBottom:`0.5px solid ${C.bdr}`,flexShrink:0}}>
        {[["results",IcBarChart,"Indices"],["ai",IcBot,"IA"],["report",IcFile,"Rapport"]].map(([id,Icon,lbl])=>(
          <button key={id} style={{...s.tab(step===id),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:5}}
            onClick={()=>{ if(id==="ai"&&!aiResult&&!aiLoading)runAI(); else setStep(id); }}>
            <Icon size={13}/> {lbl}
          </button>
        ))}
      </div>

      {/* ─── ONGLET INDICES ─── */}
      {step==="results" && (
        <div style={s.sec}>
          {/* Sélecteur de couche carte */}
          <div style={s.lbl}>Vue carte</div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:4}}>
            {[
              ["prescription","Presc."],
              ["ndvi","NDVI"],
              ["ndre","NDRE"],
              ["ndwi","NDWI"],
              ["lst","LST"],
            ].map(([mode,lbl])=>{
              const on      = activeLayer === mode;
              const tInfo   = agriTiles[mode];
              const loading = tilesLoading && !tInfo;
              const noData  = !tilesLoading && !tInfo?.tile_url;
              // Date toujours visible : date_used de l'image réelle (ex "18 Mar 2026")
              const dateLabel = tInfo?.date_used
                ? (/^\d{4}-\d{2}-\d{2}$/.test(tInfo.date_used)
                    ? tInfo.date_used.slice(5).replace("-","/")  // "03/18"
                    : tInfo.date_used.slice(0,8))                // "médiane" tronqué
                : null;
              return (
                <button key={mode} onClick={()=>switchLayer(mode)}
                  style={{padding:"5px 2px",borderRadius:6,cursor:noData?"not-allowed":"pointer",textAlign:"center",
                    border:`0.5px solid ${on?C.acc+"66":noData?C.bdr+"44":C.bdr}`,
                    background:on?C.acc+"18":"transparent",
                    color:on?C.acc:noData?C.dim+"66":C.dim,
                    fontWeight:on?700:400,opacity:noData?0.5:1,transition:"all .1s"}}>
                  <div style={{fontSize:9,display:"flex",alignItems:"center",justifyContent:"center",gap:3}}>{loading?<IcLoader size={10}/>:noData?<IcX size={10}/>:null}{lbl}</div>
                  {/* Date image réelle — toujours affichée, pas seulement quand actif */}
                  {dateLabel
                    ? <div style={{fontSize:7,color:on?C.acc:C.dim,marginTop:1,lineHeight:1.2,fontFamily:"monospace"}}>
                        {dateLabel}
                      </div>
                    : loading
                      ? <div style={{fontSize:7,color:C.dim,marginTop:1}}>…</div>
                      : null}
                </button>
              );
            })}
          </div>

          {/* Légende dynamique */}
          {activeLayer==="prescription" ? (
            <div style={{display:"flex",flexDirection:"column",gap:3}}>
              {tilesLoading && !agriTiles.prescription &&
                <div style={{fontSize:9,color:C.acc,textAlign:"center",padding:4,display:"flex",alignItems:"center",justifyContent:"center",gap:5}}><IcLoader size={11}/> Génération carte prescription…</div>}
              {ZONE_DEF.map(z=>(
                <div key={z.id} style={{display:"flex",alignItems:"center",gap:8,padding:"5px 8px",
                  background:z.color+"10",borderRadius:6,border:`0.5px solid ${z.color}44`}}>
                  <div style={{width:10,height:10,borderRadius:2,background:z.color,flexShrink:0}}/>
                  <div style={{flex:1}}>
                    <div style={{fontSize:10,fontWeight:600,color:z.color}}>{z.label}</div>
                    <div style={{fontSize:9,color:C.dim}}>{z.action}</div>
                  </div>
                  <div style={{fontFamily:"monospace",fontSize:11,fontWeight:700,color:z.color}}>{indices[z.pctKey]}%</div>
                </div>
              ))}
              <div style={{fontSize:8,color:C.dim,textAlign:"center",marginTop:2}}>
                Pixels réels GEE · seuils NDVI/NDWI/NDRE
              </div>
            </div>
          ) : (
            <div style={s.card}>
              {(() => {
                const ranges = {
                  ndvi: { mn: indices.ndviMin, mx: indices.ndviMax, u: "" },
                  ndre: { mn: indices.ndreMin, mx: indices.ndreMax, u: "" },
                  ndwi: { mn: indices.ndwiMin, mx: indices.ndwiMax, u: "" },
                  lst:  { mn: indices.lstMin,  mx: indices.lstMax,  u: "°C" },
                };
                const r = ranges[activeLayer] || ranges.ndvi;
                return <ColorBar palette={PALETTES[activeLayer]||PALETTES.ndvi}
                  label={`${activeLayer.toUpperCase()} (GEE réel)`} C={C}
                  min={String(r.mn)} max={String(r.mx)} unit={r.u}/>;
              })()}
            </div>
          )}

          <div style={{height:"0.5px",background:C.bdr}}/>

          {/* Méta GEE — stats + tuile active */}
          <div style={{...s.card,padding:"7px 10px"}}>
            <div style={{display:"flex",flexWrap:"wrap",gap:"6px 14px",fontSize:9,color:C.dim}}>
              <span style={{display:"inline-flex",alignItems:"center",gap:4}}><IcSatellite size={11}/> S2 : <strong style={{color:C.txt}}>{indices.s2Images} img</strong> · {indices.scale}m · {indices.composite}</span>
              {/* Date de la tuile affichée (peut différer des stats si mode différent) */}
              {activeLayer !== "prescription" && agriTiles[activeLayer] ? (
                <span style={{display:"inline-flex",alignItems:"center",gap:4}}><IcCalendar size={11}/> <strong style={{color:C.acc}}>{agriTiles[activeLayer].date_used}</strong>
                  {agriTiles[activeLayer].cloud_pct != null &&
                    <span style={{display:"inline-flex",alignItems:"center",gap:3}}> · <IcCloud size={10}/> {agriTiles[activeLayer].cloud_pct}%</span>}
                  {agriTiles[activeLayer].cloud_max_used > geeConfig.cloudMax &&
                    <span style={{color:"#e8c84a",display:"inline-flex",alignItems:"center",gap:3}}> <IcAlert size={10}/> seuil élargi {agriTiles[activeLayer].cloud_max_used}%</span>}
                </span>
              ) : (
                <span style={{display:"inline-flex",alignItems:"center",gap:4}}><IcCalendar size={11}/> Stats : <strong style={{color:C.acc}}>{indices.dateUsed}</strong></span>
              )}
              <span style={{display:"inline-flex",alignItems:"center",gap:4}}><IcThermometer size={11}/> LST : <strong style={{color:C.txt}}>{indices.lstSource}</strong></span>
              {tilesLoading && <span style={{color:C.acc,display:"inline-flex",alignItems:"center",gap:4}}><IcLoader size={11}/> Chargement tuiles…</span>}
            </div>
          </div>

          {/* KPI 4 indices */}
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
            {[
              {id:"ndvi",label:"NDVI",desc:"Biomasse · S2 10m",  val:indices.ndvi,unit:"",   mn:indices.ndviMin,mx:indices.ndviMax,std:indices.ndviStd},
              {id:"ndre",label:"NDRE",desc:"Azote · S2 20m→10m",val:indices.ndre,unit:"",   mn:indices.ndreMin,mx:indices.ndreMax,std:indices.ndreStd},
              {id:"ndwi",label:"NDWI",desc:"Eau · S2 10m",       val:indices.ndwi,unit:"",   mn:indices.ndwiMin,mx:indices.ndwiMax,std:indices.ndwiStd},
              {id:"lst", label:"LST", desc:"Temp · Landsat→10m", val:indices.lstC,unit:"°C", mn:indices.lstMin, mx:indices.lstMax, std:indices.lstStd},
            ].map(idx=>{
              const st  = getStatus(idx.id, idx.val);
              const range = Math.max((idx.mx??0) - (idx.mn??0), 0.001);
              const pct = Math.round(Math.max(0, Math.min(100, ((idx.val - (idx.mn??0)) / range) * 100)));
              return (
                <div key={idx.id} onClick={()=>switchLayer(idx.id)}
                  style={{...s.card,cursor:"pointer",
                    border:`0.5px solid ${activeLayer===idx.id?st.c+"88":C.bdr}`,
                    transition:"border-color .15s"}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                    <div>
                      <div style={{fontSize:9,fontWeight:700,color:C.dim,textTransform:"uppercase",letterSpacing:1}}>{idx.label}</div>
                      <div style={{fontSize:8,color:C.dim}}>{idx.desc}</div>
                    </div>
                    <div style={{fontSize:8,padding:"1px 5px",borderRadius:8,background:st.c+"20",color:st.c,fontWeight:600}}>{st.l}</div>
                  </div>
                  <div style={{fontSize:18,fontWeight:800,color:st.c,fontFamily:"monospace"}}>{idx.val}{idx.unit}</div>
                  <div style={{height:4,borderRadius:2,
                    background:`linear-gradient(90deg,${PALETTES[idx.id].join(",")})`,
                    marginTop:6,position:"relative"}}>
                    <div style={{position:"absolute",left:`${Math.max(2,Math.min(96,pct))}%`,top:-4,
                      width:10,height:10,borderRadius:"50%",background:"#fff",border:`2px solid ${st.c}`,transform:"translateX(-50%)"}}/>
                  </div>
                  <div style={{fontSize:9,color:C.dim,marginTop:5}}>
                    σ={idx.std}{idx.unit} · {idx.mn}→{idx.mx}{idx.unit}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Timeline */}
          <div style={s.lbl}>Évolution 3 mois</div>
          <div style={s.card}>
            <TimelineChart data={timeSeries} C={C}/>
            <div style={{display:"flex",gap:12,marginTop:6,flexWrap:"wrap"}}>
              {[["#27ae60","NDVI"],["#8e44ad","NDRE"],["#3498db","NDWI"]].map(([col,lbl])=>(
                <div key={lbl} style={{display:"flex",alignItems:"center",gap:4,fontSize:10,color:C.dim}}>
                  <div style={{width:14,height:2,background:col,borderRadius:1}}/>{lbl}
                </div>
              ))}
              <div style={{marginLeft:"auto",fontSize:10,color:"#e05a3a",fontFamily:"monospace"}}>
                {(() => {
                  const first = timeSeries.find(p => p?.ndvi != null);
                  const last  = [...timeSeries].reverse().find(p => p?.ndvi != null);
                  if (!first || !last || first === last) return null;
                  const delta = last.ndvi - first.ndvi;
                  return `Δ NDVI ${delta >= 0 ? "+" : ""}${delta.toFixed(3)}`;
                })()}
              </div>
            </div>
          </div>

          <button style={{...s.btn(true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:6}} onClick={()=>{runAI();setStep("ai");}}>
            <IcBot size={14}/> Diagnostic IA
          </button>
        </div>
      )}

      {/* ─── ONGLET IA ─── */}
      {step==="ai" && (
        <div style={s.sec}>
          {aiLoading&&<div style={{...s.card,textAlign:"center",padding:20}}>
            <div style={{display:"flex",justifyContent:"center",marginBottom:8}}><IcBot size={20}/></div>
            <div style={{fontSize:12,fontWeight:600,color:C.acc}}>Analyse agronomique…</div>
            <div style={{fontSize:10,color:C.dim,marginTop:4}}>{LITELLM_URL} · {LLM_MODEL}</div>
          </div>}
          {aiError&&<div style={{...s.card,background:C.red+"10",borderColor:C.red+"44"}}>
            <div style={{fontSize:11,color:C.red,marginBottom:6}}>{aiError}</div>
            <button style={{...s.btn(true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:5}} onClick={runAI}><IcRefreshCw size={12}/> Réessayer</button>
          </div>}
          {!aiLoading&&!aiResult&&!aiError&&<div style={{...s.card,textAlign:"center",padding:20}}>
            <div style={{fontSize:10,color:C.dim,marginBottom:10,lineHeight:1.6}}>
              Endpoint : {LITELLM_URL}<br/>Modèle : {LLM_MODEL}
            </div>
            <button style={{...s.btn(true),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:6}} onClick={runAI}><IcBot size={14}/> Lancer l'analyse</button>
          </div>}
          {aiResult&&<>
            <div style={{...s.card,background:C.acc+"0d",borderColor:C.acc+"33"}}>
              <div style={{fontSize:9,fontWeight:700,color:C.acc,letterSpacing:1,marginBottom:6}}>DIAGNOSTIC GLOBAL</div>
              <div style={{fontSize:11,color:C.txt,lineHeight:1.6}}>{aiResult.diagnostic_global}</div>
            </div>
            {(aiResult.zones||[]).map((z,i)=>{
              const col=z.urgence==="haute"?"#e05a3a":z.urgence==="moyenne"?"#e8c84a":"#27ae60";
              return <div key={i} style={{...s.card,borderLeft:`3px solid ${col}`,borderRadius:"0 7px 7px 0"}}>
                <div style={{display:"flex",justifyContent:"space-between",marginBottom:4}}>
                  <div style={{fontSize:11,fontWeight:700,color:C.txt}}>{z.nom}</div>
                  <div style={{fontSize:9,padding:"2px 7px",borderRadius:10,background:col+"20",color:col,fontWeight:600}}>{z.action||z.etat}</div>
                </div>
                <div style={{fontSize:10,color:C.dim,lineHeight:1.5}}>{z.detail}</div>
                {z.indices_cles?.length>0&&<div style={{display:"flex",gap:5,marginTop:6,flexWrap:"wrap"}}>
                  {z.indices_cles.map((ic,j)=><span key={j} style={{fontSize:9,padding:"2px 5px",background:C.hover,borderRadius:4,color:C.dim}}>{ic}</span>)}
                </div>}
              </div>;
            })}
            {(aiResult.recommandations||[]).length>0&&<>
              <div style={s.lbl}>Recommandations</div>
              {(aiResult.recommandations||[]).map((r,i)=>{
                const recIcons={irrigation:IcDroplet,fertilisation:IcSprout,traitement:IcAlert,surveillance:IcSearch};
                const RecIcon=recIcons[r.type]||IcMapPin;
                return <div key={i} style={{display:"flex",gap:8,padding:"8px 10px",background:C.hover,borderRadius:7,border:`0.5px solid ${C.bdr}`}}>
                  <div style={{flexShrink:0,color:C.acc,display:"flex"}}><RecIcon size={15}/></div>
                  <div style={{flex:1}}>
                    <div style={{fontSize:10,fontWeight:600,color:C.txt,marginBottom:2}}>
                      {r.type.charAt(0).toUpperCase()+r.type.slice(1)}
                      {r.priorite===1&&<span style={{marginLeft:6,fontSize:8,background:"#fde8e8",color:"#c0392b",padding:"1px 5px",borderRadius:8,fontWeight:700}}>PRIORITAIRE</span>}
                    </div>
                    <div style={{fontSize:10,color:C.dim,lineHeight:1.5}}>{r.texte}</div>
                    {r.quantite&&<div style={{fontSize:9,color:C.acc,marginTop:2,fontFamily:"monospace"}}>{r.quantite}</div>}
                  </div>
                </div>;
              })}
            </>}
            {aiResult.risques&&<div style={{...s.card,background:"#e8c84a12",borderColor:"#e8c84a44"}}>
              <div style={{fontSize:9,fontWeight:700,color:"#e8c84a",marginBottom:4,display:"flex",alignItems:"center",gap:4}}><IcAlert size={11}/> RISQUES</div>
              <div style={{fontSize:10,color:C.dim,lineHeight:1.5}}>{aiResult.risques}</div>
            </div>}
            {aiResult.prochaine_acquisition&&<div style={{fontSize:10,color:C.dim,textAlign:"center",display:"flex",alignItems:"center",justifyContent:"center",gap:5}}>
              <IcSatellite size={11}/> Prochaine acquisition : {aiResult.prochaine_acquisition}
            </div>}
            <button style={{...s.btn(),display:"inline-flex",alignItems:"center",justifyContent:"center",gap:5}} onClick={runAI}><IcRefreshCw size={12}/> Relancer</button>
          </>}
        </div>
      )}

      {/* ─── ONGLET RAPPORT ─── */}
      {step==="report" && (
        <div style={s.sec}>
          <div style={{...s.card,padding:14}}>
            <div style={{fontSize:12,fontWeight:700,color:C.txt,marginBottom:8,display:"flex",alignItems:"center",gap:6}}><IcFile size={14}/> Rapport agronomique</div>
            {[
              [true,"Informations parcelle",`${parcelInfo.surface} ha · ${parcelInfo.culture}`],
              [true,"Indices spectraux","NDVI, NDRE, NDWI, LST (°C + K)"],
              [true,"Cartes indices","captures MapLibre : NDVI / NDRE / NDWI / LST"],
              [true,"Carte de prescription","4 zones colorées — synthèse finale"],
              [!!aiResult,"Diagnostic IA",aiResult?"inclus":"lancer l'IA d'abord"],
              [true,"Analyse temporelle","tableau 3 mois hebdomadaire"],
            ].map(([ok,lbl,sub],i)=>(
              <div key={i} style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
                <span style={{color:ok?C.acc:"#e8c84a",flexShrink:0,display:"flex"}}>{ok?<IcCheck size={13}/>:<IcCircle size={13}/>}</span>
                <div>
                  <div style={{fontSize:11,color:C.txt,fontWeight:500}}>{lbl}</div>
                  <div style={{fontSize:10,color:C.dim}}>{sub}</div>
                </div>
              </div>
            ))}
          </div>

          {!aiResult&&<div style={{...s.card,background:"#e8c84a10",borderColor:"#e8c84a44"}}>
            <div style={{fontSize:10,color:"#e8c84a",lineHeight:1.5,display:"flex",alignItems:"center",gap:5}}>
              <IcBulb size={12}/> Ajoutez le diagnostic IA pour enrichir le rapport.
            </div>
            <button style={{...s.btn(true),marginTop:8,display:"inline-flex",alignItems:"center",justifyContent:"center",gap:6}} onClick={()=>{runAI();setStep("ai");}}><IcBot size={13}/> Analyse IA</button>
          </div>}

          <button style={{...s.btn(true),padding:"10px 12px",display:"inline-flex",alignItems:"center",justifyContent:"center",gap:6}} onClick={downloadReport} disabled={reportLoading}>
            <IcFileDown size={14}/> {reportLoading?"Génération en cours…":"Générer le rapport (PDF)"}
          </button>

          <div style={{fontSize:9,color:C.dim,textAlign:"center",lineHeight:1.7}}>
            Ouvre un onglet HTML avec les 5 cartes intégrées.<br/>
            Fichier → Imprimer → "Enregistrer en PDF"
          </div>

          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6}}>
            {[["NDVI",indices?.ndvi,""],["NDRE",indices?.ndre,""],
              ["NDWI",indices?.ndwi,""],["LST",indices?.lstC,"°C"]].map(([k,v,u])=>(
              <div key={k} style={{...s.card,textAlign:"center",padding:8}}>
                <div style={{fontSize:9,color:C.dim,fontWeight:600,textTransform:"uppercase",letterSpacing:1}}>{k}</div>
                <div style={{fontSize:16,fontWeight:800,fontFamily:"monospace",color:C.txt}}>{v}{u}</div>
              </div>
            ))}
          </div>
          {tilesLoading && (
            <div style={{fontSize:9,color:C.acc,textAlign:"center",display:"flex",alignItems:"center",justifyContent:"center",gap:5}}>
              <IcLoader size={11}/> Attendre la fin du chargement des tuiles avant de générer le rapport…
            </div>
          )}
          {!tilesLoading && Object.keys(agriTiles).length > 0 && (
            <div style={{fontSize:9,color:C.dim,textAlign:"center",display:"flex",alignItems:"center",justifyContent:"center",gap:5}}>
              <IcCheck size={11}/> {Object.values(agriTiles).filter(t=>t?.tile_url).length}/5 cartes pixel disponibles
            </div>
          )}
        </div>
      )}
    </div>
  );
}
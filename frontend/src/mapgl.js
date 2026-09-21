/**
 * mapgl.js — Abstraction du MOTEUR cartographique : MapLibre GL ⟷ Mapbox GL.
 *
 * L'appli peut basculer entre les deux moteurs. Comme react-map-gl v8 expose deux
 * points d'entrée distincts (`react-map-gl/maplibre` et `react-map-gl/mapbox`) liés
 * chacun à sa bibliothèque, et que les composants `Source`/`Layer` partagent le contexte
 * de LEUR `Map`, tout l'arbre carte doit importer depuis le MÊME point d'entrée. Ce
 * module choisit le moteur au chargement (localStorage) et ré-exporte l'ensemble cohérent.
 *
 * Le changement de moteur se fait par RECHARGEMENT de page (setMapEngine + reload) :
 * les imports statiques sont figés à l'évaluation du module, on ne peut pas les
 * permuter à chaud. MapLibre reste le moteur PAR DÉFAUT (aucune régression possible).
 *
 * Les deux API sont quasi identiques (addSource/addLayer/paint, Marker, Popup,
 * fitBounds, NavigationControl…), donc Source/Layer/Marker fonctionnent des deux côtés.
 * Les rares divergences (projection globe, terrain) sont gérées là où elles se trouvent.
 */
import { MAPBOX_TOKEN } from "./config";

import "maplibre-gl/dist/maplibre-gl.css";
import "mapbox-gl/dist/mapbox-gl.css";
import maplibregl from "maplibre-gl";
import mapboxgl from "mapbox-gl";
import * as RMLibre from "react-map-gl/maplibre";
import * as RMBox from "react-map-gl/mapbox";

function detectEngine() {
  try {
    return localStorage.getItem("mapEngine") === "mapbox" ? "mapbox" : "maplibre";
  } catch (_) {
    return "maplibre";
  }
}

export const MAP_ENGINE = detectEngine();
export const IS_MAPBOX = MAP_ENGINE === "mapbox";

// Token Mapbox (facultatif : les fonds OpenFreeMap/Esri s'affichent sans token ;
// il n'est requis que pour les styles natifs mapbox://…).
if (IS_MAPBOX) {
  try { mapboxgl.accessToken = MAPBOX_TOKEN || ""; } catch (_) { /* noop */ }
}
// Exposé pour les utilitaires hors React (deck3d, etc.) sans réimporter ce module.
try { if (typeof window !== "undefined") window.__MAP_ENGINE__ = MAP_ENGINE; } catch (_) {}

const R   = IS_MAPBOX ? RMBox : RMLibre;      // wrappers React du moteur choisi
const RAW = IS_MAPBOX ? mapboxgl : maplibregl; // bibliothèque brute (classes impératives)

// ── Composants React (Map = export PAR DÉFAUT de react-map-gl, d'où R.default) ──
export default (R.default || R.Map);
export const Source            = R.Source;
export const Layer             = R.Layer;
export const Popup             = R.Popup;
export const Marker            = R.Marker;
export const NavigationControl = R.NavigationControl;
export const ScaleControl      = R.ScaleControl;
export const AttributionControl = R.AttributionControl;
export const useMap            = R.useMap;
export const useControl        = R.useControl;
export const MapProvider       = R.MapProvider;

// ── Classes impératives brutes (marqueurs/popups posés à la main sur la carte) ──
export const GLMarker = RAW.Marker;
export const GLPopup  = RAW.Popup;
export const GLMap    = RAW.Map;
export const gl       = RAW;

/** Change de moteur et recharge la page (les imports statiques imposent le reload). */
export function setMapEngine(engine, viewState) {
  try {
    localStorage.setItem("mapEngine", engine === "mapbox" ? "mapbox" : "maplibre");
    if (viewState) localStorage.setItem("mapVS", JSON.stringify(viewState));
  } catch (_) { /* noop */ }
  try { window.location.reload(); } catch (_) {}
}

export const API = "/api";

/**
 * Saisie libre du chat activée ou non — pilotée au build par VITE_CHAT_INPUT.
 *
 *   VITE_CHAT_INPUT=false   → la zone de saisie disparaît (instance en lecture seule)
 *   absente ou toute autre valeur → saisie active (comportement par défaut)
 *
 * Seule la saisie PAR L'UTILISATEUR est retirée : les envois programmatiques
 * déclenchés par les autres panneaux continuent de fonctionner.
 *
 * Vite fige les `import.meta.env` au moment du build : changer la variable
 * impose de relancer `npm run build`, un redémarrage du serveur ne suffit pas.
 */
export const CHAT_INPUT_ENABLED = !["false", "0", "off", "no"].includes(
  String(import.meta.env.VITE_CHAT_INPUT ?? "true").trim().toLowerCase()
);
export const F = "'DM Sans',system-ui,sans-serif";
export const M = "'JetBrains Mono',monospace";
// Token Mapbox fourni au build via VITE_MAPBOX_TOKEN (jamais en dur — dépôt public).
export const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN || "";

export const MAP_STYLES = {
  dark: "https://tiles.openfreemap.org/styles/dark",
  liberty: "https://tiles.openfreemap.org/styles/liberty",
  positron: "https://tiles.openfreemap.org/styles/positron",

  satellite: {
    version: 8,
    sources: {
      esri: {
        type: "raster",
        tiles: [
          "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
        ],
        tileSize: 256,
        attribution: "© Esri"
      }
    },
    layers: [
      {
        id: "satellite",
        type: "raster",
        source: "esri"
      }
    ]
  },

  // ── Autres planètes (fonds raster Web-Mercator OpenPlanetaryMap) ──
  // Se visualisent en mode Globe comme la Terre. Tuiles Mercator → les pôles
  // ne sont pas couverts (petit trou au sommet du globe), limite connue.
  mars: {
    version: 8,
    sources: {
      mars: {
        type: "raster",
        tiles: ["https://cartocdn-gusc.global.ssl.fastly.net/opmbuilder/api/v1/map/named/opm-mars-basemap-v0-2/all/{z}/{x}/{y}.png"],
        tileSize: 256, maxzoom: 8,
        attribution: "Mars © NASA / USGS / OpenPlanetaryMap"
      }
    },
    layers: [{ id: "mars", type: "raster", source: "mars" }]
  },
  moon: {
    version: 8,
    sources: {
      moon: {
        type: "raster",
        tiles: ["https://cartocdn-gusc.global.ssl.fastly.net/opmbuilder/api/v1/map/named/opm-moon-basemap-v0-1/all/{z}/{x}/{y}.png"],
        tileSize: 256, maxzoom: 7,
        attribution: "Moon © NASA / USGS / OpenPlanetaryMap"
      }
    },
    layers: [{ id: "moon", type: "raster", source: "moon" }]
  },
  mercury: {
    version: 8,
    sources: {
      mercury: {
        type: "raster",
        tiles: ["https://cartocdn-gusc.global.ssl.fastly.net/opmbuilder/api/v1/map/named/opm-mercury-basemap-v0-1/all/{z}/{x}/{y}.png"],
        tileSize: 256, maxzoom: 6,
        attribution: "Mercury © NASA / USGS / OpenPlanetaryMap"
      }
    },
    layers: [{ id: "mercury", type: "raster", source: "mercury" }]
  }
};

// Fonds « planète » (à séparer des fonds Terre dans le sélecteur).
// Limités aux corps disposant de tuiles Web-Mercator publiques (OpenPlanetaryMap :
// Terre, Mercure, Mars, Lune). Vénus et les géantes gazeuses n'ont pas de tuiles
// slippy-map compatibles MapLibre (seulement des textures équirectangulaires).
export const PLANETS = [
  { key: "earth",   label: "Terre",   icon: "🌍" },
  { key: "mercury", label: "Mercure", icon: "☿" },
  { key: "mars",    label: "Mars",    icon: "🔴" },
  { key: "moon",    label: "Lune",    icon: "🌕" },
];
export const PLANET_KEYS = ["mercury", "mars", "moon"];

export const LAYER_COLORS = [
  "#EF9F27", "#378ADD", "#D4537E", "#1D9E75",
  "#D85A30", "#7F77DD", "#639922",
];

export const RAMPS = {
  // ── Séquentielles ──────────────────────────────────────────────
  viridis: ["#440154","#482777","#3e4989","#31688e","#26828e","#1f9e89","#35b779","#6ece58","#b5de2b","#fde725"],
  greens:  ["#edf8e9","#c7e9c0","#a1d99b","#74c476","#41ab5d","#238b45","#005a32"],
  blues:   ["#08306b","#08519c","#2171b5","#4292c6","#6baed6","#9ecae1","#c6dbef","#deebf7"],
  oranges: ["#fff5eb","#fdd0a2","#fdae6b","#f16913","#d94801","#8c2d04"],
  purples: ["#fcfbfd","#dadaeb","#bcbddc","#9e9ac8","#756bb1","#54278f"],
  reds:    ["#67000d","#a50f15","#cb181d","#ef3b2c","#fb6a4a","#fc9272","#fcbba1","#fff5f0"],
  magma:   ["#000004","#3b0f70","#8c2981","#de4968","#fea16e","#fcfdbf"],
  plasma:  ["#0d0887","#6a00a8","#b12a90","#e16462","#fca636","#f0f921"],
  cividis: ["#00204d","#31446b","#666970","#a69d75","#e4cf5b","#ffea46"],
  ylgnbu:  ["#ffffcc","#c7e9b4","#7fcdbb","#41b6c4","#2c7fb8","#253494"],
  turbo:   ["#30123b","#4145ab","#26bce1","#5dea6a","#f0c342","#e33b0b"],
  terrain: ["#2c7bb6","#7fcdbb","#c7e9b4","#ffffcc","#fdae61","#d7191c","#7a4a24"],
  // ── Divergentes ────────────────────────────────────────────────
  spectral: ["#9e0142","#d53e4f","#f46d43","#fdae61","#fee08b","#e6f598","#abdda4","#66c2a5","#3288bd"],
  rdylgn:   ["#a50026","#f46d43","#fee08b","#d9ef8b","#66bd63","#006837"],
  rdbu:     ["#b2182b","#ef8a62","#fddbc7","#d1e5f0","#67a9cf","#2166ac"],
  brbg:     ["#8c510a","#d8b365","#f6e8c3","#c7eae5","#5ab4ac","#01665e"],
  piyg:     ["#c51b7d","#de77ae","#f1b6da","#e6f5d0","#7fbc41","#4d9221"],
  // ── Catégorielles ──────────────────────────────────────────────
  categorial: ["#1D9E75","#EF9F27","#378ADD","#D4537E","#D85A30","#7F77DD","#639922","#E24B4A","#BA7517","#534AB7"],
  set2:   ["#66c2a5","#fc8d62","#8da0cb","#e78ac3","#a6d854","#ffd92f","#e5c494","#b3b3b3"],
  tab10:  ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f"],
  pastel: ["#b3e2cd","#fdcdac","#cbd5e8","#f4cae4","#e6f5c9","#fff2ae","#f1e2cc","#cccccc"],
  dark2:  ["#1b9e77","#d95f02","#7570b3","#e7298a","#66a61e","#e6ab02","#a6761d","#666666"],
};

// Rampes regroupées façon QGIS (maquette symbologie-couches)
export const RAMP_GROUPS = {
  "Séquentiel": ["viridis","greens","blues","oranges","purples","reds","magma","plasma","cividis","ylgnbu","turbo","terrain"],
  "Divergent":  ["spectral","rdylgn","rdbu","brbg","piyg"],
  "Catégoriel": ["categorial","set2","tab10","pastel","dark2"],
};
export const RAMP_NAMES = {
  viridis:"Viridis", greens:"Verts", blues:"Bleus", oranges:"Oranges", purples:"Violets", reds:"Rouges",
  magma:"Magma", plasma:"Plasma", cividis:"Cividis", ylgnbu:"YlGnBu", turbo:"Turbo", terrain:"Terrain",
  spectral:"Spectral", rdylgn:"RdYlGn", rdbu:"RdBu", brbg:"BrBG", piyg:"PiYG",
  categorial:"Catégoriel", set2:"Set2", tab10:"Tab10", pastel:"Pastel", dark2:"Dark2",
};
// Rampes catégorielles (couleurs discrètes, pas interpolées)
export const CAT_RAMPS = new Set(["categorial","set2","tab10","pastel","dark2"]);

export const EXPORT_FORMATS = ["GeoJSON", "GeoPackage", "Shapefile", "CSV", "FlatGeobuf"];

// Palettes bivariées 3×3 : index = classeA*3 + classeB (A = axe vertical, B = horizontal).
export const BIVARIATE_PALETTES = {
  "Teal / Rouge":   ["#e8e8e8","#e4acac","#c85a5a","#b0d5df","#ad9ea5","#985356","#64acbe","#627f8c","#574249"],
  "Bleu / Violet":  ["#e8e8e8","#b0d5df","#64acbe","#d6b0c9","#9fa5c1","#5a95b0","#c85aa2","#8c6296","#3b4994"],
  "Vert / Magenta": ["#e8e8e8","#e4acd7","#c85ab0","#b8d6a2","#a99ea0","#8d5385","#73ae4f","#5a8f52","#2a5a3b"],
  "Orange / Bleu":  ["#e8e8e8","#c3b3d8","#8c62aa","#f3b48a","#b79ca8","#7e5688","#e6842d","#a67231","#804d2a"],
};

export function hexToRgb(hex) {
  return [parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16)];
}



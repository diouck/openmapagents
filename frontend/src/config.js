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
export const MAPBOX_TOKEN = "pk.eyJ1IjoiZGlvdWNrIiwiYSI6ImNrc3E2NmlrdDA5djkydm1kMXo0NGRyOW8ifQ.B_LfncIjrhY-STiNTseOGQ";

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
  }
};

export const LAYER_COLORS = [
  "#EF9F27", "#378ADD", "#D4537E", "#1D9E75",
  "#D85A30", "#7F77DD", "#639922",
];

export const RAMPS = {
  viridis: ["#440154","#482777","#3e4989","#31688e","#26828e","#1f9e89","#35b779","#6ece58","#b5de2b","#fde725"],
  spectral: ["#9e0142","#d53e4f","#f46d43","#fdae61","#fee08b","#e6f598","#abdda4","#66c2a5","#3288bd"],
  blues: ["#08306b","#08519c","#2171b5","#4292c6","#6baed6","#9ecae1","#c6dbef","#deebf7"],
  reds: ["#67000d","#a50f15","#cb181d","#ef3b2c","#fb6a4a","#fc9272","#fcbba1","#fff5f0"],
  categorial: ["#1D9E75","#EF9F27","#378ADD","#D4537E","#D85A30","#7F77DD","#639922","#E24B4A","#BA7517","#534AB7"],
};

export const EXPORT_FORMATS = ["GeoJSON", "GeoPackage", "Shapefile", "CSV", "FlatGeobuf"];

export function hexToRgb(hex) {
  return [parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16)];
}



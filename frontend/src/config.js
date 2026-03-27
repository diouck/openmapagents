export const API = "/api";
export const F = "'DM Sans',system-ui,sans-serif";
export const M = "'JetBrains Mono',monospace";
export const MAPBOX_TOKEN = "pk.eyJ1IjoiZGlvdWNrIiwiYSI6ImNrc3E2NmlrdDA5djkydm1kMXo0NGRyOW8ifQ.B_LfncIjrhY-STiNTseOGQ";

export const MAP_STYLES = {
  dark: "https://tiles.openfreemap.org/styles/dark",
  liberty: "https://tiles.openfreemap.org/styles/liberty",
  positron: "https://tiles.openfreemap.org/styles/positron",
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
/**
 * timelapse.js — Configuration et chargement d'une animation temporelle.
 *
 * Le pas de temps proposé dépend de la revisite du capteur : offrir « jour » sur
 * du Landsat (16 j) ne produirait que des frames vides. La liste ci-dessous
 * suit la colonne `temporal` du catalogue backend.
 */

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

export const STEP_LABELS = {
  day: "Jour", "5day": "5 jours", "8day": "8 jours", "16day": "16 jours",
  month: "Mois", quarter: "Trimestre", year: "Année",
};

// Jeux sans dimension temporelle → pas d'animation possible (miroir de _TL_STATIC).
export const TL_STATIC = new Set([
  "srtm", "copdem", "etopo", "canopy_height", "gedi_agb",
  "soil_soc", "soil_ph", "soil_clay",
  "hansen", "worldcover", "jrc_water", "gpw_pop",
]);

const STEPS_BY_DATASET = {
  // Quotidien / infra-quotidien
  modis_lst:   ["day", "5day", "month", "year"],
  sentinel5p:  ["day", "5day", "month", "year"],
  chirps:      ["day", "5day", "month", "year"],
  firms:       ["day", "5day", "month", "year"],
  modis_snow:  ["day", "5day", "month", "year"],
  smap:        ["day", "5day", "month", "year"],
  // Fumée d'incendie : dispersion jour par jour (curseur temporel sur quelques jours).
  geos_cf:     ["day", "5day"],
  cams:        ["day", "5day"],
  // Composites courts
  modis_lai:   ["5day", "8day", "month", "year"],
  modis_et:    ["8day", "month", "quarter", "year"],
  modis_gpp:   ["8day", "month", "quarter", "year"],
  sentinel2:   ["5day", "16day", "month", "quarter", "year"],
  dynamicworld:["5day", "16day", "month", "quarter", "year"],
  sentinel1:   ["16day", "month", "quarter", "year"],
  modis_ndvi:  ["16day", "month", "quarter", "year"],
  landsat:     ["16day", "month", "quarter", "year"],
  landsat8:    ["16day", "month", "quarter", "year"],
  landsat9:    ["16day", "month", "quarter", "year"],
  // Mensuel natif
  era5:        ["month", "quarter", "year"],
  era5_solar:  ["month", "quarter", "year"],
  era5_wind:   ["month", "quarter", "year"],
  viirs:       ["month", "quarter", "year"],
  gpm:         ["month", "quarter", "year"],
  burned:      ["month", "quarter", "year"],
  // Millésimes tous les 5 ans
  ghsl_pop:    ["vintage"],
  ghsl_built:  ["vintage"],
  ghsl_smod:   ["vintage"],
};

export function stepsFor(dataset) {
  return STEPS_BY_DATASET[dataset] || ["month", "quarter", "year"];
}

export function defaultStep(dataset) {
  const s = stepsFor(dataset);
  return s.includes("month") ? "month" : s[0];
}

export const canAnimate = (dataset) => !!dataset && !TL_STATIC.has(dataset);

/**
 * Millésimes GHSL : les époques existent tous les 5 ans. Un pas « année » créerait
 * 4 frames vides sur 5 — on envoie donc les périodes explicitement.
 * 2030 est une PROJECTION, pas une observation : on s'arrête à l'époque échue.
 */
export function vintagePeriods(dataset) {
  if (!dataset?.startsWith("ghsl_")) return null;
  const last = Math.min(2025, Math.floor(new Date().getFullYear() / 5) * 5);
  const out = [];
  for (let y = 1975; y <= last; y += 5) {
    out.push({ date_start: `${y}-01-01`, date_end: `${y}-12-31`, label: String(y) });
  }
  return out;
}

/** Période par défaut proposée : les 2 dernières années (12 frames mensuelles). */
export function defaultRange(step) {
  const end = new Date();
  const start = new Date(end);
  if (step === "year") start.setFullYear(end.getFullYear() - 10);
  else if (step === "quarter") start.setFullYear(end.getFullYear() - 4);
  else if (step === "month") start.setFullYear(end.getFullYear() - 2);
  else start.setMonth(end.getMonth() - 6);
  const iso = d => d.toISOString().slice(0, 10);
  return { date_start: iso(start), date_end: iso(end) };
}

async function post(url, body, signal) {
  const res = await fetch(`${API}${url}`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body), signal,
  });
  const d = await res.json();
  if (!res.ok) throw new Error(d.detail || `Erreur ${res.status}`);
  return d;
}

export const planTimelapse  = (body, signal) => post("/api/gee/timelapse/plan", body, signal);
export const fetchFrames    = (body, signal) => post("/api/gee/timelapse/frames", body, signal);
// Export : assemblé CÔTÉ SERVEUR (vignettes GEE + Pillow + imageio), comme le
// module GIF. L'enregistrement du canvas par MediaRecorder produisait un
// conteneur fragmenté sans durée, que la plupart des lecteurs réduisaient à une
// seule image.
export const exportAnimation = (body, signal) => post("/api/gee/timelapse/export", body, signal);
export const API_BASE = API;

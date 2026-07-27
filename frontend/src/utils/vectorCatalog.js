/**
 * vectorCatalog.js — Catalogue de jeux VECTORIELS ajoutables sur la carte.
 *
 * Deux sources :
 *   • source:"gee" → FeatureCollection Earth Engine, chargée via /api/gee/vector
 *     (découpée sur l'emprise carte, plafonnée en nombre d'entités).
 *   • source:"url" → GeoJSON public chargé directement par le navigateur
 *     (ex. séismes USGS — absents du catalogue GEE).
 *
 * Chaque entrée : { id, name, desc, theme, geom, source, asset|url,
 *                   worldOk (chargeable sans emprise), simplify_m, attrs }
 *
 * ⚠️ Identifiants d'assets GEE à confirmer au déploiement (GEE non testable en local).
 */
export const VECTOR_CATALOG = [
  // ── Administratif ──────────────────────────────────────────────
  { id: "lsib", name: "Frontières des pays (LSIB)", desc: "Limites internationales simplifiées.",
    theme: "Administratif", geom: "polygon", source: "gee", asset: "USDOS/LSIB_SIMPLE/2017",
    worldOk: true, simplify_m: 1000, attrs: ["country_na"] },
  { id: "gaul1", name: "Régions / provinces (GAUL 1)", desc: "Découpage administratif de 1er niveau.",
    theme: "Administratif", geom: "polygon", source: "gee", asset: "FAO/GAUL_SIMPLIFIED_500m/2015/level1",
    simplify_m: 1000, attrs: ["ADM0_NAME", "ADM1_NAME"] },
  { id: "gaul2", name: "Districts / communes (GAUL 2)", desc: "Découpage administratif de 2e niveau.",
    theme: "Administratif", geom: "polygon", source: "gee", asset: "FAO/GAUL_SIMPLIFIED_500m/2015/level2",
    simplify_m: 500, attrs: ["ADM1_NAME", "ADM2_NAME"] },
  { id: "us_states", name: "États des États-Unis", desc: "US Census TIGER — états et territoires.",
    theme: "Administratif", geom: "polygon", source: "gee", asset: "TIGER/2018/States",
    worldOk: true, simplify_m: 1000, attrs: ["NAME", "STUSPS"] },
  { id: "us_counties", name: "Comtés des États-Unis", desc: "US Census TIGER — comtés.",
    theme: "Administratif", geom: "polygon", source: "gee", asset: "TIGER/2016/Counties",
    simplify_m: 500, attrs: ["NAME"] },

  // ── Infrastructure & réseaux ───────────────────────────────────
  { id: "us_roads", name: "Routes (États-Unis)", desc: "US Census TIGER — réseau routier (lignes).",
    theme: "Infrastructure", geom: "line", source: "gee", asset: "TIGER/2016/Roads",
    simplify_m: 30, attrs: ["fullname", "rttyp"] },
  { id: "rivers", name: "Rivières (monde)", desc: "HydroSHEDS — cours d'eau à écoulement libre.",
    theme: "Infrastructure", geom: "line", source: "gee", asset: "WWF/HydroSHEDS/v1/FreeFlowingRivers",
    simplify_m: 200, attrs: ["RIV_ORD"] },
  { id: "power", name: "Centrales électriques", desc: "WRI Global Power Plant Database (points).",
    theme: "Infrastructure", geom: "point", source: "gee", asset: "WRI/GPPD/power_plants",
    worldOk: true, attrs: ["name", "fuel1", "capacitymw"] },

  // ── Environnement & nature ─────────────────────────────────────
  { id: "ecoregions", name: "Écorégions terrestres", desc: "RESOLVE 2017 — 846 écorégions du monde.",
    theme: "Environnement", geom: "polygon", source: "gee", asset: "RESOLVE/ECOREGIONS/2017",
    simplify_m: 1000, attrs: ["ECO_NAME", "BIOME_NAME"] },
  { id: "wdpa", name: "Aires protégées (WDPA)", desc: "Base mondiale des aires protégées (polygones).",
    theme: "Environnement", geom: "polygon", source: "gee", asset: "WCMC/WDPA/current/polygons",
    simplify_m: 200, attrs: ["NAME", "DESIG_ENG", "IUCN_CAT"] },

  // ── Aléas & événements — flux publics DIRECTS, sans clé ni connexion ──
  { id: "eq_month", name: "Séismes — 30 derniers jours", desc: "Flux temps réel USGS, tous magnitudes (points).",
    theme: "Aléas", geom: "point", source: "url", worldOk: true, temporal: true,
    url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
    attrs: ["mag", "place", "time"] },
  { id: "eq_week", name: "Séismes M≥2,5 — 7 jours", desc: "Flux temps réel USGS (points).",
    theme: "Aléas", geom: "point", source: "url", worldOk: true, temporal: true,
    url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson",
    attrs: ["mag", "place", "time"] },
  { id: "eq_major", name: "Séismes majeurs — 30 jours", desc: "Flux temps réel USGS, événements significatifs.",
    theme: "Aléas", geom: "point", source: "url", worldOk: true, temporal: true,
    url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson",
    attrs: ["mag", "place", "time"] },
  { id: "eonet_all", name: "Événements naturels (EONET)", desc: "NASA EONET — feux, volcans, tempêtes, glace… en cours.",
    theme: "Aléas", geom: "point", source: "url", worldOk: true, temporal: true,
    url: "https://eonet.gsfc.nasa.gov/api/v3/events/geojson?status=open",
    attrs: ["title", "date", "categories"] },
  { id: "eonet_fires", name: "Feux de forêt actifs (EONET)", desc: "NASA EONET — incendies en cours (points).",
    theme: "Aléas", geom: "point", source: "url", worldOk: true, temporal: true,
    url: "https://eonet.gsfc.nasa.gov/api/v3/events/geojson?status=open&category=wildfires",
    attrs: ["title", "date"] },
  { id: "eonet_volc", name: "Volcans actifs (EONET)", desc: "NASA EONET — activité volcanique en cours.",
    theme: "Aléas", geom: "point", source: "url", worldOk: true, temporal: true,
    url: "https://eonet.gsfc.nasa.gov/api/v3/events/geojson?status=open&category=volcanoes",
    attrs: ["title", "date"] },

  // ── Natural Earth — vecteurs de référence, accès direct (GitHub raw, sans clé) ──
  { id: "ne_countries", name: "Pays (Natural Earth 110m)", desc: "Frontières des pays, échelle mondiale.",
    theme: "Administratif", geom: "polygon", source: "url", worldOk: true,
    url: "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson",
    attrs: ["NAME", "CONTINENT", "POP_EST"] },
  { id: "ne_states", name: "États / provinces (Natural Earth 50m)", desc: "Découpage administratif de 1er niveau.",
    theme: "Administratif", geom: "polygon", source: "url", worldOk: true,
    url: "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_1_states_provinces.geojson",
    attrs: ["name", "admin"] },
  { id: "ne_cities", name: "Villes principales (Natural Earth)", desc: "Lieux peuplés, avec population (points).",
    theme: "Administratif", geom: "point", source: "url", worldOk: true,
    url: "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_populated_places.geojson",
    attrs: ["NAME", "POP_MAX"] },
  { id: "ne_rivers", name: "Rivières & lacs — axes (Natural Earth 50m)", desc: "Cours d'eau principaux (lignes).",
    theme: "Infrastructure", geom: "line", source: "url", worldOk: true,
    url: "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_rivers_lake_centerlines.geojson",
    attrs: ["name"] },
  { id: "ne_lakes", name: "Lacs (Natural Earth 50m)", desc: "Plans d'eau principaux (polygones).",
    theme: "Environnement", geom: "polygon", source: "url", worldOk: true,
    url: "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_lakes.geojson",
    attrs: ["name"] },
  { id: "gb_adm1", name: "Limites ADM1 détaillées (geoBoundaries)", desc: "Régions/provinces du monde (CGAZ). Fichier volumineux.",
    theme: "Administratif", geom: "polygon", source: "url", worldOk: true, heavy: true,
    url: "https://raw.githubusercontent.com/wmgeolab/geoBoundaries/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM1.geojson",
    attrs: ["shapeName", "shapeGroup"] },

  // ── Biodiversité — API directes (JSON → points), sur l'emprise carte ──
  { id: "gbif", name: "Occurrences d'espèces (GBIF)", desc: "Observations biodiversité mondiales, sur la vue carte.",
    theme: "Environnement", geom: "point", source: "api", api: "gbif", needsBbox: true, temporal: true,
    attrs: ["species", "eventDate", "country"] },
  { id: "inat", name: "Observations naturalistes (iNaturalist)", desc: "Signalements récents, sur la vue carte.",
    theme: "Environnement", geom: "point", source: "api", api: "inat", needsBbox: true, temporal: true,
    attrs: ["species", "observed_on", "place"] },

  // ── Catastrophes — GDACS via proxy backend (pas de CORS direct) ──
  { id: "gdacs", name: "Alertes catastrophes (GDACS)", desc: "Séismes, cyclones, inondations… en cours, mondial.",
    theme: "Aléas", geom: "point", source: "proxy", worldOk: true, temporal: true,
    url: "https://www.gdacs.org/gdacsapi/api/events/geteventlist/MAP",
    attrs: ["eventtype", "alertlevel", "fromdate"] },
];

export const VECTOR_THEMES = ["Administratif", "Infrastructure", "Environnement", "Aléas"];

const _norm = (s) => (s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");

/** Recherche insensible casse/accents sur nom + description + thème. */
export function searchVectorCatalog(query) {
  const q = _norm(query).trim();
  if (!q) return VECTOR_CATALOG;
  const terms = q.split(/\s+/).filter(Boolean);
  return VECTOR_CATALOG.filter(e => {
    const hay = _norm(`${e.name} ${e.desc} ${e.theme} ${e.geom} ${(e.attrs || []).join(" ")}`);
    return terms.every(t => hay.includes(t));
  });
}

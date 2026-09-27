/**
 * spatialSections.js — Arborescence unique de l'analyse spatiale.
 * Utilisée par le hub (SpatialAnalysisPanel) ET le menu latéral (ThematicMenu)
 * pour qu'ils partagent EXACTEMENT les mêmes sections, catégories et clés.
 *
 * Structure : sections [{ id, label, icon, groups [{ key, name, tools[] }] }]
 *   - id      : "vecteur" | "raster" | "avance"  (pour spatialNav)
 *   - key     : identifiant de catégorie (clé de dépliage dans le hub)
 *   - tool    : { id, name, kind: "vector"|"raster"|"module", module?, implemented, desc }
 */
import { SPATIAL_OPS, SPATIAL_GROUPS } from "./spatial";
import { getToolsByCategory, getCategories } from "./whiteboxTools";

export function buildSpatialSections() {
  const cats = getCategories();
  const rasterGroups = cats.filter(c => c.key !== "avance").map(c => ({
    key: `ras_${c.key}`, name: c.name,
    tools: getToolsByCategory(c.key).map(t => ({ id: t.id, name: t.name, kind: "raster", implemented: t.implemented, desc: t.description })),
  }));
  const avanceGroups = cats.filter(c => c.key === "avance").map(c => ({
    key: `ras_${c.key}`, name: c.name,
    tools: getToolsByCategory(c.key).map(t => ({ id: t.id, name: t.name, kind: "raster", implemented: t.implemented, desc: t.description })),
  }));
  return [
    { id: "vecteur", label: "Vecteur", groups: [
      ...SPATIAL_GROUPS.map(g => ({ key: `vec_${g}`, name: g, tools: SPATIAL_OPS.filter(o => o.group === g).map(o => ({ id: o.id, name: o.name, kind: "vector", implemented: true, desc: o.desc })) })),
      { key: "mod_spatialstats", name: "Stats spatiales", tools: [{ id: "spatialstats", name: "Moran & hotspots", kind: "module", module: "spatialstats", implemented: true, desc: "Autocorrélation, points chauds/froids" }] },
      { key: "mod_vectorviz", name: "Chaleur & clusters", tools: [{ id: "vectorviz", name: "Chaleur & clusters", kind: "module", module: "vectorviz", implemented: true, desc: "Densité et regroupement de points" }] },
      { key: "mod_join", name: "Jointure attributaire", tools: [{ id: "join", name: "Jointure CSV → couche", kind: "module", module: "join", implemented: true, desc: "Rapatrie des colonnes d'un CSV" }] },
    ]},
    { id: "raster", label: "Raster", groups: [
      ...rasterGroups,
      { key: "mod_rasteranalysis", name: "Analyse zonale + calc", tools: [{ id: "rasteranalysis", name: "Zonal + map algebra", kind: "module", module: "rasteranalysis", implemented: true, desc: "Stats zonales et calculatrice" }] },
      { key: "mod_rastervec", name: "Vectorisation raster", tools: [{ id: "rastervec", name: "Polygones + contours", kind: "module", module: "rastervec", implemented: true, desc: "Raster → polygones/contours" }] },
    ]},
    { id: "avance", label: "Avancé", groups: [
      ...avanceGroups,
      { key: "mod_classif", name: "Classification supervisée", tools: [{ id: "classif", name: "Classif. supervisée", kind: "module", module: "classif", implemented: true, desc: "Entraîne un modèle sur échantillons" }] },
      { key: "mod_sql", name: "SQL Workspace", tools: [{ id: "sql", name: "SQL spatial (DuckDB)", kind: "module", module: "sql", implemented: true, desc: "Requêtes SQL sur vos couches" }] },
    ]},
  ];
}

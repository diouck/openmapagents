/**
 * Spatial Analysis Engine — turf.js operations
 * Groupes : Overlay, Proximité, Géométrie, Mesure, Statistiques, Mobilité/Flux, Génération
 */
import * as turf from "@turf/turf";

// ─── OPERATION REGISTRY ──────────────────────────────────────
export const SPATIAL_OPS = [

  // ═══════════════════ OVERLAY ═══════════════════
  {
    id: "intersection", name: "Intersection", group: "Overlay",
    desc: "Zone commune entre deux couches de polygones",
    inputs: ["A", "B"], params: [],
  },
  {
    id: "union", name: "Union", group: "Overlay",
    desc: "Fusionner deux polygones en un seul",
    inputs: ["A", "B"], params: [],
  },
  {
    id: "difference", name: "Différence A − B", group: "Overlay",
    desc: "Couche A moins les zones de B",
    inputs: ["A", "B"], params: [],
  },
  {
    id: "clip", name: "Clip (découper)", group: "Overlay",
    desc: "Garder les features de A qui sont dans B",
    inputs: ["A", "B"], params: [],
  },
  {
    id: "spatial_join", name: "Jointure spatiale", group: "Overlay",
    desc: "Transférer les attributs de B vers les points de A contenus dans B",
    inputs: ["A (points)", "B (polygones)"], params: [],
  },
  {
    id: "points_in_polygon", name: "Points dans polygone", group: "Overlay",
    desc: "Compter les points de A dans chaque polygone de B",
    inputs: ["A (points)", "B (polygones)"], params: [],
  },
  {
    id: "tag_points", name: "Étiqueter points", group: "Overlay",
    desc: "Ajouter à chaque point de A tous les attributs du polygone de B qui le contient",
    inputs: ["A (points)", "B (polygones)"], params: [],
  },
  {
    id: "erase", name: "Effacement (Erase)", group: "Overlay",
    desc: "Supprimer de A toutes les zones couvertes par B",
    inputs: ["A (polygones)", "B (polygones)"], params: [],
  },
  {
    id: "identity", name: "Identité", group: "Overlay",
    desc: "Diviser A par B et conserver tous les attributs des deux couches",
    inputs: ["A", "B"], params: [],
  },

  // ═══════════════════ PROXIMITÉ ═══════════════════
  {
    id: "buffer", name: "Buffer", group: "Proximité",
    desc: "Zone tampon autour de chaque feature",
    inputs: ["A"],
    params: [{ id: "radius", label: "Rayon (m)", type: "number", default: 500 }],
  },
  {
    id: "buffer_variable", name: "Buffer variable", group: "Proximité",
    desc: "Zone tampon dont le rayon dépend d'un attribut numérique",
    inputs: ["A"],
    params: [
      { id: "attribute", label: "Attribut rayon", type: "attribute", default: "" },
      { id: "factor",    label: "Facteur multiplicateur", type: "number", default: 1 },
    ],
  },
  {
    id: "nearest", name: "Plus proche voisin", group: "Proximité",
    desc: "Pour chaque point de A, trouver le plus proche dans B",
    inputs: ["A (points)", "B (points)"], params: [],
  },
  {
    id: "distance_matrix", name: "Matrice de distance", group: "Proximité",
    desc: "Distance au point le plus proche de B pour chaque feature de A",
    inputs: ["A", "B"], params: [],
  },
  {
    id: "within_distance", name: "Dans un rayon", group: "Proximité",
    desc: "Garder uniquement les points de A à moins de X mètres de B",
    inputs: ["A (points)", "B (points)"],
    params: [{ id: "maxDist", label: "Distance max (m)", type: "number", default: 500 }],
  },
  {
    id: "furthest_point", name: "Point le plus éloigné", group: "Proximité",
    desc: "Trouver le point de A le plus éloigné de B",
    inputs: ["A (points)", "B (points)"], params: [],
  },

  // ═══════════════════ GÉOMÉTRIE ═══════════════════
  {
    id: "centroid", name: "Centroïdes", group: "Géométrie",
    desc: "Centre géométrique de chaque feature",
    inputs: ["A"], params: [],
  },
  {
    id: "center_of_mass", name: "Centre de masse", group: "Géométrie",
    desc: "Centre de masse pondéré de chaque polygone",
    inputs: ["A"], params: [],
  },
  {
    id: "convex_hull", name: "Enveloppe convexe", group: "Géométrie",
    desc: "Plus petit polygone convexe contenant tous les points",
    inputs: ["A"], params: [],
  },
  {
    id: "concave_hull", name: "Enveloppe concave", group: "Géométrie",
    desc: "Polygone concave épousant la forme du nuage de points",
    inputs: ["A (points)"],
    params: [{ id: "maxEdge", label: "Longueur max arête (km)", type: "number", default: 1 }],
  },
  {
    id: "bounding_box", name: "Rectangle englobant", group: "Géométrie",
    desc: "Bounding box de l'ensemble des features de A",
    inputs: ["A"], params: [],
  },
  {
    id: "dissolve", name: "Dissolve", group: "Géométrie",
    desc: "Fusionner les polygones par attribut commun",
    inputs: ["A"],
    params: [{ id: "attribute", label: "Attribut de regroupement", type: "attribute", default: "" }],
  },
  {
    id: "simplify", name: "Simplifier", group: "Géométrie",
    desc: "Réduire le nombre de sommets (Douglas-Peucker)",
    inputs: ["A"],
    params: [{ id: "tolerance", label: "Tolérance (degrés)", type: "number", default: 0.001 }],
  },
  {
    id: "smooth", name: "Lisser", group: "Géométrie",
    desc: "Lissage B-spline des géométries",
    inputs: ["A"],
    params: [{ id: "iterations", label: "Itérations", type: "number", default: 3 }],
  },
  {
    id: "voronoi", name: "Voronoï", group: "Géométrie",
    desc: "Diagramme de Voronoï depuis des points",
    inputs: ["A (points)"], params: [],
  },
  {
    id: "delaunay", name: "Triangulation Delaunay", group: "Géométrie",
    desc: "Triangulation de Delaunay depuis des points",
    inputs: ["A (points)"], params: [],
  },
  {
    id: "hex_grid", name: "Grille hexagonale", group: "Géométrie",
    desc: "Grille hexagonale dans l'emprise de A",
    inputs: ["A"],
    params: [{ id: "cellSide", label: "Taille cellule (km)", type: "number", default: 0.5 }],
  },
  {
    id: "square_grid", name: "Grille carrée", group: "Géométrie",
    desc: "Grille carrée dans l'emprise de A",
    inputs: ["A"],
    params: [{ id: "cellSide", label: "Taille cellule (km)", type: "number", default: 0.5 }],
  },
  {
    id: "triangle_grid", name: "Grille triangulaire", group: "Géométrie",
    desc: "Grille de triangles dans l'emprise de A",
    inputs: ["A"],
    params: [{ id: "cellSide", label: "Taille cellule (km)", type: "number", default: 0.5 }],
  },
  {
    id: "polygon_to_line", name: "Polygone → Lignes", group: "Géométrie",
    desc: "Convertir les contours de polygones en lignes",
    inputs: ["A (polygones)"], params: [],
  },
  {
    id: "line_to_polygon", name: "Lignes → Polygone", group: "Géométrie",
    desc: "Convertir des lignes fermées en polygones",
    inputs: ["A (lignes)"], params: [],
  },
  {
    id: "points_to_line", name: "Points → Ligne", group: "Géométrie",
    desc: "Relier des points dans l'ordre des features pour former une ligne",
    inputs: ["A (points)"], params: [],
  },
  {
    id: "flatten", name: "Aplatir (Multi → Simple)", group: "Géométrie",
    desc: "Convertir MultiPolygon/MultiLine en features simples",
    inputs: ["A"], params: [],
  },
  {
    id: "explode", name: "Exploser en points", group: "Géométrie",
    desc: "Extraire tous les sommets de chaque géométrie comme points",
    inputs: ["A"], params: [],
  },

  // ═══════════════════ MESURE ═══════════════════
  {
    id: "area_perimeter", name: "Surface / périmètre", group: "Mesure",
    desc: "Calculer l'aire (m²) et le périmètre (m) de chaque polygone",
    inputs: ["A"], params: [],
  },
  {
    id: "line_length", name: "Longueur de lignes", group: "Mesure",
    desc: "Calculer la longueur de chaque ligne en mètres",
    inputs: ["A (lignes)"], params: [],
  },
  {
    id: "bearing", name: "Azimut / orientation", group: "Mesure",
    desc: "Calculer l'azimut entre chaque point de A et le point le plus proche de B",
    inputs: ["A (points)", "B (points)"], params: [],
  },
  {
    id: "clustering", name: "Clustering DBSCAN", group: "Mesure",
    desc: "Grouper les points proches par densité (DBSCAN)",
    inputs: ["A (points)"],
    params: [
      { id: "maxDistance", label: "Distance max (km)", type: "number", default: 0.5 },
      { id: "minPoints",   label: "Points minimum",    type: "number", default: 3 },
    ],
  },
  {
    id: "clustering_kmeans", name: "Clustering K-Means", group: "Mesure",
    desc: "Partitionner les points en K groupes (K-Means)",
    inputs: ["A (points)"],
    params: [{ id: "k", label: "Nombre de groupes (K)", type: "number", default: 5 }],
  },
  {
    id: "tin", name: "Triangulation (TIN)", group: "Mesure",
    desc: "Réseau irrégulier de triangles depuis des points",
    inputs: ["A (points)"], params: [],
  },
  {
    id: "interpolate", name: "Interpolation spatiale", group: "Mesure",
    desc: "Interpoler une valeur numérique sur une grille depuis des points",
    inputs: ["A (points)"],
    params: [
      { id: "attribute",   label: "Attribut à interpoler", type: "attribute", default: "" },
      { id: "cellSize",    label: "Taille cellule (km)",   type: "number",    default: 0.5 },
    ],
  },

  // ═══════════════════ STATISTIQUES ═══════════════════
  {
    id: "collect", name: "Collect (stats par zone)", group: "Statistiques",
    desc: "Agréger les valeurs des points de A dans les polygones de B (count, sum, mean)",
    inputs: ["A (points)", "B (polygones)"],
    params: [{ id: "attribute", label: "Attribut à agréger", type: "attribute", default: "" }],
  },
  {
    id: "tag_with_stats", name: "Stats zonales", group: "Statistiques",
    desc: "Pour chaque polygone de B, calculer min/max/mean des points de A à l'intérieur",
    inputs: ["A (points)", "B (polygones)"],
    params: [{ id: "attribute", label: "Attribut numérique", type: "attribute", default: "" }],
  },
  {
    id: "density_grid", name: "Densité sur grille", group: "Statistiques",
    desc: "Compter les points de A dans chaque cellule d'une grille hexagonale",
    inputs: ["A (points)"],
    params: [{ id: "cellSide", label: "Taille cellule (km)", type: "number", default: 0.5 }],
  },

  // ═══════════════════ MOBILITÉ / FLUX ═══════════════════
  {
    id: "desire_lines", name: "Lignes de désir (OD)", group: "Mobilité/Flux",
    desc: "Tracer des lignes droites entre chaque point de A (origines) et le plus proche de B (destinations)",
    inputs: ["A (origines)", "B (destinations)"], params: [],
  },
  {
    id: "flow_lines", name: "Lignes de flux pondérées", group: "Mobilité/Flux",
    desc: "Lignes OD dont la largeur dépend d'un attribut de volume (flow)",
    inputs: ["A (origines)", "B (destinations)"],
    params: [{ id: "attribute", label: "Attribut volume", type: "attribute", default: "" }],
  },
  {
    id: "route_density", name: "Densité de réseau", group: "Mobilité/Flux",
    desc: "Calculer la longueur totale de lignes (routes) dans chaque polygone de B",
    inputs: ["A (lignes)", "B (polygones)"], params: [],
  },
  {
    id: "along_line", name: "Points le long d'une ligne", group: "Mobilité/Flux",
    desc: "Créer des points régulièrement espacés le long de chaque ligne",
    inputs: ["A (lignes)"],
    params: [{ id: "interval", label: "Intervalle (m)", type: "number", default: 100 }],
  },
  {
    id: "line_slice", name: "Découper ligne par points", group: "Mobilité/Flux",
    desc: "Découper les lignes de A aux intersections avec les points de B",
    inputs: ["A (lignes)", "B (points)"], params: [],
  },
  {
    id: "snap_points_to_line", name: "Projeter points sur ligne", group: "Mobilité/Flux",
    desc: "Déplacer chaque point de A sur la ligne la plus proche de B",
    inputs: ["A (points)", "B (lignes)"], params: [],
  },
  {
    id: "catchment", name: "Zone de chalandise", group: "Mobilité/Flux",
    desc: "Buffer multiple autour de points (3 anneaux concentriques)",
    inputs: ["A (points)"],
    params: [
      { id: "ring1", label: "Anneau 1 (m)", type: "number", default: 500 },
      { id: "ring2", label: "Anneau 2 (m)", type: "number", default: 1000 },
      { id: "ring3", label: "Anneau 3 (m)", type: "number", default: 2000 },
    ],
  },


  // ── OD avancé ───────────────────────────────────────────────
  {
    id: "od_flows", name: "Flux OD (matrice)", group: "Mobilité/Flux",
    desc: "Construire une matrice OD : lignes de A = origines, lignes de B = destinations. Chaque paire génère une ligne OD avec volume optionnel.",
    inputs: ["A (origines)", "B (destinations)"],
    params: [
      { id: "vol_attr",  label: "Attribut volume (optionnel)", type: "attribute", default: "" },
      { id: "min_vol",   label: "Volume minimum à afficher",   type: "number",    default: 0 },
      { id: "max_pairs", label: "Nb max de paires OD",         type: "number",    default: 500 },
    ],
  },
  {
    id: "flow_aggregation", name: "Agrégation de flux par zone", group: "Mobilité/Flux",
    desc: "Compter les origines ET destinations qui tombent dans chaque polygone de B. Produit entrant, sortant, et flux net par zone.",
    inputs: ["A (points O/D)", "B (zones polygones)"],
    params: [
      { id: "type_attr", label: "Attribut type O/D (ex: 'type')", type: "attribute", default: "" },
      { id: "origin_val",label: "Valeur = origine (ex: 'O')",     type: "text",      default: "O" },
      { id: "dest_val",  label: "Valeur = destination (ex: 'D')", type: "text",      default: "D" },
    ],
  },
  {
    id: "desire_lines_agg", name: "Lignes de désir agrégées", group: "Mobilité/Flux",
    desc: "Agréger les flux OD entre zones : une seule ligne par paire de zones avec le volume total. A = zones origines, B = zones destinations, les points sont les centroïdes.",
    inputs: ["A (zones origines)", "B (zones destinations)"],
    params: [
      { id: "vol_attr",  label: "Attribut volume dans A",     type: "attribute", default: "" },
      { id: "id_attr_a", label: "Attribut ID zone A",         type: "attribute", default: "" },
      { id: "id_attr_b", label: "Attribut ID zone B",         type: "attribute", default: "" },
      { id: "min_vol",   label: "Volume minimum à afficher",  type: "number",    default: 1 },
    ],
  },
  {
    id: "flow_clustering", name: "Clustering de flux", group: "Mobilité/Flux",
    desc: "Regrouper les origines par densité (DBSCAN) puis créer des lignes OD agrégées entre clusters. Révèle les zones d'émission/attraction dominantes.",
    inputs: ["A (origines)", "B (destinations)"],
    params: [
      { id: "eps",       label: "Rayon cluster (km)",     type: "number", default: 1 },
      { id: "minPts",    label: "Points min par cluster", type: "number", default: 3 },
      { id: "vol_attr",  label: "Attribut volume",        type: "attribute", default: "" },
    ],
  },

  // ═══════════════════ GÉNÉRATION ═══════════════════
  {
    id: "random_points", name: "Points aléatoires", group: "Génération",
    desc: "Générer N points aléatoires dans l'emprise de A",
    inputs: ["A"],
    params: [{ id: "count", label: "Nombre de points", type: "number", default: 100 }],
  },
  {
    id: "random_points_in_polygon", name: "Points aléatoires dans polygone", group: "Génération",
    desc: "Générer N points uniformément dans chaque polygone de A",
    inputs: ["A (polygones)"],
    params: [{ id: "count", label: "Points par polygone", type: "number", default: 10 }],
  },
  {
    id: "regular_points", name: "Points réguliers", group: "Génération",
    desc: "Grille régulière de points dans l'emprise de A",
    inputs: ["A"],
    params: [{ id: "spacing", label: "Espacement (km)", type: "number", default: 0.5 }],
  },
];

// ─── GROUP LIST (ordre fixe) ─────────────────────────────────
export const SPATIAL_GROUPS = [
  "Overlay", "Proximité", "Géométrie", "Mesure",
  "Statistiques", "Mobilité/Flux", "Génération",
];

// ─── GET BBOX FROM GEOJSON ───────────────────────────────────
function getBbox(geojson) { return turf.bbox(geojson); }

// ─── HELPER FC ───────────────────────────────────────────────
function fc(features) {
  return { type: "FeatureCollection", features: features || [] };
}

// ─── EXECUTE OPERATION ───────────────────────────────────────
export function executeSpatialOp(opId, layerA, layerB, params = {}) {
  const gjA = layerA?.geojson;
  const gjB = layerB?.geojson;
  if (!gjA) throw new Error("Couche A requise");
  const featA = gjA.features || [];
  const featB = gjB?.features || [];

  switch (opId) {

    // ── OVERLAY ──────────────────────────────────────────────
    case "intersection": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) for (const b of featB) {
        try {
          if (a.geometry.type.includes("Polygon") && b.geometry.type.includes("Polygon")) {
            const inter = turf.intersect(turf.featureCollection([a, b]));
            if (inter) { inter.properties = { ...a.properties, ...b.properties, _op: "intersection" }; results.push(inter); }
          }
        } catch {}
      }
      return fc(results);
    }

    case "union": {
      if (!gjB) throw new Error("Couche B requise");
      const all = [...featA, ...featB].filter(f => f.geometry.type.includes("Polygon"));
      if (all.length < 2) return fc(all);
      try { const r = turf.union(turf.featureCollection(all)); return fc(r ? [r] : all); }
      catch { return fc(all); }
    }

    case "difference": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        let cur = a;
        for (const b of featB) {
          try {
            if (cur.geometry.type.includes("Polygon") && b.geometry.type.includes("Polygon")) {
              const d = turf.difference(turf.featureCollection([cur, b]));
              if (d) cur = d; else { cur = null; break; }
            }
          } catch {}
        }
        if (cur) results.push({ ...cur, properties: { ...a.properties, _op: "difference" } });
      }
      return fc(results);
    }

    case "clip": {
      if (!gjB) throw new Error("Couche B requise");
      const mask = featB.find(f => f.geometry.type.includes("Polygon"));
      if (!mask) throw new Error("B doit contenir un polygone");
      const results = [];
      for (const a of featA) {
        try {
          if (a.geometry.type === "Point") { if (turf.booleanPointInPolygon(a, mask)) results.push(a); }
          else if (a.geometry.type.includes("Polygon")) {
            const inter = turf.intersect(turf.featureCollection([a, mask]));
            if (inter) { inter.properties = a.properties; results.push(inter); }
          } else if (a.geometry.type === "LineString") {
            if (a.geometry.coordinates.some(p => turf.booleanPointInPolygon(turf.point(p), mask))) results.push(a);
          }
        } catch {}
      }
      return fc(results);
    }

    case "spatial_join": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        for (const b of featB) {
          try {
            if (b.geometry.type.includes("Polygon") && turf.booleanPointInPolygon(a, b)) {
              results.push({ ...a, properties: { ...a.properties, ...b.properties, _joined: true } }); break;
            }
          } catch {}
        }
      }
      return fc(results);
    }

    case "points_in_polygon": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const b of featB) {
        if (!b.geometry.type.includes("Polygon")) continue;
        let count = 0; const names = [];
        for (const a of featA) {
          if (a.geometry.type !== "Point") continue;
          try { if (turf.booleanPointInPolygon(a, b)) { count++; if (a.properties?.name) names.push(a.properties.name); } } catch {}
        }
        results.push({ ...b, properties: { ...b.properties, point_count: count, point_names: names.slice(0, 10).join(", ") } });
      }
      return fc(results);
    }

    case "tag_points": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        const found = featB.find(b => b.geometry.type.includes("Polygon") && turf.booleanPointInPolygon(a, b));
        results.push({ ...a, properties: { ...a.properties, ...(found?.properties || {}), _tagged: !!found } });
      }
      return fc(results);
    }

    case "erase": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        let cur = a;
        for (const b of featB) {
          try {
            if (cur?.geometry?.type.includes("Polygon") && b.geometry.type.includes("Polygon")) {
              const d = turf.difference(turf.featureCollection([cur, b]));
              if (d) cur = { ...d, properties: a.properties }; else { cur = null; break; }
            }
          } catch {}
        }
        if (cur) results.push(cur);
      }
      return fc(results);
    }

    case "identity": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        let intersected = false;
        for (const b of featB) {
          try {
            if (a.geometry.type.includes("Polygon") && b.geometry.type.includes("Polygon")) {
              const inter = turf.intersect(turf.featureCollection([a, b]));
              if (inter) { inter.properties = { ...a.properties, ...b.properties }; results.push(inter); intersected = true; }
            }
          } catch {}
        }
        if (!intersected) results.push(a);
      }
      return fc(results);
    }

    // ── PROXIMITÉ ────────────────────────────────────────────
    case "buffer": {
      const radius = (params.radius || 500) / 1000;
      return fc(featA.map(f => {
        try { const b = turf.buffer(f, radius, { units: "kilometers" }); if (b) b.properties = { ...f.properties, _buffer_m: params.radius || 500 }; return b; }
        catch { return null; }
      }).filter(Boolean));
    }

    case "buffer_variable": {
      const attr = params.attribute; const factor = parseFloat(params.factor) || 1;
      if (!attr) throw new Error("Attribut requis pour buffer variable");
      return fc(featA.map(f => {
        const r = (parseFloat(f.properties?.[attr]) || 0) * factor / 1000;
        try { const b = turf.buffer(f, r, { units: "kilometers" }); if (b) b.properties = { ...f.properties, _buffer_m: r * 1000 }; return b; }
        catch { return null; }
      }).filter(Boolean));
    }

    case "nearest": {
      if (!gjB) throw new Error("Couche B requise");
      const targets = turf.featureCollection(featB.filter(f => f.geometry.type === "Point"));
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        try {
          const near = turf.nearestPoint(a, targets);
          const dist = turf.distance(a, near, { units: "meters" });
          results.push({ type: "Feature", geometry: { type: "LineString", coordinates: [a.geometry.coordinates, near.geometry.coordinates] },
            properties: { ...a.properties, nearest_name: near.properties?.name || "", distance_m: Math.round(dist) } });
        } catch {}
      }
      return fc(results);
    }

    case "distance_matrix": {
      if (!gjB) throw new Error("Couche B requise");
      return fc(featA.map(a => {
        let minDist = Infinity, nearestName = "";
        const ptA = a.geometry.type === "Point" ? a : turf.centroid(a);
        for (const b of featB) {
          try { const ptB = b.geometry.type === "Point" ? b : turf.centroid(b); const d = turf.distance(ptA, ptB, { units: "meters" }); if (d < minDist) { minDist = d; nearestName = b.properties?.name || ""; } } catch {}
        }
        return { ...a, properties: { ...a.properties, dist_nearest_m: Math.round(minDist), nearest_name: nearestName } };
      }));
    }

    case "within_distance": {
      if (!gjB) throw new Error("Couche B requise");
      const maxDist = (params.maxDist || 500) / 1000;
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        for (const b of featB) {
          try {
            if (b.geometry.type !== "Point") continue;
            const d = turf.distance(a, b, { units: "kilometers" });
            if (d <= maxDist) { results.push({ ...a, properties: { ...a.properties, dist_m: Math.round(d * 1000), matched_name: b.properties?.name || "" } }); break; }
          } catch {}
        }
      }
      return fc(results);
    }

    case "furthest_point": {
      if (!gjB) throw new Error("Couche B requise");
      let maxDist = -Infinity, furthest = null;
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        for (const b of featB) {
          try { const d = turf.distance(a, b, { units: "meters" }); if (d > maxDist) { maxDist = d; furthest = { ...a, properties: { ...a.properties, max_distance_m: Math.round(d) } }; } } catch {}
        }
      }
      return fc(furthest ? [furthest] : []);
    }

    // ── GÉOMÉTRIE ────────────────────────────────────────────
    case "centroid":
      return fc(featA.map(f => { try { const c = turf.centroid(f); c.properties = { ...f.properties, _type: "centroid" }; return c; } catch { return null; } }).filter(Boolean));

    case "center_of_mass":
      return fc(featA.map(f => { try { const c = turf.centerOfMass(f); c.properties = { ...f.properties, _type: "center_of_mass" }; return c; } catch { return null; } }).filter(Boolean));

    case "convex_hull": {
      try { const h = turf.convex(gjA); return fc(h ? [h] : []); }
      catch { throw new Error("Impossible de calculer l'enveloppe convexe"); }
    }

    case "concave_hull": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 3) throw new Error("3 points minimum");
      try { const h = turf.concave(turf.featureCollection(pts), { maxEdge: params.maxEdge || 1, units: "kilometers" }); return fc(h ? [h] : []); }
      catch (e) { throw new Error("Erreur enveloppe concave: " + e.message); }
    }

    case "bounding_box": {
      const bbox = getBbox(gjA);
      const poly = turf.bboxPolygon(bbox);
      poly.properties = { _type: "bounding_box", xmin: bbox[0], ymin: bbox[1], xmax: bbox[2], ymax: bbox[3] };
      return fc([poly]);
    }

    case "dissolve": {
      const polys = featA.filter(f => f.geometry.type === "Polygon");
      if (!polys.length) throw new Error("Aucun polygone à dissoudre");
      try {
        const r = params.attribute
          ? turf.dissolve(turf.featureCollection(polys), { propertyName: params.attribute })
          : turf.dissolve(turf.featureCollection(polys));
        return r;
      } catch (e) { throw new Error("Erreur dissolve: " + e.message); }
    }

    case "simplify":
      return fc(featA.map(f => { try { return turf.simplify(f, { tolerance: params.tolerance || 0.001, highQuality: true }); } catch { return f; } }));

    case "smooth":
      return fc(featA.map(f => { try { return turf.polygonSmooth(f, { iterations: parseInt(params.iterations) || 3 }).features[0] || f; } catch { return f; } }));

    case "voronoi": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 3) throw new Error("3 points minimum");
      try { return turf.voronoi(turf.featureCollection(pts), { bbox: getBbox(gjA) }) || fc([]); }
      catch (e) { throw new Error("Erreur Voronoï: " + e.message); }
    }

    case "delaunay": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 3) throw new Error("3 points minimum");
      try { return turf.tin(turf.featureCollection(pts)); }
      catch (e) { throw new Error("Erreur Delaunay: " + e.message); }
    }

    case "hex_grid":
      try { return turf.hexGrid(getBbox(gjA), params.cellSide || 0.5, { units: "kilometers" }); }
      catch (e) { throw new Error("Erreur grille hex: " + e.message); }

    case "square_grid":
      try { return turf.squareGrid(getBbox(gjA), params.cellSide || 0.5, { units: "kilometers" }); }
      catch (e) { throw new Error("Erreur grille carrée: " + e.message); }

    case "triangle_grid":
      try { return turf.triangleGrid(getBbox(gjA), params.cellSide || 0.5, { units: "kilometers" }); }
      catch (e) { throw new Error("Erreur grille triangulaire: " + e.message); }

    case "polygon_to_line":
      return fc(featA.map(f => { try { return turf.polygonToLine(f); } catch { return null; } }).filter(Boolean).flat().map(f => Array.isArray(f) ? f : [f]).flat());

    case "line_to_polygon":
      return fc(featA.map(f => { try { return turf.lineToPolygon(f); } catch { return null; } }).filter(Boolean));

    case "points_to_line": {
      const coords = featA.filter(f => f.geometry.type === "Point").map(f => f.geometry.coordinates);
      if (coords.length < 2) throw new Error("2 points minimum");
      return fc([turf.lineString(coords, { _type: "points_to_line", count: coords.length })]);
    }

    case "flatten":
      return turf.flatten(gjA);

    case "explode":
      return fc(featA.flatMap(f => { try { return turf.explode(f).features; } catch { return []; } }));

    // ── MESURE ───────────────────────────────────────────────
    case "area_perimeter":
      return fc(featA.map(f => {
        if (!f.geometry.type.includes("Polygon")) return { ...f, properties: { ...f.properties, area_m2: 0, perimeter_m: 0 } };
        try {
          const area = turf.area(f);
          const perim = turf.length(turf.polygonToLine(f), { units: "meters" });
          return { ...f, properties: { ...f.properties, area_m2: Math.round(area), area_ha: Math.round(area / 100) / 100, perimeter_m: Math.round(perim) } };
        } catch { return f; }
      }));

    case "line_length":
      return fc(featA.map(f => {
        if (f.geometry.type !== "LineString") return f;
        try { const l = turf.length(f, { units: "meters" }); return { ...f, properties: { ...f.properties, length_m: Math.round(l), length_km: Math.round(l / 10) / 100 } }; }
        catch { return f; }
      }));

    case "bearing": {
      if (!gjB) throw new Error("Couche B requise");
      const targets2 = turf.featureCollection(featB.filter(f => f.geometry.type === "Point"));
      return fc(featA.filter(f => f.geometry.type === "Point").map(a => {
        try {
          const near = turf.nearestPoint(a, targets2);
          const b = turf.bearing(a, near);
          return { ...a, properties: { ...a.properties, bearing_deg: Math.round(b), bearing_cardinal: bearingToCardinal(b) } };
        } catch { return a; }
      }));
    }

    case "clustering": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 2) throw new Error("2 points minimum");
      try { return turf.clustersDbscan(turf.featureCollection(pts), params.maxDistance || 0.5, { units: "kilometers", minPoints: params.minPoints || 3 }); }
      catch (e) { throw new Error("Erreur clustering: " + e.message); }
    }

    case "clustering_kmeans": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 2) throw new Error("2 points minimum");
      try { return turf.clustersKmeans(turf.featureCollection(pts), { numberOfClusters: parseInt(params.k) || 5 }); }
      catch (e) { throw new Error("Erreur K-Means: " + e.message); }
    }

    case "tin": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 3) throw new Error("3 points minimum");
      try { return turf.tin(turf.featureCollection(pts)); }
      catch (e) { throw new Error("Erreur TIN: " + e.message); }
    }

    case "interpolate": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (!pts.length) throw new Error("Points requis");
      if (!params.attribute) throw new Error("Attribut requis");
      try {
        return turf.interpolate(
          turf.featureCollection(pts),
          params.cellSize || 0.5,
          { gridType: "hex", property: params.attribute, units: "kilometers" }
        );
      } catch (e) { throw new Error("Erreur interpolation: " + e.message); }
    }

    // ── STATISTIQUES ─────────────────────────────────────────
    case "collect": {
      if (!gjB) throw new Error("Couche B requise");
      if (!params.attribute) throw new Error("Attribut requis");
      try { return turf.collect(turf.featureCollection(featB), turf.featureCollection(featA), params.attribute, `${params.attribute}_list`); }
      catch (e) { throw new Error("Erreur collect: " + e.message); }
    }

    case "tag_with_stats": {
      if (!gjB) throw new Error("Couche B requise");
      const attr = params.attribute;
      const results = [];
      for (const b of featB) {
        if (!b.geometry.type.includes("Polygon")) continue;
        const vals = featA.filter(a => a.geometry.type === "Point" && turf.booleanPointInPolygon(a, b))
          .map(a => parseFloat(a.properties?.[attr])).filter(v => !isNaN(v));
        const stats = vals.length
          ? { count: vals.length, sum: vals.reduce((s, v) => s + v, 0), mean: vals.reduce((s, v) => s + v, 0) / vals.length, min: Math.min(...vals), max: Math.max(...vals) }
          : { count: 0, sum: 0, mean: null, min: null, max: null };
        results.push({ ...b, properties: { ...b.properties, [`${attr}_count`]: stats.count, [`${attr}_sum`]: Math.round(stats.sum * 100) / 100, [`${attr}_mean`]: stats.mean !== null ? Math.round(stats.mean * 100) / 100 : null, [`${attr}_min`]: stats.min, [`${attr}_max`]: stats.max } });
      }
      return fc(results);
    }

    case "density_grid": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (!pts.length) throw new Error("Points requis");
      const grid = turf.hexGrid(getBbox(gjA), params.cellSide || 0.5, { units: "kilometers" });
      for (const cell of grid.features) {
        const count = pts.filter(p => { try { return turf.booleanPointInPolygon(p, cell); } catch { return false; } }).length;
        cell.properties = { ...cell.properties, density: count };
      }
      return fc(grid.features.filter(c => c.properties.density > 0));
    }

    // ── MOBILITÉ / FLUX ──────────────────────────────────────
    case "desire_lines": {
      if (!gjB) throw new Error("Couche B requise");
      const targets3 = turf.featureCollection(featB.filter(f => f.geometry.type === "Point"));
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        try {
          const near = turf.nearestPoint(a, targets3);
          const dist = turf.distance(a, near, { units: "meters" });
          results.push(turf.lineString([a.geometry.coordinates, near.geometry.coordinates], { ...a.properties, dest_name: near.properties?.name || "", distance_m: Math.round(dist) }));
        } catch {}
      }
      return fc(results);
    }

    case "flow_lines": {
      if (!gjB) throw new Error("Couche B requise");
      const attr = params.attribute;
      const results = [];
      const usedB = new Set();
      for (let i = 0; i < Math.min(featA.length, featB.length); i++) {
        const a = featA[i], b = featB[i];
        if (!a || !b || a.geometry.type !== "Point" || b.geometry.type !== "Point") continue;
        try {
          const vol = attr ? (parseFloat(a.properties?.[attr]) || 1) : 1;
          results.push(turf.lineString([a.geometry.coordinates, b.geometry.coordinates], { ...a.properties, _volume: vol, _flow: true }));
        } catch {}
      }
      return fc(results);
    }

    case "route_density": {
      if (!gjB) throw new Error("Couche B requise");
      const lines = featA.filter(f => f.geometry.type === "LineString");
      const results = [];
      for (const b of featB) {
        if (!b.geometry.type.includes("Polygon")) continue;
        let totalLen = 0;
        for (const l of lines) {
          try {
            const clipped = turf.lineSplit(l, b);
            clipped.features.forEach(seg => {
              const mid = turf.midpoint(turf.point(seg.geometry.coordinates[0]), turf.point(seg.geometry.coordinates[seg.geometry.coordinates.length - 1]));
              if (turf.booleanPointInPolygon(mid, b)) totalLen += turf.length(seg, { units: "meters" });
            });
          } catch {}
        }
        results.push({ ...b, properties: { ...b.properties, road_length_m: Math.round(totalLen), road_density: Math.round(totalLen / (turf.area(b) / 1e6)) } });
      }
      return fc(results);
    }

    case "along_line": {
      const lines = featA.filter(f => f.geometry.type === "LineString");
      if (!lines.length) throw new Error("Lignes requises");
      const interval = (params.interval || 100) / 1000;
      const results = [];
      for (const l of lines) {
        try {
          const len = turf.length(l, { units: "kilometers" });
          let d = 0;
          while (d <= len) {
            const pt = turf.along(l, d, { units: "kilometers" });
            pt.properties = { ...l.properties, dist_m: Math.round(d * 1000) };
            results.push(pt); d += interval;
          }
        } catch {}
      }
      return fc(results);
    }

    case "line_slice": {
      if (!gjB) throw new Error("Couche B (points) requise");
      const results = [];
      for (const l of featA.filter(f => f.geometry.type === "LineString")) {
        for (const pt of featB.filter(f => f.geometry.type === "Point")) {
          try {
            const nearest = turf.nearestPointOnLine(l, pt);
            const d = nearest.properties?.location ?? 0;
            const len = turf.length(l, { units: "kilometers" });
            if (d > 0 && d < len) {
              const s1 = turf.lineSliceAlong(l, 0, d, { units: "kilometers" });
              const s2 = turf.lineSliceAlong(l, d, len, { units: "kilometers" });
              s1.properties = { ...l.properties, _slice: 1 };
              s2.properties = { ...l.properties, _slice: 2 };
              results.push(s1, s2);
            } else { results.push(l); }
          } catch { results.push(l); }
        }
      }
      return fc(results);
    }

    case "snap_points_to_line": {
      if (!gjB) throw new Error("Couche B (lignes) requise");
      const lines2 = featB.filter(f => f.geometry.type === "LineString");
      if (!lines2.length) throw new Error("B doit contenir des lignes");
      const results = [];
      for (const pt of featA.filter(f => f.geometry.type === "Point")) {
        try {
          let bestDist = Infinity, bestPt = null;
          for (const l of lines2) {
            const snapped = turf.nearestPointOnLine(l, pt);
            const d = snapped.properties?.dist ?? Infinity;
            if (d < bestDist) { bestDist = d; bestPt = snapped; }
          }
          if (bestPt) results.push({ ...bestPt, properties: { ...pt.properties, snap_dist_m: Math.round(bestDist * 1000) } });
        } catch {}
      }
      return fc(results);
    }

    case "catchment": {
      const r1 = (params.ring1 || 500) / 1000;
      const r2 = (params.ring2 || 1000) / 1000;
      const r3 = (params.ring3 || 2000) / 1000;
      const results = [];
      for (const f of featA) {
        try {
          const b3 = turf.buffer(f, r3, { units: "kilometers" });
          const b2 = turf.buffer(f, r2, { units: "kilometers" });
          const b1 = turf.buffer(f, r1, { units: "kilometers" });
          const ring3 = turf.difference(turf.featureCollection([b3, b2]));
          const ring2 = turf.difference(turf.featureCollection([b2, b1]));
          if (b1) { b1.properties = { ...f.properties, ring: 1, radius_m: params.ring1 || 500 }; results.push(b1); }
          if (ring2) { ring2.properties = { ...f.properties, ring: 2, radius_m: params.ring2 || 1000 }; results.push(ring2); }
          if (ring3) { ring3.properties = { ...f.properties, ring: 3, radius_m: params.ring3 || 2000 }; results.push(ring3); }
        } catch {}
      }
      return fc(results);
    }


    // ── OD AVANCÉ ────────────────────────────────────────────
    case "od_flows": {
      // Matrice OD complète : chaque point de A → chaque point de B
      if (!gjB) throw new Error("Couche B (destinations) requise");
      const origines = featA.filter(f => f.geometry.type === "Point");
      const dests    = featB.filter(f => f.geometry.type === "Point");
      if (!origines.length) throw new Error("A doit contenir des points (origines)");
      if (!dests.length)    throw new Error("B doit contenir des points (destinations)");

      const volAttr  = params.vol_attr || "";
      const minVol   = parseFloat(params.min_vol) || 0;
      const maxPairs = parseInt(params.max_pairs) || 500;

      const results = [];
      outer: for (const o of origines) {
        for (const d of dests) {
          if (results.length >= maxPairs) break outer;
          try {
            const vol = volAttr ? (parseFloat(o.properties?.[volAttr]) || 1) : 1;
            if (vol < minVol) continue;
            const dist = turf.distance(o, d, { units: "meters" });
            const line = turf.lineString(
              [o.geometry.coordinates, d.geometry.coordinates],
              {
                origin_name:  o.properties?.name || o.properties?.id || "",
                dest_name:    d.properties?.name || d.properties?.id || "",
                volume:       vol,
                distance_m:   Math.round(dist),
                _od:          true,
              }
            );
            results.push(line);
          } catch {}
        }
      }
      if (!results.length) throw new Error("Aucun flux généré — vérifiez les paramètres");
      return fc(results);
    }

    case "flow_aggregation": {
      // Agréger flux entrants/sortants par zone polygone
      if (!gjB) throw new Error("Couche B (zones) requise");
      const zones   = featB.filter(f => f.geometry.type.includes("Polygon"));
      const pts     = featA.filter(f => f.geometry.type === "Point");
      const typeAttr = params.type_attr || "";
      const origVal  = params.origin_val || "O";
      const destVal  = params.dest_val   || "D";

      const results = [];
      for (const zone of zones) {
        let entrant = 0, sortant = 0, total = 0;
        for (const pt of pts) {
          try {
            if (!turf.booleanPointInPolygon(pt, zone)) continue;
            total++;
            if (typeAttr) {
              const t = String(pt.properties?.[typeAttr] || "");
              if (t === origVal)  sortant++;
              else if (t === destVal) entrant++;
            }
          } catch {}
        }
        results.push({
          ...zone,
          properties: {
            ...zone.properties,
            flux_entrant:  entrant,
            flux_sortant:  sortant,
            flux_total:    total,
            flux_net:      entrant - sortant,
            flux_ratio:    total > 0 ? Math.round(entrant / total * 100) : 0,
          }
        });
      }
      if (!results.length) throw new Error("Aucun résultat — vérifiez que B contient des polygones");
      return fc(results);
    }

    case "desire_lines_agg": {
      // Lignes de désir agrégées entre centroïdes de zones
      if (!gjB) throw new Error("Couche B (zones destinations) requise");
      const zonesA = featA.filter(f => f.geometry.type.includes("Polygon"));
      const zonesB = featB.filter(f => f.geometry.type.includes("Polygon"));
      if (!zonesA.length) throw new Error("A doit contenir des polygones (zones origines)");
      if (!zonesB.length) throw new Error("B doit contenir des polygones (zones destinations)");

      const volAttr  = params.vol_attr  || "";
      const idAttrA  = params.id_attr_a || "";
      const idAttrB  = params.id_attr_b || "";
      const minVol   = parseFloat(params.min_vol) || 1;

      // Calculer centroïdes
      const centroidsA = zonesA.map(z => ({ zone: z, centroid: turf.centroid(z) }));
      const centroidsB = zonesB.map(z => ({ zone: z, centroid: turf.centroid(z) }));

      // Agréger flux par paire (idA, idB)
      const pairMap = new Map();
      for (const cA of centroidsA) {
        const idA = idAttrA ? (cA.zone.properties?.[idAttrA] || "") : cA.zone.properties?.name || "";
        const vol = volAttr ? (parseFloat(cA.zone.properties?.[volAttr]) || 1) : 1;
        // Trouver la zone B la plus proche
        let bestB = null, bestDist = Infinity;
        for (const cB of centroidsB) {
          const d = turf.distance(cA.centroid, cB.centroid, { units: "meters" });
          if (d < bestDist) { bestDist = d; bestB = cB; }
        }
        if (!bestB) continue;
        const idB = idAttrB ? (bestB.zone.properties?.[idAttrB] || "") : bestB.zone.properties?.name || "";
        const key = `${idA}→${idB}`;
        pairMap.set(key, (pairMap.get(key) || { vol: 0, coordA: cA.centroid.geometry.coordinates, coordB: bestB.centroid.geometry.coordinates, idA, idB, dist: bestDist }) );
        pairMap.get(key).vol += vol;
      }

      const results = [];
      for (const [key, pair] of pairMap) {
        if (pair.vol < minVol) continue;
        try {
          results.push(turf.lineString(
            [pair.coordA, pair.coordB],
            { od_pair: key, origin_id: pair.idA, dest_id: pair.idB, volume: pair.vol, distance_m: Math.round(pair.dist) }
          ));
        } catch {}
      }
      results.sort((a, b) => b.properties.volume - a.properties.volume);
      if (!results.length) throw new Error("Aucune ligne OD générée — vérifiez les paramètres");
      return fc(results);
    }

    case "flow_clustering": {
      // Clustering DBSCAN sur origines + agrégation OD entre clusters
      if (!gjB) throw new Error("Couche B (destinations) requise");
      const origines2 = featA.filter(f => f.geometry.type === "Point");
      const dests2    = featB.filter(f => f.geometry.type === "Point");
      if (origines2.length < 2) throw new Error("A : au moins 2 points requis");
      if (!dests2.length)       throw new Error("B : au moins 1 point requis");

      const eps    = parseFloat(params.eps)    || 1;
      const minPts = parseInt(params.minPts)   || 3;
      const volAttr2 = params.vol_attr         || "";

      // 1. Clustériser les origines
      const clustered = turf.clustersDbscan(
        turf.featureCollection(origines2), eps,
        { units: "kilometers", minPoints: minPts }
      );

      // 2. Calculer le centroïde de chaque cluster origine
      const clusterMap = new Map();
      for (const pt of clustered.features) {
        const cid = pt.properties?.cluster ?? -1; // -1 = bruit
        if (cid < 0) continue;
        if (!clusterMap.has(cid)) clusterMap.set(cid, { points: [], vol: 0 });
        clusterMap.get(cid).points.push(pt);
        clusterMap.get(cid).vol += volAttr2 ? (parseFloat(pt.properties?.[volAttr2]) || 1) : 1;
      }

      // 3. Clustériser les destinations
      const clusteredB = dests2.length >= minPts
        ? turf.clustersDbscan(turf.featureCollection(dests2), eps, { units: "kilometers", minPoints: Math.max(1, minPts - 1) })
        : turf.featureCollection(dests2.map((p, i) => ({ ...p, properties: { ...p.properties, cluster: i } })));

      const clusterMapB = new Map();
      for (const pt of clusteredB.features) {
        const cid = pt.properties?.cluster ?? 0;
        if (!clusterMapB.has(cid)) clusterMapB.set(cid, { points: [], vol: 0 });
        clusterMapB.get(cid).points.push(pt);
        clusterMapB.get(cid).vol += 1;
      }

      const results = [];

      // 4. Lignes OD cluster→cluster
      for (const [cidA, dataA] of clusterMap) {
        const centA = turf.centroid(turf.featureCollection(dataA.points));

        // Zone convexe du cluster origine
        if (dataA.points.length >= 3) {
          try {
            const hull = turf.convex(turf.featureCollection(dataA.points));
            if (hull) { hull.properties = { _type: "cluster_origin", cluster_id: cidA, point_count: dataA.points.length, volume: dataA.vol }; results.push(hull); }
          } catch {}
        }

        // Ligne vers le cluster destination le plus proche
        let bestB = null, bestDist = Infinity;
        for (const [cidB, dataB] of clusterMapB) {
          const centB = turf.centroid(turf.featureCollection(dataB.points));
          const d = turf.distance(centA, centB, { units: "meters" });
          if (d < bestDist) { bestDist = d; bestB = { cid: cidB, centroid: centB, vol: dataB.vol }; }
        }

        if (bestB) {
          try {
            results.push(turf.lineString(
              [centA.geometry.coordinates, bestB.centroid.geometry.coordinates],
              { _type: "flow_cluster", origin_cluster: cidA, dest_cluster: bestB.cid, origin_count: dataA.points.length, origin_volume: dataA.vol, distance_m: Math.round(bestDist) }
            ));
          } catch {}
        }
      }

      // 5. Centroïdes des clusters destinations
      for (const [cidB, dataB] of clusterMapB) {
        const centB = turf.centroid(turf.featureCollection(dataB.points));
        centB.properties = { _type: "cluster_dest", cluster_id: cidB, point_count: dataB.points.length };
        results.push(centB);
      }

      if (!results.length) throw new Error("Aucun cluster généré — diminuez eps ou minPoints");
      return fc(results);
    }

    // ── GÉNÉRATION ───────────────────────────────────────────
    case "random_points":
      try { return turf.randomPoint(params.count || 100, { bbox: getBbox(gjA) }); }
      catch (e) { throw new Error("Erreur points aléatoires: " + e.message); }

    case "random_points_in_polygon": {
      const polys2 = featA.filter(f => f.geometry.type === "Polygon" || f.geometry.type === "MultiPolygon");
      if (!polys2.length) throw new Error("Polygones requis");
      const results = [];
      for (const poly of polys2) {
        const bbox2 = getBbox(turf.featureCollection([poly]));
        const count = parseInt(params.count) || 10;
        let attempts = 0, added = 0;
        while (added < count && attempts < count * 20) {
          attempts++;
          const pt = turf.randomPoint(1, { bbox: bbox2 }).features[0];
          try { if (turf.booleanPointInPolygon(pt, poly)) { pt.properties = { ...poly.properties }; results.push(pt); added++; } } catch {}
        }
      }
      return fc(results);
    }

    case "regular_points": {
      const spacing = params.spacing || 0.5;
      const grid = turf.pointGrid(getBbox(gjA), spacing, { units: "kilometers" });
      return grid;
    }

    default:
      throw new Error(`Opération inconnue: ${opId}`);
  }
}

// ── Helper azimut → cardinal ──────────────────────────────────
function bearingToCardinal(b) {
  const dirs = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"];
  return dirs[Math.round(((b % 360) + 360) % 360 / 45) % 8];
}

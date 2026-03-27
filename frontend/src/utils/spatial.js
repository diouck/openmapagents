/**
 * Spatial Analysis Engine — turf.js operations
 * Each operation: { id, name, group, description, inputs, params, execute }
 */
import * as turf from "@turf/turf";

// ─── OPERATION REGISTRY ──────────────────────────────────────
export const SPATIAL_OPS = [
  // === OVERLAY ===
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
    id: "difference", name: "Différence", group: "Overlay",
    desc: "Couche A moins les zones de B",
    inputs: ["A", "B"], params: [],
  },
  {
    id: "clip", name: "Clip (découper)", group: "Overlay",
    desc: "Garder uniquement les features de A qui sont dans B",
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

  // === PROXIMITY ===
  {
    id: "buffer", name: "Buffer", group: "Proximité",
    desc: "Zone tampon autour de chaque feature",
    inputs: ["A"], params: [{ id: "radius", label: "Rayon (m)", type: "number", default: 500 }],
  },
  {
    id: "nearest", name: "Plus proche voisin", group: "Proximité",
    desc: "Pour chaque point de A, trouver le plus proche dans B",
    inputs: ["A (points)", "B (points)"], params: [],
  },
  {
    id: "distance_matrix", name: "Matrice de distance", group: "Proximité",
    desc: "Ajouter la distance au point le plus proche de B pour chaque feature de A",
    inputs: ["A", "B"], params: [],
  },

  // === GEOMETRY ===
  {
    id: "centroid", name: "Centroïdes", group: "Géométrie",
    desc: "Centre de chaque polygone",
    inputs: ["A"], params: [],
  },
  {
    id: "convex_hull", name: "Enveloppe convexe", group: "Géométrie",
    desc: "Plus petit polygone convexe contenant tous les points",
    inputs: ["A"], params: [],
  },
  {
    id: "dissolve", name: "Dissolve", group: "Géométrie",
    desc: "Fusionner les polygones par attribut commun",
    inputs: ["A"], params: [{ id: "attribute", label: "Attribut de regroupement", type: "attribute", default: "" }],
  },
  {
    id: "simplify", name: "Simplifier", group: "Géométrie",
    desc: "Réduire le nombre de sommets",
    inputs: ["A"], params: [{ id: "tolerance", label: "Tolérance (degrés)", type: "number", default: 0.001 }],
  },
  {
    id: "voronoi", name: "Voronoï", group: "Géométrie",
    desc: "Diagramme de Voronoï depuis des points",
    inputs: ["A (points)"], params: [],
  },
  {
    id: "hex_grid", name: "Grille hexagonale", group: "Géométrie",
    desc: "Créer une grille hex dans l'emprise de A",
    inputs: ["A"], params: [{ id: "cellSide", label: "Taille cellule (km)", type: "number", default: 0.5 }],
  },
  {
    id: "square_grid", name: "Grille carrée", group: "Géométrie",
    desc: "Créer une grille carrée dans l'emprise de A",
    inputs: ["A"], params: [{ id: "cellSide", label: "Taille cellule (km)", type: "number", default: 0.5 }],
  },

  // === MEASUREMENT ===
  {
    id: "area_perimeter", name: "Surface / périmètre", group: "Mesure",
    desc: "Calculer l'aire et le périmètre de chaque polygone",
    inputs: ["A"], params: [],
  },
  {
    id: "clustering", name: "Clustering DBSCAN", group: "Mesure",
    desc: "Grouper les points proches (DBSCAN)",
    inputs: ["A (points)"], params: [
      { id: "maxDistance", label: "Distance max (km)", type: "number", default: 0.5 },
      { id: "minPoints", label: "Points minimum", type: "number", default: 3 },
    ],
  },
  {
    id: "tin", name: "Triangulation (TIN)", group: "Mesure",
    desc: "Triangulation de Delaunay depuis des points",
    inputs: ["A (points)"], params: [],
  },

  // === GENERATION ===
  {
    id: "random_points", name: "Points aléatoires", group: "Génération",
    desc: "Générer N points dans l'emprise de A",
    inputs: ["A"], params: [{ id: "count", label: "Nombre de points", type: "number", default: 100 }],
  },
];

// ─── GROUP LIST ──────────────────────────────────────────────
export const SPATIAL_GROUPS = [...new Set(SPATIAL_OPS.map(op => op.group))];

// ─── GET BBOX FROM GEOJSON ───────────────────────────────────
function getBbox(geojson) {
  return turf.bbox(geojson);
}

// ─── EXECUTE OPERATION ───────────────────────────────────────
export function executeSpatialOp(opId, layerA, layerB, params = {}) {
  const gjA = layerA?.geojson;
  const gjB = layerB?.geojson;
  if (!gjA) throw new Error("Couche A requise");

  const featA = gjA.features || [];
  const featB = gjB?.features || [];

  switch (opId) {

    // ── OVERLAY ──
    case "intersection": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        for (const b of featB) {
          try {
            if (a.geometry.type.includes("Polygon") && b.geometry.type.includes("Polygon")) {
              const inter = turf.intersect(turf.featureCollection([a, b]));
              if (inter) {
                inter.properties = { ...a.properties, ...b.properties, _op: "intersection" };
                results.push(inter);
              }
            }
          } catch {}
        }
      }
      return fc(results);
    }

    case "union": {
      if (!gjB) throw new Error("Couche B requise");
      const polysA = featA.filter(f => f.geometry.type.includes("Polygon"));
      const polysB = featB.filter(f => f.geometry.type.includes("Polygon"));
      const all = [...polysA, ...polysB];
      if (all.length < 2) return fc(all);
      try {
        const result = turf.union(turf.featureCollection(all));
        return fc(result ? [result] : all);
      } catch { return fc(all); }
    }

    case "difference": {
      if (!gjB) throw new Error("Couche B requise");
      const results = [];
      for (const a of featA) {
        let current = a;
        for (const b of featB) {
          try {
            if (current.geometry.type.includes("Polygon") && b.geometry.type.includes("Polygon")) {
              const diff = turf.difference(turf.featureCollection([current, b]));
              if (diff) current = diff; else { current = null; break; }
            }
          } catch {}
        }
        if (current) results.push({ ...current, properties: { ...a.properties, _op: "difference" } });
      }
      return fc(results);
    }

    case "clip": {
      if (!gjB) throw new Error("Couche B (masque polygone) requise");
      const mask = featB.find(f => f.geometry.type.includes("Polygon"));
      if (!mask) throw new Error("B doit contenir au moins un polygone");
      const results = [];
      for (const a of featA) {
        try {
          if (a.geometry.type === "Point") {
            if (turf.booleanPointInPolygon(a, mask)) results.push(a);
          } else if (a.geometry.type.includes("Polygon")) {
            const inter = turf.intersect(turf.featureCollection([a, mask]));
            if (inter) { inter.properties = a.properties; results.push(inter); }
          } else if (a.geometry.type === "LineString") {
            // Simplified: check if any point is in mask
            const pts = a.geometry.coordinates;
            if (pts.some(p => turf.booleanPointInPolygon(turf.point(p), mask))) results.push(a);
          }
        } catch {}
      }
      return fc(results);
    }

    case "spatial_join": {
      if (!gjB) throw new Error("Couche B (polygones) requise");
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        for (const b of featB) {
          try {
            if (b.geometry.type.includes("Polygon") && turf.booleanPointInPolygon(a, b)) {
              results.push({ ...a, properties: { ...a.properties, ...b.properties, _joined: true } });
              break;
            }
          } catch {}
        }
      }
      return fc(results);
    }

    case "points_in_polygon": {
      if (!gjB) throw new Error("Couche B (polygones) requise");
      const results = [];
      for (const b of featB) {
        if (!b.geometry.type.includes("Polygon")) continue;
        let count = 0;
        const pointNames = [];
        for (const a of featA) {
          if (a.geometry.type !== "Point") continue;
          try {
            if (turf.booleanPointInPolygon(a, b)) {
              count++;
              if (a.properties?.name) pointNames.push(a.properties.name);
            }
          } catch {}
        }
        results.push({
          ...b,
          properties: { ...b.properties, point_count: count, point_names: pointNames.slice(0, 10).join(", ") },
        });
      }
      return fc(results);
    }

    // ── PROXIMITY ──
    case "buffer": {
      const radius = (params.radius || 500) / 1000; // m to km
      const results = featA.map(f => {
        try {
          const buf = turf.buffer(f, radius, { units: "kilometers" });
          if (buf) buf.properties = { ...f.properties, _buffer_m: params.radius || 500 };
          return buf;
        } catch { return null; }
      }).filter(Boolean);
      return fc(results);
    }

    case "nearest": {
      if (!gjB) throw new Error("Couche B (cibles) requise");
      const targetPts = turf.featureCollection(featB.filter(f => f.geometry.type === "Point"));
      const results = [];
      for (const a of featA) {
        if (a.geometry.type !== "Point") continue;
        try {
          const near = turf.nearestPoint(a, targetPts);
          const dist = turf.distance(a, near, { units: "meters" });
          results.push({
            type: "Feature",
            geometry: { type: "LineString", coordinates: [a.geometry.coordinates, near.geometry.coordinates] },
            properties: { ...a.properties, nearest_name: near.properties?.name || "", distance_m: Math.round(dist) },
          });
        } catch {}
      }
      return fc(results);
    }

    case "distance_matrix": {
      if (!gjB) throw new Error("Couche B requise");
      const results = featA.map(a => {
        let minDist = Infinity, nearestName = "";
        const ptA = a.geometry.type === "Point" ? a : turf.centroid(a);
        for (const b of featB) {
          try {
            const ptB = b.geometry.type === "Point" ? b : turf.centroid(b);
            const d = turf.distance(ptA, ptB, { units: "meters" });
            if (d < minDist) { minDist = d; nearestName = b.properties?.name || ""; }
          } catch {}
        }
        return { ...a, properties: { ...a.properties, dist_nearest_m: Math.round(minDist), nearest_name: nearestName } };
      });
      return fc(results);
    }

    // ── GEOMETRY ──
    case "centroid": {
      const results = featA.map(f => {
        try {
          const c = turf.centroid(f);
          c.properties = { ...f.properties, _type: "centroid" };
          return c;
        } catch { return null; }
      }).filter(Boolean);
      return fc(results);
    }

    case "convex_hull": {
      try {
        const hull = turf.convex(gjA);
        return fc(hull ? [hull] : []);
      } catch { throw new Error("Impossible de calculer l'enveloppe convexe"); }
    }

    case "dissolve": {
      const attr = params.attribute;
      const polys = featA.filter(f => f.geometry.type === "Polygon");
      if (!polys.length) throw new Error("Aucun polygone à dissoudre");
      try {
        if (attr) {
          const result = turf.dissolve(turf.featureCollection(polys), { propertyName: attr });
          return result;
        } else {
          const result = turf.dissolve(turf.featureCollection(polys));
          return result;
        }
      } catch (e) { throw new Error("Erreur dissolve: " + e.message); }
    }

    case "simplify": {
      const tol = params.tolerance || 0.001;
      const results = featA.map(f => {
        try { return turf.simplify(f, { tolerance: tol, highQuality: true }); }
        catch { return f; }
      });
      return fc(results);
    }

    case "voronoi": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 3) throw new Error("Au moins 3 points requis");
      try {
        const bbox = getBbox(gjA);
        const voronoi = turf.voronoi(turf.featureCollection(pts), { bbox });
        return voronoi || fc([]);
      } catch (e) { throw new Error("Erreur Voronoï: " + e.message); }
    }

    case "hex_grid": {
      const cellSide = params.cellSide || 0.5;
      const bbox = getBbox(gjA);
      try {
        return turf.hexGrid(bbox, cellSide, { units: "kilometers" });
      } catch (e) { throw new Error("Erreur grille hex: " + e.message); }
    }

    case "square_grid": {
      const cellSide = params.cellSide || 0.5;
      const bbox = getBbox(gjA);
      try {
        return turf.squareGrid(bbox, cellSide, { units: "kilometers" });
      } catch (e) { throw new Error("Erreur grille carrée: " + e.message); }
    }

    // ── MEASUREMENT ──
    case "area_perimeter": {
      const results = featA.map(f => {
        if (!f.geometry.type.includes("Polygon")) return { ...f, properties: { ...f.properties, area_m2: 0, perimeter_m: 0 } };
        try {
          const area = turf.area(f);
          const perim = turf.length(turf.polygonToLine(f), { units: "meters" });
          return { ...f, properties: { ...f.properties, area_m2: Math.round(area), perimeter_m: Math.round(perim) } };
        } catch { return f; }
      });
      return fc(results);
    }

    case "clustering": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 2) throw new Error("Au moins 2 points requis");
      const maxDist = params.maxDistance || 0.5;
      const minPts = params.minPoints || 3;
      try {
        const clustered = turf.clustersDbscan(turf.featureCollection(pts), maxDist, { units: "kilometers", minPoints: minPts });
        return clustered;
      } catch (e) { throw new Error("Erreur clustering: " + e.message); }
    }

    case "tin": {
      const pts = featA.filter(f => f.geometry.type === "Point");
      if (pts.length < 3) throw new Error("Au moins 3 points requis");
      try {
        return turf.tin(turf.featureCollection(pts));
      } catch (e) { throw new Error("Erreur TIN: " + e.message); }
    }

    case "random_points": {
      const count = params.count || 100;
      const bbox = getBbox(gjA);
      try {
        return turf.randomPoint(count, { bbox });
      } catch (e) { throw new Error("Erreur points aléatoires: " + e.message); }
    }

    default:
      throw new Error(`Opération inconnue: ${opId}`);
  }
}

function fc(features) {
  return { type: "FeatureCollection", features: features || [] };
}

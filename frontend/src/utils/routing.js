/**
 * Routing & Isochrone Engine.
 * Route + Isochrone : passent par le BACKEND (/api/route/*) qui détient le jeton
 * (ORS ou Mapbox) — ne dépend donc PAS du build frontend (VITE_MAPBOX_TOKEN).
 * Geocoding : Nominatim (client, sans clé) pour l'autocomplétion d'adresse.
 */
import { API } from "../config";

const minOf = (f) => {
  const p = f.properties || {};
  return p.value != null ? p.value / 60 : (p.contour != null ? p.contour : (p.time_min != null ? p.time_min : null));
};

// ─── GEOCODE ADDRESS (Nominatim) ─────────────────────────────
export async function geocodeAddress(query) {
  if (!query || query.trim().length < 3) return [];
  const res = await fetch(
    `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=5&addressdetails=1`,
    { headers: { "User-Agent": "OvertureExplorer/1.0" } }
  );
  if (!res.ok) return [];
  const data = await res.json();
  return data.map(r => ({
    label: r.display_name,
    lon: parseFloat(r.lon),
    lat: parseFloat(r.lat),
  }));
}

// ─── ROUTE (backend → ORS/Mapbox) ─────────────────────────────
export async function computeRoute(waypoints, profile = "foot") {
  if (waypoints.length < 2) throw new Error("Au moins 2 points requis");
  const res = await fetch(`${API}/route/directions`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ waypoints, profile }),
  });
  if (!res.ok) { let m = `Directions ${res.status}`; try { m = (await res.json()).detail || m; } catch (_) {} throw new Error(m); }
  const data = await res.json();
  const route = (data.routes || [])[0];
  if (!route) throw new Error("Aucun itinéraire trouvé");

  const distKm = Math.round(route.distance / 10) / 100;
  const durMin = Math.round(route.duration / 6) / 10;
  const features = [{
    type: "Feature",
    geometry: { type: "LineString", coordinates: route.coordinates },
    properties: { type: "route", distance_km: distKm, duration_min: durMin, profile,
      summary: `${distKm} km — ${Math.round(route.duration / 60)} min` },
  }];
  waypoints.forEach((p, i) => features.push({
    type: "Feature", geometry: { type: "Point", coordinates: p },
    properties: { type: "waypoint", index: i, label: i === 0 ? "A" : i === waypoints.length - 1 ? "B" : `${i}` },
  }));
  const steps = (route.steps || []).map(s => ({
    instruction: s.instruction || "", distance_m: s.distance_m || 0, duration_s: s.duration_s || 0, name: s.name || "",
  }));
  return { type: "FeatureCollection", features, metadata: { distance_km: distKm, duration_min: durMin, profile, steps } };
}

// ─── ISOCHRONE (backend → ORS/Mapbox) ─────────────────────────
export async function computeIsochrone(center, timeMinutes = 10, profile = "foot", intervals = null) {
  const res = await fetch(`${API}/route/isochrone`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ center, minutes: timeMinutes, profile, intervals }),
  });
  if (!res.ok) { let m = `Isochrone ${res.status}`; try { m = (await res.json()).detail || m; } catch (_) {} throw new Error(m); }
  const data = await res.json();
  const gj = data.geojson || { type: "FeatureCollection", features: [] };
  if (!gj.features?.length) throw new Error("Aucun isochrone calculé");

  const colors = ["#1a9850", "#91cf60", "#d9ef8b", "#fee08b", "#fc8d59", "#d73027"];
  const feats = gj.features.map(f => ({ ...f, properties: { ...(f.properties || {}) } }));
  feats.sort((a, b) => (minOf(a) || 0) - (minOf(b) || 0));   // du plus petit au plus grand
  feats.forEach((f, i) => {
    const mn = minOf(f);
    f.properties.type = "isochrone";
    f.properties.profile = profile;
    f.properties.time_min = mn;
    f.properties.contour = mn;
    f.properties.label = `${mn} min (${profile})`;
    f.properties.color = colors[i % colors.length];
  });
  feats.reverse();   // grands polygones dessous, petits au-dessus
  feats.push({ type: "Feature", geometry: { type: "Point", coordinates: center }, properties: { type: "isochrone_center", label: "Centre" } });
  return { type: "FeatureCollection", features: feats, metadata: { center, breaks: data.intervals_min, profile } };
}

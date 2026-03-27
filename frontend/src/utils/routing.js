/**
 * Routing & Isochrone Engine — Mapbox APIs
 * Route: Mapbox Directions API (free tier 100k/month)
 * Isochrone: Mapbox Isochrone API (native polygons)
 * Geocoding: Nominatim (free, for address autocomplete)
 */
import { MAPBOX_TOKEN } from "../config";

const MB = "https://api.mapbox.com";
const PROFILES = { foot: "walking", bike: "cycling", car: "driving" };

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

// ─── ROUTE (Mapbox Directions) ───────────────────────────────
export async function computeRoute(waypoints, profile = "foot") {
  if (waypoints.length < 2) throw new Error("Au moins 2 points requis");
  const mbProfile = PROFILES[profile] || "walking";
  const coords = waypoints.map(p => `${p[0]},${p[1]}`).join(";");
  const url = `${MB}/directions/v5/mapbox/${mbProfile}/${coords}?geometries=geojson&overview=full&steps=true&language=fr&access_token=${MAPBOX_TOKEN}`;

  const res = await fetch(url);
  if (!res.ok) throw new Error(`Mapbox Directions: ${res.status}`);
  const data = await res.json();
  if (!data.routes?.length) throw new Error("Aucun itinéraire trouvé");

  const route = data.routes[0];
  const features = [];

  // Route line
  features.push({
    type: "Feature",
    geometry: route.geometry,
    properties: {
      type: "route",
      distance_km: Math.round(route.distance / 10) / 100,
      duration_min: Math.round(route.duration / 6) / 10,
      profile,
      summary: `${Math.round(route.distance / 10) / 100} km — ${Math.round(route.duration / 60)} min`,
    },
  });

  // Waypoints
  waypoints.forEach((p, i) => {
    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: p },
      properties: {
        type: "waypoint",
        index: i,
        label: i === 0 ? "A" : i === waypoints.length - 1 ? "B" : `${i}`,
      },
    });
  });

  // Turn-by-turn steps
  const steps = [];
  route.legs?.forEach(leg => {
    leg.steps?.forEach(step => {
      steps.push({
        instruction: step.maneuver?.instruction || "",
        distance_m: Math.round(step.distance),
        duration_s: Math.round(step.duration),
        name: step.name || "",
      });
    });
  });

  return {
    type: "FeatureCollection",
    features,
    metadata: {
      distance_km: Math.round(route.distance / 10) / 100,
      duration_min: Math.round(route.duration / 6) / 10,
      profile,
      steps,
    },
  };
}

// ─── ISOCHRONE (Mapbox native) ────────────────────────────────
export async function computeIsochrone(center, timeMinutes = 10, profile = "foot") {
  const mbProfile = PROFILES[profile] || "walking";
  // Mapbox supports up to 4 contours
  const contours = [];
  if (timeMinutes >= 15) contours.push(5, 10, timeMinutes);
  else if (timeMinutes >= 10) contours.push(5, timeMinutes);
  else contours.push(timeMinutes);

  const contoursStr = contours.join(",");
  const colors = contours.map((_, i) => {
    const c = ["1D9E75", "EF9F27", "D85A30", "E24B4A"];
    return c[i % c.length];
  }).join(",");

  const url = `${MB}/isochrone/v1/mapbox/${mbProfile}/${center[0]},${center[1]}?contours_minutes=${contoursStr}&contours_colors=${colors}&polygons=true&access_token=${MAPBOX_TOKEN}`;

  const res = await fetch(url);
  if (!res.ok) throw new Error(`Mapbox Isochrone: ${res.status}`);
  const data = await res.json();

  if (!data.features?.length) throw new Error("Aucun isochrone calculé");

  // Add metadata to each feature
  data.features.forEach(f => {
    f.properties.type = "isochrone";
    f.properties.profile = profile;
    f.properties.label = `${f.properties.contour} min (${profile})`;
    f.properties.time_min = f.properties.contour;
  });

  // Add center point
  data.features.push({
    type: "Feature",
    geometry: { type: "Point", coordinates: center },
    properties: { type: "isochrone_center", label: "Centre" },
  });

  data.metadata = { center, breaks: contours, profile };
  return data;
}

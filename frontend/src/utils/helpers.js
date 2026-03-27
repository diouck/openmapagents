import { hexToRgb } from "../config";

// ─── PERMALINK ───────────────────────────────────────────────
export function encodePermalink(vs, mapSt, layers) {
  const state = {
    c: [Math.round(vs.longitude * 1e5) / 1e5, Math.round(vs.latitude * 1e5) / 1e5],
    z: Math.round(vs.zoom * 10) / 10, s: mapSt,
    l: layers.map(l => ({ n: l.name, t: l.theme, fc: l.featureCount })).slice(0, 5),
  };
  try { return btoa(JSON.stringify(state)); } catch { return ""; }
}

export function decodePermalink() {
  try {
    const hash = window.location.hash.slice(1);
    if (!hash) return null;
    return JSON.parse(atob(hash));
  } catch { return null; }
}

// ─── PDF EXPORT ──────────────────────────────────────────────
export async function exportPDF(mapRef, vs, layers, format = "A4") {
  const html2canvas = (await import("html2canvas")).default;
  const { jsPDF } = await import("jspdf");
  const mapEl = mapRef.current?.getMap?.()?.getCanvas?.();
  if (!mapEl) { alert("Carte non disponible"); return; }

  const canvas = await html2canvas(mapEl.parentElement, { useCORS: true, scale: 2, backgroundColor: null });
  const isA3 = format === "A3";
  const pdf = new jsPDF({ orientation: "landscape", unit: "mm", format: isA3 ? "a3" : "a4" });
  const pw = isA3 ? 420 : 297, ph = isA3 ? 297 : 210, mg = 10;

  pdf.setFontSize(16);
  pdf.text("Overture Maps Explorer", mg, mg + 6);
  pdf.setFontSize(9); pdf.setTextColor(120);
  pdf.text(`${vs.longitude.toFixed(4)}, ${vs.latitude.toFixed(4)} | z${vs.zoom.toFixed(1)} | ${new Date().toLocaleString("fr-FR")}`, mg, mg + 12);

  const my = mg + 16, mw = pw - mg * 2, mh = ph - my - mg - 20;
  pdf.addImage(canvas.toDataURL("image/jpeg", 0.92), "JPEG", mg, my, mw, mh);
  pdf.setDrawColor(180); pdf.rect(mg, my, mw, mh);

  // North arrow
  const nx = pw - mg - 8, ny = my + 8;
  pdf.setFillColor(50); pdf.triangle(nx, ny - 6, nx - 3, ny + 2, nx + 3, ny + 2, "F");
  pdf.setFontSize(7); pdf.setTextColor(50); pdf.text("N", nx - 1.5, ny - 7);

  // Scale bar
  const sy = ph - mg - 4;
  const mpp = 156543.03 * Math.cos(vs.latitude * Math.PI / 180) / Math.pow(2, vs.zoom);
  const barM = Math.round(mpp * 100), barMM = 100 * (mw / canvas.width * 2);
  pdf.setDrawColor(50); pdf.setLineWidth(0.5);
  pdf.line(mg, sy, mg + barMM, sy);
  pdf.line(mg, sy - 1, mg, sy + 1); pdf.line(mg + barMM, sy - 1, mg + barMM, sy + 1);
  pdf.setFontSize(7);
  pdf.text(barM < 1000 ? `${barM} m` : `${(barM / 1000).toFixed(1)} km`, mg + barMM / 2, sy - 2, { align: "center" });

  // Legend
  let ly = ph - mg - 16;
  layers.filter(l => l.visible).forEach(l => {
    pdf.setFillColor(...hexToRgb(l.color));
    pdf.circle(pw - mg - 50, ly, 2, "F");
    pdf.setFontSize(7); pdf.setTextColor(80);
    pdf.text(`${l.name} (${l.featureCount})`, pw - mg - 46, ly + 1);
    ly += 5;
  });

  pdf.setFontSize(7); pdf.setTextColor(150);
  pdf.text("Overture Maps Explorer | OpenFreeMap | OSM", mg, ph - mg);
  pdf.save(`carte_${format}_${Date.now()}.pdf`);
}

// ─── FILE IMPORT ─────────────────────────────────────────────
export async function importFile(file) {
  const name = file.name.toLowerCase();

  if (name.endsWith(".geojson") || name.endsWith(".json")) {
    const text = await file.text();
    let gj = JSON.parse(text);
    if (gj.type === "Feature") gj = { type: "FeatureCollection", features: [gj] };
    if (gj.type !== "FeatureCollection") throw new Error("GeoJSON invalide");
    return gj;
  }

  if (name.endsWith(".csv") || name.endsWith(".tsv")) {
    const text = await file.text();
    const sep = name.endsWith(".tsv") ? "\t" : ",";
    const lines = text.trim().split("\n");
    const headers = lines[0].split(sep).map(h => h.trim().replace(/"/g, ""));
    const latCol = headers.find(h => /^(lat|latitude|y)$/i.test(h));
    const lonCol = headers.find(h => /^(lon|lng|longitude|x)$/i.test(h));
    if (!latCol || !lonCol) throw new Error("Colonnes lat/lon non trouvées");
    const features = [];
    for (let i = 1; i < lines.length; i++) {
      const vals = lines[i].split(sep).map(v => v.trim().replace(/"/g, ""));
      const props = {};
      headers.forEach((h, j) => { props[h] = vals[j] || null; });
      const lat = parseFloat(props[latCol]), lon = parseFloat(props[lonCol]);
      if (!isNaN(lat) && !isNaN(lon)) {
        features.push({ type: "Feature", geometry: { type: "Point", coordinates: [lon, lat] }, properties: props });
      }
    }
    return { type: "FeatureCollection", features };
  }

  if (name.endsWith(".zip") || name.endsWith(".shp")) {
    const shp = await import("shpjs");
    const buffer = await file.arrayBuffer();
    let gj = await shp.default(buffer);
    if (Array.isArray(gj)) gj = { type: "FeatureCollection", features: gj.flatMap(g => g.features || []) };
    return gj;
  }

  throw new Error("Format non supporté (GeoJSON, CSV, Shapefile ZIP)");
}

// ─── BOUNDS HELPER ───────────────────────────────────────────
export function computeBounds(features) {
  let n1 = Infinity, n2 = Infinity, x1 = -Infinity, x2 = -Infinity;
  const ex = (lo, la) => { n1 = Math.min(n1, lo); x1 = Math.max(x1, lo); n2 = Math.min(n2, la); x2 = Math.max(x2, la); };
  features.forEach(f => {
    const g = f.geometry; if (!g?.coordinates) return;
    if (g.type === "Point") ex(g.coordinates[0], g.coordinates[1]);
    else if (g.type === "Polygon") (g.coordinates[0] || []).forEach(c => ex(c[0], c[1]));
    else if (g.type === "MultiPolygon") (g.coordinates || []).forEach(p => (p[0] || []).forEach(c => ex(c[0], c[1])));
    else if (g.type === "LineString") (g.coordinates || []).forEach(c => ex(c[0], c[1]));
  });
  return n1 === Infinity ? null : [[n1, n2], [x1, x2]];
}

// ─── AUTO-DETECT POPUP FIELDS ────────────────────────────────
export function getPopupFields(properties) {
  if (!properties) return [];
  return Object.entries(properties)
    .filter(([k, v]) => {
      if (["id", "color", "geom_json", "geom_wkt", "_i", "_g", "_idx"].includes(k)) return false;
      if (v == null || v === "" || v === "None" || v === "null") return false;
      return true;
    })
    .map(([k, v]) => ({
      key: k,
      value: typeof v === "object" ? JSON.stringify(v) : String(v),
      isName: /^(name|nom|label|titre|title)$/i.test(k),
    }));
}

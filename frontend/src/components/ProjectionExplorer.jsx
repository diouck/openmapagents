/**
 * ProjectionExplorer.jsx — Composeur de carte en projection (« Le pouvoir des cartes »).
 *
 * Produit une VRAIE carte imprimable dans une projection cartographique choisie
 * (Robinson, Equal Earth, Peters, Mollweide, Winkel-Tripel, Mercator, Orthographique…),
 * DÉCOUPLÉE de MapLibre (qui ne fait que Mercator/Globe). Rendu sur canvas via d3-geo :
 *   - couches VECTORIELLES avec leur sémiologie (couleur / classif catégorisée ou graduée) ;
 *   - une couche RASTER (NDVI, LST, WorldCover…) reprojetée depuis ses tuiles Web Mercator
 *     déjà générées — reprojection pixel par pixel, SANS back-end ni redéploiement ;
 *   - habillage : titre, graticule, indicatrices de Tissot (option), légende, crédit ;
 *   - export PNG / PDF (jsPDF) / impression.
 *
 * Base d3-geo via le méta-paquet "d3" ; classiques via d3-geo-projection ; fond de pays
 * world-atlas (110m) décodé par topojson-client.
 */
import { useState, useMemo, useEffect, useRef, useCallback } from "react";
import { useThemeContext } from "../theme";
import { IcX, IcMap, IcImage, IcFileDown, IcPrint, IcLayers3 } from "../icons";
import {
  geoPath, geoGraticule10, geoCircle, geoEqualEarth, geoNaturalEarth1,
  geoMercator, geoEquirectangular, geoOrthographic, geoAzimuthalEqualArea,
} from "d3";
import { geoRobinson, geoMollweide, geoWinkel3, geoCylindricalEqualArea } from "d3-geo-projection";
import { geoSpilhaus } from "../utils/adams2";
import { feature } from "topojson-client";
import worldTopo from "world-atlas/countries-110m.json";

const SPHERE = { type: "Sphere" };
const MERC_RECT = { type: "Polygon", coordinates: [[[-180, -82], [-180, 82], [180, 82], [180, -82], [-180, -82]]] };
const MERC_LAT = 85.0511;   // limite Web Mercator

const PROJ = [
  { key: "mercator", label: "Mercator", type: "Conforme", make: geoMercator },
  { key: "equalEarth", label: "Equal Earth", type: "Équivalente", make: geoEqualEarth },
  { key: "naturalEarth", label: "Natural Earth", type: "Aphylactique", make: geoNaturalEarth1 },
  { key: "robinson", label: "Robinson", type: "Aphylactique", make: geoRobinson },
  { key: "winkel3", label: "Winkel-Tripel", type: "Aphylactique", make: geoWinkel3 },
  { key: "gallPeters", label: "Gall-Peters", type: "Équivalente", make: () => geoCylindricalEqualArea().parallel(45) },
  { key: "mollweide", label: "Mollweide", type: "Équivalente", make: geoMollweide },
  { key: "equirect", label: "Plate-carrée", type: "Aphylactique", make: geoEquirectangular },
  { key: "ortho", label: "Orthographique", type: "Perspective", make: () => geoOrthographic().rotate([-10, -25]).clipAngle(90) },
  // clipAngle 179 (pas 180) : évite la singularité de l'antipode qui faisait déborder le rendu.
  { key: "azimuthal", label: "Azimutale équiv. (pôle)", type: "Équivalente", make: () => geoAzimuthalEqualArea().rotate([0, -90]).clipAngle(179) },
  { key: "spilhaus", label: "Spilhaus (océans)", type: "Conforme", make: geoSpilhaus },
];
const TYPE_COLOR = (C, t) => t === "Conforme" ? C.blu : t === "Équivalente" ? C.acc : t === "Perspective" ? C.pnk : C.amb;

// ── Fonds de plan raster (tuiles {z}/{x}/{y} CORS) reprojetables dans la projection.
// Les styles vectoriels de la carte live (MapLibre) ne sont pas reprojetables →
// équivalents raster : Carto (positron/dark), Esri World Imagery (satellite), OSM.
const BASEMAPS = {
  none:      { label: "Aucun (pays gris)", url: null },
  positron:  { label: "Positron (clair)",  url: "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png" },
  dark:      { label: "Sombre (Carto)",    url: "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png" },
  satellite: { label: "Satellite (Esri)",  url: "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}" },
  osm:       { label: "OpenStreetMap",     url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png" },
};
// Style courant de la carte live → fond raster équivalent (pré-sélection).
const STYLE_TO_BASE = { positron: "positron", dark: "dark", satellite: "satellite", liberty: "osm" };

// ── Sémiologie d'une entité vecteur selon la classification de la couche ──
function featureStyle(layer, f) {
  const base = layer.color || "#3388ff";
  const cr = layer.classResult;
  if (cr && cr.attribute && f.properties) {
    const v = f.properties[cr.attribute];
    if (cr.type === "categorized" && cr.entries) {
      const e = cr.entries.find(en => String(en.value) === String(v));
      if (e) return e.color;
    } else if (cr.type === "graduated" && cr.classes) {
      const num = Number(v);
      const c = cr.classes.find(cl => num >= cl.min && num <= cl.max);
      if (c) return c.color;
    }
  }
  return base;
}
const geomType = (f) => f?.geometry?.type || f?.type || "";

// ── Charge les tuiles Web Mercator d'un tileUrl en un canvas monde (z niveaux) ──
async function loadMercatorWorld(tileUrl, z) {
  const n = 2 ** z, size = 256;
  const cv = document.createElement("canvas");
  cv.width = n * size; cv.height = n * size;
  const ctx = cv.getContext("2d");
  const jobs = [];
  for (let x = 0; x < n; x++)
    for (let y = 0; y < n; y++) {
      const url = tileUrl.replace("{z}", z).replace("{x}", x).replace("{y}", y).replace("{-y}", (n - 1 - y));
      jobs.push(new Promise(res => {
        const img = new Image();
        img.crossOrigin = "anonymous";
        img.onload = () => { try { ctx.drawImage(img, x * size, y * size, size, size); } catch (_) {} res(true); };
        img.onerror = () => res(false);
        img.src = url;
      }));
    }
  await Promise.all(jobs);
  return { canvas: cv, W: n * size, H: n * size };
}

// ── Reprojette un canvas Web Mercator monde vers la projection choisie ──
// projection déjà cadrée sur [x0,y0]-[x1,y1] (espace plein outW×outH). Calcule à
// résolution réduite (`sample` ≤ 1) puis le caller upscale via drawImage — borne le
// coût des inverts chers (Newton de Spilhaus) et des exports haute résolution.
function reprojectMercator(src, projection, outW, outH, box, sample = 1) {
  const [[x0, y0], [x1, y1]] = box;
  const sctx = src.canvas.getContext("2d");
  let sdata;
  try { sdata = sctx.getImageData(0, 0, src.W, src.H).data; }   // peut lever si tuiles non-CORS
  catch (e) { throw new Error("Tuiles raster non lisibles (CORS) — reprojection impossible."); }
  const rw = Math.max(1, Math.round(outW * sample)), rh = Math.max(1, Math.round(outH * sample));
  const inv = 1 / sample, tol = 0.75 * inv;
  const out = document.createElement("canvas");
  out.width = rw; out.height = rh;
  const octx = out.getContext("2d");
  const dst = octx.createImageData(rw, rh);
  const d = dst.data;
  const sW = src.W, sH = src.H;
  for (let py = Math.max(0, Math.floor(y0 * sample)); py < Math.min(rh, y1 * sample); py++) {
    for (let px = Math.max(0, Math.floor(x0 * sample)); px < Math.min(rw, x1 * sample); px++) {
      const fx = (px + 0.5) * inv, fy = (py + 0.5) * inv;   // coordonnée pleine échelle
      const ll = projection.invert && projection.invert([fx, fy]);
      if (!ll) continue;
      const lon = ll[0], lat = ll[1];
      if (!(lon >= -180 && lon <= 180) || !(lat >= -MERC_LAT && lat <= MERC_LAT)) continue;   // hors domaine ou NaN
      // Aller-retour : ne garder que si le forward retombe sur le pixel (rejette
      // hémisphère caché, hors-carré Spilhaus ET les NaN — « NaN > tol » vaut false,
      // d'où la comparaison inversée).
      const fwd = projection([lon, lat]);
      if (!fwd || !(Math.abs(fwd[0] - fx) <= tol) || !(Math.abs(fwd[1] - fy) <= tol)) continue;
      const xm = (lon + 180) / 360;
      const ym = (1 - Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI / 180) / 2)) / Math.PI) / 2;
      const sx = Math.min(sW - 1, Math.max(0, (xm * sW) | 0));
      const sy = Math.min(sH - 1, Math.max(0, (ym * sH) | 0));
      const si = (sy * sW + sx) * 4, di = (py * rw + px) * 4;
      if (sdata[si + 3] === 0) continue;   // pixel source transparent
      d[di] = sdata[si]; d[di + 1] = sdata[si + 1]; d[di + 2] = sdata[si + 2]; d[di + 3] = sdata[si + 3];
    }
  }
  octx.putImageData(dst, 0, 0);
  // Colmatage des fissures de couture (λ'=±180 → colonne de pixels rejetés = trait
  // vide de 1-2 px) : dilatation par recopies décalées. Les décalages sont dessinés
  // d'abord (comblent les vides), puis l'image vraie par-dessus (préserve le reste).
  const grown = document.createElement("canvas");
  grown.width = rw; grown.height = rh;
  const g = grown.getContext("2d");
  for (const [dx, dy] of [[3, 0], [-3, 0], [2, 0], [-2, 0], [1, 0], [-1, 0], [0, 1], [0, -1], [0, 0]]) g.drawImage(out, dx, dy);
  return grown;   // rw×rh — à étirer sur outW×outH par le caller
}

export default function ProjectionExplorer({ layers = [], mapStyle = "positron", onClose }) {
  const C = useThemeContext();
  const isMobile = typeof window !== "undefined" && window.innerWidth < 640;
  const [key, setKey] = useState("mercator");
  const [grat, setGrat] = useState(true);
  const [tissot, setTissot] = useState(false);
  const [baseKey, setBaseKey] = useState(() => STYLE_TO_BASE[mapStyle] || "positron");
  const [title, setTitle] = useState("Carte OpenMapAgents");
  const [titleEdited, setTitleEdited] = useState(false);   // saisie manuelle → stop auto-titre

  const rasterLayers = useMemo(() => layers.filter(l => l.isRaster && l.tileUrl), [layers]);
  const vectorLayers = useMemo(() => layers.filter(l => l.geojson && !l.isRaster), [layers]);
  const [rasterId, setRasterId] = useState(rasterLayers[0]?.id || "");
  const [vecIds, setVecIds] = useState(() => new Set(vectorLayers.map(l => l.id)));
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);

  const def = PROJ.find(p => p.key === key) || PROJ[0];
  const land = useMemo(() => feature(worldTopo, worldTopo.objects.countries), []);
  const canvasRef = useRef(null);
  const mercCache = useRef({ id: null, data: null });    // tuiles Mercator du raster indicateur
  const baseCache = useRef({ key: null, data: null });   // tuiles Mercator du fond de plan

  const rasterLayer = useMemo(() => rasterLayers.find(l => l.id === rasterId) || null, [rasterLayers, rasterId]);
  const shownVectors = useMemo(() => vectorLayers.filter(l => vecIds.has(l.id)), [vectorLayers, vecIds]);

  // Titre auto = indicateur affiché (nom de la couche raster, sinon vecteur unique),
  // tant que l'utilisateur n'a pas saisi son propre titre.
  useEffect(() => {
    if (titleEdited) return;
    if (rasterLayer) setTitle(rasterLayer.name || "Carte");
    else if (shownVectors.length === 1) setTitle(shownVectors[0].name || "Carte");
    else setTitle("Carte OpenMapAgents");
  }, [rasterLayer, shownVectors, titleEdited]);

  // ── Construit une projection cadrée sur la zone carte du canvas ──
  const makeProjection = useCallback((W, H) => {
    const PAD = Math.round(W * 0.03);
    const titleH = Math.round(H * 0.1);
    const footerH = Math.round(H * 0.06);
    const box = [[PAD, titleH], [W - PAD, H - footerH]];
    const proj = def.make();
    const bw = box[1][0] - box[0][0], bh = box[1][1] - box[0][1];
    const cx = (box[0][0] + box[1][0]) / 2, cy = (box[0][1] + box[1][1]) / 2;
    if (key === "spilhaus") {
      // fitExtent (mesure du contour de sphère par échantillonnage) est peu fiable
      // avec la couture singulière d'Adams II + angle(-45) → sur-zoom et débordements
      // constatés. Cadrage DÉTERMINISTE : le carré redressé a un demi-côté
      // K = F(π/2 | m=½) = 1.85407 à scale 1 (validé numériquement) → on inscrit
      // le carré dans la zone carte, centré.
      const K = 1.8541;
      proj.scale((Math.min(bw, bh) / 2) / K * 0.995).translate([cx, cy]);
    } else {
      const fitObj = key === "mercator" ? MERC_RECT : SPHERE;
      try { proj.fitExtent(box, fitObj); } catch { /* fallback ci-dessous */ }
      // Garde-fou : certaines projections (azimutale/ortho) donnent un scale NaN/0
      // ou aberrant via fitExtent → repli sur un cadrage manuel centré.
      const s = proj.scale(), t = proj.translate();
      if (!Number.isFinite(s) || s <= 0 || !t || !Number.isFinite(t[0]) || !Number.isFinite(t[1])) {
        proj.scale(Math.min(bw, bh) / 6.4).translate([cx, cy]);
      }
    }
    try { proj.clipExtent([[box[0][0], box[0][1]], [box[1][0], box[1][1]]]); } catch { /* nop */ }
    return { proj, box, PAD, titleH, footerH };
  }, [def, key]);

  // ── Rendu complet sur un canvas (utilisé pour l'aperçu ET l'export) ──
  const render = useCallback(async (canvas, W, H, forExport) => {
    const { proj, box, titleH, footerH } = makeProjection(W, H);
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "#ffffff"; ctx.fillRect(0, 0, W, H);   // fond papier
    setErr(null);
    const sample = Math.min(1, 900 / Math.max(1, box[1][0] - box[0][0]));

    // ── Contenu carte sur un canvas séparé (permet le détourage océans) ──
    const mc = document.createElement("canvas");
    mc.width = W; mc.height = H;
    const m = mc.getContext("2d");
    const gp = geoPath(proj, m);

    // Clip au contour de la projection (le carré pour Spilhaus) → aucun débordement
    // du raster/fond/vecteurs au-delà de l'emprise, quelles que soient les options.
    m.save();
    m.beginPath(); gp(SPHERE); m.clip();

    // Mer (sous tout)
    m.beginPath(); gp(SPHERE); m.fillStyle = "#e9eef4"; m.fill();

    // Fond de plan reprojeté (Positron / Dark / Satellite / OSM)
    const baseUrl = BASEMAPS[baseKey]?.url;
    let baseOk = false;
    if (baseUrl) {
      try {
        if (baseCache.current.key !== baseKey || !baseCache.current.data) {
          baseCache.current = { key: baseKey, data: await loadMercatorWorld(baseUrl, 3) };
        }
        const bc = reprojectMercator(baseCache.current.data, proj, W, H, box, sample);
        m.drawImage(bc, 0, 0, W, H);
        baseOk = true;
      } catch (e) { setErr("Fond de plan : " + e.message); }
    }
    if (!baseOk) {
      // Repli : pays gris
      m.fillStyle = "#d8dee3"; m.strokeStyle = "#b6bfc6"; m.lineWidth = 0.4;
      land.features.forEach(f => { m.beginPath(); gp(f); m.fill(); m.stroke(); });
    }

    // Raster indicateur (NDVI, LST…) par-dessus le fond
    if (rasterLayer) {
      try {
        if (mercCache.current.id !== rasterLayer.id || !mercCache.current.data) {
          // z=3 → monde 2048×2048 en Web Mercator (assez pour un A4).
          mercCache.current = { id: rasterLayer.id, data: await loadMercatorWorld(rasterLayer.tileUrl, 3) };
        }
        const rc = reprojectMercator(mercCache.current.data, proj, W, H, box, sample);
        m.globalAlpha = rasterLayer.opacity ?? 0.9;
        m.drawImage(rc, 0, 0, W, H);
        m.globalAlpha = 1;
      } catch (e) { setErr(e.message); }
    }

    // Frontières fines par-dessus fond/raster (repères)
    if (baseOk || rasterLayer) {
      m.strokeStyle = "rgba(70,80,90,0.35)"; m.lineWidth = forExport ? 0.7 : 0.4;
      m.beginPath(); land.features.forEach(f => gp(f)); m.stroke();
    }

    // Graticule
    if (grat) {
      m.beginPath(); gp(geoGraticule10());
      m.strokeStyle = "rgba(90,110,120,0.4)"; m.lineWidth = forExport ? 0.7 : 0.4; m.stroke();
    }

    // Couches vectorielles + sémiologie
    gp.pointRadius(forExport ? 3.5 : 2.5);
    shownVectors.forEach(l => {
      const op = l.opacity ?? 0.85;
      const fc = l.geojson.type === "FeatureCollection" ? l.geojson.features
        : (l.geojson.type === "Feature" ? [l.geojson] : [{ type: "Feature", geometry: l.geojson, properties: {} }]);
      fc.forEach(f => {
        const t = geomType(f), col = featureStyle(l, f);
        m.beginPath(); gp(f);
        if (t.includes("Polygon")) {
          m.globalAlpha = op * 0.55; m.fillStyle = col; m.fill();
          m.globalAlpha = op; m.strokeStyle = col; m.lineWidth = forExport ? 1 : 0.6; m.stroke();
        } else if (t.includes("LineString")) {
          m.globalAlpha = op; m.strokeStyle = col; m.lineWidth = forExport ? 1.6 : 1; m.stroke();
        } else {
          m.globalAlpha = op; m.fillStyle = col; m.fill();
          m.lineWidth = forExport ? 1 : 0.6; m.strokeStyle = "#ffffff"; m.stroke();
        }
      });
    });
    m.globalAlpha = 1;

    // Indicatrices de Tissot
    if (tissot) {
      const gen = geoCircle().radius(5).precision(2);
      m.fillStyle = "rgba(239,159,39,0.30)"; m.strokeStyle = "rgba(239,159,39,0.9)"; m.lineWidth = forExport ? 1 : 0.6;
      for (let lon = -150; lon <= 150; lon += 30)
        for (let lat = -60; lat <= 60; lat += 30) {
          m.beginPath(); gp(gen.center([lon, lat])()); m.fill(); m.stroke();
        }
    }

    // Contour de la projection (bord net)
    m.beginPath(); gp(SPHERE);
    m.strokeStyle = "rgba(60,70,80,0.5)"; m.lineWidth = forExport ? 1.2 : 0.8; m.stroke();

    m.restore();   // fin du clip au contour

    ctx.drawImage(mc, 0, 0);

    // ── Habillage : titre, projection, légende, crédit ──
    const S = W / 760;   // échelle typographique
    ctx.textBaseline = "alphabetic"; ctx.textAlign = "left";
    ctx.fillStyle = "#111"; ctx.font = `700 ${Math.round(19 * S)}px sans-serif`;
    ctx.fillText((title || "Carte").slice(0, 70), Math.round(20 * S), Math.round(titleH * 0.55));
    ctx.fillStyle = "#667"; ctx.font = `${Math.round(11 * S)}px sans-serif`;
    ctx.fillText(`Projection ${def.label} · ${def.type}`, Math.round(20 * S), Math.round(titleH * 0.85));

    drawLegend(ctx, box, S, rasterLayer, shownVectors);

    ctx.fillStyle = "#99a"; ctx.font = `${Math.round(10 * S)}px sans-serif`;
    ctx.textAlign = "left"; ctx.fillText(new Date().toLocaleDateString("fr-FR"), Math.round(20 * S), H - Math.round(footerH * 0.35));
    ctx.textAlign = "right"; ctx.fillText("OpenMapAgents", W - Math.round(20 * S), H - Math.round(footerH * 0.35));
    ctx.textAlign = "left";
  }, [makeProjection, rasterLayer, shownVectors, grat, tissot, baseKey, title, land, def]);

  // Aperçu (re-render à chaque changement)
  useEffect(() => {
    const cv = canvasRef.current; if (!cv) return;
    let cancelled = false;
    setBusy(true);
    (async () => {
      try { await render(cv, cv.width, cv.height, false); } catch (e) { if (!cancelled) setErr(e.message); }
      if (!cancelled) setBusy(false);
    })();
    return () => { cancelled = true; };
  }, [render]);

  // Reset du cache raster si la couche change
  useEffect(() => { mercCache.current = { id: null, data: null }; }, [rasterId]);

  // ── Export ──
  const exportCanvas = useCallback(async () => {
    const cv = document.createElement("canvas");
    cv.width = 1600; cv.height = 1050;
    await render(cv, cv.width, cv.height, true);
    return cv;
  }, [render]);

  const savePNG = async () => {
    setBusy(true);
    try {
      const cv = await exportCanvas();
      const a = document.createElement("a");
      a.href = cv.toDataURL("image/png"); a.download = `carte_${def.key}_${Date.now()}.png`; a.click();
    } catch (e) { setErr(e.message); }
    setBusy(false);
  };
  const savePDF = async () => {
    setBusy(true);
    try {
      const { jsPDF } = await import("jspdf");
      const cv = await exportCanvas();
      const url = cv.toDataURL("image/jpeg", 0.94);
      const pdf = new jsPDF({ orientation: "landscape", unit: "mm", format: "a4" });
      const pw = pdf.internal.pageSize.getWidth(), ph = pdf.internal.pageSize.getHeight();
      // Fit en gardant le ratio du canvas
      const r = Math.min(pw / cv.width, ph / cv.height);
      const w = cv.width * r, h = cv.height * r;
      pdf.addImage(url, "JPEG", (pw - w) / 2, (ph - h) / 2, w, h);
      pdf.save(`carte_${def.key}_${Date.now()}.pdf`);
    } catch (e) { setErr(e.message); }
    setBusy(false);
  };
  const doPrint = async () => {
    setBusy(true);
    try {
      const cv = await exportCanvas();
      const url = cv.toDataURL("image/png");
      const w = window.open(""); if (!w) throw new Error("Pop-up bloquée.");
      w.document.write(`<img src="${url}" style="max-width:100%" onload="window.print()"/>`);
      w.document.close();
    } catch (e) { setErr(e.message); }
    setBusy(false);
  };

  const toggleVec = (id) => setVecIds(s => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n; });

  const inp = { fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" };
  const btn = (bg, fg) => ({ flex: 1, fontSize: 11, fontWeight: 600, padding: "8px 0", borderRadius: 7, border: "none", cursor: busy ? "default" : "pointer", background: bg, color: fg, display: "flex", alignItems: "center", justifyContent: "center", gap: 6 });

  return (
    <div onMouseDown={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,.55)", zIndex: 10040, display: "flex", alignItems: "center", justifyContent: "center", padding: isMobile ? 8 : "4vh 14px" }}>
      <div onMouseDown={e => e.stopPropagation()} style={{ width: 1060, maxWidth: "97vw", maxHeight: isMobile ? "95vh" : "92vh", background: C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 14, boxShadow: "0 24px 64px rgba(0,0,0,.5)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* En-tête */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 15px", borderBottom: `0.5px solid ${C.bdr}` }}>
          <IcMap size={18} color={C.acc} />
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 13.5, fontWeight: 600, color: C.txt }}>Composeur de carte — projections</div>
            <div style={{ fontSize: 10.5, color: C.dim }}>Vos données (raster + vecteurs) dans la projection choisie, à imprimer / exporter</div>
          </div>
          <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex", padding: 3 }}><IcX size={17} /></button>
        </div>

        {/* Corps */}
        <div style={{ flex: 1, overflowY: "auto", padding: isMobile ? 12 : 15, display: "flex", flexDirection: isMobile ? "column" : "row", gap: 14 }}>
          {/* Colonne gauche : réglages */}
          <div style={{ width: isMobile ? "auto" : 250, flexShrink: 0, display: "flex", flexDirection: "column", gap: 12 }}>
            {/* Projection */}
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 5 }}>Projection</div>
              <div style={{ display: "flex", gap: 5, flexDirection: isMobile ? "row" : "column", overflowX: isMobile ? "auto" : "visible" }}>
                {PROJ.map(p => {
                  const on = p.key === key;
                  return (
                    <button key={p.key} onClick={() => setKey(p.key)} style={{ display: "flex", alignItems: "center", gap: 7, flexShrink: 0, textAlign: "left", cursor: "pointer", borderRadius: 6, padding: "6px 8px", background: on ? C.acc + "16" : "transparent", border: on ? `1.5px solid ${C.acc}` : `0.5px solid ${C.bdr}`, whiteSpace: "nowrap" }}>
                      <span style={{ width: 6, height: 6, borderRadius: 3, background: TYPE_COLOR(C, p.type), flexShrink: 0 }} />
                      <span style={{ fontSize: 11.5, fontWeight: on ? 600 : 500, color: on ? C.acc : C.txt }}>{p.label}</span>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Titre */}
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4 }}>Titre</div>
              <input value={title} onChange={e => { setTitle(e.target.value); setTitleEdited(true); }} style={inp} />
              {titleEdited && <button onClick={() => setTitleEdited(false)} style={{ marginTop: 3, background: "none", border: "none", color: C.dim, fontSize: 9, cursor: "pointer", padding: 0 }}>↺ Titre auto (indicateur)</button>}
            </div>

            {/* Fond de plan */}
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4 }}>Fond de plan</div>
              <select value={baseKey} onChange={e => setBaseKey(e.target.value)} style={{ ...inp, cursor: "pointer" }}>
                {Object.entries(BASEMAPS).map(([k, b]) => <option key={k} value={k}>{b.label}</option>)}
              </select>
            </div>

            {/* Raster */}
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4 }}>Couche raster</div>
              <select value={rasterId} onChange={e => setRasterId(e.target.value)} style={{ ...inp, cursor: "pointer" }}>
                <option value="">— Aucune —</option>
                {rasterLayers.map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
              </select>
              {rasterLayers.length === 0 && <div style={{ fontSize: 9, color: C.dim, marginTop: 3 }}>Générez d'abord un indice (NDVI, LST…) à l'échelle voulue.</div>}
            </div>

            {/* Vecteurs */}
            {vectorLayers.length > 0 && (
              <div>
                <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4, display: "flex", alignItems: "center", gap: 5 }}><IcLayers3 size={11} /> Couches vectorielles</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 3, maxHeight: 130, overflowY: "auto" }}>
                  {vectorLayers.map(l => (
                    <label key={l.id} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 11, color: C.txt, cursor: "pointer", padding: "3px 4px" }}>
                      <input type="checkbox" checked={vecIds.has(l.id)} onChange={() => toggleVec(l.id)} />
                      <span style={{ width: 9, height: 9, borderRadius: 2, background: l.color || C.acc, flexShrink: 0 }} />
                      <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{l.name}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}

            {/* Habillage */}
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.txt, cursor: "pointer" }}>
                <input type="checkbox" checked={grat} onChange={e => setGrat(e.target.checked)} /> Graticule
              </label>
              <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.txt, cursor: "pointer" }}>
                <input type="checkbox" checked={tissot} onChange={e => setTissot(e.target.checked)} /> Tissot
              </label>
            </div>
          </div>

          {/* Colonne droite : aperçu + export */}
          <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", gap: 10 }}>
            <div style={{ position: "relative", borderRadius: 10, overflow: "hidden", border: `0.5px solid ${C.bdr}`, background: "#fff" }}>
              <canvas ref={canvasRef} width={760} height={500} style={{ width: "100%", height: "auto", display: "block" }} />
              {busy && <div style={{ position: "absolute", top: 8, right: 10, fontSize: 10, color: "#556", background: "rgba(255,255,255,.8)", borderRadius: 5, padding: "2px 8px" }}>Rendu…</div>}
            </div>

            {err && <div style={{ fontSize: 10.5, color: C.red, background: C.red + "12", border: `0.5px solid ${C.red}33`, borderRadius: 6, padding: "6px 9px" }}>{err}</div>}

            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={savePNG} disabled={busy} style={btn(C.hover, C.txt)}><IcImage size={14} /> PNG</button>
              <button onClick={savePDF} disabled={busy} style={btn(C.acc, "#fff")}><IcFileDown size={14} /> PDF</button>
              <button onClick={doPrint} disabled={busy} style={btn(C.hover, C.txt)}><IcPrint size={14} /> Imprimer</button>
            </div>
            <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.5 }}>
              Le raster est reprojeté depuis ses tuiles (échelle mondiale conseillée pour une carte du monde). Les pôles au-delà de ±85° ne sont pas couverts par les tuiles (Web Mercator).
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Légende (canvas) : barre raster + puces vecteurs ──
function drawLegend(ctx, box, S, rasterLayer, vectors) {
  const items = [];
  if (rasterLayer?.visParams?.palette?.length && !/RGB|False Color/i.test(rasterLayer.name || "")) {
    items.push({ kind: "ramp", colors: rasterLayer.visParams.palette.map(c => c.startsWith("#") ? c : "#" + c), min: rasterLayer.visParams.min ?? 0, max: rasterLayer.visParams.max ?? 1, name: (rasterLayer.name || "").slice(0, 26) });
  } else if (rasterLayer?.legend?.length) {
    rasterLayer.legend.slice(0, 8).forEach(e => items.push({ kind: "chip", color: e.color, label: (e.label || "").slice(0, 24) }));
  }
  vectors.forEach(l => {
    const cr = l.classResult;
    if (cr?.type === "categorized" && cr.entries) cr.entries.slice(0, 6).forEach(e => items.push({ kind: "chip", color: e.color, label: String(e.value).slice(0, 22) }));
    else if (cr?.type === "graduated" && cr.classes) cr.classes.forEach(c => items.push({ kind: "chip", color: c.color, label: `${(+c.min).toFixed(1)}–${(+c.max).toFixed(1)}` }));
    else items.push({ kind: "chip", color: l.color || "#3388ff", label: (l.name || "").slice(0, 22) });
  });
  if (!items.length) return;

  const pad = Math.round(9 * S), row = Math.round(16 * S), fs = Math.round(10.5 * S);
  let h = pad;
  items.forEach(it => { h += it.kind === "ramp" ? row * 1.9 : row; });
  h += pad - row + Math.round(6 * S);
  const boxW = Math.round(190 * S);
  const bx = box[0][0] + Math.round(8 * S), by = box[1][1] - h - Math.round(8 * S);

  ctx.save();
  ctx.fillStyle = "rgba(255,255,255,0.88)"; ctx.strokeStyle = "rgba(0,0,0,0.15)"; ctx.lineWidth = 1;
  ctx.beginPath();
  if (ctx.roundRect) ctx.roundRect(bx, by, boxW, h, 6 * S); else ctx.rect(bx, by, boxW, h);
  ctx.fill(); ctx.stroke();

  let y = by + pad + fs;
  ctx.textAlign = "left"; ctx.font = `${fs}px sans-serif`;
  items.forEach(it => {
    if (it.kind === "ramp") {
      const barW = boxW - pad * 2, barH = Math.round(9 * S), gx = bx + pad, gy = y - fs * 0.8;
      const grad = ctx.createLinearGradient(gx, 0, gx + barW, 0);
      it.colors.forEach((c, i) => grad.addColorStop(i / (it.colors.length - 1), c));
      ctx.fillStyle = grad; ctx.fillRect(gx, gy, barW, barH);
      ctx.fillStyle = "#333"; ctx.font = `${Math.round(9 * S)}px sans-serif`;
      ctx.textAlign = "left"; ctx.fillText(fmt(it.min), gx, gy + barH + fs);
      ctx.textAlign = "right"; ctx.fillText(fmt(it.max), gx + barW, gy + barH + fs);
      ctx.textAlign = "left"; ctx.font = `${fs}px sans-serif`;
      y += row * 1.9;
    } else {
      ctx.fillStyle = it.color; ctx.fillRect(bx + pad, y - fs * 0.85, fs * 0.9, fs * 0.9);
      ctx.fillStyle = "#222"; ctx.fillText(it.label, bx + pad + fs * 1.4, y);
      y += row;
    }
  });
  ctx.restore();
}
const fmt = (v) => Math.abs(v) >= 1000 ? (+v).toFixed(0) : Math.abs(v) >= 1 ? (+v).toFixed(1) : (+v).toFixed(2);

/**
 * ChartLayer.jsx — Rend les graphiques d'une couche vecteur sur la carte.
 *
 * Une couche `symbol` dont l'`icon-image` est piloté par les données. Trois
 * mécanismes natifs répondent au problème du chevauchement :
 *   • icon-allow-overlap:false + icon-padding → MapLibre masque les symboles
 *     qui se recouvrent, sans code de notre part ;
 *   • symbol-sort-key = total décroissant → ce sont les PLUS GROS qui
 *     survivent à l'arbitrage, et non un ordre arbitraire ;
 *   • minzoom → rien ne s'affiche tant qu'il n'y a pas la place.
 *
 * La taille combine le total de l'entité (aire proportionnelle, d'où la racine)
 * et le niveau de zoom, dans une seule expression `interpolate`.
 */
import { useMemo, useEffect } from "react";
import { Source, Layer } from "react-map-gl/maplibre";
// Import depuis @turf/turf : dépendance DÉCLARÉE, déjà présente dans le bundle
// principal (App.jsx l'importe), et dont la résolution est éprouvée ici. Le
// sous-paquet @turf/point-on-feature n'est qu'une dépendance transitive : son
// interop CJS/ESM peut rendre un objet de module au lieu de la fonction, et
// l'appel échouait alors silencieusement pour toute géométrie non ponctuelle.
import { pointOnFeature, centroid, booleanPointInPolygon, area } from "@turf/turf";
import { RAMPS } from "../config";
import { renderSprite, spriteKey, sizeFactor, resolveChartColors, DEFAULT_PX } from "../utils/chartSprites";

const valid = (c) => Array.isArray(c) && Number.isFinite(c[0]) && Number.isFinite(c[1]);

/**
 * Point d'ancrage du graphique, [lon, lat], ou null si la géométrie est
 * inexploitable. Gère Point, Polygon, MultiPolygon, lignes et collections.
 *
 * CENTROÏDE d'abord : c'est le centre visuel attendu, et `pointOnFeature` ne
 * le donne pas — il part du centre de la boîte englobante puis, s'il tombe
 * hors de la forme, se rabat sur le point de contour le plus proche, ce qui
 * décale nettement le graphique vers un bord.
 *
 * Repli sur `pointOnFeature` uniquement si le centroïde sort de la forme, ce
 * qui arrive sur les géométries concaves (pays en croissant, littoral échancré) :
 * mieux vaut alors un point excentré mais DANS le polygone qu'un graphique posé
 * en pleine mer.
 *
 * Pour un MULTIpolygone, tout se joue sur la **plus grande partie** : un pays à
 * territoires d'outre-mer ou un archipel verrait sinon son centroïde tiré au
 * large, entre les morceaux.
 */
function anchorOf(f) {
  const g = f?.geometry;
  if (!g) return null;
  try {
    if (g.type === "Point") return g.coordinates.slice(0, 2);
    if (g.type === "MultiPoint") return g.coordinates[0]?.slice(0, 2) ?? null;

    let target = f;
    if (g.type === "MultiPolygon" && g.coordinates.length > 1) {
      let best = null, bestA = -1;
      for (const coords of g.coordinates) {
        const part = { type: "Feature", properties: {}, geometry: { type: "Polygon", coordinates: coords } };
        let a = 0;
        try { a = area(part); } catch (_) { a = 0; }
        if (a > bestA) { bestA = a; best = part; }
      }
      if (best) target = best;
    }

    const c = centroid(target)?.geometry?.coordinates;
    const polygonal = target.geometry?.type === "Polygon" || target.geometry?.type === "MultiPolygon";
    if (valid(c)) {
      if (!polygonal) return c.slice(0, 2);                       // lignes : centroïde suffit
      try { if (booleanPointInPolygon(c, target)) return c.slice(0, 2); } catch (_) {}
    }

    const p = pointOnFeature(target)?.geometry?.coordinates;      // garanti DANS la forme
    return valid(p) ? p.slice(0, 2) : null;
  } catch (e) {
    console.warn("[ChartLayer] ancrage impossible:", g.type, e?.message);
    return null;
  }
}

export default function ChartLayer({ layer, mapRef }) {
  const cfg = layer?.chartCfg;

  // ── Points porteurs + définition des vignettes ──
  const { points, sprites } = useMemo(() => {
    const empty = { points: { type: "FeatureCollection", features: [] }, sprites: [] };
    if (!cfg?.vars?.length || !layer?.geojson?.features?.length) return empty;

    const { kind, vars, size = "proportional" } = cfg;
    // Palette + inversion + remplacements manuels, résolus au même endroit que
    // dans la légende et le panneau : les trois ne peuvent pas diverger.
    const colors = resolveChartColors(cfg, RAMPS, vars.length);
    const px = DEFAULT_PX[kind] || 64;

    const raw = layer.geojson.features.map(f => vars.map(v => {
      const n = Number(f?.properties?.[v]);
      return Number.isFinite(n) ? Math.max(n, 0) : 0;
    }));
    const totals = raw.map(vs => vs.reduce((a, b) => a + b, 0));
    const maxTotal = Math.max(...totals, 0);
    // Barres groupées : normalisées sur le maximum GLOBAL, sinon chaque entité
    // serait mise à sa propre échelle et deviendrait incomparable aux autres.
    const gMax = Math.max(...raw.flat(), 1e-9);

    const seen = new Map();
    const feats = [];
    let skipped = 0;
    layer.geojson.features.forEach((f, i) => {
      if (totals[i] <= 0) return;
      const pos = anchorOf(f);
      if (!pos) { skipped++; return; }

      // Toujours transmettre des valeurs NORMALISÉES : la clé de cache les
      // quantifie en pourcentages, des valeurs brutes rendraient chaque entité
      // unique et feraient exploser le nombre d'images.
      //   • barres groupées → rapport au maximum global (entités comparables)
      //   • parts d'un tout → rapport au total de l'entité
      const vals = kind === "bars"
        ? raw[i].map(v => v / gMax)
        : raw[i].map(v => (totals[i] > 0 ? v / totals[i] : 0));
      const key = spriteKey(kind, vals, colors, px);
      if (!seen.has(key)) seen.set(key, { key, kind, vals, colors, px });

      feats.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: pos },
        properties: {
          _icon: key,
          _s: size === "fixed" ? 1 : sizeFactor(totals[i], maxTotal),
          _sort: totals[i],
          ...Object.fromEntries(vars.map(v => [v, f?.properties?.[v]])),
        },
      });
    });

    // Un échec d'ancrage silencieux donnerait une carte vide sans explication —
    // c'est exactement ce qui masquait le problème sur les polygones.
    if (skipped) console.warn(`[ChartLayer] ${skipped} entité(s) sans point d'ancrage exploitable.`);

    return { points: { type: "FeatureCollection", features: feats }, sprites: [...seen.values()] };
  }, [layer, cfg]);

  // ── Enregistrement des images dans le style ──
  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map || !sprites.length) return;
    const added = [];
    for (const s of sprites) {
      try {
        if (map.hasImage(s.key)) continue;
        map.addImage(s.key, renderSprite(s.kind, s.vals, s.colors, s.px), { pixelRatio: 2 });
        added.push(s.key);
      } catch (e) { console.warn("[ChartLayer] addImage:", e); }
    }
    return () => {
      for (const k of added) { try { if (map.hasImage(k)) map.removeImage(k); } catch (_) {} }
    };
  }, [sprites, mapRef]);

  if (!points.features.length) return null;

  const minz = cfg.minzoom ?? 5;
  const k = Math.max(0, cfg.scale ?? 1);   // 0 = graphiques masqués
  return (
    <Source id={`${layer.id}-charts`} type="geojson" data={points}>
      <Layer
        id={`${layer.id}-charts-layer`}
        type="symbol"
        minzoom={minz}
        layout={{
          visibility: layer.visible ? "visible" : "none",
          "icon-image": ["get", "_icon"],
          // Taille = f(total de l'entité) × f(zoom) × facteur global réglable.
          // Le facteur est le levier direct contre la surcharge quand on choisit
          // d'afficher toutes les entités : réduire desserre sans rien masquer.
          "icon-size": [
            "interpolate", ["linear"], ["zoom"],
            minz,      ["*", 0.55 * k, ["get", "_s"]],
            minz + 8,  ["*", 1.50 * k, ["get", "_s"]],
          ],
          "icon-allow-overlap": !!cfg.overlap,      // false = anti-collision active
          "icon-ignore-placement": !!cfg.overlap,
          "icon-padding": 2,
          // Priorité aux plus gros totaux quand il faut trancher
          "symbol-sort-key": ["-", 0, ["get", "_sort"]],
        }}
        // Opacité PROPRE aux graphiques, indépendante de celle de la couche :
        // c'est ce qui permet d'estomper l'un pour lire l'autre.
        paint={{ "icon-opacity": cfg.opacity ?? 1 }}
      />
    </Source>
  );
}

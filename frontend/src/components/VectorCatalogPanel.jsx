/**
 * VectorCatalogPanel.jsx — Catalogue de données vectorielles ajoutables.
 *
 * Recherche dans un catalogue curaté (utils/vectorCatalog.js) puis « Ajouter »
 * une couche sur la carte. Deux sources : FeatureCollection GEE (via
 * /api/gee/vector, découpée sur l'emprise) ou GeoJSON public direct (séismes
 * USGS). Panneau autonome : reçoit onAddLayer, comme les autres outils.
 */
import { useMemo, useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { searchVectorCatalog, VECTOR_THEMES } from "../utils/vectorCatalog";
import {
  IcSearch, IcMapPin, IcSpline, IcHexagon, IcPlus,
  IcLoader, IcAlert, IcCheck, IcX, IcCalendar,
} from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const GEOM_ICON = { point: IcMapPin, line: IcSpline, polygon: IcHexagon };
const GEOM_LABEL = { point: "Points", line: "Lignes", polygon: "Polygones" };

// APIs renvoyant du JSON (pas du GeoJSON) → construction d'URL + conversion en
// points, sur l'emprise carte [w, s, e, n]. Les champs temporels sont conservés.
const API_BUILD = {
  gbif: (bb) => ({
    url: `https://api.gbif.org/v1/occurrence/search?hasCoordinate=true&limit=300&geometry=${
      encodeURIComponent(`POLYGON((${bb[0]} ${bb[1]},${bb[2]} ${bb[1]},${bb[2]} ${bb[3]},${bb[0]} ${bb[3]},${bb[0]} ${bb[1]}))`)}`,
    parse: (j) => (j.results || [])
      .filter(r => r.decimalLatitude != null && r.decimalLongitude != null)
      .map(r => ({ type: "Feature", geometry: { type: "Point", coordinates: [r.decimalLongitude, r.decimalLatitude] },
        properties: { species: r.species || r.scientificName || "", eventDate: r.eventDate || "",
                      country: r.country || "", dataset: r.datasetName || "" } })),
  }),
  inat: (bb) => ({
    url: `https://api.inaturalist.org/v1/observations?geo=true&per_page=200&swlng=${bb[0]}&swlat=${bb[1]}&nelng=${bb[2]}&nelat=${bb[3]}`,
    parse: (j) => (j.results || [])
      .filter(o => o.geojson?.coordinates)
      .map(o => ({ type: "Feature", geometry: o.geojson,
        properties: { species: o.species_guess || o.taxon?.name || "", observed_on: o.observed_on || "",
                      place: o.place_guess || "", quality: o.quality_grade || "" } })),
  }),
};

// Aplatit une valeur imbriquée en TEXTE lisible → propriétés exploitables en
// tableau (ex. EONET categories:[{title:"Wildfires"}] → "Wildfires",
// sources:[{id:"GDACS"}] → "GDACS"). Sans ça : « [object Object] ».
const flatVal = (v) => {
  if (v == null || typeof v !== "object") return v;
  if (Array.isArray(v)) return v.map(flatVal).filter(x => x !== null && x !== "").join(", ");
  return v.title ?? v.name ?? v.NAME ?? v.label ?? v.id ?? v.url ?? JSON.stringify(v);
};
const flattenFeatures = (gj) => {
  (gj?.features || []).forEach(f => {
    const p = f.properties || {};
    const o = {};
    for (const k in p) o[k] = flatVal(p[k]);
    f.properties = o;
  });
  return gj;
};

// Emprise d'un GeoJSON, pour cadrer la vue sur la couche ajoutée.
const geoBbox = (gj) => {
  let x0 = 180, y0 = 90, x1 = -180, y1 = -90, seen = false;
  const walk = (a) => {
    if (typeof a[0] === "number") {
      const [x, y] = a;
      if (x < x0) x0 = x; if (x > x1) x1 = x;
      if (y < y0) y0 = y; if (y > y1) y1 = y; seen = true;
    } else a.forEach(walk);
  };
  (gj?.features || []).forEach(f => f?.geometry?.coordinates && walk(f.geometry.coordinates));
  return seen ? [x0, y0, x1, y1] : null;
};

export default function VectorCatalogPanel({ layers, mapRef, onAddLayer, onAddLayerSilent }) {
  const C = useThemeContext();
  const [query, setQuery]   = useState("");
  const [scope, setScope]   = useState("view");     // "view" (emprise carte) | "world"
  const [busyId, setBusyId] = useState(null);
  const [err, setErr]       = useState(null);
  const [added, setAdded]   = useState({});          // id → {count, truncated}

  const results = useMemo(() => searchVectorCatalog(query), [query]);
  const byTheme = useMemo(() => {
    const m = {};
    results.forEach(e => { (m[e.theme] ||= []).push(e); });
    return m;
  }, [results]);

  const mapBbox = () => {
    const m = mapRef?.current?.getMap?.(); if (!m) return null;
    try {
      const b = m.getBounds();
      return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    } catch (_) { return null; }
  };

  const add = async (item) => {
    setErr(null); setBusyId(item.id);
    try {
      let geojson, count, truncated = false;
      if (item.source === "url") {                       // GeoJSON public direct
        const r = await fetch(item.url);
        if (!r.ok) throw new Error(`flux indisponible (${r.status})`);
        geojson = await r.json();
        count = geojson?.features?.length || 0;
      } else if (item.source === "api") {                // API JSON → points, sur l'emprise
        const bb = mapBbox();
        if (!bb) throw new Error("emprise carte introuvable");
        const b = API_BUILD[item.api]?.(bb);
        if (!b) throw new Error("API inconnue");
        const r = await fetch(b.url);
        if (!r.ok) throw new Error(`API indisponible (${r.status})`);
        const feats = b.parse(await r.json());
        geojson = { type: "FeatureCollection", features: feats }; count = feats.length;
        truncated = count >= 200;
      } else if (item.source === "proxy") {              // relayé par le backend (CORS)
        const r = await fetch(`${API}/api/gee/vector/proxy?url=${encodeURIComponent(item.url)}`);
        const j = await r.json();
        if (!r.ok) throw new Error(j.detail || `proxy ${r.status}`);
        geojson = (j?.type === "FeatureCollection") ? j : { type: "FeatureCollection", features: j?.features || [] };
        count = geojson.features?.length || 0;
      } else {                                           // FeatureCollection GEE
        const bbox = (scope === "world" && item.worldOk) ? null : mapBbox();
        const r = await fetch(`${API}/api/gee/vector`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ asset: item.asset, bbox, limit: 2000, simplify_m: item.simplify_m }),
        });
        const d = await r.json();
        if (!r.ok) throw new Error(d.detail || `Erreur ${r.status}`);
        geojson = d.geojson; count = d.count; truncated = d.truncated;
      }
      if (!count) throw new Error("aucune entité dans cette emprise");
      // Propriétés aplaties en texte → tableau exploitable ; champs de date conservés.
      flattenFeatures(geojson);
      // Ajout BRUT : pas de classification ni d'analyse imposée (couleur unie).
      // L'utilisateur choisira sa sémiologie.
      (onAddLayerSilent || onAddLayer)?.(geojson, item.name, "world_data", {});
      // Zoom sur l'emprise de la couche (flyTo si une seule entité ponctuelle).
      const bb = geoBbox(geojson);
      const m = mapRef?.current?.getMap?.();
      if (bb && m) {
        const [w, s, e, n] = bb;
        try {
          if (e - w < 0.02 && n - s < 0.02) m.flyTo({ center: [(w + e) / 2, (s + n) / 2], zoom: 8, duration: 800 });
          else m.fitBounds([[w, s], [e, n]], { padding: 40, duration: 800, maxZoom: 9 });
        } catch (_) {}
      }
      setAdded(a => ({ ...a, [item.id]: { count, truncated } }));
    } catch (e) {
      setErr(`${item.name} — ${e.message}`);
    }
    setBusyId(null);
  };

  const box = { background: C.bg, borderRadius: 8, border: `0.5px solid ${C.bdr}` };

  return (
    <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto" }}>

      <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
        Cherchez un jeu <b style={{ color: C.mut }}>vectoriel</b> (limites, routes, rivières, aires
        protégées, séismes…) et ajoutez-le sur la carte. Source Earth Engine ou flux public.
      </div>

      {/* Recherche */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, background: C.input,
                    border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "6px 9px" }}>
        <IcSearch size={14} color={C.dim} />
        <input value={query} onChange={e => setQuery(e.target.value)}
          placeholder="Rechercher : routes, écorégions, séismes…"
          style={{ flex: 1, minWidth: 0, background: "transparent", border: "none", outline: "none",
                   color: C.txt, fontSize: 11.5 }} />
        {query && (
          <button onClick={() => setQuery("")} title="Effacer" style={{ background: "none", border: "none",
            color: C.dim, cursor: "pointer", display: "flex", padding: 0 }}><IcX size={13} /></button>
        )}
      </div>

      {/* Emprise */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 10.5 }}>
        <span style={{ color: C.dim, flexShrink: 0 }}>Charger sur</span>
        {[["view", "Vue carte"], ["world", "Monde entier"]].map(([k, l]) => (
          <button key={k} onClick={() => setScope(k)} style={{
            fontFamily: F, fontSize: 10.5, padding: "4px 10px", borderRadius: 6, cursor: "pointer",
            background: scope === k ? C.acc + "18" : "transparent",
            border: `0.5px solid ${scope === k ? C.acc : C.bdr}`, color: scope === k ? C.acc : C.mut,
          }}>{l}</button>
        ))}
      </div>
      {scope === "world" && (
        <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.4, marginTop: -4 }}>
          « Monde » n'est possible que pour les petits jeux (⊕ pays, séismes…) ; les jeux denses
          (routes, districts) restent découpés sur la vue carte.
        </div>
      )}

      {err && (
        <div style={{ display: "flex", gap: 6, fontSize: 10, color: C.red, background: C.red + "12",
                      borderRadius: 6, padding: "7px 9px" }}>
          <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
        </div>
      )}

      {/* Résultats groupés par thème */}
      {results.length === 0 && (
        <div style={{ padding: "16px 8px", textAlign: "center", color: C.dim, fontSize: 11 }}>
          Aucun jeu pour « {query} ».
        </div>
      )}
      {VECTOR_THEMES.filter(t => byTheme[t]?.length).map(theme => (
        <div key={theme} style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em" }}>{theme}</div>
          {byTheme[theme].map(item => {
            const GIcon = GEOM_ICON[item.geom] || IcMapPin;
            const busy = busyId === item.id;
            const ok = added[item.id];
            const worldableNow = scope !== "world" || item.worldOk;
            return (
              <div key={item.id} style={{ ...box, padding: 9, display: "flex", alignItems: "flex-start", gap: 9 }}>
                <span style={{ marginTop: 1, color: C.mut, flexShrink: 0 }}><GIcon size={15} /></span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <span style={{ fontSize: 11.5, fontWeight: 500, color: C.txt, overflow: "hidden",
                                   textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.name}</span>
                    <span style={{ fontSize: 7.5, color: C.dim, border: `0.5px solid ${C.bdr}`,
                                   borderRadius: 3, padding: "0 3px", flexShrink: 0 }}>
                      {item.source === "gee" ? "GEE" : item.source === "proxy" ? "proxy" : "direct"}
                    </span>
                    {item.heavy && (
                      <span title="Fichier volumineux — le chargement peut être long" style={{
                        fontSize: 7.5, color: C.amb, border: `0.5px solid ${C.amb}55`, borderRadius: 3,
                        padding: "0 3px", flexShrink: 0 }}>lourd</span>
                    )}
                    {item.temporal && (
                      <span title="Données datées — filtrage/animation temporelle possible" style={{
                        display: "inline-flex", alignItems: "center", gap: 2, fontSize: 7.5, color: C.acc,
                        border: `0.5px solid ${C.acc}55`, borderRadius: 3, padding: "0 3px", flexShrink: 0 }}>
                        <IcCalendar size={8} /> temporel
                      </span>
                    )}
                  </div>
                  <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.4, marginTop: 1 }}>{item.desc}</div>
                  <div style={{ fontSize: 8.5, color: C.dim, marginTop: 2 }}>
                    {GEOM_LABEL[item.geom]}
                    {ok && <span style={{ color: C.acc }}> · {ok.count.toLocaleString("fr")} entités{ok.truncated ? " (plafonné, zoomez)" : ""}</span>}
                  </div>
                </div>
                <button onClick={() => add(item)} disabled={busy}
                  title={worldableNow ? "Ajouter à la carte" : "Sera découpé sur la vue carte"} style={{
                  flexShrink: 0, width: 26, height: 26, borderRadius: 6, cursor: busy ? "default" : "pointer",
                  background: ok ? C.acc + "18" : C.acc, color: ok ? C.acc : "#fff",
                  border: ok ? `0.5px solid ${C.acc}66` : "none",
                  display: "flex", alignItems: "center", justifyContent: "center",
                }}>
                  {busy ? <IcLoader size={13} className="vc-spin" /> : ok ? <IcCheck size={13} /> : <IcPlus size={14} />}
                </button>
              </div>
            );
          })}
        </div>
      ))}

      <div style={{ fontSize: 8.5, color: C.dim, lineHeight: 1.4, marginTop: 2 }}>
        Les jeux GEE sont découpés sur l'emprise et plafonnés à 2000 entités — zoomez pour un jeu
        dense. Catalogue de départ, extensible.
      </div>

      <style>{`@keyframes vc-spin{to{transform:rotate(360deg)}} .vc-spin{animation:vc-spin 1s linear infinite}`}</style>
    </div>
  );
}

/**
 * SharedMap.jsx — Affichage public d'une carte partagée
 * URL : /m/{slug}
 * Charge la carte en lecture seule depuis l'API
 */
import { useState, useEffect, useRef } from "react";
import Map, { Source, Layer, NavigationControl, ScaleControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { API, MAP_STYLES } from "../config";
import { IcMap } from "../icons";

export default function SharedMap({ slug }) {
  const [mapData, setMapData] = useState(null);
  const [state,   setState]   = useState(null);
  const [error,   setError]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [styleKey, setStyleKey] = useState("dark");
  const mapRef = useRef(null);

  useEffect(() => {
    fetch(`${API}/maps/share/${slug}`)
      .then(r => r.ok ? r.json() : Promise.reject("Carte introuvable"))
      .then(data => {
        setMapData(data);
        try { setState(JSON.parse(data.state_json)); } catch {}
      })
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [slug]);

  if (loading) return (
    <div style={{ height: "100vh", display: "flex", alignItems: "center",
                  justifyContent: "center", background: "#0d1117", color: "#888",
                  fontFamily: "system-ui, sans-serif", fontSize: 14 }}>
      Chargement...
    </div>
  );

  if (error) return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column",
                  alignItems: "center", justifyContent: "center",
                  background: "#0d1117", color: "#888", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ marginBottom: 12 }}><IcMap size={48}/></div>
      <div style={{ fontSize: 16, color: "#fff", marginBottom: 6 }}>Carte introuvable</div>
      <div style={{ fontSize: 12 }}>Cette carte n'existe pas ou n'est plus partagée</div>
      <a href="/" style={{ marginTop: 20, color: "#4af", fontSize: 12 }}>← Ouvrir OpenMapAgents</a>
    </div>
  );

  const vp = state?.viewport || { longitude: 2.35, latitude: 48.85, zoom: 5 };
  const layers = state?.layers || [];

  const getPaint = (l, gt) => {
    const cr = l.classResult;
    const ce = cr?.expression || l.color || "#3388ff";
    if (gt === "fill") return { "fill-color": ce, "fill-opacity": l.opacity || 0.6 };
    if (gt === "line") return { "line-color": ce, "line-width": 1.5, "line-opacity": l.opacity || 0.8 };
    if (gt === "circle") {
      if (cr?.type === "proportional" && cr.radiusExpression)
        return { "circle-radius": cr.radiusExpression, "circle-color": ce, "circle-opacity": l.opacity || 0.8,
                 "circle-stroke-width": 1, "circle-stroke-color": "#fff" };
      return { "circle-radius": l.radius || 5, "circle-color": ce, "circle-opacity": l.opacity || 0.8 };
    }
    return {};
  };

  return (
    <div style={{ height: "100vh", position: "relative", background: "#0d1117" }}>
      <Map
        ref={mapRef}
        initialViewState={vp}
        mapStyle={MAP_STYLES[styleKey]}
        style={{ width: "100%", height: "100%" }}
      >
        <NavigationControl position="top-right" />
        <ScaleControl position="bottom-right" />

        {layers.filter(l => l.visible !== false && l.geojson).map(l => (
          <Source key={l.id} id={l.id} type="geojson" data={l.geojson}>
            <Layer id={`${l.id}-fill`}    type="fill"   filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={getPaint(l,"fill")} />
            <Layer id={`${l.id}-line`}    type="line"   filter={["any",["==",["geometry-type"],"LineString"],["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={getPaint(l,"line")} />
            <Layer id={`${l.id}-circle`}  type="circle" filter={["==",["geometry-type"],"Point"]} paint={getPaint(l,"circle")} />
          </Source>
        ))}
      </Map>

      {/* Bandeau info */}
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0,
        background: "rgba(13,17,23,0.92)", backdropFilter: "blur(8px)",
        borderBottom: "1px solid rgba(255,255,255,0.08)",
        padding: "12px 20px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        fontFamily: "system-ui, sans-serif",
      }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: "#fff" }}>{mapData?.title}</div>
          {mapData?.description && (
            <div style={{ fontSize: 11, color: "#888", marginTop: 2 }}>{mapData.description}</div>
          )}
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <div style={{ fontSize: 10, color: "#666" }}>
            {mapData?.view_count} vue{mapData?.view_count > 1 ? "s" : ""}
          </div>
          <a href="/" style={{
            fontSize: 11, padding: "6px 14px", borderRadius: 6,
            background: "#4af3", color: "#4af", textDecoration: "none",
            border: "1px solid #4af4",
          }}>Ouvrir OpenMapAgents →</a>
        </div>
      </div>

      {/* Style switcher */}
      <div style={{
        position: "absolute", bottom: 40, left: 12,
        display: "flex", gap: 4,
      }}>
        {Object.keys(MAP_STYLES).map(k => (
          <button key={k} onClick={() => setStyleKey(k)} style={{
            fontFamily: "system-ui", fontSize: 10, padding: "4px 8px",
            borderRadius: 5, cursor: "pointer",
            background: styleKey === k ? "#4af" : "rgba(0,0,0,0.6)",
            color: styleKey === k ? "#fff" : "#aaa",
            border: "1px solid rgba(255,255,255,0.1)",
          }}>{k}</button>
        ))}
      </div>
    </div>
  );
}

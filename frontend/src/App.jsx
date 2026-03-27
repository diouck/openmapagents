import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import Map, { Source, Layer, Popup, NavigationControl, ScaleControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import * as turf from "@turf/turf";

import { useTheme } from "./theme";
import { F, M, API, MAP_STYLES, LAYER_COLORS, EXPORT_FORMATS } from "./config";
import { buildClassification } from "./utils/classification";
import { encodePermalink, decodePermalink, exportPDF, importFile, computeBounds, getPopupFields } from "./utils/helpers";
import { executeSpatialOp } from "./utils/spatial";
import { computeRoute, computeIsochrone } from "./utils/routing";
import { Badge } from "./components/ui";
import Legend from "./components/Legend";
import LayerPanel from "./components/LayerPanel";
import ChatPanel from "./components/ChatPanel";
import BottomPanel from "./components/BottomPanel";
import MapToolbar from "./components/MapToolbar";
import MiniMap from "./components/MiniMap";
import RoutePanel from "./components/RoutePanel";
import PrintPanel from "./components/PrintPanel";
import DBPanel from "./components/DBPanel";
import OGCPanel from "./components/OGCPanel";

export default function App() {
  const { name: themeName, C, toggle: toggleTheme } = useTheme();
  const [layers, setLayers] = useState([]);
  const [mapSt, setMapSt] = useState("dark");
  const [vs, setVs] = useState({ longitude: -1.55, latitude: 47.22, zoom: 12, pitch: 0, bearing: 0 });
  const [popup, setPopup] = useState(null);
  const [chatOpen, setChatOpen] = useState(true);
  const [btab, setBtab] = useState(null);
  const [tool, setTool] = useState("pointer");
  const [measurePts, setMeasurePts] = useState([]);
  const [measureRes, setMeasureRes] = useState(null);
  const [bufferLayer, setBufferLayer] = useState(null);
  const [drawPts, setDrawPts] = useState([]);
  const [dragOver, setDragOver] = useState(false);
  const [bufferRadius, setBufferRadius] = useState(500);
  const [routeProfile, setRouteProfile] = useState("foot");
  const [isoTime, setIsoTime] = useState(10);
  const [routePts, setRoutePts] = useState([]);
  const [routeLayer, setRouteLayer] = useState(null);
  const [isoLayer, setIsoLayer] = useState(null);
  const [routeLoading, setRouteLoading] = useState(false);
  const routeMapClickRef = useRef(null); // ref to avoid stale closure in handleMapClick
  const setRouteMapClick = useCallback((fn) => { routeMapClickRef.current = fn; }, []);
  const [routeMarkers, setRouteMarkers] = useState(null);
  const mapRef = useRef(null);
  const lctr = useRef(0);
  const fileRef = useRef(null);

  // Restore permalink
  useEffect(() => {
    const s = decodePermalink();
    if (s?.c) setVs(p => ({ ...p, longitude: s.c[0], latitude: s.c[1], zoom: s.z || 12 }));
    if (s?.s) setMapSt(s.s);
  }, []);

  const mapCtx = useMemo(() => ({
    layers: layers.map(l => {
      // Compute bbox and geometry types for the LLM
      const feats = l.geojson?.features || [];
      const geomTypes = [...new Set(feats.map(f => f.geometry?.type).filter(Boolean))];
      const bbox = l.geojson?.metadata?.bbox || (() => {
        const bounds = computeBounds(feats);
        return bounds ? [bounds[0][0], bounds[0][1], bounds[1][0], bounds[1][1]] : null;
      })();
      return { id: l.id, name: l.name, featureCount: l.featureCount, visible: l.visible, theme: l.theme, geomTypes, bbox };
    }),
    center: [vs.longitude, vs.latitude], zoom: vs.zoom,
  }), [layers, vs]);

  // ─── FIT BOUNDS ──────────────────────────────────────
  const fitFeatures = useCallback((feats) => {
    const bounds = computeBounds(feats);
    if (!bounds) return;
    const m = mapRef.current?.getMap?.();
    if (m) setTimeout(() => m.fitBounds(bounds, {
      padding: { top: 60, bottom: btab ? 280 : 60, left: 300, right: chatOpen ? 400 : 60 },
      maxZoom: 17, duration: 1200,
    }), 100);
  }, [btab, chatOpen]);

  // ─── ADD LAYER HELPER ────────────────────────────────
  const addLayer = useCallback((geojson, name, theme = "data") => {
    const ci = lctr.current % LAYER_COLORS.length;
    const lid = `layer_${Date.now()}_${lctr.current++}`;
    setLayers(p => [...p, {
      id: lid, name, theme, geojson, visible: true,
      color: LAYER_COLORS[ci], opacity: 0.8, radius: 6,
      featureCount: geojson.features?.length || 0,
      classCfg: null, classResult: null,
      heatmap: false, extrude: false, extrudeAttr: "", extrudeScale: 1,
      cluster: false, labels: false, labelAttr: "name",
    }]);
    if (geojson.features?.length) fitFeatures(geojson.features);
  }, [fitFeatures]);

  // Use a ref to always have fresh layers for handleToolResult
  const layersRef = useRef(layers);
  useEffect(() => { layersRef.current = layers; }, [layers]);

  // Helper: clip a GeoJSON to a polygon layer
  const clipToPolygonLayer = useCallback((gj, polyLayer) => {
    const polygons = (polyLayer.geojson?.features || []).filter(f =>
      f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
    );
    if (!polygons.length) return gj;
    // Use the LARGEST polygon (first one = outermost for isochrones)
    const mask = polygons[0];
    const clipped = (gj.features || []).filter(f => {
      try {
        if (f.geometry?.type === "Point") return turf.booleanPointInPolygon(f, mask);
        if (f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon") {
          const inter = turf.intersect(turf.featureCollection([f, mask]));
          return !!inter;
        }
        return true;
      } catch { return false; }
    });
    console.log(`Auto-clip: ${gj.features?.length || 0} → ${clipped.length} features (clipped to ${polyLayer.name})`);
    return { ...gj, features: clipped, metadata: { ...gj.metadata, clipped: true, clip_layer: polyLayer.name, original_count: gj.features?.length || 0 } };
  }, []);

  // ─── TOOL RESULTS FROM LLM ──────────────────────────
  const handleToolResult = useCallback((action) => {
    if (action.type === "add_layer") {
      let gj = action.data;
      const params = gj.metadata?.query_params || {};
      const theme = gj.metadata?.theme || "data";
      const currentLayers = layersRef.current;

      // Find a visible polygon layer (isochrone, buffer, etc.)
      const polyLayer = currentLayers.find(l => {
        if (!l.visible) return false;
        return (l.geojson?.features || []).some(f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon");
      });

      // If there's a polygon layer and new data has points → smart re-query + clip
      const hasPoints = gj.features?.some(f => f.geometry?.type === "Point");
      if (polyLayer && hasPoints) {
        // Calculate EXACT bbox of the polygon layer
        const polyBbox = turf.bbox(polyLayer.geojson);
        // Check if the query bbox covers the polygon bbox
        const queryBbox = gj.metadata?.bbox;
        const coversWell = queryBbox && queryBbox[0] <= polyBbox[0] && queryBbox[1] <= polyBbox[1] && queryBbox[2] >= polyBbox[2] && queryBbox[3] >= polyBbox[3];

        if (!coversWell) {
          // The LLM query didn't cover the full polygon — RE-QUERY with correct bbox
          console.log(`Re-query needed: polygon bbox [${polyBbox.map(v=>v.toFixed(4))}] not covered by query bbox ${queryBbox ? `[${queryBbox.map(v=>v.toFixed(4))}]` : 'none'}`);
          const pad = 0.005; // ~500m padding
          fetch(`${API}/query?theme=${params.theme || theme}&xmin=${polyBbox[0]-pad}&ymin=${polyBbox[1]-pad}&xmax=${polyBbox[2]+pad}&ymax=${polyBbox[3]+pad}&limit=1000${params.category ? `&category=${params.category}` : ""}`)
            .then(r => r.json())
            .then(fullGj => {
              if (fullGj.features?.length) {
                const clipped = clipToPolygonLayer(fullGj, polyLayer);
                const name = params.category ? `${params.category} (clipped)` : `${theme} (clipped)`;
                addLayer(clipped, name, theme);
              } else {
                // Fallback: clip what we have
                const clipped = clipToPolygonLayer(gj, polyLayer);
                const name = params.category ? `${params.category} (${theme})` : theme;
                addLayer(clipped, name, theme);
              }
            })
            .catch(() => {
              // On error, just clip what we have
              const clipped = clipToPolygonLayer(gj, polyLayer);
              const name = params.category ? `${params.category} (${theme})` : theme;
              addLayer(clipped, name, theme);
            });
          return; // async — don't continue
        } else {
          // Query covers the polygon — just clip
          gj = clipToPolygonLayer(gj, polyLayer);
        }
      }

      const name = params.category ? `${params.category} (${theme})` : theme;
      addLayer(gj, name, theme);
    } else if (action.type === "fly_to") {
      const m = mapRef.current?.getMap?.();
      if (m) m.flyTo({ center: [action.longitude, action.latitude], zoom: action.zoom || 14, pitch: action.pitch || 0, duration: 1500 });
    } else if (action.type === "remove_layer") {
      if (action.layer_id === "all") setLayers([]); else setLayers(p => p.filter(l => l.id !== action.layer_id));
    } else if (action.type === "spatial_analysis") {
      // LLM triggered spatial analysis — execute client-side with turf.js
      try {
        const layerA = layers.find(l => l.name === action.layer_a_name);
        const layerB = action.layer_b_name ? layers.find(l => l.name === action.layer_b_name) : null;
        if (!layerA) { console.warn("Spatial: layer A not found:", action.layer_a_name); return; }
        const result = executeSpatialOp(action.operation, layerA, layerB, action.params || {});
        if (result?.features?.length) {
          addLayer(result, action.result_name || `${action.operation}_result`, "analysis");
        }
      } catch (e) { console.error("Spatial analysis error:", e); }
    } else if (action.type === "compute_route") {
      computeRoute(action.waypoints, action.profile || "foot")
        .then(gj => {
          setRouteLayer(gj);
          if (gj.features?.length) fitFeatures(gj.features);
          // Also add as a permanent layer
          addLayer(gj, `Route ${action.profile || "foot"} ${gj.metadata?.distance_km || ""}km`, "route");
        })
        .catch(e => console.error("Route error:", e));
    } else if (action.type === "compute_isochrone") {
      computeIsochrone(action.center, action.time_minutes || 10, action.profile || "foot")
        .then(gj => {
          setIsoLayer(gj);
          if (gj.features?.length) fitFeatures(gj.features);
          // Also add as a permanent layer
          addLayer(gj, `Isochrone ${action.time_minutes || 10}min ${action.profile || "foot"}`, "isochrone");
        })
        .catch(e => console.error("Isochrone error:", e));
    }
  }, [addLayer]);

  // ─── LAYER OPERATIONS ────────────────────────────────
  const toggleL = (id) => {
    setLayers(p => p.map(l => {
      if (l.id !== id) return l;
      const newVisible = !l.visible;
      // Couche raster : masquer/afficher sur la map MapLibre
      if (l.isRaster && l.mapLayerId) {
        try {
          const map = mapRef.current?.getMap?.();
          if (map?.getLayer(l.mapLayerId)) {
            map.setLayoutProperty(l.mapLayerId, "visibility", newVisible ? "visible" : "none");
          }
        } catch (_) {}
      }
      return { ...l, visible: newVisible };
    }));
  };
  const removeL = (id) => {
    const layer = layers.find(l => l.id === id);
    if (layer?.isRaster) {
      try {
        const map = mapRef.current?.getMap?.();
        if (map) {
          if (layer.mapLayerId && map.getLayer(layer.mapLayerId))   map.removeLayer(layer.mapLayerId);
          if (layer.mapSourceId && map.getSource(layer.mapSourceId)) map.removeSource(layer.mapSourceId);
          // Couches vectorielles ajoutées avec suffixes
          ["_fill","_line","_circle","_layer"].forEach(sfx => {
            const lid = layer.mapSourceId + sfx;
            if (map.getLayer(lid)) map.removeLayer(lid);
          });
        }
      } catch (_) {}
    }
    setLayers(p => p.filter(l => l.id !== id));
  };
  const styleL = (id, s) => {
    setLayers(p => p.map(l => {
      if (l.id !== id) return l;
      // Propager l'opacité sur les couches raster MapLibre
      if (l.isRaster && s.opacity !== undefined) {
        try {
          const map = mapRef.current?.getMap?.();
          if (map) {
            const layerId = `${id}-layer`;
            if (map.getLayer(layerId)) {
              map.setPaintProperty(layerId, "raster-opacity", s.opacity);
            }
            // Tuiles vectorielles — plusieurs layers
            ["fill", "line", "circle"].forEach(t => {
              const lid = `${id}-${t}`;
              if (map.getLayer(lid)) {
                const prop = t === "fill" ? "fill-opacity" : t === "line" ? "line-opacity" : "circle-opacity";
                map.setPaintProperty(lid, prop, s.opacity);
              }
            });
          }
        } catch (_) {}
      }
      return { ...l, ...s };
    }));
  };
  const renameL = (id, name) => setLayers(p => p.map(l => l.id === id ? { ...l, name } : l));
  const classifyL = useCallback((id, cfg) => {
    setLayers(p => p.map(l => {
      if (l.id !== id) return l;
      const r = cfg ? buildClassification(l, cfg) : null;
      return { ...l, classCfg: cfg, classResult: r };
    }));
  }, []);

  const exportL = id => {
    const l = layers.find(x => x.id === id); if (!l) return;
    const blob = new Blob([JSON.stringify(l.geojson, null, 2)], { type: "application/json" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
    a.download = `${l.name.replace(/\s+/g, "_")}.geojson`; a.click();
  };

  const exportFmt = async (id, fmt) => {
    const l = layers.find(x => x.id === id); if (!l) return;
    try {
      const res = await fetch(`${API}/export`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ theme: l.theme, bbox: l.geojson.metadata?.bbox || [-2, 47, -1, 48], format: fmt, limit: l.featureCount + 100 }),
      });
      const blob = await res.blob(); const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
      const ext = { GeoPackage: ".gpkg", Shapefile: ".shp", CSV: ".csv", FlatGeobuf: ".fgb" }[fmt] || ".geojson";
      a.download = `${l.name.replace(/\s+/g, "_")}${ext}`; a.click();
    } catch (e) { alert(`Export ${fmt}: ${e.message}`); }
  };


  // ── Réordonner les couches ─────────────────────────────────
  const moveLayerUp   = id => setLayers(p => {
    const i = p.findIndex(l => l.id === id);
    if (i <= 0) return p;
    const n = [...p]; [n[i-1], n[i]] = [n[i], n[i-1]]; return n;
  });
  const moveLayerDown = id => setLayers(p => {
    const i = p.findIndex(l => l.id === id);
    if (i < 0 || i >= p.length - 1) return p;
    const n = [...p]; [n[i], n[i+1]] = [n[i+1], n[i]]; return n;
  });

  // ── Zoomer sur l'emprise d'une couche ──────────────────────
  const zoomToLayer = useCallback((id) => {
    const layer = layers.find(l => l.id === id);
    if (!layer) return;

    if (layer.isRaster) {
      // Tenter de récupérer les features rendues depuis la source MapLibre
      try {
        const map = mapRef.current?.getMap?.();
        if (!map) return;
        // Pour WMS/WMTS : interroger le serveur GetCapabilities bbox ou utiliser la vue actuelle
        if (layer.bbox) {
          const [w, s, e, n] = layer.bbox;
          map.fitBounds([[w, s], [e, n]], { padding: 60, duration: 1000 });
        } else {
          // Pas de bbox connue — zoomer sur les features rendues (vector tiles) ou garder la vue
          const feats = map.querySourceFeatures(id);
          if (feats.length) {
            const coords = feats.flatMap(f => {
              const g = f.geometry;
              if (!g) return [];
              if (g.type === "Point") return [g.coordinates];
              if (g.type === "LineString") return g.coordinates;
              if (g.type === "Polygon") return g.coordinates[0] || [];
              return [];
            }).filter(c => c?.length >= 2);
            if (coords.length) {
              const lons = coords.map(c => c[0]), lats = coords.map(c => c[1]);
              map.fitBounds(
                [[Math.min(...lons), Math.min(...lats)], [Math.max(...lons), Math.max(...lats)]],
                { padding: 60, duration: 1000 }
              );
              return;
            }
          }
          // Dernier recours : message info
          console.info("Zoom couche raster : bbox non disponible, utilisez le serveur GetCapabilities.");
        }
      } catch (e) { console.warn("zoomToLayer raster:", e.message); }
      return;
    }

    // Couche vecteur GeoJSON
    const feats = layer.geojson?.features || [];
    if (!feats.length) return;
    const bounds = computeBounds(feats);
    if (bounds) mapRef.current?.getMap?.()?.fitBounds(bounds, { padding: 60, maxZoom: 17, duration: 1000 });
  }, [layers]);

  // ── Ajouter une couche raster WMS/WMTS ─────────────────────
  const addRasterLayer = useCallback((info) => {
    // info: { id, layerId, name, type, url, tileUrl, opacity }
    const ci = lctr.current % LAYER_COLORS.length;
    lctr.current++;
    setLayers(p => [...p, {
      id:           info.id,
      name:         info.name,
      theme:        info.type,   // "wms" | "wmts" | "vector"
      isRaster:     true,
      mapSourceId:  info.id,
      mapLayerId:   info.layerId,
      tileUrl:      info.tileUrl || info.url,
      geojson:      null,
      visible:      true,
      color:        LAYER_COLORS[ci],
      opacity:      info.opacity ?? 0.85,
      featureCount: "raster",
      classCfg:     null,
      classResult:  null,
      bbox:         info.bbox || null,
    }]);
  }, []);

  // Drag state DB
  const [dbPos,    setDbPos]    = useState({ x: null, y: null });
  const dbDragRef  = useRef({ dragging: false, ox: 0, oy: 0 });
  const dbPanelRef = useRef(null);
  const onDbDragStart = useCallback((e) => {
    if (e.button !== 0) return; e.preventDefault();
    const rect = dbPanelRef.current?.getBoundingClientRect();
    dbDragRef.current = { dragging: true, ox: e.clientX-(rect?.left??0), oy: e.clientY-(rect?.top??0) };
    const mv = ev => { if(!dbDragRef.current.dragging)return; setDbPos({x:ev.clientX-dbDragRef.current.ox,y:ev.clientY-dbDragRef.current.oy}); };
    const up = () => { dbDragRef.current.dragging=false; window.removeEventListener("mousemove",mv); window.removeEventListener("mouseup",up); };
    window.addEventListener("mousemove",mv); window.addEventListener("mouseup",up);
  }, []);

  // Drag state OGC
  const [ogcPos,    setOgcPos]    = useState({ x: null, y: null });
  const ogcDragRef  = useRef({ dragging: false, ox: 0, oy: 0 });
  const ogcPanelRef = useRef(null);
  const onOgcDragStart = useCallback((e) => {
    if (e.button !== 0) return; e.preventDefault();
    const rect = ogcPanelRef.current?.getBoundingClientRect();
    ogcDragRef.current = { dragging: true, ox: e.clientX-(rect?.left??0), oy: e.clientY-(rect?.top??0) };
    const mv = ev => { if(!ogcDragRef.current.dragging)return; setOgcPos({x:ev.clientX-ogcDragRef.current.ox,y:ev.clientY-ogcDragRef.current.oy}); };
    const up = () => { ogcDragRef.current.dragging=false; window.removeEventListener("mousemove",mv); window.removeEventListener("mouseup",up); };
    window.addEventListener("mousemove",mv); window.addEventListener("mouseup",up);
  }, []);

  const zoomFeat = useCallback((ln, lt) => {
    const m = mapRef.current?.getMap?.(); if (m) m.flyTo({ center: [ln, lt], zoom: 17, duration: 800 });
  }, []);

  // ─── FILE IMPORT ─────────────────────────────────────
  const doImport = useCallback(async (file) => {
    try {
      const gj = await importFile(file);
      if (gj?.features?.length) addLayer(gj, file.name.replace(/\.[^.]+$/, ""), "import");
      else alert("Fichier vide ou format non reconnu.");
    } catch (e) { alert("Erreur import: " + e.message); }
  }, [addLayer]);

  // ─── PDF / PERMALINK ─────────────────────────────────
  const doPDF = useCallback((fmt) => exportPDF(mapRef, vs, layers, fmt), [vs, layers]);
  const shareLink = useCallback(() => {
    const hash = encodePermalink(vs, mapSt, layers);
    const url = `${window.location.origin}${window.location.pathname}#${hash}`;
    navigator.clipboard?.writeText(url).then(() => alert("Lien copié !")).catch(() => prompt("Copiez :", url));
    window.location.hash = hash;
  }, [vs, mapSt, layers]);

  // ─── PAINT EXPRESSIONS ───────────────────────────────
  const getPaint = useCallback((layer, gt) => {
    const ce = layer.classResult?.expression || layer.color;
    if (gt === "fill") return { "fill-color": ce, "fill-opacity": layer.opacity * 0.4 };
    if (gt === "line") return { "line-color": ce, "line-width": 1.5, "line-opacity": layer.opacity };
    if (gt === "circle") return { "circle-radius": layer.radius || 5, "circle-color": ce, "circle-opacity": layer.opacity, "circle-stroke-width": 1, "circle-stroke-color": "#fff", "circle-stroke-opacity": 0.3 };
    return {};
  }, []);

  // ─── MAP CLICK ───────────────────────────────────────
  const handleMapClick = useCallback((e) => {
    const lng = e.lngLat.lng, lat = e.lngLat.lat;
    if (tool === "measure_dist") {
      const pts = [...measurePts, [lng, lat]]; setMeasurePts(pts);
      if (pts.length >= 2) { const d = turf.length(turf.lineString(pts), { units: "kilometers" }); setMeasureRes(d < 1 ? `${(d * 1000).toFixed(0)}m` : `${d.toFixed(2)}km`); }
    } else if (tool === "measure_area") {
      const pts = [...measurePts, [lng, lat]]; setMeasurePts(pts);
      if (pts.length >= 3) { const a = turf.area(turf.polygon([[...pts, pts[0]]])); setMeasureRes(a < 10000 ? `${Math.round(a)} m²` : `${(a / 10000).toFixed(2)} ha`); }
    } else if (tool === "buffer") {
      const buf = turf.buffer(turf.point([lng, lat]), bufferRadius / 1000, { units: "kilometers" });
      setBufferLayer({ type: "FeatureCollection", features: [buf, { type: "Feature", geometry: { type: "Point", coordinates: [lng, lat] }, properties: {} }] });
    } else if (tool === "draw") {
      setDrawPts(p => [...p, [lng, lat]]);
    } else if ((tool === "route" || tool === "isochrone") && routeMapClickRef.current) {
      routeMapClickRef.current(lng, lat);
    } else {
      // Auto-detect popup from any feature
      if (!e.features?.length) { setPopup(null); return; }
      const f = e.features[0];
      setPopup({ lng, lat, properties: f.properties, layerName: f.layer?.id || "" });
    }
  }, [tool, measurePts, bufferRadius]);

  useEffect(() => { setMeasurePts([]); setMeasureRes(null); setBufferLayer(null); setDrawPts([]); setRoutePts([]); setRouteLayer(null); setIsoLayer(null); setRouteMarkers(null); }, [tool]);

  // ─── OVERLAY GEOJSON ─────────────────────────────────
  const measureGJ = useMemo(() => {
    if (!measurePts.length) return null;
    const feats = measurePts.map(p => ({ type: "Feature", geometry: { type: "Point", coordinates: p }, properties: {} }));
    if (measurePts.length >= 2 && tool === "measure_dist") feats.push({ type: "Feature", geometry: { type: "LineString", coordinates: measurePts }, properties: {} });
    if (measurePts.length >= 3 && tool === "measure_area") feats.push({ type: "Feature", geometry: { type: "Polygon", coordinates: [[...measurePts, measurePts[0]]] }, properties: {} });
    return { type: "FeatureCollection", features: feats };
  }, [measurePts, tool]);

  const drawGJ = useMemo(() => {
    if (!drawPts.length) return null;
    const feats = drawPts.map(p => ({ type: "Feature", geometry: { type: "Point", coordinates: p }, properties: {} }));
    if (drawPts.length >= 3) feats.push({ type: "Feature", geometry: { type: "Polygon", coordinates: [[...drawPts, drawPts[0]]] }, properties: {} });
    else if (drawPts.length >= 2) feats.push({ type: "Feature", geometry: { type: "LineString", coordinates: drawPts }, properties: {} });
    return { type: "FeatureCollection", features: feats };
  }, [drawPts]);

  const intIds = useMemo(() => {
    const ids = [];
    layers.filter(l => l.visible).forEach(l => {
      if (l.cluster) { ids.push(`${l.id}-unclustered`, `${l.id}-clusters`); }
      else if (!l.heatmap && !l.extrude) { ids.push(`${l.id}-circle`, `${l.id}-fill`); }
      else if (l.extrude) { ids.push(`${l.id}-extrude`); }
    });
    return ids;
  }, [layers]);

  // ─── RENDER ──────────────────────────────────────────
  return (
    <div style={{ fontFamily: F, background: C.bg, color: C.txt, height: "100vh", display: "flex", flexDirection: "column", overflow: "hidden" }}
      onDragOver={e => { e.preventDefault(); setDragOver(true); }}
      onDragLeave={e => { e.preventDefault(); setDragOver(false); }}
      onDrop={e => { e.preventDefault(); setDragOver(false); if (e.dataTransfer?.files?.[0]) doImport(e.dataTransfer.files[0]); }}>

      <style>{`@keyframes spin{to{transform:rotate(360deg)}} @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}`}</style>
      <input ref={fileRef} type="file" accept=".geojson,.json,.csv,.tsv,.zip,.shp" style={{ display: "none" }}
        onChange={e => { if (e.target.files?.[0]) doImport(e.target.files[0]); e.target.value = ""; }} />

      {/* Drag overlay */}
      {dragOver && <div style={{ position: "fixed", inset: 0, zIndex: 9999, background: "rgba(29,158,117,0.15)", border: "3px dashed #1D9E75", display: "flex", alignItems: "center", justifyContent: "center", pointerEvents: "none" }}>
        <div style={{ background: C.card, padding: "24px 40px", borderRadius: 12, fontSize: 16, fontWeight: 500, color: C.acc }}>Déposez votre fichier ici</div>
      </div>}

      {/* Header */}
      <header style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: `0.5px solid ${C.bdr}`, background: C.card, flexShrink: 0, gap: 8, flexWrap: "wrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 28, height: 28, borderRadius: 7, background: C.acc, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, fontWeight: 700, color: "#fff" }}>O</div>
          <div><div style={{ fontSize: 13, fontWeight: 600 }}>Overture Maps Agent</div><div style={{ fontSize: 10, color: C.dim }}>Carte conversationnelle</div></div>
        </div>
        <div style={{ display: "flex", gap: 2 }}>
          {Object.keys(MAP_STYLES).map(k => (
            <button key={k} onClick={() => setMapSt(k)} style={{ fontFamily: F, fontSize: 10, padding: "4px 8px", borderRadius: 5, border: `0.5px solid ${mapSt === k ? C.acc + "55" : C.bdr}`, background: mapSt === k ? C.acc + "12" : "transparent", color: mapSt === k ? C.acc : C.dim, cursor: "pointer", textTransform: "capitalize" }}>{k}</button>
          ))}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <button onClick={toggleTheme} style={{ fontFamily: F, fontSize: 11, padding: "4px 10px", borderRadius: 6, background: C.hover, border: `0.5px solid ${C.bdr}`, color: C.txt, cursor: "pointer" }}>{themeName === "dark" ? "☀ Light" : "◑ Dark"}</button>
          <button onClick={() => fileRef.current?.click()} style={{ fontFamily: F, fontSize: 11, padding: "4px 10px", borderRadius: 6, background: C.hover, border: `0.5px solid ${C.bdr}`, color: C.txt, cursor: "pointer" }}>↑ Import</button>
          <button onClick={shareLink} style={{ fontFamily: F, fontSize: 11, padding: "4px 10px", borderRadius: 6, background: C.hover, border: `0.5px solid ${C.bdr}`, color: C.txt, cursor: "pointer" }}>Partager</button>
          <Badge color={C.acc}>DuckDB</Badge>
          <button onClick={() => setChatOpen(o => !o)} style={{ fontFamily: F, fontSize: 11, padding: "5px 10px", borderRadius: 6, background: chatOpen ? C.acc + "18" : "transparent", border: `0.5px solid ${chatOpen ? C.acc + "44" : C.bdr}`, color: chatOpen ? C.acc : C.dim, cursor: "pointer" }}>{chatOpen ? "Chat ✕" : "Chat"}</button>
        </div>
      </header>

      {/* Body */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>
        <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>

          {/* Map */}
          <div style={{ flex: 1, position: "relative" }}>
            <Map ref={mapRef} {...vs} onMove={e => setVs(e.viewState)}
              style={{ width: "100%", height: "100%" }} mapStyle={MAP_STYLES[mapSt]}
              maplibreLogo={false} preserveDrawingBuffer={true}
              onClick={handleMapClick} interactiveLayerIds={tool === "pointer" ? intIds : []}
              cursor={tool !== "pointer" ? "crosshair" : "grab"}
              onContextMenu={async (e) => {
                e.preventDefault();
                const { lng, lat } = e.lngLat;
                try {
                  const res = await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json`, { headers: { "User-Agent": "OvertureExplorer/1.0" } });
                  const data = await res.json();
                  setPopup({ lng, lat, properties: { adresse: data.display_name || "Adresse inconnue", lat: lat.toFixed(5), lon: lng.toFixed(5) }, layerName: "reverse_geocode" });
                } catch {
                  setPopup({ lng, lat, properties: { lat: lat.toFixed(5), lon: lng.toFixed(5) }, layerName: "coords" });
                }
              }}>
              <NavigationControl position="top-right" />
              <ScaleControl position="bottom-left" />

              {/* ── RASTER LAYERS (WMS / WMTS / Vector Tiles) ── */}
              {layers.map(l => {
                if (!l.isRaster) return null;
                if (l.type === "vector") {
                  // Tuiles vectorielles
                  return (
                    <Source key={l.id} id={l.id} type="vector" tiles={[l.tileUrl]} minzoom={0} maxzoom={22}>
                      <Layer id={`${l.id}-fill`}   type="fill"   layout={{ visibility: l.visible ? "visible" : "none" }} filter={["==", ["geometry-type"], "Polygon"]}    paint={{ "fill-color": l.color || "#1D9E75", "fill-opacity": l.opacity ?? 0.3 }} />
                      <Layer id={`${l.id}-line`}   type="line"   layout={{ visibility: l.visible ? "visible" : "none" }} filter={["any", ["==", ["geometry-type"], "LineString"], ["==", ["geometry-type"], "Polygon"]]} paint={{ "line-color": l.color || "#1D9E75", "line-width": 1.5, "line-opacity": l.opacity ?? 1 }} />
                      <Layer id={`${l.id}-circle`} type="circle" layout={{ visibility: l.visible ? "visible" : "none" }} filter={["==", ["geometry-type"], "Point"]}       paint={{ "circle-color": l.color || "#1D9E75", "circle-radius": 4, "circle-stroke-width": 1, "circle-stroke-color": "#fff", "circle-opacity": l.opacity ?? 1 }} />
                    </Source>
                  );
                }
                // WMS / WMTS — source raster
                return (
                  <Source key={l.id} id={l.id} type="raster" tiles={[l.tileUrl]} tileSize={256}>
                    <Layer id={`${l.id}-layer`} type="raster"
                      layout={{ visibility: l.visible ? "visible" : "none" }}
                      paint={{ "raster-opacity": l.opacity ?? 0.85 }} />
                  </Source>
                );
              })}

              {/* ── NORMAL LAYERS (flat) ── */}
              {layers.map(l => l.visible && !l.isRaster && !l.heatmap && !l.extrude && !l.cluster && (
                <Source key={l.id} id={l.id} type="geojson" data={l.geojson}>
                  <Layer id={`${l.id}-fill`} type="fill" filter={["any", ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]]} paint={getPaint(l, "fill")} />
                  <Layer id={`${l.id}-outline`} type="line" filter={["any", ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]]} paint={getPaint(l, "line")} />
                  <Layer id={`${l.id}-road`} type="line" filter={["==", ["geometry-type"], "LineString"]} paint={{ "line-color": l.classResult?.expression || l.color, "line-width": 2, "line-opacity": l.opacity }} />
                  <Layer id={`${l.id}-circle`} type="circle" filter={["==", ["geometry-type"], "Point"]} paint={getPaint(l, "circle")} />
                  {l.labels && <Layer id={`${l.id}-label`} type="symbol" layout={{ "text-field": ["get", l.labelAttr || "name"], "text-size": 11, "text-offset": [0, 1.2], "text-anchor": "top", "text-max-width": 10 }} paint={{ "text-color": l.color, "text-halo-color": "#fff", "text-halo-width": 1 }} />}
                </Source>
              ))}

              {/* ── 3D EXTRUSION LAYERS ── */}
              {layers.map(l => l.visible && !l.isRaster && l.extrude && (
                <Source key={`${l.id}-3d`} id={`${l.id}-3d`} type="geojson" data={l.geojson}>
                  <Layer id={`${l.id}-extrude`} type="fill-extrusion"
                    filter={["any", ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]]}
                    paint={{
                      "fill-extrusion-color": l.classResult?.expression || l.color,
                      "fill-extrusion-height": l.extrudeAttr
                        ? ["*", ["to-number", ["get", l.extrudeAttr], 5], l.extrudeScale || 1]
                        : ["*", ["to-number", ["get", "height"], 5], l.extrudeScale || 1],
                      "fill-extrusion-base": 0,
                      "fill-extrusion-opacity": l.opacity * 0.85,
                    }} />
                  {l.labels && <Layer id={`${l.id}-3dlabel`} type="symbol" layout={{ "text-field": ["get", l.labelAttr || "name"], "text-size": 10, "text-anchor": "center" }} paint={{ "text-color": "#fff", "text-halo-color": "#000", "text-halo-width": 1 }} />}
                </Source>
              ))}

              {/* ── CLUSTER LAYERS ── */}
              {layers.map(l => l.visible && !l.isRaster && l.cluster && (
                <Source key={`${l.id}-cl`} id={`${l.id}-cl`} type="geojson" data={l.geojson}
                  cluster={true} clusterMaxZoom={14} clusterRadius={50}>
                  <Layer id={`${l.id}-clusters`} type="circle"
                    filter={["has", "point_count"]}
                    paint={{ "circle-color": ["step", ["get", "point_count"], l.color, 10, C.amb, 50, C.red], "circle-radius": ["step", ["get", "point_count"], 18, 10, 24, 50, 32], "circle-opacity": 0.85, "circle-stroke-width": 2, "circle-stroke-color": "#fff" }} />
                  <Layer id={`${l.id}-cluster-count`} type="symbol"
                    filter={["has", "point_count"]}
                    layout={{ "text-field": "{point_count_abbreviated}", "text-size": 12 }}
                    paint={{ "text-color": "#fff" }} />
                  <Layer id={`${l.id}-unclustered`} type="circle"
                    filter={["!", ["has", "point_count"]]}
                    paint={{ "circle-radius": l.radius || 5, "circle-color": l.color, "circle-opacity": l.opacity, "circle-stroke-width": 1, "circle-stroke-color": "#fff", "circle-stroke-opacity": 0.3 }} />
                </Source>
              ))}

              {/* ── HEATMAP LAYERS ── */}
              {layers.map(l => l.visible && !l.isRaster && l.heatmap && (
                <Source key={`${l.id}-hm`} id={`${l.id}-hm`} type="geojson" data={l.geojson}>
                  <Layer id={`${l.id}-heat`} type="heatmap" paint={{
                    "heatmap-weight": 1,
                    "heatmap-intensity": ["interpolate", ["linear"], ["zoom"], 0, 1, 15, 3],
                    "heatmap-color": ["interpolate", ["linear"], ["heatmap-density"], 0, "rgba(0,0,0,0)", .2, "#1D9E75", .4, "#EF9F27", .6, "#D85A30", .8, "#E24B4A", 1, "#fff"],
                    "heatmap-radius": ["interpolate", ["linear"], ["zoom"], 0, 4, 15, 30],
                    "heatmap-opacity": l.opacity,
                  }} />
                </Source>
              ))}

              {/* Measure overlay */}
              {measureGJ && <Source id="measure" type="geojson" data={measureGJ}>
                <Layer id="measure-pts" type="circle" filter={["==", ["geometry-type"], "Point"]} paint={{ "circle-radius": 5, "circle-color": "#fff", "circle-stroke-width": 2, "circle-stroke-color": C.amb }} />
                <Layer id="measure-line" type="line" filter={["==", ["geometry-type"], "LineString"]} paint={{ "line-color": C.amb, "line-width": 2, "line-dasharray": [4, 2] }} />
                <Layer id="measure-poly" type="fill" filter={["==", ["geometry-type"], "Polygon"]} paint={{ "fill-color": C.amb, "fill-opacity": .15 }} />
              </Source>}

              {/* Buffer overlay */}
              {bufferLayer && <Source id="buffer" type="geojson" data={bufferLayer}>
                <Layer id="buffer-fill" type="fill" filter={["==", ["geometry-type"], "Polygon"]} paint={{ "fill-color": C.pnk, "fill-opacity": .15 }} />
                <Layer id="buffer-line" type="line" filter={["==", ["geometry-type"], "Polygon"]} paint={{ "line-color": C.pnk, "line-width": 2, "line-dasharray": [4, 2] }} />
                <Layer id="buffer-pt" type="circle" filter={["==", ["geometry-type"], "Point"]} paint={{ "circle-radius": 6, "circle-color": C.pnk, "circle-stroke-width": 2, "circle-stroke-color": "#fff" }} />
              </Source>}

              {/* Draw overlay */}
              {drawGJ && <Source id="draw" type="geojson" data={drawGJ}>
                <Layer id="draw-pts" type="circle" filter={["==", ["geometry-type"], "Point"]} paint={{ "circle-radius": 5, "circle-color": "#fff", "circle-stroke-width": 2, "circle-stroke-color": C.blu }} />
                <Layer id="draw-line" type="line" filter={["any", ["==", ["geometry-type"], "LineString"], ["==", ["geometry-type"], "Polygon"]]} paint={{ "line-color": C.blu, "line-width": 2 }} />
                <Layer id="draw-fill" type="fill" filter={["==", ["geometry-type"], "Polygon"]} paint={{ "fill-color": C.blu, "fill-opacity": .1 }} />
              </Source>}

              {/* Route overlay */}
              {routeLayer && <Source id="route" type="geojson" data={routeLayer}>
                <Layer id="route-line" type="line" filter={["==", ["geometry-type"], "LineString"]} paint={{ "line-color": "#378ADD", "line-width": 4, "line-opacity": 0.85 }} />
                <Layer id="route-pts" type="circle" filter={["all", ["==", ["geometry-type"], "Point"], ["==", ["get", "type"], "waypoint"]]}
                  paint={{ "circle-radius": 8, "circle-color": "#378ADD", "circle-stroke-width": 2, "circle-stroke-color": "#fff" }} />
                <Layer id="route-labels" type="symbol" filter={["all", ["==", ["geometry-type"], "Point"], ["==", ["get", "type"], "waypoint"]]}
                  layout={{ "text-field": ["get", "label"], "text-size": 11, "text-offset": [0, 1.5], "text-anchor": "top" }}
                  paint={{ "text-color": "#378ADD", "text-halo-color": "#fff", "text-halo-width": 1.5 }} />
              </Source>}

              {/* Isochrone overlay */}
              {isoLayer && <Source id="isochrone" type="geojson" data={isoLayer}>
                <Layer id="iso-fill" type="fill" filter={["==", ["geometry-type"], "Polygon"]}
                  paint={{ "fill-color": ["step", ["get", "time_min"], "#1D9E75", 10, "#EF9F27", 15, "#D85A30", 20, "#E24B4A"], "fill-opacity": 0.2 }} />
                <Layer id="iso-line" type="line" filter={["==", ["geometry-type"], "Polygon"]}
                  paint={{ "line-color": ["step", ["get", "time_min"], "#1D9E75", 10, "#EF9F27", 15, "#D85A30", 20, "#E24B4A"], "line-width": 2, "line-dasharray": [4, 2] }} />
                <Layer id="iso-center" type="circle" filter={["==", ["geometry-type"], "Point"]}
                  paint={{ "circle-radius": 8, "circle-color": "#1D9E75", "circle-stroke-width": 2, "circle-stroke-color": "#fff" }} />
                <Layer id="iso-labels" type="symbol" filter={["==", ["geometry-type"], "Polygon"]}
                  layout={{ "text-field": ["get", "label"], "text-size": 12, "text-anchor": "center" }}
                  paint={{ "text-color": "#333", "text-halo-color": "#fff", "text-halo-width": 1.5 }} />
              </Source>}

              {/* Route planning markers (A/B) */}
              {routeMarkers && <Source id="route-markers" type="geojson" data={routeMarkers}>
                <Layer id="route-marker-circle" type="circle" paint={{
                  "circle-radius": 10,
                  "circle-color": ["match", ["get", "type"], "origin", "#378ADD", "dest", "#E24B4A", "#888"],
                  "circle-stroke-width": 3, "circle-stroke-color": "#fff",
                }} />
                <Layer id="route-marker-label" type="symbol"
                  layout={{ "text-field": ["get", "label"], "text-size": 14, "text-anchor": "center", "text-allow-overlap": true }}
                  paint={{ "text-color": "#fff" }} />
              </Source>}

              {/* Popup — auto-detects all properties */}
              {popup && (
                <Popup longitude={popup.lng} latitude={popup.lat} anchor="bottom" onClose={() => setPopup(null)} closeButton closeOnClick={false}>
                  <div style={{ fontFamily: F, padding: "2px 0", minWidth: 160, maxWidth: 280 }}>
                    {(() => {
                      const fields = getPopupFields(popup.properties);
                      const nameField = fields.find(f => f.isName);
                      return <>
                        <div style={{ fontSize: 12, fontWeight: 600, color: "#222", marginBottom: 4 }}>
                          {nameField ? nameField.value : "Sans nom"}
                        </div>
                        {fields.filter(f => !f.isName).map(f => (
                          <div key={f.key} style={{ fontSize: 11, color: "#555", padding: "1px 0" }}>
                            <span style={{ color: "#888" }}>{f.key}:</span> {f.value}
                          </div>
                        ))}
                      </>;
                    })()}
                  </div>
                </Popup>
              )}
            </Map>

            {/* Map overlays */}
            <MapToolbar activeTool={tool} onTool={setTool} measureResult={measureRes}
              bufferRadius={bufferRadius} onBufferRadius={setBufferRadius}
              routeProfile={routeProfile} onRouteProfile={setRouteProfile}
              isoTime={isoTime} onIsoTime={setIsoTime} />

            {/* Route/Isochrone Panel */}
            {(tool === "route" || tool === "isochrone") && (
              <RoutePanel mode={tool} profile={routeProfile} onProfileChange={setRouteProfile}
                onClose={() => { setTool("pointer"); setRouteMarkers(null); }}
                onSetMapClick={setRouteMapClick}
                onAddLayer={addLayer}
                onMarkers={(origin, dest) => {
                  const feats = [];
                  if (origin) feats.push({ type: "Feature", geometry: { type: "Point", coordinates: origin }, properties: { type: "origin", label: "A" } });
                  if (dest) feats.push({ type: "Feature", geometry: { type: "Point", coordinates: dest }, properties: { type: "dest", label: "B" } });
                  setRouteMarkers(feats.length ? { type: "FeatureCollection", features: feats } : null);
                }}
                onResult={(gj) => {
                  if (gj.features?.some(f => f.properties?.type === "route")) {
                    setRouteLayer(gj); setIsoLayer(null);
                  } else {
                    setIsoLayer(gj); setRouteLayer(null);
                  }
                  if (gj.features?.length) fitFeatures(gj.features);
                }} />
            )}


            {/* Database Panel — draggable */}
            {tool === "database" && (
              <div ref={dbPanelRef} style={{
                position: "fixed",
                ...(dbPos.x !== null ? { left: dbPos.x, top: dbPos.y } : { top: 50, left: 10 }),
                zIndex: 25, width: 340, maxHeight: "80vh",
                background: C.card, borderRadius: 10,
                border: `0.5px solid ${C.bdr}`, boxShadow: "0 4px 20px rgba(0,0,0,0.25)",
                display: "flex", flexDirection: "column", overflow: "hidden", userSelect: "none",
              }}>
                <div onMouseDown={onDbDragStart} style={{ padding: "10px 14px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0, cursor: "grab" }}>
                  <div style={{ fontSize: 13, fontWeight: 500, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}>
                    <span style={{ fontSize: 11, color: C.dim, letterSpacing: 2 }}>⠿</span>🗄 Base de données
                  </div>
                  <button onClick={() => { setTool("pointer"); setDbPos({ x: null, y: null }); }} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", fontSize: 16 }}>✕</button>
                </div>
                <DBPanel onAddLayer={addLayer} />
              </div>
            )}

            {/* OGC Panel — WMS/WMTS/WFS/Tuiles vectorielles — draggable */}
            {tool === "ogc" && (
              <div ref={ogcPanelRef} style={{
                position: "fixed",
                ...(ogcPos.x !== null ? { left: ogcPos.x, top: ogcPos.y } : { top: 50, left: 10 }),
                zIndex: 25, width: 360, maxHeight: "82vh",
                background: C.card, borderRadius: 10,
                border: `0.5px solid ${C.bdr}`, boxShadow: "0 4px 20px rgba(0,0,0,0.25)",
                display: "flex", flexDirection: "column", overflow: "hidden", userSelect: "none",
              }}>
                <div onMouseDown={onOgcDragStart} style={{ padding: "10px 14px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0, cursor: "grab" }}>
                  <div style={{ fontSize: 13, fontWeight: 500, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}>
                    <span style={{ fontSize: 11, color: C.dim, letterSpacing: 2 }}>⠿</span>📡 Services OGC
                  </div>
                  <button onClick={() => { setTool("pointer"); setOgcPos({ x: null, y: null }); }} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", fontSize: 16 }}>✕</button>
                </div>
                <OGCPanel mapRef={mapRef} onAddLayer={addLayer} onAddRasterLayer={addRasterLayer} />
              </div>
            )}

            {/* Print Panel */}
            {tool === "print" && (
              <PrintPanel mapRef={mapRef} layers={layers} viewState={vs}
                onClose={() => setTool("pointer")} />
            )}
            <LayerPanel layers={layers} onToggle={toggleL} onRemove={removeL} onStyle={styleL} onExport={exportL} onClassify={classifyL} onExportFmt={exportFmt} onRename={renameL} onMoveUp={moveLayerUp} onMoveDown={moveLayerDown} onZoomExtent={zoomToLayer} />
            <Legend layers={layers} />
            <MiniMap center={[vs.longitude, vs.latitude]} zoom={vs.zoom} mapStyle={MAP_STYLES[mapSt]} />
            {layers.length === 0 && tool === "pointer" && (
              <div style={{ position: "absolute", top: "50%", left: "50%", transform: "translate(-50%,-50%)", textAlign: "center", color: C.dim, fontSize: 14, pointerEvents: "none" }}>
                <div style={{ color: C.mut }}>Carte vide</div>
                <div style={{ fontSize: 12 }}>Utilisez le chat ou importez un fichier</div>
              </div>
            )}
          </div>

          {/* Bottom panel */}
          <BottomPanel layers={layers.filter(l => l.visible)} activeTab={btab} onTab={setBtab} onZoom={zoomFeat} onAddLayer={addLayer} />
        </div>

        {/* Chat */}
        {chatOpen && (
          <div style={{ width: 360, flexShrink: 0 }}>
            <ChatPanel onToolResult={handleToolResult} mapContext={mapCtx} />
          </div>
        )}
      </div>
    </div>
  );
}

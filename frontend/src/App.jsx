import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import Map, { Source, Layer, Popup, NavigationControl, ScaleControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import * as turf from "@turf/turf";
import { Link } from "react-router-dom";

import { useTheme, ThemeContext, useThemeContext } from "./theme";
import { F, M, API, MAP_STYLES, LAYER_COLORS, EXPORT_FORMATS, PLANETS, PLANET_KEYS } from "./config";
import { buildClassification } from "./utils/classification";
import { encodePermalink, decodePermalink, importFile, computeBounds, getPopupFields } from "./utils/helpers";
import { executeSpatialOp } from "./utils/spatial";
import { computeRoute, computeIsochrone } from "./utils/routing";
import Legend from "./components/Legend";
import LayerPanel from "./components/LayerPanel";
import ChatPanel    from "./components/ChatPanel";
import AuthModal    from "./components/AuthModal";
import SaveMapModal from "./components/SaveMapModal";
import Dashboard    from "./components/Dashboard";
import { useAuth }  from "./useAuth";
import TimelapseModal from "./components/TimelapseModal";
import BottomPanel from "./components/BottomPanel";
import MiniMap from "./components/MiniMap";
import PrintPanel from "./components/PrintPanel";
import SpatialPanel from "./components/SpatialPanel";
import JoinPanel from "./components/JoinPanel";
import BurnSeverityPanel from "./components/BurnSeverityPanel";
import WatershedPanel from "./components/WatershedPanel";
import VectorCatalogPanel from "./components/VectorCatalogPanel";
import FloodPanel from "./components/FloodPanel";
import WeatherPanel from "./components/WeatherPanel";
import ChartLayer from "./components/ChartLayer";
import DBPanel from "./components/DBPanel";
import SqlPanel from "./components/SqlPanel";
import RasterAnalysisPanel from "./components/RasterAnalysisPanel";
import RasterVectorPanel from "./components/RasterVectorPanel";
import SpatialStatsPanel from "./components/SpatialStatsPanel";
import GeorefPanel from "./components/GeorefPanel";
import SolarSystemPanel from "./components/SolarSystemPanel";
import VectorVizPanel from "./components/VectorVizPanel";
import ViewshedPanel from "./components/ViewshedPanel";
import StacPanel from "./components/StacPanel";
import MaxarPanel from "./components/MaxarPanel";
import ShadowPanel from "./components/ShadowPanel";
import StoryPanel from "./components/StoryPanel";
import OGCPanel from "./components/OGCPanel";
import GEEPanel from "./components/GEEPanel";
import Scene3DPanel from "./components/Scene3DPanel";
import LidarPanel from "./components/LidarPanel";
import ThematicMenu from "./components/ThematicMenu";
import IndicatorModal from "./components/IndicatorModal";
import BivariateModal from "./components/BivariateModal";
import SearchPalette from "./components/SearchPalette";
import ProjectionExplorer from "./components/ProjectionExplorer";
import StarField from "./components/StarField";
import PrecipLayer from "./components/PrecipLayer";
import { nextZ, bumpZ } from "./utils/zorder";
import { set3DVisible, remove3DLayer, setGlobe } from "./utils/deck3d";
import { setPCOpacity, removePC } from "./utils/lidarStyle";
import ProfilPanel from "./components/ProfilPanel";
import EditorPanel from "./components/EditorPanel";
import TimelapsePanel from "./components/TimelapsePanel";
import ChangeDetectionPanel from "./components/ChangeDetectionPanel";
import ComparatorPanel from "./components/ComparatorPanel";
import OsmPanel from "./components/OsmPanel";
import AgriPanel from "./components/AgriPanel";
import ClassifSupPanel from "./components/ClassifSupPanel";
import { loadMakiIcon } from "./utils/makiLoader";


 
import { useLocation } from "react-router-dom";

function useAnalytics() {
  const location = useLocation();

  useEffect(() => {
    window.gtag("config", "G-TV8HRDDDTN", {
      page_path: location.pathname + location.search,
    });
  }, [location]);
}

// ─── Icônes : Lucide React uniquement (voir src/icons.jsx) ───────────────────
import {
  IcArrow, IcRulerTool, IcHexagon, IcCircleDot, IcPencil, IcNavigation, IcRadar,
  IcStack, IcBarChart, IcArrowDown, IcVenn, IcDatabase, IcSatellite, IcServer,
  IcMountain, IcCube, IcEdit, IcFilm, IcDiff, IcCompare, IcOSM, IcLeaf, IcClassif,
  IcPrint, IcUpload, IcShare, IcSun, IcMoon, IcChat, IcX, IcGlobe, IcMap,
  IcCloudRain, IcSnowflake, IcChevronDown, IcTable, IcFlame, IcDroplets, IcBoxes, IcInfo,
  IcGrid, IcAlert,
} from "./icons";

// ─── Configuration du rail — groupes logiques, icônes uniques ─
const RAIL_GROUPS = [
  {
    id: "nav",
    label: null,
    items: [
      { id: "pointer",  sub: "Sélect.", label: "Sélection", Icon: IcArrow, hasPanel: false },
    ],
  },
  {
    id: "measure",
    label: "Mesure",
    items: [
      { id: "measure_dist", sub: "Distance", label: "Mesure distance", Icon: IcRulerTool, hasPanel: false },
      { id: "measure_area", sub: "Surface",  label: "Mesure surface",  Icon: IcHexagon,   hasPanel: false },
      { id: "buffer",       sub: "Tampon",   label: "Zone tampon",     Icon: IcCircleDot, hasPanel: false },
      { id: "draw",         sub: "Dessin",   label: "Dessin libre",    Icon: IcPencil,    hasPanel: false },
    ],
  },
  {
    id: "routing",
    label: "Routage",
    items: [
      { id: "route",     sub: "Itinér.",  label: "Itinéraire", Icon: IcNavigation, hasPanel: true },
      { id: "isochrone", sub: "Iso.",     label: "Isochrone",  Icon: IcRadar,      hasPanel: true },
    ],
  },
  {
    id: "layers",
    label: "Couches",
    items: [
      { id: "layers",  sub: "Gérer",   label: "Gestionnaire",  Icon: IcStack,    hasPanel: true },
      { id: "stats",   sub: "Stats",   label: "Statistiques",  Icon: IcBarChart, hasPanel: true },
      { id: "compare", sub: "Compar.", label: "Comparateur A/B", Icon: IcCompare,  hasPanel: true },
      { id: "story",   sub: "Story",   label: "Story map (scrollytelling)", Icon: IcFilm, hasPanel: true },
    ],
  },
  {
    id: "analysis",
    label: "Analyse",
    items: [
      { id: "spatial",  sub: "Spatial",  label: "Analyse spatiale",          Icon: IcVenn,     hasPanel: true },
      { id: "join",     sub: "Jointure", label: "Jointure attributaire",     Icon: IcTable,    hasPanel: true },
      { id: "burnsev",  sub: "Incendie", label: "Sévérité d'incendie (dNBR)", Icon: IcFlame,  hasPanel: true },
      { id: "flood",    sub: "Inondation", label: "Cartographie des inondations", Icon: IcDroplets, hasPanel: true },
      { id: "watershed", sub: "Bassin",  label: "Bassin versant",            Icon: IcDroplets, hasPanel: true },
      { id: "profil",   sub: "Profil",   label: "Profil altimétrique",       Icon: IcMountain, hasPanel: true },
      { id: "classif",  sub: "Classif.", label: "Classification supervisée", Icon: IcClassif,  hasPanel: true },
      { id: "rasteranalysis", sub: "Raster", label: "Analyse raster (zonal + calc)", Icon: IcGrid, hasPanel: true },
      { id: "rastervec", sub: "Vecto", label: "Vectorisation raster (polygones + contours)", Icon: IcHexagon, hasPanel: true },
      { id: "spatialstats", sub: "Autocorr.", label: "Stats spatiales (Moran, hotspots)", Icon: IcCircleDot, hasPanel: true },
      { id: "vectorviz", sub: "Chaleur", label: "Chaleur & clusters (densité de points)", Icon: IcRadar, hasPanel: true },
      { id: "viewshed", sub: "Visibilité", label: "Analyse de visibilité (viewshed)", Icon: IcMountain, hasPanel: true },
      { id: "georef", sub: "Caler", label: "Géoréférenceur (caler une image)", Icon: IcMap, hasPanel: true },
      { id: "shadow", sub: "Ombres", label: "Ombres portées des bâtiments", Icon: IcSun, hasPanel: true },
    ],
  },
  {
    id: "remote",
    label: "Données",
    items: [
      { id: "vectorcat", sub: "Catalogue", label: "Catalogue vectoriel",  Icon: IcBoxes,     hasPanel: true },
      { id: "stac",      sub: "STAC",      label: "Navigateur STAC / COG", Icon: IcSatellite, hasPanel: true },
      { id: "maxar",     sub: "Maxar",     label: "Maxar Open Data (catastrophes avant/après)", Icon: IcAlert, hasPanel: true },
      { id: "sql",       sub: "SQL",       label: "SQL Workspace",        Icon: IcTable,     hasPanel: true },
      { id: "database",  sub: "BDD",       label: "Base de données",      Icon: IcDatabase,  hasPanel: true },
      { id: "ogc",       sub: "OGC",       label: "Services OGC/WMS",     Icon: IcServer,    hasPanel: true },
      { id: "osm",       sub: "OSM",       label: "Import OSM",           Icon: IcOSM,       hasPanel: true },
      { id: "gee",       sub: "GEE",       label: "Google Earth Engine",  Icon: IcSatellite, hasPanel: true },
      { id: "timelapse", sub: "Timelapse", label: "Timelapse GEE",        Icon: IcFilm,      hasPanel: true },
      { id: "change",    sub: "Diff",      label: "Détection changement", Icon: IcDiff,      hasPanel: true },
    ],
  },

  {
    id: "view3d",
    label: "3D",
    items: [
      { id: "scene3d", sub: "3D", label: "Vue 3D / Globe (nuages, 3D Tiles, glTF, splats)", Icon: IcCube, hasPanel: true },
      { id: "solarsystem", sub: "Planètes", label: "Système solaire (globes 3D texturés)", Icon: IcGlobe, hasPanel: true },
    ],
  },
  {
    id: "lidar",
    label: "LiDAR",
    items: [
      { id: "lidar", sub: "Forêt", label: "LiDAR — MNT/MNS/MNH, arbres & houppiers", Icon: IcMountain, hasPanel: true },
    ],
  },
  {
    id: "agri",
    label: "Agri",
    items: [
      { id: "agri", sub: "Précision", label: "Agriculture de précision", Icon: IcLeaf, hasPanel: true },
    ],
  },
  {
    id: "edit",
    label: "Édition",
    items: [
      { id: "editor", sub: "Éditeur", label: "Éditeur vectoriel", Icon: IcEdit, hasPanel: true },
    ],
  },
  {
    id: "meteo",
    label: "Météo",
    items: [
      { id: "weather", sub: "Météo", label: "Météo temps réel", Icon: IcCloudRain, hasPanel: true },
    ],
  },

];

const ALL_ITEMS = RAIL_GROUPS.flatMap(g => g.items);
const PANEL_IDS = new Set(ALL_ITEMS.filter(i => i.hasPanel).map(i => i.id));

const SIDEBAR_MIN = 240;
const SIDEBAR_MAX = 540;
const SIDEBAR_DEF = 290;
const CHAT_MIN    = 260;
const CHAT_MAX    = 580;
const CHAT_DEF    = 340;

// ─── Petit bouton générique ───────────────────────────────────
const BtnRow = ({ onClick, children, C, accent }) => (
  <button onClick={onClick} style={{
    fontFamily: F, width: "100%", fontSize: 11, padding: "7px 10px",
    borderRadius: 7, border: `0.5px solid ${accent ? C.acc + "55" : C.bdr}`,
    background: accent ? C.acc : C.hover, color: accent ? "#fff" : C.txt,
    cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
    transition: "opacity .12s",
  }}>
    {children}
  </button>
);

// ─── Wrapper qui force un composant à s'intégrer dans le flux ─
// Neutralise tout position:absolute/fixed interne en créant un
// contexte d'empilement isolé avec overflow:hidden
const Embed = ({ children, style }) => (
  <div style={{
    position: "relative",   // crée un nouveau stacking context
    width: "100%",
    flex: 1,
    minHeight: 0,
    overflow: "hidden",     // capture tout enfant absolute
    display: "flex",
    flexDirection: "column",
    ...style,
  }}>
    {children}
  </div>
);

// ── Autocomplétion d'adresse pour les champs route/isochrone ─────
function AddressInput({ value, onChange, onSelect, placeholder, style: extraStyle, C, F }) {
  const [suggestions, setSuggestions] = React.useState([]);
  const [open, setOpen]               = React.useState(false);
  const [loading, setLoading]         = React.useState(false);
  const timer  = React.useRef(null);
  const wrapRef = React.useRef(null);

  // Fermer si clic extérieur
  React.useEffect(() => {
    const handler = (e) => { if (wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const search = (q) => {
    onChange(q);
    clearTimeout(timer.current);
    if (q.trim().length < 3) { setSuggestions([]); setOpen(false); return; }
    setLoading(true);
    timer.current = setTimeout(async () => {
      try {
        const res = await fetch(
          `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=6&addressdetails=1`,
          { headers: { "User-Agent": "OpenMapAgents/1.0" } }
        );
        const data = await res.json();
        setSuggestions(data);
        setOpen(data.length > 0);
      } catch (_) { setSuggestions([]); }
      setLoading(false);
    }, 350);
  };

  const select = (item) => {
    const label = item.display_name.split(",").slice(0, 2).join(", ");
    onChange(label);
    onSelect?.({ label, lat: parseFloat(item.lat), lon: parseFloat(item.lon), raw: item });
    setSuggestions([]);
    setOpen(false);
  };

  return (
    <div ref={wrapRef} style={{ position: "relative", flex: 1 }}>
      <input
        value={value}
        onChange={e => search(e.target.value)}
        onFocus={() => suggestions.length > 0 && setOpen(true)}
        placeholder={placeholder}
        style={{ ...extraStyle, width: "100%", boxSizing: "border-box" }}
      />
      {loading && (
        <span style={{ position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)", fontSize: 10, color: C.dim }}>⏳</span>
      )}
      {open && suggestions.length > 0 && (
        <div style={{
          position: "absolute", top: "100%", left: 0, right: 0, zIndex: 999,
          background: C.card, border: `0.5px solid ${C.bdr}`,
          borderRadius: "0 0 8px 8px", boxShadow: "0 4px 16px rgba(0,0,0,0.25)",
          maxHeight: 200, overflowY: "auto",
        }}>
          {suggestions.map((s, i) => (
            <div key={i} onClick={() => select(s)}
              style={{
                padding: "7px 10px", fontSize: 11, cursor: "pointer",
                borderBottom: i < suggestions.length - 1 ? `0.5px solid ${C.bdr}` : "none",
                color: C.txt, display: "flex", flexDirection: "column", gap: 1,
              }}
              onMouseEnter={e => e.currentTarget.style.background = C.hover}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}
            >
              <span style={{ fontWeight: 500 }}>{s.display_name.split(",")[0]}</span>
              <span style={{ fontSize: 9, color: C.dim }}>{s.display_name.split(",").slice(1, 3).join(",").trim()}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Geocoder flottant — style Mapbox, collé au NavigationControl ─
function GeocoderControl({ mapRef, C }) {
  const [open,        setOpen]        = React.useState(false);
  const [query,       setQuery]       = React.useState("");
  const [suggestions, setSuggestions] = React.useState([]);
  const [loading,     setLoading]     = React.useState(false);
  const timer  = React.useRef(null);
  const inputRef = React.useRef(null);

  const search = (q) => {
    setQuery(q);
    clearTimeout(timer.current);
    if (q.trim().length < 3) { setSuggestions([]); return; }
    setLoading(true);
    timer.current = setTimeout(async () => {
      try {
        const res = await fetch(
          `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=6&addressdetails=1`,
          { headers: { "User-Agent": "OpenMapAgents/1.0" } }
        );
        setSuggestions(await res.json());
      } catch (_) { setSuggestions([]); }
      setLoading(false);
    }, 350);
  };

  const select = (item) => {
    setQuery(item.display_name.split(",").slice(0, 2).join(", "));
    setSuggestions([]);
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    const bb = item.boundingbox;
    if (bb) {
      map.fitBounds(
        [[parseFloat(bb[2]), parseFloat(bb[0])], [parseFloat(bb[3]), parseFloat(bb[1])]],
        { padding: 60, maxZoom: 17, duration: 900 }
      );
    } else {
      map.flyTo({ center: [parseFloat(item.lon), parseFloat(item.lat)], zoom: 15, duration: 900 });
    }
  };

  const toggle = () => {
    const next = !open;
    setOpen(next);
    if (next) setTimeout(() => inputRef.current?.focus(), 50);
    else { setQuery(""); setSuggestions([]); }
  };

  return (
    <div style={{
      position: "absolute", top: 100, right: 10, zIndex: 10,
      display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 0,
    }}>
      {/* Bouton loupe — même style que NavigationControl */}
      <button onClick={toggle} title="Rechercher une adresse"
        style={{
          width: 29, height: 29, borderRadius: open ? "4px 4px 0 0" : 4,
          background: open ? C.acc : "#fff",
          border: `1px solid rgba(0,0,0,0.1)`,
          boxShadow: "0 0 0 2px rgba(0,0,0,.1)",
          cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: 14, color: open ? "#fff" : "#333",
          marginBottom: open ? 0 : 5,
          transition: "all .15s",
        }}>
        {open ? "✕" : "🔍"}
      </button>

      {/* Champ de recherche rétractable */}
      {open && (
        <div style={{
          background: "#fff",
          border: "1px solid rgba(0,0,0,0.1)",
          boxShadow: "0 0 0 2px rgba(0,0,0,.1)",
          borderRadius: "0 0 4px 4px",
          width: 260,
          marginBottom: 5,
        }}>
          <div style={{ position: "relative" }}>
            <input
              ref={inputRef}
              value={query}
              onChange={e => search(e.target.value)}
              placeholder="Rechercher une adresse…"
              style={{
                fontFamily: F, fontSize: 12, padding: "7px 28px 7px 10px",
                border: "none", outline: "none", width: "100%",
                boxSizing: "border-box", background: "transparent", color: "#333",
                borderRadius: "0 0 4px 4px",
              }}
            />
            {loading
              ? <span style={{ position:"absolute", right:8, top:"50%", transform:"translateY(-50%)", fontSize:10, color:"#aaa" }}>⏳</span>
              : query
                ? <button onClick={() => { setQuery(""); setSuggestions([]); inputRef.current?.focus(); }}
                    style={{ position:"absolute", right:6, top:"50%", transform:"translateY(-50%)", background:"none", border:"none", cursor:"pointer", fontSize:12, color:"#999", padding:0, lineHeight:1 }}>✕</button>
                : null
            }
          </div>

          {/* Suggestions */}
          {suggestions.length > 0 && (
            <div style={{ borderTop: "1px solid rgba(0,0,0,0.08)", maxHeight: 220, overflowY: "auto" }}>
              {suggestions.map((s, i) => (
                <div key={i} onClick={() => select(s)}
                  style={{
                    padding: "6px 10px", fontSize: 11, cursor: "pointer", color: "#333",
                    borderBottom: i < suggestions.length-1 ? "1px solid rgba(0,0,0,0.06)" : "none",
                    display: "flex", flexDirection: "column", gap: 1,
                  }}
                  onMouseEnter={e => e.currentTarget.style.background="#f0f0f0"}
                  onMouseLeave={e => e.currentTarget.style.background="transparent"}
                >
                  <span style={{ fontWeight: 500 }}>{s.display_name.split(",")[0]}</span>
                  <span style={{ fontSize: 9, color: "#888" }}>{s.display_name.split(",").slice(1,3).join(",").trim()}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* NavigationControl placeholder — le vrai est rendu par MapLibre */}
    </div>
  );
}

// ── Tailles initiales par module ────────────────────────────────
const PANEL_SIZES = {
  route:     { w: 300, h: "auto" },
  isochrone: { w: 300, h: "auto" },
  layers:    { w: 310, h: 500 },
  stats:     { w: 420, h: 460 },
  solarsystem: { w: 460, h: 520 },
  export:    { w: 280, h: "auto" },
  spatial:   { w: 520, h: 480 },
  database:  { w: 380, h: 480 },
  gee:       { w: 360, h: 500 },
  ogc:       { w: 360, h: 480 },
  agri:      { w: 440, h: 640 },
};
const DEFAULT_SIZE = { w: 340, h: 480 };
const MIN_W = 260, MAX_W = 860, MIN_H = 120;

// ── FloatingPanel — redimensionnable sur les 8 côtés/coins ──────
function FloatingPanel({ id, title, onClose, children, offset = 0 }) {
  const C = useThemeContext();
  const preset  = PANEL_SIZES[id] || DEFAULT_SIZE;
  const initW   = preset.w;
  const autoH   = preset.h === "auto";

  const [pos,  setPos]  = useState({ x: null, y: null });
  const [size, setSize] = useState({ w: initW, h: autoH ? 60 : preset.h });
  const [z, setZ] = useState(() => nextZ());   // profondeur partagée : clic = au premier plan
  const stateRef  = useRef({ pos: { x: null, y: null }, size: { w: initW, h: autoH ? 60 : preset.h } });
  const panelRef  = useRef(null);
  const contentRef = useRef(null);

  // Positionnement initial centré
  useEffect(() => {
    const vw = window.innerWidth, vh = window.innerHeight;
    const x = Math.round((vw - initW) / 2) + offset * 24;
    const y = Math.round((vh - (autoH ? 400 : preset.h)) / 2) + offset * 24;
    stateRef.current.pos = { x, y };
    setPos({ x, y });
  }, []);

  // Auto-fit hauteur au contenu (uniquement pour les modules "auto")
  useEffect(() => {
    if (!autoH || !contentRef.current) return;
    const measure = () => {
      const sh = contentRef.current?.scrollHeight || 0;
      if (sh < 10) return;
      const maxH = window.innerHeight - 80;
      const h = Math.min(sh + 42, maxH); // 42 = hauteur header
      stateRef.current.size = { ...stateRef.current.size, h };
      setSize(s => ({ ...s, h }));
    };
    // Mesure immédiate + après paint
    measure();
    const t = requestAnimationFrame(measure);
    return () => cancelAnimationFrame(t);
  }, [children, autoH]);

  // Garde stateRef en sync
  useEffect(() => { stateRef.current.pos  = pos;  }, [pos]);
  useEffect(() => { stateRef.current.size = size; }, [size]);

  // Drag titre
  const onDragStart = useCallback((e) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const rect = panelRef.current?.getBoundingClientRect();
    const ox = e.clientX - rect.left, oy = e.clientY - rect.top;
    const onMove = (ev) => {
      const { size: s } = stateRef.current;
      const x = Math.max(0, Math.min(window.innerWidth  - s.w, ev.clientX - ox));
      const y = Math.max(0, Math.min(window.innerHeight - s.h, ev.clientY - oy));
      stateRef.current.pos = { x, y };
      setPos({ x, y });
    };
    const onUp = () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  // Resize générique — dir = combinaison de "n","s","e","w"
  const onResizeStart = useCallback((e, dir) => {
    e.preventDefault(); e.stopPropagation();
    const startX = e.clientX, startY = e.clientY;
    const { pos: p0, size: s0 } = stateRef.current;
    const onMove = (ev) => {
      const dx = ev.clientX - startX, dy = ev.clientY - startY;
      let { x, y, w, h } = { x: p0.x, y: p0.y, w: s0.w, h: s0.h };
      const maxH = window.innerHeight - 60;
      if (dir.includes("e")) w = Math.max(MIN_W, Math.min(MAX_W, s0.w + dx));
      if (dir.includes("s")) h = Math.max(MIN_H, Math.min(maxH, s0.h + dy));
      if (dir.includes("w")) {
        const nw = Math.max(MIN_W, Math.min(MAX_W, s0.w - dx));
        x = p0.x + (s0.w - nw); w = nw;
      }
      if (dir.includes("n")) {
        const nh = Math.max(MIN_H, Math.min(maxH, s0.h - dy));
        y = p0.y + (s0.h - nh); h = nh;
      }
      stateRef.current.pos  = { x, y };
      stateRef.current.size = { w, h };
      setPos({ x, y });
      setSize({ w, h });
    };
    const onUp = () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  const E = 6;
  const edgeStyle = (cursor, extra) => ({ position: "absolute", zIndex: 10, cursor, ...extra });

  return (
    <div ref={panelRef} onMouseDown={() => setZ(bumpZ)} style={{
      position: "fixed",
      ...(pos.x !== null ? { left: pos.x, top: pos.y } : { top: "50%", left: "50%", transform: "translate(-50%,-50%)" }),
      width: size.w, height: size.h, zIndex: z,
      background: C.card, border: `0.5px solid ${C.bdr}`,
      borderRadius: 10, boxShadow: "0 4px 24px rgba(0,0,0,0.35)",
      display: "flex", flexDirection: "column", overflow: "hidden", userSelect: "none",
    }}>

      {/* ── Poignées de bord ── */}
      <div onMouseDown={e => onResizeStart(e, "n")}  style={edgeStyle("n-resize",  { top: 0,    left: E,    right: E,   height: E })} />
      <div onMouseDown={e => onResizeStart(e, "s")}  style={edgeStyle("s-resize",  { bottom: 0, left: E,    right: E,   height: E })} />
      <div onMouseDown={e => onResizeStart(e, "w")}  style={edgeStyle("w-resize",  { left: 0,   top: E,     bottom: E,  width: E })} />
      <div onMouseDown={e => onResizeStart(e, "e")}  style={edgeStyle("e-resize",  { right: 0,  top: E,     bottom: E,  width: E })} />
      <div onMouseDown={e => onResizeStart(e, "nw")} style={edgeStyle("nw-resize", { top: 0,    left: 0,    width: E,   height: E })} />
      <div onMouseDown={e => onResizeStart(e, "ne")} style={edgeStyle("ne-resize", { top: 0,    right: 0,   width: E,   height: E })} />
      <div onMouseDown={e => onResizeStart(e, "sw")} style={edgeStyle("sw-resize", { bottom: 0, left: 0,    width: E,   height: E })} />
      <div onMouseDown={e => onResizeStart(e, "se")} style={edgeStyle("se-resize", { bottom: 0, right: 0,   width: E,   height: E })} />

      {/* ── Titre / drag ── */}
      <div onMouseDown={onDragStart} style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "9px 12px 8px", borderBottom: `0.5px solid ${C.bdr}`,
        cursor: "grab", flexShrink: 0, background: C.card,
      }}>
        <div style={{ fontSize: 12.5, fontWeight: 600, color: C.txt, display: "flex", alignItems: "center", gap: 6, userSelect: "none", minWidth: 0 }}>
          <span style={{ fontSize: 11, color: C.dim, letterSpacing: 2, flexShrink: 0 }}>⠿</span>
          <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{title}</span>
        </div>
        <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex", flexShrink: 0, padding: 2 }}><IcX size={16}/></button>
      </div>

      {/* ── Contenu ── */}
      <div ref={contentRef} style={{
        flex: 1, minHeight: 0,
        overflowY: autoH ? "visible" : "auto",
        overflowX: "hidden",
        display: "flex", flexDirection: "column",
      }}>
        {children}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
// ── Wrapper Stats — gère l'état activeTab localement ─────────
function StatsPanelWrapper({ layers, onZoom, onAddLayer }) {
  const [activeTab, setActiveTab] = useState("stats");
  const visLayers = layers.filter(l => l.visible && !l.isRaster && l.geojson);
  return (
    <BottomPanel
      layers={visLayers}
      activeTab={activeTab}
      onTab={tab => setActiveTab(tab === activeTab ? activeTab : tab)}
      onZoom={onZoom}
      onAddLayer={onAddLayer}
    />
  );
}

export default function App() {
  const { name: themeName, C, toggle: toggleTheme } = useTheme();

  // ── Map ───────────────────────────────────────────────────
  const [layers, setLayers] = useState([]);
  // Fenêtres flottantes empilables (indicateurs + bivariée) : plusieurs ouvertes
  // en même temps, déplaçables, et click = passe au-dessus (z-index).
  const [modals, setModals] = useState([]);   // [{ id, type:"indicator"|"bivariate", indKey?, z, pos }]
  const modalSeq = useRef(0);
  // (profondeur des fenêtres : compteur partagé nextZ() — voir utils/zorder.js)
  const openModal = useCallback((m) => {
    setModals(prev => {
      const id = `mw_${++modalSeq.current}`;
      const k = prev.length % 6;
      // Ouvrir à DROITE du rail thématique (bande 52 + accordéon 258 ≈ 320) pour ne
      // pas apparaître sous le panneau ; en cascade. Sur petit écran, on borne.
      const baseX = Math.min(340 + k * 28, (typeof window !== "undefined" ? window.innerWidth - 400 : 340));
      return [...prev, { id, z: nextZ(), pos: { x: Math.max(60, baseX), y: 88 + k * 28 }, ...m }];
    });
  }, []);
  const closeModal = useCallback((id) => setModals(prev => prev.filter(m => m.id !== id)), []);
  // Palette de recherche globale (Ctrl/⌘+K)
  const [searchOpen, setSearchOpen] = useState(false);
  useEffect(() => {
    const onKey = (e) => {
      if ((e.ctrlKey || e.metaKey) && (e.key === "k" || e.key === "K")) { e.preventDefault(); setSearchOpen(v => !v); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);
  // Passe la fenêtre au premier plan via le compteur PARTAGÉ (panneaux inclus).
  // bumpZ compare au compteur GLOBAL : comparer au max des seules `modals` faisait
  // croire à une fenêtre déjà en tête de SA famille qu'elle était au premier plan,
  // alors qu'un FloatingPanel (ex. Gestionnaire de couches) était passé devant.
  const focusModal = useCallback((id) => setModals(prev => {
    const cur = prev.find(m => m.id === id);
    if (!cur) return prev;
    const nz = bumpZ(cur.z);
    if (nz === cur.z) return prev;                // déjà au sommet, tous types confondus
    return prev.map(m => m.id === id ? { ...m, z: nz } : m);
  }), []);
  const [notification, setNotification] = useState(null); // {message, type}
  const [showAuth,   setShowAuth]   = useState(false);
  const [showSave,   setShowSave]   = useState(false);
  const [showDash,   setShowDash]   = useState(false);
  const [currentMap, setCurrentMap] = useState(null);
  const [saveThumb,      setSaveThumb]     = useState("");
  const [pendingRestore, setPendingRestore] = useState(null); // viewport à restaurer après re-render
  const { user } = useAuth();
  const [timelapse,     setTimelapse]     = useState(null); // données timelapse pour modale
  const [mapSt,  setMapSt]  = useState("positron");
  const [vs,     setVs]     = useState({ longitude: -1.55, latitude: 47.22, zoom: 12, pitch: 0, bearing: 0 });
  const [popup,  setPopup]  = useState(null);

  // ── Sidebar gauche ────────────────────────────────────────
  const [activeTool,   setActiveTool]   = useState("pointer");
  const [sidebarOpen,  setSidebarOpen]  = useState(false); // conservé pour compat (tools sans panel)
  const [openPanels,   setOpenPanels]   = useState(new Set()); // ids des panneaux ouverts
  const [openGroup,    setOpenGroup]    = useState(() => new Set(RAIL_GROUPS.filter(g => g.label).map(g => g.id))); // tous les groupes ouverts par défaut
  const [sidebarWidth, setSidebarWidth] = useState(SIDEBAR_DEF);
  const sbResizing  = useRef(false);
  const sbStartX    = useRef(0);
  const sbStartW    = useRef(SIDEBAR_DEF);

  // ── Chat droit ────────────────────────────────────────────
  const [chatOpen,  setChatOpen]  = useState(false);
  const [chatWidth, setChatWidth] = useState(CHAT_DEF);
  const chResizing = useRef(false);
  const chStartX   = useRef(0);
  const chStartW   = useRef(CHAT_DEF);

  // ── Mesure / buffer / dessin ──────────────────────────────
  const [measurePts,   setMeasurePts]   = useState([]);
  const [measureRes,   setMeasureRes]   = useState(null);
  const [bufferLayer,  setBufferLayer]  = useState(null);
  const [drawPts,      setDrawPts]      = useState([]);
  const [bufferRadius, setBufferRadius] = useState(500);
  const [drawProfilPts, setDrawProfilPts] = useState([]); // points tracés pour le profil altimétrique
  const [profilSlopeLayer, setProfilSlopeLayer] = useState(null); // couche colorée par pente
  const [profilDrawMode, setProfilDrawMode] = useState(null); // "polyline" | "twopoints" | null

  // ── Routing ───────────────────────────────────────────────
  const [routeProfile, setRouteProfile] = useState("foot");
  const [isoTime,      setIsoTime]      = useState(10);
  // Points de départ/arrivée pour route/iso (saisis manuellement)
  const [routeOrigin,  setRouteOrigin]  = useState("");
  const [routeDest,    setRouteDest]    = useState("");
  const [isoCenter,    setIsoCenter]    = useState("");
  const [routeLoading, setRouteLoading] = useState(false);
  const [routeLayer,   setRouteLayer]   = useState(null);
  const [isoLayer,     setIsoLayer]     = useState(null);
  const [routeMarkers, setRouteMarkers] = useState(null);
  const [routePickMode, setRoutePickMode] = useState(null); // "origin"|"dest"|"center"

  // ── Drag & drop ───────────────────────────────────────────
  const [dragOver, setDragOver] = useState(false);

  // ── Refs ──────────────────────────────────────────────────
  const mapRef           = useRef(null);
  const fileRef          = useRef(null);
  const lctr             = useRef(0);
  const classifClickRef  = useRef(null);  // handler de clic pour ClassifSupPanel
  const georefClickRef   = useRef(null);  // handler de clic pour GeorefPanel (point d'appui)
  const viewshedClickRef = useRef(null);  // handler de clic pour ViewshedPanel (observateur)

  // ── Mode globe (projection MapLibre) ──────────────────────
  const [globeOn, setGlobeOn] = useState(false);
  const globeRef = useRef(false);   // miroir de globeOn lisible dans les callbacks MapLibre
  // Couche actuellement animée (timelapse) : sa tuile fixe est masquée, l'animation
  // pilote deux couches raster en ping-pong à la place.
  const [tlLayerId, setTlLayerId] = useState(null);
  const [projOpen, setProjOpen] = useState(false);  // explorateur de projections (d3)
  const [solarBody, setSolarBody] = useState("earth");  // corps affiché par le viewer Système solaire
  const [georefGcpGJ, setGeorefGcpGJ] = useState(null); // points d'appui du géoréférenceur affichés sur la carte
  const [viewshedPtGJ, setViewshedPtGJ] = useState(null); // observateur du viewshed affiché sur la carte
  const [planet3D, setPlanet3D] = useState(null);       // corps en plein écran (remplace la carte) ; null = carte
  const planet3DRef = useRef(null);
  useEffect(() => { planet3DRef.current = planet3D; }, [planet3D]);
  // ── Ciel ────────────────────────────────────────────────────
  // Un seul point de décision : le ciel dépend du MODE, et les réglages des
  // différents modes se contredisent (l'espace du globe doit être transparent
  // pour laisser voir les étoiles, le ciel du relief doit être opaque et coloré).
  //
  // Moments de la journée : MapLibre ne dessine pas de disque solaire — son ciel
  // est un dégradé VERTICAL, sans direction. On restitue donc l'heure par la
  // couleur du ciel, de l'horizon et de la brume, pas par une source ponctuelle.
  //
  // Déclaré AVANT applyTerrain, qui en dépend : l'ordre compte, une const lue
  // avant son initialisation lève une ReferenceError au montage.
  const SKY_MOMENTS = {
    day:   { label: "Jour",       sky: "#7fb2e5", horizon: "#dbe8f2", fog: "#e6eef4", ground: 0.10 },
    dawn:  { label: "Aube",       sky: "#5d84cc", horizon: "#f7c89a", fog: "#efd6bd", ground: 0.18 },
    dusk:  { label: "Crépuscule", sky: "#3a4a80", horizon: "#ef9a5e", fog: "#d5a189", ground: 0.20 },
    night: { label: "Nuit",       sky: "#080d1e", horizon: "#1a2445", fog: "#141c34", ground: 0.25 },
  };
  const [skyMoment, setSkyMoment] = useState("day");
  // "none" | "rain" | "snow" — la neige couvre moins fort que l'averse
  const WEATHER = { none: { label: "Dégagé", k: 0, fog: 0 },
                    rain: { label: "Pluie",  k: 0.62, fog: 0.22 },
                    snow: { label: "Neige",  k: 0.45, fog: 0.16 } };
  const [weather, setWeather] = useState("none");
  const [amb3DOpen, setAmb3DOpen] = useState(false);   // panneau d'ambiance (liste)
  const skyRef = useRef({ globe: false, terrain: false, moment: "day", weather: "none" });

  // Couverture nuageuse : désature ET assombrit. La cible est un gris tiré de la
  // CLARTÉ de la couleur d'origine, plafonné puis atténué — viser un gris moyen
  // fixe éclaircirait les teintes sombres, et une nuit pluvieuse se retrouverait
  // plus claire qu'une nuit dégagée.
  const overcast = (hex, k) => {
    if (!k) return hex;
    const n = parseInt(hex.slice(1), 16);
    const r0 = (n >> 16) & 255, g0 = (n >> 8) & 255, b0 = n & 255;
    const target = Math.min(0.299 * r0 + 0.587 * g0 + 0.114 * b0, 150) * 0.82;
    const mix = (c) => Math.max(0, Math.min(255, Math.round(c + (target - c) * k)));
    return `#${((mix(r0) << 16) | (mix(g0) << 8) | mix(b0)).toString(16).padStart(6, "0")}`;
  };

  const refreshSky = useCallback((map) => {
    const m = map || mapRef.current?.getMap?.(); if (!m?.setSky) return;
    const { globe, terrain, moment, weather: w } = skyRef.current;
    try {
      if (globe) {
        // Espace transparent : sans ça le ciel par défaut (bleu, atmosphere-blend
        // 0.8) recouvre tout le canvas et masque complètement le StarField.
        m.setSky({
          "sky-color": "rgba(0,0,0,0)", "horizon-color": "rgba(0,0,0,0)",
          "fog-color": "rgba(0,0,0,0)", "sky-horizon-blend": 0,
          "fog-ground-blend": 0, "atmosphere-blend": 0,
        });
      } else if (terrain) {
        const p = SKY_MOMENTS[moment] || SKY_MOMENTS.day;
        const wx = WEATHER[w] || WEATHER.none;
        m.setSky({
          "sky-color": overcast(p.sky, wx.k),
          "horizon-color": overcast(p.horizon, wx.k),
          "fog-color": overcast(p.fog, wx.k),
          "sky-horizon-blend": 0.55,    // dégradé zénith → horizon
          "horizon-fog-blend": 0.55,    // fondu horizon → brume
          // Intempérie : la brume monte, les lointains se noient — c'est ce qui
          // distingue visuellement une averse d'un simple filtre gris.
          "fog-ground-blend": Math.min(0.45, p.ground + wx.fog),
          "atmosphere-blend": 0.85,
        });
      } else {
        m.setSky({});   // vue à plat : ciel par défaut, invisible à pitch 0
      }
    } catch (_) { /* setSky non critique */ }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const changeMoment = useCallback((k) => {
    setSkyMoment(k); skyRef.current.moment = k; refreshSky();
  }, [refreshSky]);

  const changeWeather = useCallback((w) => {
    setWeather(w); skyRef.current.weather = w; refreshSky();
  }, [refreshSky]);

  // ── Relief 3D (terrain MapLibre + DEM Terrarium public) ──
  const [terrain3D, setTerrain3D] = useState(false);
  const [terrainExag, setTerrainExag] = useState(1.5);
  const exagRef = useRef(1.5);   // relu après un changement de fond de plan
  const applyTerrain = useCallback((on, exag) => {
    const map = mapRef.current?.getMap?.(); if (!map) return;
    try {
      if (on) {
        if (!map.getSource("terrain-dem")) {
          map.addSource("terrain-dem", {
            type: "raster-dem", encoding: "terrarium", tileSize: 256, maxzoom: 14,
            tiles: ["https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"],
          });
        }
        map.setTerrain({ source: "terrain-dem", exaggeration: exag });
        // 72° : au-delà de ~60 le regard porte vers la ligne d'horizon et le ciel
        // occupe le haut de l'écran. C'est ce basculement qui donne la lecture
        // « depuis le sol » d'un relief montagneux.
        if (map.getPitch() < 20) map.easeTo({ pitch: 72, duration: 900, essential: true });
      } else {
        map.setTerrain(null);
        map.easeTo({ pitch: 0, duration: 600, essential: true });
      }
      skyRef.current.terrain = !!on;
      refreshSky(map);
    } catch (_) { /* setTerrain non critique */ }
  }, [refreshSky]);
  const toggleTerrain = useCallback(() => {
    setTerrain3D(v => { const nv = !v; applyTerrain(nv, terrainExag); return nv; });
  }, [applyTerrain, terrainExag]);
  const changeExag = useCallback((v) => {
    setTerrainExag(v); exagRef.current = v;
    if (terrain3D) applyTerrain(true, v);
  }, [applyTerrain, terrain3D]);

  // Sélecteur explicite Plan (Mercator) / Globe de la carte live (MapLibre v5).
  const setProjectionMode = useCallback((globe) => {
    const map = mapRef.current?.getMap?.(); if (!map) return;
    if (!setGlobe(map, globe)) return;
    setGlobeOn(globe); globeRef.current = globe;
    skyRef.current.globe = globe;
    try {
      // Zoom automatique au niveau mondial pour cadrer le globe entier.
      if (globe) map.easeTo({ zoom: 1.4, duration: 900, essential: true });
      refreshSky(map);
    } catch (_) { /* setSky/easeTo non critiques */ }
  }, [refreshSky]);

  // ── Sélecteur de planète (Terre / Mars / Lune) ────────────
  // Une planète = un fond raster ; on la regarde en Globe. « Terre » restaure
  // le dernier fond terrestre utilisé.
  const lastEarthRef = useRef("positron");
  useEffect(() => { if (!PLANET_KEYS.includes(mapSt)) lastEarthRef.current = mapSt; }, [mapSt]);
  const selectPlanet = useCallback((key) => {
    if (key === "earth") { setMapSt(lastEarthRef.current || "positron"); return; }
    setMapSt(key);
    setProjectionMode(true);   // une planète se regarde en globe
  }, [setProjectionMode]);
  const currentPlanet = PLANET_KEYS.includes(mapSt) ? mapSt : "earth";

  // ── Changement de style carte via API MapLibre ────────────
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    const style = MAP_STYLES[mapSt];
    try {
      map.setStyle(style);
      // setStyle réinitialise projection, terrain ET ciel → on les réapplique une
      // fois le nouveau style chargé, sinon changer de fond de plan fait perdre le
      // relief et rallume le ciel par défaut.
      map.once?.("style.load", () => {
        if (globeRef.current) setGlobe(map, true);
        if (skyRef.current.terrain) {
          try {
            if (!map.getSource("terrain-dem")) {
              map.addSource("terrain-dem", {
                type: "raster-dem", encoding: "terrarium", tileSize: 256, maxzoom: 14,
                tiles: ["https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"],
              });
            }
            map.setTerrain({ source: "terrain-dem", exaggeration: exagRef.current });
          } catch (_) {}
        }
        refreshSky(map);
      });
    } catch (_) {}
  }, [mapSt, refreshSky]);

  // ── Restauration viewport différée (après fermeture Dashboard) ────
  useEffect(() => {
    if (!pendingRestore) return;
    const map = mapRef.current?.getMap?.();
    if (!map) return;

    const { features, viewport, mapStyle: style } = pendingRestore;

    const doZoom = () => {
      if (features?.length) {
        // Priorité : emprise réelle des données
        const b = computeBounds(features);
        if (b) {
          map.fitBounds(b, {
            padding:  { top:60, bottom:60, left:70, right:70 },
            maxZoom:  17,
            duration: 1000,
          });
          return;
        }
      }
      // Fallback : viewport sauvegardé
      if (viewport?.longitude != null) {
        map.flyTo({
          center:   [viewport.longitude, viewport.latitude],
          zoom:     viewport.zoom    ?? 12,
          pitch:    viewport.pitch   ?? 0,
          bearing:  viewport.bearing ?? 0,
          duration: 1000,
        });
      }
    };

    if (style && style !== mapSt) {
      try { map.setStyle(style); } catch {}
      map.once("styledata", doZoom);
    } else {
      // Petit délai pour laisser les couches se monter dans le DOM React
      setTimeout(doZoom, 200);
    }
    setPendingRestore(null);
  }, [pendingRestore, mapSt]);

  // ── Mobile ────────────────────────────────────────────────
  const [isMobile, setIsMobile] = useState(() => window.innerWidth < 768);
  useEffect(() => {
    const h = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener("resize", h);
    return () => window.removeEventListener("resize", h);
  }, []);

  // ══════════════════════════════════════════════════════════
  //  RESIZE — sidebar gauche (bord droit)
  // ══════════════════════════════════════════════════════════
  const startSbResize = useCallback((e) => {
    e.preventDefault();
    sbResizing.current = true;
    sbStartX.current = e.clientX;
    sbStartW.current = sidebarWidth;
    const mv = (ev) => {
      if (!sbResizing.current) return;
      setSidebarWidth(Math.min(SIDEBAR_MAX, Math.max(SIDEBAR_MIN, sbStartW.current + ev.clientX - sbStartX.current)));
    };
    const up = () => { sbResizing.current = false; window.removeEventListener("mousemove", mv); window.removeEventListener("mouseup", up); };
    window.addEventListener("mousemove", mv);
    window.addEventListener("mouseup", up);
  }, [sidebarWidth]);

  // ══════════════════════════════════════════════════════════
  //  RESIZE — chat droit (bord gauche)
  // ══════════════════════════════════════════════════════════
  const startChResize = useCallback((e) => {
    e.preventDefault();
    chResizing.current = true;
    chStartX.current = e.clientX;
    chStartW.current = chatWidth;
    const mv = (ev) => {
      if (!chResizing.current) return;
      setChatWidth(Math.min(CHAT_MAX, Math.max(CHAT_MIN, chStartW.current + chStartX.current - ev.clientX)));
    };
    const up = () => { chResizing.current = false; window.removeEventListener("mousemove", mv); window.removeEventListener("mouseup", up); };
    window.addEventListener("mousemove", mv);
    window.addEventListener("mouseup", up);
  }, [chatWidth]);

  // ══════════════════════════════════════════════════════════
  //  ACTIVATION D'UN OUTIL
  // ══════════════════════════════════════════════════════════
  const activateItem = useCallback((id) => {
    if (id === "bivariate") { openModal({ type: "bivariate" }); return; }  // fenêtre bivariée (pas un panneau du rail)
    if (id === "projections") { setProjOpen(true); return; }               // explorateur de projections (modal d3)
    // Planètes → prise en main plein écran (remplace la carte temporairement).
    if (id === "solarsystem" || id.startsWith("planet_")) {
      setPlanet3D(id === "solarsystem" ? (planet3DRef.current || solarBody) : id.slice(7));
      setOpenPanels(new Set());   // vue planète en grand, sans panneaux flottants
      return;
    }
    // Toute autre activation de module → on revient à la carte.
    if (planet3DRef.current) setPlanet3D(null);
    if (PANEL_IDS.has(id)) {
      setOpenPanels(prev => {
        const next = new Set(prev);
        if (next.has(id)) { next.delete(id); } else { next.add(id); }
        return next;
      });
      setActiveTool(id);
    } else {
      setActiveTool(id);
    }
  }, [openModal]);

  // Deep-link depuis la documentation : /app.html?open=CLE&kind=indicator|tool
  // → ouvre directement l'indicateur (IndicatorModal) ou l'outil, puis nettoie l'URL.
  useEffect(() => {
    const q = new URLSearchParams(window.location.search);
    const open = q.get("open");
    if (!open) return;
    const kind = q.get("kind");
    try { window.history.replaceState({}, "", window.location.pathname); } catch (_) {}
    const t = setTimeout(() => {
      if (kind === "tool") activateItem(open);
      else openModal({ type: "indicator", indKey: open });
    }, 300);
    return () => clearTimeout(t);
  }, [activateItem, openModal]);

  const closePanel = useCallback((id) => {
    setOpenPanels(prev => { const next = new Set(prev); next.delete(id); return next; });
    // Fermeture du panel profil → nettoyer le tracé carte
    if (id === "profil") {
      setDrawProfilPts([]);
      setProfilDrawMode(null);
      // Nettoyer le curseur GEE s'il reste
      try {
        const map = mapRef.current?.getMap?.();
        if (map) {
          if (map.getLayer("profil-cursor-layer")) map.removeLayer("profil-cursor-layer");
          if (map.getSource("profil-cursor"))      map.removeSource("profil-cursor");
        }
      } catch(_) {}
    }
  }, [mapRef]);

  // ── Helpers couches ───────────────────────────────────────
  const moveLayerUp   = id => setLayers(p => { const i = p.findIndex(l => l.id === id); if (i <= 0) return p; const n = [...p]; [n[i-1], n[i]] = [n[i], n[i-1]]; return n; });
  const moveLayerDown = id => setLayers(p => { const i = p.findIndex(l => l.id === id); if (i < 0 || i >= p.length-1) return p; const n = [...p]; [n[i], n[i+1]] = [n[i+1], n[i]]; return n; });

  const zoomToLayer = useCallback((id) => {
    const l = layers.find(x => x.id === id); if (!l) return;
    if (l.isRaster && l.bbox) { const [w,s,e,n] = l.bbox; mapRef.current?.getMap?.()?.fitBounds([[w,s],[e,n]], {padding:60,duration:1000}); return; }
    const feats = l.geojson?.features || []; if (!feats.length) return;
    const b = computeBounds(feats); if (b) mapRef.current?.getMap?.()?.fitBounds(b, {padding:60,maxZoom:17,duration:1000});
  }, [layers]);

  const addRasterLayer = useCallback((info) => {
    setPlanet3D(null);
    const ci = lctr.current % LAYER_COLORS.length; lctr.current++;
    setLayers(p => [...p, {
      id:info.id, name:info.name, theme:info.type||"wms", isRaster:true,
      tileUrl:info.tileUrl, geojson:null, visible:true,
      color:LAYER_COLORS[ci], opacity:info.opacity??0.85,
      featureCount:"raster", classCfg:null, classResult:null,
      bbox:     info.bbox      || null,
      visParams:  info.visParams  || null,  // palette/min/max GEE → RasterStylePanel
      _geeParams: info.geeParams || info._geeParams || null, // params pour restyle sans rechargement
      legend:     info.legend    || null,   // [{class_id, label, color}] pour couches classif
      bivariate:  info.bivariate || null,   // matrice 3×3 sémiologie bivariée → Legend
      job_id:     info.job_id    || null,   // pour restyle depuis LayerPanel
    }]);
  }, []);

  // ── Ajoute une couche raster IMAGE (overlay PNG géoréférencé, 4 coins lon/lat) ──
  // Rendue en <Source type="image"> déclaratif → survit au changement de fond,
  // apparaît dans le menu Couches (toggle/opacité/suppression standard).
  const addImageLayer = useCallback((info) => {
    setPlanet3D(null);
    const ci = lctr.current % LAYER_COLORS.length; lctr.current++;
    const id = info.id || `img_${Date.now()}_${lctr.current}`;
    setLayers(p => [...p, {
      id, name: info.name, theme: "image", isRaster: true, kind: "image",
      imageUrl: info.imageUrl,            // data URL PNG
      coordinates: info.coordinates,      // [[lng,lat] × 4] : TL,TR,BR,BL
      geojson: null, visible: true,
      color: LAYER_COLORS[ci], opacity: info.opacity ?? 0.85,
      featureCount: "raster", classCfg: null, classResult: null,
      bbox: info.bbox || null,
      // restyle (rasters mono-bande importés) — comme les rasters GEE
      rasterToken: info.rasterToken || null,
      bands: info.bands || null,
      bandRanges: info.bandRanges || null,
      vmin: info.vmin, vmax: info.vmax, dataMin: info.dataMin, dataMax: info.dataMax,
      legend: info.legend || null,
    }]);
    if (info.bbox && info.fit !== false) {
      const [w, s, e, n] = info.bbox;
      setTimeout(() => { try { mapRef.current?.getMap?.()?.fitBounds([[w, s], [e, n]], { padding: 60, duration: 1000 }); } catch (_) {} }, 150);
    }
    return id;
  }, []);

  // ── Enregistre un nuage de points (deck.gl) comme couche du menu Couches ──
  // Rendu par deck.gl (overlay, survit au fond) ; ici on l'expose juste pour le
  // toggle/suppression via le gestionnaire de couches (pas de <Source> natif).
  const addPointcloudLayer = useCallback((info) => {
    setPlanet3D(null);
    const ci = lctr.current % LAYER_COLORS.length; lctr.current++;
    setLayers(p => [
      ...p.filter(l => l.id !== info.id),   // remplace si même id (re-import)
      {
        id: info.id, name: info.name, kind: "pointcloud", theme: "pointcloud",
        isRaster: false, geojson: null, visible: true,
        color: LAYER_COLORS[ci], opacity: 1, featureCount: info.count || "nuage 3D",
        bbox: info.bbox || null, classCfg: null, classResult: null,
      },
    ]);
  }, []);

  // ── Met à jour une couche raster après restyle GEE (swap source MapLibre) ──
  const updateRasterLayer = useCallback((id, updates) => {
    setLayers(p => p.map(l => {
      if (l.id !== id) return l;
      if (updates.tileUrl && updates.tileUrl !== l.tileUrl) {
        try {
          const map = mapRef.current?.getMap?.();
          if (map) {
            if (map.getLayer(`${id}-layer`)) map.removeLayer(`${id}-layer`);
            if (map.getSource(id)) map.removeSource(id);
            map.addSource(id, { type:"raster", tiles:[updates.tileUrl], tileSize:256 });
            map.addLayer({ id:`${id}-layer`, type:"raster", source:id, paint:{"raster-opacity":l.opacity} });
          }
        } catch(e) { console.warn("restyle swap:", e); }
      }
      return { ...l, ...updates };
    }));
  }, [mapRef]);

  useEffect(() => {
    const s = decodePermalink();
    if (s?.c) setVs(p => ({...p, longitude:s.c[0], latitude:s.c[1], zoom:s.z||12}));
    if (s?.s) setMapSt(s.s);
  }, []);

  const mapCtx = useMemo(() => ({
    layers: layers.map(l => {
      const feats    = l.geojson?.features || [];
      const geomTypes = [...new Set(feats.map(f => f.geometry?.type).filter(Boolean))];
      const b    = computeBounds(feats);
      const bbox = l.geojson?.metadata?.bbox || (b ? [b[0][0],b[0][1],b[1][0],b[1][1]] : null);

      // Attributs complets (numériques + string) pour le LLM
      const firstFeat  = feats[0];
      const allAttrs   = firstFeat
        ? Object.entries(firstFeat.properties || {}).map(([k, v]) => ({
            name:   k,
            type:   typeof v === "number" ? "number" : typeof v === "string" ? "string" : "other",
            sample: typeof v === "number" ? v : String(v).slice(0, 30),
          }))
        : [];
      const numericAttrs  = allAttrs.filter(a => a.type === "number").map(a => a.name);
      const stringAttrs   = allAttrs.filter(a => a.type === "string").map(a => a.name);
      const classCfg      = l.classCfg || null;

      return {
        id:           l.id,
        name:         l.name,
        featureCount: l.featureCount,
        visible:      l.visible,
        theme:        l.theme,
        geomTypes,
        bbox,
        // Attributs disponibles pour le stylage
        numeric_attributes: numericAttrs,
        string_attributes:  stringAttrs,
        all_attributes:     allAttrs,
        // Style actuel
        current_style: {
          color:     l.color,
          opacity:   l.opacity,
          classCfg:  classCfg ? { type: classCfg.type, attribute: classCfg.attribute, ramp: classCfg.ramp } : null,
        },
      };
    }),
    center: [vs.longitude, vs.latitude],
    zoom:   vs.zoom,
    bbox:   (() => {
      const map = mapRef.current?.getMap?.();
      if (!map) return null;
      try {
        const b = map.getBounds();
        return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
      } catch { return null; }
    })(),
  }), [layers, vs]);

  const fitFeatures = useCallback((feats) => {
    const b = computeBounds(feats); if (!b) return;
    const m = mapRef.current?.getMap?.();
    if (m) setTimeout(() => m.fitBounds(b, {
      padding: { top:60, bottom:60, left: 66, right: chatOpen ? chatWidth+12 : 60 },
      maxZoom:17, duration:1200,
    }), 100);
  }, [sidebarOpen, sidebarWidth, chatOpen, chatWidth]);

  // ── detectBestClassification — choisit la meilleure classification auto ──
  const detectBestClassification = useCallback((geojson, name) => {
    const feats = geojson?.features || [];
    if (!feats.length) return null;
    const props = feats[0]?.properties || {};
    // Chercher un attribut numérique pertinent
    const numAttrs = Object.entries(props)
      .filter(([k, v]) => typeof v === "number" && isFinite(v))
      .map(([k]) => k);
    if (!numAttrs.length) return null;

    // Priorité : attribut qui ressemble à une population / valeur
    const preferred = numAttrs.find(k =>
      /pop|popu|population|value|val|area|surface|count|nb|nombre|hab|density|income|gdp|pib/i.test(k)
    ) || numAttrs[0];

    // Choisir le type selon la géométrie
    const geomType = feats[0]?.geometry?.type || "";
    const isPoint  = geomType === "Point" || geomType === "MultiPoint";
    const isPoly   = geomType === "Polygon" || geomType === "MultiPolygon";

    if (isPoint && feats.length > 1) {
      return { type: "proportional", attribute: preferred, method: "quantile", nClasses: 5, ramp: "viridis" };
    } else if (isPoly) {
      return { type: "graduated",   attribute: preferred, method: "quantile", nClasses: 5, ramp: "viridis" };
    }
    return null;
  }, []);

  // Remplace le GeoJSON d'une couche après ajout d'un champ calculé. La
  // classification en place reste valide : elle porte sur d'autres attributs,
  // et la nouvelle colonne devient aussitôt disponible pour en créer une.
  const updateGeojson = useCallback((id, geojson, col) => {
    setLayers(p => p.map(l => l.id === id
      ? { ...l, geojson, featureCount: geojson.features?.length ?? l.featureCount }
      : l));
    setNotification({ message: `Colonne « ${col} » ajoutée.`, msgType: "success" });
    setTimeout(() => setNotification(null), 4000);
  }, []);

  const addLayer = useCallback((geojson, name, theme = "data") => {
    setPlanet3D(null);   // ajouter une donnée → retour à la carte
    const ci  = lctr.current % LAYER_COLORS.length;
    const lid = `layer_${Date.now()}_${lctr.current++}`;
    const _THEME_COLORS = { isochrone:"#4A90E2", route:"#E74C3C", analysis:"#27AE60", world_data:"#8E44AD" };
    const layerColor   = _THEME_COLORS[theme] || LAYER_COLORS[ci];
    const layerOpacity = theme === "isochrone" ? 0.35 : 0.8;
    const layer = {
      id:lid, name, theme, geojson, visible:true,
      color:layerColor, opacity:layerOpacity, radius:6,
      featureCount: geojson.features?.length || 0,
      classCfg:null, classResult:null,
      heatmap:false, extrude:false, extrudeAttr:"", extrudeScale:1,
      cluster:false, labels:false, labelAttr:"name",
    };
    setLayers(p => [...p, layer]);
    if (geojson.features?.length) fitFeatures(geojson.features);

    // Auto-classification si attributs numériques détectés
    const autoCfg = detectBestClassification(geojson, name);
    if (autoCfg) {
      setTimeout(() => {
        const cr = buildClassification({ ...layer, geojson }, autoCfg);
        if (cr?.classes?.length || cr?.entries?.length) {
          setLayers(p => p.map(l => l.id === lid ? { ...l, classCfg: autoCfg, classResult: cr } : l));
        }
      }, 200);
    }
  }, [fitFeatures, detectBestClassification]);

  // addLayerSilent — comme addLayer mais sans fitFeatures (pour restauration)
  const addLayerSilent = useCallback((geojson, name, theme = "data", overrides = {}) => {
    setPlanet3D(null);
    const ci = lctr.current % LAYER_COLORS.length;
    const lid = `layer_${Date.now()}_${lctr.current++}`;
    setLayers(p => [...p, {
      id:lid, name, theme, geojson, visible:true,
      color: overrides.color || LAYER_COLORS[ci],
      opacity: overrides.opacity ?? 0.8,
      radius: overrides.radius ?? 6,
      featureCount: geojson.features?.length || 0,
      classCfg: overrides.classCfg || null,
      classResult: null,
      heatmap:false, extrude:false, extrudeAttr:"", extrudeScale:1,
      cluster:false, labels:false, labelAttr:"name",
      ...overrides,   // permet heatmap / cluster / classResult / clusterRadius… personnalisés
    }]);
  }, []);

  const layersRef = useRef(layers);
  useEffect(() => { layersRef.current = layers; }, [layers]);

  // ── Précharger les icônes Maki + forcer re-render quand prêtes ──
  const [makiTick, setMakiTick] = useState(0);
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    const tryLoad = () => {
      let loaded = 0;
      layersRef.current.forEach(l => {
        const cr = l.classResult;
        if (cr?.type === "symbol" && cr.symbolMode === "maki" && cr.makiName) {
          const id = loadMakiIcon(map, cr.makiName, cr.makiColor || "#ffffff", parseInt(cr.makiSize) || 30);
          if (id && map.hasImage(id)) loaded++;
        }
      });
      if (loaded > 0) setMakiTick(t => t + 1);
    };
    if (map.isStyleLoaded()) tryLoad();
    else map.once("styledata", tryLoad);
  }, [layers]);
  // ── Précharger les icônes Maki quand les couches changent ────
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map || !map.isStyleLoaded()) return;
    layers.forEach(l => {
      const cr = l.classResult;
      if (cr?.type === "symbol" && cr.symbolMode === "maki" && cr.makiName) {
        loadMakiIcon(map, cr.makiName, cr.makiColor || "#ffffff", cr.makiSize || 30);
      }
    });
  }, [layers]);
  
  // ── Synchronisation ordre MapLibre ↔ state layers ──────────
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map || !map.isStyleLoaded()) return;
    const orderedIds = [];
    layers.forEach(l => {
      if (l.isRaster) {
        if (l.theme === "vector") orderedIds.push(`${l.id}-fill`, `${l.id}-line`, `${l.id}-circle`);
        else orderedIds.push(`${l.id}-layer`);
      } else if (l.heatmap)  { orderedIds.push(`${l.id}-heat`); }
      else if (l.extrude)    { orderedIds.push(`${l.id}-extrude`); if (l.labels) orderedIds.push(`${l.id}-3dlabel`); }
      else if (l.cluster)    { orderedIds.push(`${l.id}-clusters`, `${l.id}-cluster-count`, `${l.id}-unclustered`); }
      else {
        orderedIds.push(`${l.id}-fill`, `${l.id}-outline`, `${l.id}-road`, `${l.id}-circle`);
        if (l.labels) orderedIds.push(`${l.id}-label`);
        if (l.classResult?.type === "symbol") orderedIds.push(`${l.id}-icon`, `${l.id}-sym`, `${l.id}-sym-fb`);
      }
    });
    const existingIds = orderedIds.filter(id => map.getLayer(id));
    for (let i = 0; i < existingIds.length; i++) {
      try { map.moveLayer(existingIds[i], existingIds[i + 1]); } catch (_) {}
    }
  }, [layers]);

  const clipToPolygonLayer = useCallback((gj, polyLayer, bufferM = 0) => {
    const features = polyLayer.geojson?.features || [];
    if (!features.length) return gj;

    // ── Construire les masques selon le type de géométrie ──
    let polys = [];

    const polygonFeatures = features.filter(f =>
      f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
    );
    const lineFeatures = features.filter(f =>
      f.geometry?.type === "LineString" || f.geometry?.type === "MultiLineString"
    );

    if (polygonFeatures.length > 0) {
      // ── CAS 1 : Isochrone / polygone ──────────────────────
      // Tester chaque feature contre TOUS les polygones (OR logique)
      // Gère les isochrones concentriques (5min + 10min)
      polys = polygonFeatures;

    } else if (lineFeatures.length > 0 && bufferM > 0) {
      // ── CAS 2 : Itinéraire + buffer demandé ───────────────
      try {
        polys = lineFeatures
          .map(f => turf.buffer(f, bufferM / 1000, { units: "kilometers" }))
          .filter(Boolean);
      } catch (e) {
        console.warn("[clip] buffer LineString échoué:", e);
        polys = [];
      }

    } else if (lineFeatures.length > 0 && bufferM === 0) {
      // ── CAS 3 : Itinéraire sans buffer → demander distance ─
      // Ne pas clipper — l'orchestrateur aurait dû demander la distance
      console.warn("[clip] LineString sans buffer_m → clip ignoré");
      return gj;
    }

    if (!polys.length) return gj;

    // ── Appliquer le clip (OR logique sur tous les masques) ──
    const clipped = (gj.features || []).filter(f => {
      try {
        if (f.geometry?.type === "Point") {
          return polys.some(poly => turf.booleanPointInPolygon(f, poly));
        }
        if (f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon") {
          return polys.some(poly => {
            try { return !!turf.intersect(turf.featureCollection([f, poly])); }
            catch { return false; }
          });
        }
        return true;
      } catch { return false; }
    });

    return {
      ...gj,
      features: clipped,
      metadata: { ...gj.metadata, clipped: true, clip_layer: polyLayer.name }
    };
  }, []);

  const handleToolResult = useCallback((action) => {
    if (action.type === "add_layer") {
      let gj = action.data;
      const params = gj.metadata?.query_params || {};
      const theme = gj.metadata?.theme || "data";

      // ── World Bank : classification automatique ───────────────────────────
      if (theme === "world_data" && gj.metadata?.indicator) {
        // Validation : erreur API ou pas de correspondance
        if (gj.metadata?.error) {
          const msg = gj.metadata.message || "Erreur World Bank";
          setNotification({ message: `⚠️ ${msg}`, msgType: "warning" });
          setTimeout(() => setNotification(null), 8000);
          return;
        }
        // Validation : 0 features retournées
        if (!gj.features?.length) {
          setNotification({ message: "⚠️ Aucune donnée disponible pour cet indicateur.", msgType: "warning" });
          setTimeout(() => setNotification(null), 6000);
          return;
        }
        const label     = gj.metadata.label || gj.metadata.indicator;
        const unit      = gj.metadata.unit  || "";
        const matched   = gj.metadata.matched || gj.features.length;
        const layerName = `${label} (${gj.metadata.year || ""})`;
        addLayer(gj, layerName, "world_data");
        // Zoom monde
        mapRef.current?.getMap?.()?.flyTo({ center: [0, 20], zoom: 2, duration: 1500 });
        // Notification avec stats
        setNotification({
          message: `✅ ${label} — ${matched} pays avec données`,
          msgType: "info"
        });
        setTimeout(() => setNotification(null), 5000);
        setTimeout(() => {
          const l = layersRef.current.find(x => x.name === layerName);
          if (l) {
            const cfg = { type: "graduated", attribute: "value",
                          method: "quantile", nClasses: 5, ramp: "viridis" };
            const cr = buildClassification(l, cfg);
            if (cr?.classes?.length) {
              setLayers(p => p.map(x => x.id === l.id ? { ...x, classCfg: cfg, classResult: cr } : x));
            }
          }
        }, 400);
        return;
      }
      const cur = layersRef.current;
      // Chercher la couche de clip : par metadata.clip_to_layer d'abord,
      // sinon la couche isochrone/polygone visible la plus récente
      const clipName = action.data?.metadata?.clip_to_layer || params.clip_to_layer;

      const isValidIsoLayer = (l) =>
        l.visible &&
        l.name &&
        !l.name.includes(" — ") &&
        !l.name.includes(" - ") &&
        (
          // Isochrone (polygones)
          l.theme === "isochrone" ||
          l.name.toLowerCase().includes("sochrone") ||
          // Itinéraire (lignes — utilisable avec buffer)
          l.theme === "route" ||
          l.name.toLowerCase().includes("itin")
        ) &&
        (l.geojson?.features || []).some(f =>
          f.geometry?.type === "Polygon" ||
          f.geometry?.type === "MultiPolygon" ||
          f.geometry?.type === "LineString" ||
          f.geometry?.type === "MultiLineString"
        );

      let polyL = [...cur].reverse().find(l => isValidIsoLayer(l)) || null;

      console.log("[clip] clipName=", clipName, "→ polyL=", polyL?.name, "polys=", polyL?.geojson?.features?.filter(f=>f.geometry?.type==="Polygon").length);
      const hasPoints = gj.features?.some(f => f.geometry?.type === "Point");
      const layerDisplayName = action.data?.metadata?.layer_name
        || action.data?.layer_name
        || params.layer_name
        || (polyL ? `${(params.category || theme).replace(/_/g," ")} dans ${polyL.name}` : null)
        || (params.category ? params.category.replace(/_/g," ") : theme);

      if (polyL && hasPoints) {
        const pb  = turf.bbox(polyL.geojson);
        const pad = 0.005;
        const qb  = gj.metadata?.bbox;
        const coversAll = qb &&
          qb[0] <= pb[0]+pad && qb[1] <= pb[1]+pad &&
          qb[2] >= pb[2]-pad && qb[3] >= pb[3]-pad;

        const doClipAndAdd = (source) => {
          const bufM = action?.data?._buffer_m
            || action?.data?.metadata?._buffer_m
            || action?._buffer_m
            || 0;
          const clipped = clipToPolygonLayer(source, polyL, bufM);
          // Corriger feature_count avec le vrai nombre après clip
          const realCount = clipped.features?.length || 0;
          clipped.metadata = {
            ...clipped.metadata,
            total:         realCount,
            feature_count: realCount,
          };
          // Mettre à jour aussi dans tool_results pour que le message chat soit correct
          clipped._clipped_count = realCount;
          addLayer(clipped, layerDisplayName, theme);
        };

        if (!coversAll) {
          fetch(`${API}/query?theme=${params.theme||theme}&xmin=${pb[0]-pad}&ymin=${pb[1]-pad}&xmax=${pb[2]+pad}&ymax=${pb[3]+pad}&limit=2000${params.category?`&category=${params.category}`:""}`)
            .then(r => r.json())
            .then(fg => doClipAndAdd(fg.features?.length ? fg : gj))
            .catch(()  => doClipAndAdd(gj));
          return;
        }
        doClipAndAdd(gj);
        return;
      }
      addLayer(gj, layerDisplayName, theme);
    } else if (action.type === "fly_to") {
      mapRef.current?.getMap?.()?.flyTo({ center:[action.longitude,action.latitude], zoom:action.zoom||14, pitch:action.pitch||0, duration:1500 });
    } else if (action.type === "remove_layer") {
      if (action.layer_id === "all") setLayers([]); else setLayers(p => p.filter(l => l.id !== action.layer_id));
    } else if (action.type === "spatial_analysis") {
      try {
        const lA = layersRef.current.find(l => l.name === action.layer_a_name
          || l.name?.toLowerCase() === action.layer_a_name?.toLowerCase()
          || l.name?.toLowerCase().includes(action.layer_a_name?.toLowerCase()));
        const lB = action.layer_b_name
          ? layersRef.current.find(l => l.name === action.layer_b_name
              || l.name?.toLowerCase().includes(action.layer_b_name?.toLowerCase()))
          : null;
        if (!lA) {
          console.warn("[spatial] Couche introuvable:", action.layer_a_name, "Disponibles:", layersRef.current.map(l=>l.name));
          return;
        }
        const op     = action.operation;
        const params = action.params || {};
        const attr   = params.attribute || params.attr || "";

        // ── Opérations thématiques (classification) ──────────────
        if (op === "proportional_symbols" || op === "thematic_proportional") {
          const ramp    = params.ramp    || "viridis";
          const minSize = params.minSize || 4;
          const maxSize = params.maxSize || 40;
          const cfg = { type: "proportional", attribute: attr, minSize, maxSize, ramp };
          const cr  = buildClassification(lA, cfg);
          if (cr) setLayers(p => p.map(l => l.id === lA.id ? { ...l, classCfg: cfg, classResult: cr } : l));
          return;
        }
        if (op === "thematic_choropleth" || op === "choropleth" || op === "graduated") {
          const ramp     = params.ramp     || "viridis";
          const nClasses = params.nClasses || 5;
          const method   = params.method   || "quantile";
          const cfg = { type: "graduated", attribute: attr, method, nClasses, ramp };
          const cr  = buildClassification(lA, cfg);
          if (cr) setLayers(p => p.map(l => l.id === lA.id ? { ...l, classCfg: cfg, classResult: cr } : l));
          return;
        }
        if (op === "categorized" || op === "thematic_categorized") {
          const cfg = { type: "categorized", attribute: attr };
          const cr  = buildClassification(lA, cfg);
          if (cr) setLayers(p => p.map(l => l.id === lA.id ? { ...l, classCfg: cfg, classResult: cr } : l));
          return;
        }

        // ── Opérations géospatiales classiques ───────────────────
        const r = executeSpatialOp(op, lA, lB, params);
        if (r?.features?.length) addLayer(r, action.result_name || `${op}_result`, "analysis");
      } catch (e) { console.error("[spatial_analysis]", e); }
    } else if (action.type === "compute_route") {
      // layer_name vient du routing_agent (ex: Itinéraire_Rennes_vers_Dinard_car)
      const _rName = action.layer_name || `Itinéraire_${action.profile||"foot"}`;
      computeRoute(action.waypoints, action.profile || "foot")
        .then(gj => {
          if (gj.features?.length) fitFeatures(gj.features);
          // Injecter theme + layer_name dans metadata pour que _is_route() fonctionne
          if (!gj.metadata) gj.metadata = {};
          gj.metadata.theme      = "route";
          gj.metadata.layer_name = _rName;
          setRouteLayer(gj);
          addLayer(gj, _rName, "route");
        })
        .catch(console.error);
    } else if (action.type === "compute_isochrone") {
      const _isoName = (action.layer_name || `Isochrone_${action.time_minutes||10}min_${action.profile||"foot"}`).replace(/\s+/g, "_");
      computeIsochrone(action.center, action.time_minutes || 10, action.profile || "foot", action.intervals)
        .then(gj => {
          if (!gj.metadata) gj.metadata = {};
          gj.metadata.theme = "isochrone";
          setIsoLayer(gj);
          if (gj.features?.length) fitFeatures(gj.features);
          addLayer(gj, _isoName, "isochrone");
        })
        .catch(console.error);
    } else if (action.type === "add_isochrone") {
      const gj = action.geojson;
      if (gj?.features?.length) {
        const name = (action.layer_name || `Isochrone_${action.time_minutes||10}min`).replace(/\s+/g, "_");
        setIsoLayer(gj); fitFeatures(gj.features); addLayer(gj, name, "isochrone");
      }
    } else if (action.type === "add_raster_layer") {
      // Couche raster GEE (NDVI, LST, WorldCover...) — tile_url XYZ
      const layerId = `gee_${action.dataset || "raster"}_${(action.index||"").replace(/\s/g,"_")}_${Date.now()}`;
      // _geeParams : paramètres complets pour permettre le restyle depuis LayerPanel
      const geeParams = {
        dataset:    action.dataset,
        index:      action.index,
        date_start: action.date_start || action.date || null,
        date_end:   action.date_end   || action.date || null,
        cloud_max:  action.cloud_max  || 20,
        composite:  action.composite  || "median",
        bbox:       action.clip_bbox  || action.bbox || null,
        roi_geojson: null,
      };
      addRasterLayer({
        id:        layerId,
        name:      action.name      || `${action.dataset} — ${action.index}` || "GEE layer",
        tileUrl:   action.tile_url,
        bbox:      action.clip_bbox || action.bbox  || null,
        visParams: action.vis_params || null,
        geeParams,
        opacity:   0.85,
      });
      // Zoomer sur la bbox si disponible
      const bb = action.clip_bbox || action.bbox;
      if (bb && bb.length === 4) {
        const [w,s,e,n] = bb;
        mapRef.current?.getMap?.()?.fitBounds([[w,s],[e,n]], { padding:60, duration:1500 });
      }
    } else if (action.type === "set_3d_extrusion") {
      const layerName = action.layer_name;
      const layer = layersRef.current.find(l =>
        l.name === layerName ||
        l.name?.toLowerCase() === layerName?.toLowerCase() ||
        l.name?.toLowerCase().includes(layerName?.toLowerCase()) ||
        layerName?.toLowerCase().includes(l.name?.toLowerCase())
      );
      if (layer) {
        const attribute  = action.attribute;
        const scale      = action.scale   || 1;
        const pitch      = action.pitch   || 45;
        const color      = action.color   || null;
        // Activer extrusion sur la couche
        setLayers(p => p.map(l => l.id === layer.id
          ? { ...l, extrude: true, extrudeAttr: attribute, extrudeScale: scale,
              ...(color ? { color } : {}) }
          : l));
        // Basculer vue 3D
        const map = mapRef.current?.getMap?.();
        if (map) {
          map.easeTo({ pitch, bearing: -20, duration: 1200 });
        }
        setNotification({
          message: `🏗️ Extrusion 3D activée — ${layer.name} (${attribute})`,
          msgType: "info"
        });
        setTimeout(() => setNotification(null), 5000);
      } else {
        setNotification({
          message: `⚠️ Couche "${action.layer_name}" introuvable. Couches disponibles : ${layers.map(l=>l.name).join(", ")}`,
          msgType: "warning"
        });
        setTimeout(() => setNotification(null), 8000);
      }

    } else if (action.type === "thematic_analysis") {
      const layerName = action.layer_name;
      const currentLayers = layersRef.current;
      const layer = currentLayers.find(l =>
        l.name === layerName ||
        l.name?.toLowerCase() === layerName?.toLowerCase() ||
        l.name?.toLowerCase().includes(layerName?.toLowerCase()) ||
        layerName?.toLowerCase().includes(l.name?.toLowerCase())
      );
      console.log("[thematic_analysis] op=", action.operation, "attr=", action.attribute, "layer=", !!layer);
      if (!layer) {
        console.warn(`[thematic] Couche "${layerName}" introuvable. Disponibles:`, currentLayers.map(l => l.name));
      }
      if (layer) {
        const operation = action.operation;
        const attribute = action.attribute;
        const palette   = action.palette   || "viridis";
        const method    = action.method    || "jenks";
        const nClasses  = action.n_classes || 5;
        const minSize   = action.min_size  || 3;
        const maxSize   = action.max_size  || 40;
        const vals = (layer.geojson?.features || [])
          .map(f => f.properties?.[attribute]).filter(v => v != null);
        const isNumeric = vals.length > 0 && vals.every(v => typeof v === "number" || !isNaN(Number(v)));
        const applyClass = (targetLayer, cfg) => {
          const freshLayer = { ...targetLayer, classResult: null, classCfg: null };
          const cr = buildClassification(freshLayer, cfg);
          console.log("[thematic] buildClassification cr=", cr ? "ok" : "null", "cfg=", cfg);
          setLayers(p => p.map(l => l.id === targetLayer.id ? { ...l, classCfg: cfg, classResult: cr } : l));
          return cr;
        };
        if (operation === "proportional_symbols") {
          const cfg = { type: "proportional", attribute, minSize, maxSize };
          const geomTypes = (layer.geojson?.features || []).map(f => f.geometry?.type);
          const isPolygon = geomTypes.some(t => ["Polygon","MultiPolygon"].includes(t));
          const cfgColor  = { type: "graduated", attribute, method, nClasses, ramp: palette };
          const rawLayer  = { ...layer, classResult: null, classCfg: null };
          const crColor   = buildClassification(rawLayer, cfgColor);
          const crProp    = buildClassification(rawLayer, cfg);
          const crFinal   = { ...crProp, expression: crColor?.expression || null,
                               classes: crColor?.classes || [], breaks: crColor?.breaks || [] };
          const cfgFinal  = { ...cfgColor, minSize, maxSize };
          if (!isPolygon) {
            setLayers(p => p.map(l => l.id === layer.id ? { ...l, classCfg: cfgFinal, classResult: crFinal } : l));
          } else {
            const centroids = { type: "FeatureCollection",
              features: (layer.geojson.features || []).map(f => {
                const c = turf.centroid(f); c.properties = { ...f.properties }; return c;
              }).filter(Boolean) };
            const ptName = action.result_name || `${layerName} — symboles`;
            addLayer(centroids, ptName, "analysis");
            setTimeout(() => {
              const ptLayer = layersRef.current.find(l => l.name === ptName);
              if (ptLayer) {
                const crP2 = buildClassification(ptLayer, cfg);
                const crC2 = buildClassification(ptLayer, cfgColor);
                const crF2 = { ...crP2, expression: crC2?.expression || null,
                                classes: crC2?.classes || [], breaks: crC2?.breaks || [] };
                setLayers(p => p.map(l => l.id === ptLayer.id
                  ? { ...l, classCfg: { ...cfgColor, minSize, maxSize }, classResult: crF2 } : l));
              }
            }, 300);
          }
        } else if (operation === "choropleth" || operation === "graduated_colors") {
          applyClass(layer, isNumeric
            ? { type: "graduated",   attribute, method, nClasses, ramp: palette }
            : { type: "categorized", attribute, ramp: palette });
        } else if (operation === "classification") {
          applyClass(layer, isNumeric
            ? { type: "graduated",   attribute, method, nClasses, ramp: palette }
            : { type: "categorized", attribute, ramp: palette });
        } else if (operation === "categorized") {
          applyClass(layer, { type: "categorized", attribute, ramp: palette });
        } else if (operation === "heatmap") {
          setLayers(p => p.map(l => l.id === layer.id ? { ...l, heatmap: true, heatmapAttribute: attribute } : l));
        } else {
          console.warn("[thematic] operation inconnue:", operation, "→ fallback choropleth");
          applyClass(layer, isNumeric
            ? { type: "graduated",   attribute, method, nClasses, ramp: palette }
            : { type: "categorized", attribute, ramp: palette });
        }
      }

    } else if (action.type === "add_timelapse") {
      // Ouvrir la modale TimelapseModal avec les données du GIF
      if (action.gif_url) {
        setTimelapse(action);
      }
    } else if (action.type === "notify") {
      // Notification éphémère — warning (erreur) ou info (date alternative)
      const msgType = action.msgType || "warning";
      setNotification({ message: action.message, msgType });
      setTimeout(() => setNotification(null), msgType === "info" ? 8000 : 6000);
    }
  }, [addLayer, addRasterLayer, layers, fitFeatures, clipToPolygonLayer]);

  // Layer ops
  const toggleL = id => setLayers(p => p.map(l => {
    if (l.id !== id) return l; const nv = !l.visible;
    if (l.kind === "pointcloud") { try { set3DVisible(mapRef.current?.getMap?.(), id, nv); } catch(_){} return { ...l, visible: nv }; }
    try {
      const map = mapRef.current?.getMap?.();
      if (map) {
        const vis = nv ? "visible" : "none";
        const candidates = l.isRaster
          ? [`${id}-layer`, `${id}-fill`, `${id}-line`, `${id}-circle`]
          : l.heatmap  ? [`${id}-heat`]
          : l.extrude  ? [`${id}-extrude`, `${id}-3dlabel`]
          : l.cluster  ? [`${id}-clusters`, `${id}-cluster-count`, `${id}-unclustered`]
          : [`${id}-fill`, `${id}-outline`, `${id}-road`, `${id}-circle`,
             `${id}-label`, `${id}-icon`, `${id}-sym`, `${id}-sym-fb`];
        candidates.forEach(lid => { if (map.getLayer(lid)) map.setLayoutProperty(lid, "visibility", vis); });
      }
    } catch(_) {}
    return { ...l, visible: nv };
  }));
  const removeL = id => {
    const l = layers.find(x => x.id === id);
    if (l?.kind === "pointcloud") { try { remove3DLayer(mapRef.current?.getMap?.(), id); removePC(id); } catch(_){} }
    else if (l?.isRaster) { try { const map = mapRef.current?.getMap?.(); if (map) { [`${id}-layer`,`${id}-fill`,`${id}-line`,`${id}-circle`].forEach(lid => { if (map.getLayer(lid)) map.removeLayer(lid); }); if (map.getSource(id)) map.removeSource(id); } } catch(_){} }
    setLayers(p => p.filter(x => x.id !== id));
  };
  // Suppression par NOM — pour qu'un outil (bassin versant, inondation…) remplace
  // ses propres couches à la relance au lieu de les empiler. Les couches RASTER
  // sont ajoutées impérativement à la carte → on nettoie source/layer ; les
  // vecteurs sont rendus par React (le filtrage de setLayers suffit).
  const removeLayersByName = (names) => setLayers(prev => {
    const map = mapRef.current?.getMap?.();
    prev.filter(l => names.includes(l.name) && l.isRaster && map).forEach(l => {
      try {
        [`${l.id}-layer`, `${l.id}-fill`, `${l.id}-line`, `${l.id}-circle`]
          .forEach(lid => { if (map.getLayer(lid)) map.removeLayer(lid); });
        if (map.getSource(l.id)) map.removeSource(l.id);
      } catch (_) {}
    });
    return prev.filter(l => !names.includes(l.name));
  });
  const styleL = (id, s) => setLayers(p => p.map(l => {
    if (l.id !== id) return l;
    if (l.kind === "pointcloud") { if (s.opacity !== undefined) { try { setPCOpacity(mapRef.current?.getMap?.(), id, s.opacity); } catch(_){} } return { ...l, ...s }; }
    if (l.isRaster && s.opacity !== undefined) { try { const map = mapRef.current?.getMap?.(); if (map) { if (map.getLayer(`${id}-layer`)) map.setPaintProperty(`${id}-layer`,"raster-opacity",s.opacity); if (map.getLayer(`${id}-fill`)) map.setPaintProperty(`${id}-fill`,"fill-opacity",s.opacity); if (map.getLayer(`${id}-line`)) map.setPaintProperty(`${id}-line`,"line-opacity",s.opacity); if (map.getLayer(`${id}-circle`)) map.setPaintProperty(`${id}-circle`,"circle-opacity",s.opacity); } } catch(_){} }
    return { ...l, ...s };
  }));
  const renameL   = (id, name) => setLayers(p => p.map(l => l.id === id ? {...l, name} : l));
  const classifyL = useCallback((id, cfg) => {
    setLayers(p => p.map(l => {
      if (l.id !== id) return l;
      const r = cfg
        ? (cfg.type === "symbol" ? cfg : buildClassification(l, cfg))
        : null;
      return {...l, classCfg: cfg, classResult: r};
    }));
  }, []);
  // ── Filtre attributaire — met à jour geojson affiché sur la carte ──
  const filterL = useCallback((id, { filterState, geojson, _sourceGeojson }) => {
    setLayers(p => p.map(l => l.id !== id ? l : {
      ...l,
      geojson,          // geojson filtré (ou source complète si filtre effacé)
      _sourceGeojson,   // source originale préservée pour re-filtrage
      filterState,
      featureCount: geojson?.features?.length ?? l.featureCount,
    }));
  }, []);
  const exportL = id => {
    const l = layers.find(x => x.id === id); if (!l) return;
    const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([JSON.stringify(l.geojson,null,2)],{type:"application/json"}));
    a.download = `${l.name.replace(/\s+/g,"_")}.geojson`; a.click();
  };
  const exportFmt = async (id, fmt) => {
    const l = layers.find(x => x.id === id); if (!l) return;
    try {
      const res = await fetch(`${API}/export`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({theme:l.theme, bbox:l.geojson.metadata?.bbox||[-2,47,-1,48], format:fmt, limit:l.featureCount+100}) });
      const blob = await res.blob();
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
      a.download = `${l.name.replace(/\s+/g,"_")}${({GeoPackage:".gpkg",Shapefile:".shp",CSV:".csv",FlatGeobuf:".fgb"}[fmt]||".geojson")}`; a.click();
    } catch (e) { alert(`Export ${fmt}: ${e.message}`); }
  };
  const zoomFeat = useCallback((ln, lt) => { mapRef.current?.getMap?.()?.flyTo({center:[ln,lt],zoom:17,duration:800}); }, []);

  const doImport = useCallback(async (file) => {
    const ext = (file.name.split(".").pop() || "").toLowerCase();
    // ── GeoTIFF → reprojection serveur en 4326 + overlay image ──
    if (ext === "tif" || ext === "tiff") {
      try {
        const fd = new FormData(); fd.append("file", file);
        const r = await fetch(`${API}/raster/import`, { method: "POST", body: fd });
        if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
        const d = await r.json();
        addImageLayer({
          name: file.name.replace(/\.[^.]+$/, ""),
          imageUrl: `data:image/png;base64,${d.png_b64}`,
          coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.9,
          rasterToken: d.raster_token, bands: d.bands, bandRanges: d.band_ranges,
          vmin: d.vmin, vmax: d.vmax, dataMin: d.data_min, dataMax: d.data_max,
        });
      } catch (e) { alert("Import GeoTIFF : " + (e.message || e)); }
      return;
    }
    // ── Vecteurs (GeoJSON, Shapefile, CSV…) ──
    try { const gj = await importFile(file); if (gj?.features?.length) addLayer(gj, file.name.replace(/\.[^.]+$/,""), "import"); else alert("Fichier vide ou format non reconnu."); }
    catch (e) { alert("Erreur import: " + e.message); }
  }, [addLayer, addImageLayer]);

  const shareLink = useCallback(() => {
    const hash = encodePermalink(vs, mapSt, layers);
    const url = `${window.location.origin}${window.location.pathname}#${hash}`;
    navigator.clipboard?.writeText(url).then(() => alert("Lien copié !")).catch(() => prompt("Copiez :", url));
    window.location.hash = hash;
  }, [vs, mapSt, layers]);

  const getPaint = useCallback((layer, gt) => {
    const cr = layer.classResult; const ce = cr?.expression || layer.color;
    if (gt==="fill") return {"fill-color":ce,"fill-opacity":layer.opacity*0.4};
    if (gt==="line") { if (cr?.type==="proportional_line"&&cr.widthExpression) return {"line-color":layer.color,"line-width":cr.widthExpression,"line-opacity":layer.opacity}; return {"line-color":ce,"line-width":1.5,"line-opacity":layer.opacity}; }
    if (gt==="circle") { if (cr?.type==="symbol"&&cr.symbolMode==="maki") { const _map=mapRef.current?.getMap?.(); const _mkSz=parseInt(cr.makiSize)||30; const _imgId=cr.makiImageId||(`maki_${cr.makiName}_${(cr.makiColor||"#ffffff").replace("#","").toLowerCase()}_${_mkSz}`); const _imgReady=_imgId&&_map&&_map.isStyleLoaded()&&_map.hasImage(_imgId); if(_imgReady) return {"circle-radius":0,"circle-opacity":0}; return {"circle-radius":Math.max(4,_mkSz/6),"circle-color":cr.makiColor||layer.color,"circle-opacity":layer.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.3}; } if(cr?.type==="symbol"&&cr.symbolMode==="image") return {"circle-radius":0,"circle-opacity":0}; if (cr?.type==="proportional"&&cr.radiusExpression) return {"circle-radius":cr.radiusExpression,"circle-color":cr.expression||layer.color,"circle-opacity":layer.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.4}; return {"circle-radius":layer.radius||5,"circle-color":ce,"circle-opacity":layer.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.3}; }
    return {};
  }, []);

  // Map click — gère aussi la sélection de point pour route/iso
  const handleMapClick = useCallback((e) => {
    const lng = e.lngLat.lng, lat = e.lngLat.lat;
    // Classification supervisée — intercept si le panel est en mode dessin
    if (classifClickRef.current) { classifClickRef.current(lng, lat); return; }
    // Géoréférenceur — intercept si on place un point d'appui sur la carte
    if (georefClickRef.current) { georefClickRef.current(lng, lat); return; }
    // Viewshed — intercept si on place l'observateur sur la carte
    if (viewshedClickRef.current) { viewshedClickRef.current(lng, lat); return; }
    if (routePickMode) {
      const coord = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
      if (routePickMode === "origin") { setRouteOrigin(coord); if (activeTool==="route") { const feats = routeMarkers?.features?.filter(f=>f.properties?.type!=="origin")||[]; feats.unshift({type:"Feature",geometry:{type:"Point",coordinates:[lng,lat]},properties:{type:"origin",label:"A"}}); setRouteMarkers({type:"FeatureCollection",features:feats}); } else { setRouteMarkers({type:"FeatureCollection",features:[{type:"Feature",geometry:{type:"Point",coordinates:[lng,lat]},properties:{type:"origin",label:"●"}}]}); setIsoCenter(coord); } }
      else if (routePickMode === "dest") { setRouteDest(coord); const feats = routeMarkers?.features?.filter(f=>f.properties?.type!=="dest")||[]; feats.push({type:"Feature",geometry:{type:"Point",coordinates:[lng,lat]},properties:{type:"dest",label:"B"}}); setRouteMarkers({type:"FeatureCollection",features:feats}); }
      setRoutePickMode(null);
      return;
    }
    if (activeTool==="measure_dist") { const pts=[...measurePts,[lng,lat]]; setMeasurePts(pts); if(pts.length>=2){const d=turf.length(turf.lineString(pts),{units:"kilometers"});setMeasureRes(d<1?`${(d*1000).toFixed(0)} m`:`${d.toFixed(2)} km`);} }
    else if (activeTool==="measure_area") { const pts=[...measurePts,[lng,lat]]; setMeasurePts(pts); if(pts.length>=3){const a=turf.area(turf.polygon([[...pts,pts[0]]]));setMeasureRes(a<10000?`${Math.round(a)} m²`:`${(a/10000).toFixed(2)} ha`);} }
    else if (activeTool==="buffer") { const buf=turf.buffer(turf.point([lng,lat]),bufferRadius/1000,{units:"kilometers"}); setBufferLayer({type:"FeatureCollection",features:[buf,{type:"Feature",geometry:{type:"Point",coordinates:[lng,lat]},properties:{}}]}); }
    else if (activeTool==="draw") { setDrawPts(p=>[...p,[lng,lat]]); }
    else if (activeTool==="profil") {
      if (profilDrawMode==="twopoints") {
        // Max 2 points — remplace le deuxième si déjà 2
        setDrawProfilPts(p => p.length < 2 ? [...p,[lng,lat]] : [p[0],[lng,lat]]);
      } else {
        // Polyline libre — ajouter le point
        setDrawProfilPts(p=>[...p,[lng,lat]]);
      }
    }
    else { if (!e.features?.length){setPopup(null);return;} const f=e.features[0]; setPopup({lng,lat,properties:f.properties,layerName:f.layer?.id||""}); }
  }, [activeTool, measurePts, bufferRadius, routePickMode, routeMarkers, profilDrawMode]);

  useEffect(() => { setMeasurePts([]); setMeasureRes(null); setBufferLayer(null); setDrawPts([]); setRoutePickMode(null); if (!openPanels.has("route")&&!openPanels.has("isochrone")) { setRouteLayer(null); setIsoLayer(null); setRouteMarkers(null); } }, [activeTool, openPanels]);

  const measureGJ = useMemo(() => {
    if (!measurePts.length) return null;
    const feats = measurePts.map(p => ({type:"Feature",geometry:{type:"Point",coordinates:p},properties:{}}));
    if (measurePts.length>=2 && activeTool==="measure_dist") feats.push({type:"Feature",geometry:{type:"LineString",coordinates:measurePts},properties:{}});
    if (measurePts.length>=3 && activeTool==="measure_area") feats.push({type:"Feature",geometry:{type:"Polygon",coordinates:[[...measurePts,measurePts[0]]]},properties:{}});
    return {type:"FeatureCollection",features:feats};
  }, [measurePts, activeTool]);

  const drawGJ = useMemo(() => {
    if (!drawPts.length) return null;
    const feats = drawPts.map(p => ({type:"Feature",geometry:{type:"Point",coordinates:p},properties:{}}));
    if (drawPts.length>=3) feats.push({type:"Feature",geometry:{type:"Polygon",coordinates:[[...drawPts,drawPts[0]]]},properties:{}});
    else if (drawPts.length>=2) feats.push({type:"Feature",geometry:{type:"LineString",coordinates:drawPts},properties:{}});
    return {type:"FeatureCollection",features:feats};
  }, [drawPts]);

  const intIds = useMemo(() => {
    const ids = [];
    layers.filter(l=>l.visible).forEach(l => {
      if (l.cluster) { ids.push(`${l.id}-unclustered`,`${l.id}-clusters`); }
      else if (!l.heatmap&&!l.extrude) { ids.push(`${l.id}-circle`,`${l.id}-fill`); }
      else if (l.extrude) { ids.push(`${l.id}-extrude`); }
    });
    return ids;
  }, [layers]);

  // ── Calcul itinéraire ─────────────────────────────────────
  const doRoute = useCallback(async () => {
    if (!routeOrigin || !routeDest) return;
    setRouteLoading(true);
    try {
      const geocode = async (q) => {
        const r = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1`, {headers:{"User-Agent":"OpenMapAgents/1.0"}});
        const d = await r.json(); if (!d.length) throw new Error(`Lieu non trouvé: ${q}`);
        return [parseFloat(d[0].lon), parseFloat(d[0].lat)];
      };
      const parseCoord = (s) => { const m = s.match(/^([\d.-]+)\s*,\s*([\d.-]+)$/); return m ? [parseFloat(m[2]),parseFloat(m[1])] : null; };
      const A = parseCoord(routeOrigin) || await geocode(routeOrigin);
      const B = parseCoord(routeDest)   || await geocode(routeDest);
      const gj = await computeRoute([A, B], routeProfile);
      setRouteLayer(gj); setIsoLayer(null);
      setRouteMarkers({type:"FeatureCollection",features:[
        {type:"Feature",geometry:{type:"Point",coordinates:A},properties:{type:"origin",label:"A"}},
        {type:"Feature",geometry:{type:"Point",coordinates:B},properties:{type:"dest",label:"B"}},
      ]});
      if (gj.features?.length) fitFeatures(gj.features);
      addLayer(gj, `Route ${routeProfile}`, "route");
    } catch (e) { alert("Erreur : " + e.message); }
    finally { setRouteLoading(false); }
  }, [routeOrigin, routeDest, routeProfile, fitFeatures, addLayer]);

  // ── Calcul isochrone ──────────────────────────────────────
  const doIsochrone = useCallback(async () => {
    if (!isoCenter) return;
    setRouteLoading(true);
    try {
      const geocode = async (q) => {
        const r = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1`, {headers:{"User-Agent":"OpenMapAgents/1.0"}});
        const d = await r.json(); if (!d.length) throw new Error(`Lieu non trouvé: ${q}`);
        return [parseFloat(d[0].lon), parseFloat(d[0].lat)];
      };
      const parseCoord = (s) => { const m = s.match(/^([\d.-]+)\s*,\s*([\d.-]+)$/); return m ? [parseFloat(m[2]),parseFloat(m[1])] : null; };
      const C2 = parseCoord(isoCenter) || await geocode(isoCenter);
      const gj = await computeIsochrone(C2, isoTime, routeProfile);
      setIsoLayer(gj); setRouteLayer(null);
      if (gj.features?.length) fitFeatures(gj.features);
      addLayer(gj, `Isochrone_${isoTime}min_${routeProfile}`, "isochrone");
    } catch (e) { alert("Erreur : " + e.message); }
    finally { setRouteLoading(false); }
  }, [isoCenter, isoTime, routeProfile, fitFeatures, addLayer]);

  // ════════════════════════════════════════════════════════════
  //  CONTENU SIDEBAR — modules intégrés dans le flux
  // ════════════════════════════════════════════════════════════
  const renderPanelContent = (activeTool) => {
    const P = 12; // padding standard
    const sb    = { flex:1, minHeight:0, overflowY:"auto", overflowX:"hidden", display:"flex", flexDirection:"column" };
    // Pour les modules "auto" — pas de flex-grow ni overflow, le contenu dicte la hauteur
    const sbAuto = { display:"flex", flexDirection:"column", padding:`${P}px` };
    const sec   = { padding:`${P}px ${P}px 0`, display:"flex", flexDirection:"column", gap:8 };
    const secAuto = { display:"flex", flexDirection:"column", gap:8 };
    const label = { fontSize:10, fontWeight:500, color:C.dim, textTransform:"uppercase", letterSpacing:"0.05em", marginBottom:2 };
    const inp   = { fontFamily:F, fontSize:11, padding:"7px 10px", borderRadius:7, border:`0.5px solid ${C.bdr}`, background:C.input, color:C.txt, width:"100%", outline:"none", boxSizing:"border-box" };
    const row   = { display:"flex", gap:6 };

    // ── Itinéraire ──────────────────────────────────────────
    // ── Agriculture de précision ─────────────────────────────
    if (activeTool === "agri") return (
        <AgriPanel
          mapRef={mapRef}
          addLayer={addLayer}
          C={C}
          F={F}
          M={M}
        />
      );

    if (activeTool === "classif") return (
      <ClassifSupPanel
        mapRef={mapRef}
        layers={layers}
        addRasterLayer={addRasterLayer}
        updateRasterLayer={updateRasterLayer}
        classifClickRef={classifClickRef}
      />
    );

      if (activeTool === "route") return (
      <div style={sbAuto}>
        <div style={secAuto}>
          {/* Mode de transport */}
          <div style={label}>Transport</div>
          <div style={{display:"flex",gap:4}}>
            {["foot","bike","car"].map(m=>(
              <button key={m} onClick={()=>setRouteProfile(m)}
                style={{fontFamily:F,flex:1,padding:"6px 4px",borderRadius:7,border:`0.5px solid ${routeProfile===m?C.acc+"55":C.bdr}`,background:routeProfile===m?C.acc+"18":"transparent",color:routeProfile===m?C.acc:C.mut,cursor:"pointer",fontSize:11}}>
                {m==="foot"?"À pied":m==="bike"?"Vélo":"Voiture"}
              </button>
            ))}
          </div>

          {/* Départ */}
          <div style={label}>Départ</div>
          <div style={row}>
            <AddressInput
              value={routeOrigin}
              onChange={setRouteOrigin}
              onSelect={({label, lat, lon}) => setRouteOrigin(`${lat.toFixed(5)}, ${lon.toFixed(5)}`)}
              placeholder="Adresse ou lat, lon"
              style={inp} C={C} F={F}
            />
            <button onClick={()=>setRoutePickMode("origin")} title="Cliquer sur la carte"
              style={{fontFamily:F,padding:"7px 10px",borderRadius:7,border:`0.5px solid ${routePickMode==="origin"?C.acc+"55":C.bdr}`,background:routePickMode==="origin"?C.acc+"22":"transparent",color:routePickMode==="origin"?C.acc:C.mut,cursor:"pointer",fontSize:13,flexShrink:0}}>
              📍
            </button>
          </div>

          {/* Arrivée */}
          <div style={label}>Arrivée</div>
          <div style={row}>
            <AddressInput
              value={routeDest}
              onChange={setRouteDest}
              onSelect={({label, lat, lon}) => setRouteDest(`${lat.toFixed(5)}, ${lon.toFixed(5)}`)}
              placeholder="Adresse ou lat, lon"
              style={inp} C={C} F={F}
            />
            <button onClick={()=>setRoutePickMode("dest")} title="Cliquer sur la carte"
              style={{fontFamily:F,padding:"7px 10px",borderRadius:7,border:`0.5px solid ${routePickMode==="dest"?C.acc+"55":C.bdr}`,background:routePickMode==="dest"?C.acc+"22":"transparent",color:routePickMode==="dest"?C.acc:C.mut,cursor:"pointer",fontSize:13,flexShrink:0}}>
              📍
            </button>
          </div>

          {routePickMode && (
            <div style={{fontSize:11,color:C.acc,padding:"6px 10px",background:C.acc+"12",borderRadius:7}}>
              Cliquez sur la carte pour définir {routePickMode==="origin"?"le départ":"l'arrivée"}
            </div>
          )}

          <BtnRow onClick={doRoute} C={C} accent>{routeLoading?"Calcul en cours…":"Calculer l'itinéraire"}</BtnRow>

          {routeLayer && (
            <div style={{background:C.hover,borderRadius:8,padding:"10px 12px"}}>
              <div style={{fontSize:11,color:C.acc,fontWeight:500,marginBottom:6}}>Itinéraire calculé</div>
              {routeLayer.metadata && <div style={{fontSize:10,color:C.dim}}>{routeLayer.metadata.distance_km?.toFixed(1)} km · {routeLayer.metadata.duration_min?.toFixed(0)} min</div>}
              <div style={{display:"flex",gap:4,marginTop:8}}>
                <button onClick={()=>addLayer(routeLayer,"Itinéraire","route")} style={{fontFamily:F,flex:1,fontSize:10,padding:"5px 0",borderRadius:6,border:`0.5px solid ${C.acc}55`,background:C.acc+"18",color:C.acc,cursor:"pointer"}}>Ajouter couche</button>
                <button onClick={()=>{setRouteLayer(null);setRouteMarkers(null);setRouteOrigin("");setRouteDest("");}} style={{fontFamily:F,flex:1,fontSize:10,padding:"5px 0",borderRadius:6,border:`0.5px solid ${C.bdr}`,background:"transparent",color:C.mut,cursor:"pointer"}}>Effacer</button>
              </div>
            </div>
          )}
        </div>
      </div>
    );

    // ── Isochrone ───────────────────────────────────────────
    if (activeTool === "isochrone") return (
      <div style={sbAuto}>
        <div style={secAuto}>
          <div style={label}>Transport</div>
          <div style={{display:"flex",gap:4}}>
            {["foot","bike","car"].map(m=>(
              <button key={m} onClick={()=>setRouteProfile(m)}
                style={{fontFamily:F,flex:1,padding:"6px 4px",borderRadius:7,border:`0.5px solid ${routeProfile===m?C.acc+"55":C.bdr}`,background:routeProfile===m?C.acc+"18":"transparent",color:routeProfile===m?C.acc:C.mut,cursor:"pointer",fontSize:11}}>
                {m==="foot"?"À pied":m==="bike"?"Vélo":"Voiture"}
              </button>
            ))}
          </div>

          <div style={label}>Centre</div>
          <div style={row}>
            <AddressInput
              value={isoCenter}
              onChange={setIsoCenter}
              onSelect={({lat, lon}) => setIsoCenter(`${lat.toFixed(5)}, ${lon.toFixed(5)}`)}
              placeholder="Adresse ou lat, lon"
              style={inp} C={C} F={F}
            />
            <button onClick={()=>setRoutePickMode("origin")} title="Cliquer sur la carte"
              style={{fontFamily:F,padding:"7px 10px",borderRadius:7,border:`0.5px solid ${routePickMode==="origin"?C.acc+"55":C.bdr}`,background:routePickMode==="origin"?C.acc+"22":"transparent",color:routePickMode==="origin"?C.acc:C.mut,cursor:"pointer",fontSize:13,flexShrink:0}}>
              📍
            </button>
          </div>

          {routePickMode && (
            <div style={{fontSize:11,color:C.acc,padding:"6px 10px",background:C.acc+"12",borderRadius:7}}>
              Cliquez sur la carte pour définir le centre
            </div>
          )}

          <div style={label}>Temps de trajet</div>
          <div style={{display:"flex",alignItems:"center",gap:8}}>
            <input type="range" min={5} max={60} step={5} value={isoTime} onChange={e=>setIsoTime(Number(e.target.value))} style={{flex:1}}/>
            <span style={{fontFamily:M,fontSize:12,color:C.txt,minWidth:40}}>{isoTime} min</span>
          </div>

          <BtnRow onClick={doIsochrone} C={C} accent>{routeLoading?"Calcul…":"Calculer l'isochrone"}</BtnRow>

          {isoLayer && (
            <div style={{background:C.hover,borderRadius:8,padding:"10px 12px"}}>
              <div style={{fontSize:11,color:C.acc,fontWeight:500,marginBottom:6}}>Isochrone calculée</div>
              <div style={{display:"flex",gap:4,marginTop:4}}>
                <button onClick={()=>addLayer(isoLayer,`Isochrone_${isoTime}min`,"isochrone")} style={{fontFamily:F,flex:1,fontSize:10,padding:"5px 0",borderRadius:6,border:`0.5px solid ${C.acc}55`,background:C.acc+"18",color:C.acc,cursor:"pointer"}}>Ajouter couche</button>
                <button onClick={()=>{setIsoLayer(null);setRouteMarkers(null);setIsoCenter("");}} style={{fontFamily:F,flex:1,fontSize:10,padding:"5px 0",borderRadius:6,border:`0.5px solid ${C.bdr}`,background:"transparent",color:C.mut,cursor:"pointer"}}>Effacer</button>
              </div>
            </div>
          )}
        </div>
      </div>
    );

    // ── Couches — intégrées directement (Embed neutralise tout absolute) ──
    if (activeTool === "layers") return (
      <Embed>
        <LayerPanel
          layers={layers} onToggle={toggleL} onRemove={removeL} onStyle={styleL}
          onExport={exportL} onClassify={classifyL} onExportFmt={exportFmt}
          onRename={renameL} onMoveUp={moveLayerUp} onMoveDown={moveLayerDown}
          onZoomExtent={zoomToLayer} onUpdateRasterLayer={updateRasterLayer} mapRef={mapRef}
          onFilter={filterL} onUpdateGeojson={updateGeojson}
        />
      </Embed>
    );

    // ── Statistiques ────────────────────────────────────────
    if (activeTool === "stats") return (
      <Embed>
        <StatsPanelWrapper layers={layers} onZoom={zoomFeat} onAddLayer={addLayer} />
      </Embed>
    );

    // ── Export ──────────────────────────────────────────────
    if (activeTool === "export") return (
      <div style={sbAuto}>
        {layers.length === 0 && <div style={{fontSize:11,color:C.dim}}>Aucune couche à exporter.</div>}
        {layers.map(l => (
          <div key={l.id} style={{background:C.hover,borderRadius:8,padding:"8px 10px",marginBottom:8}}>
            <div style={{fontSize:11,fontWeight:500,marginBottom:6,color:C.txt,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}}>{l.name}</div>
            <div style={{display:"flex",flexWrap:"wrap",gap:4}}>
              {["GeoJSON",...EXPORT_FORMATS.filter(f=>f!=="GeoJSON")].map(fmt=>(
                <button key={fmt} onClick={()=>fmt==="GeoJSON"?exportL(l.id):exportFmt(l.id,fmt)}
                  style={{fontFamily:F,fontSize:10,padding:"3px 8px",borderRadius:5,border:`0.5px solid ${C.bdr}`,background:C.card,color:C.mut,cursor:"pointer"}}>
                  {fmt}
                </button>
              ))}
            </div>
          </div>
        ))}
        <div style={{borderTop:`0.5px solid ${C.bdr}`,paddingTop:10,display:"flex",flexDirection:"column",gap:6}}>
          <BtnRow onClick={()=>fileRef.current?.click()} C={C}><IcUpload/> Importer un fichier</BtnRow>
          <BtnRow onClick={shareLink} C={C}><IcShare/> Partager le lien</BtnRow>
        </div>
      </div>
    );

    // ── Analyse spatiale ────────────────────────────────────
    if (activeTool === "burnsev") return (
      <Embed>
        <BurnSeverityPanel layers={layers} mapRef={mapRef}
          onAddRasterLayer={addRasterLayer} onAddLayer={addLayer} />
      </Embed>
    );

    if (activeTool === "watershed") return (
      <Embed>
        <WatershedPanel layers={layers} mapRef={mapRef}
          onAddLayer={addLayer} onAddLayerSilent={addLayerSilent}
          onRemoveLayers={removeLayersByName} />
      </Embed>
    );

    if (activeTool === "vectorcat") return (
      <Embed>
        <VectorCatalogPanel layers={layers} mapRef={mapRef}
          onAddLayer={addLayer} onAddLayerSilent={addLayerSilent} />
      </Embed>
    );

    if (activeTool === "flood") return (
      <Embed>
        <FloodPanel layers={layers} mapRef={mapRef}
          onAddRasterLayer={addRasterLayer} onAddLayer={addLayer}
          onRemoveLayers={removeLayersByName} />
      </Embed>
    );

    if (activeTool === "weather") return (
      <Embed>
        <WeatherPanel mapRef={mapRef} />
      </Embed>
    );

    if (activeTool === "join") return (
      <Embed>
        <JoinPanel layers={layers.filter(l=>!l.isRaster&&l.geojson)} onAddLayer={addLayer} />
      </Embed>
    );

    if (activeTool === "spatial") return (
      <Embed>
        <SpatialPanel layers={layers.filter(l=>l.visible&&!l.isRaster)} onAddLayer={addLayer} />
      </Embed>
    );

    // ── Navigateur STAC + COG (chercher/ajouter des scènes) ───
    if (activeTool === "stac") return (
      <Embed>
        <StacPanel mapRef={mapRef} onAddImageLayer={addImageLayer} />
      </Embed>
    );

    // ── Maxar Open Data (imagerie catastrophe avant/après) ────
    if (activeTool === "maxar") return (
      <Embed>
        <MaxarPanel mapRef={mapRef} onAddImageLayer={addImageLayer} />
      </Embed>
    );

    // ── Ombres portées des bâtiments (simulation soleil) ──────
    if (activeTool === "shadow") return (
      <Embed>
        <ShadowPanel mapRef={mapRef} layers={layers} />
      </Embed>
    );

    // ── Story map (scrollytelling + export HTML) ──────────────
    if (activeTool === "story") return (
      <Embed>
        <StoryPanel mapRef={mapRef} layers={layers} />
      </Embed>
    );

    // ── SQL Workspace (DuckDB spatial → carte) ───────────────
    if (activeTool === "sql") return (
      <Embed>
        <SqlPanel onAddLayer={addLayer} layers={layers} />
      </Embed>
    );

    // ── Analyse raster : stats zonales + calculatrice ─────────
    if (activeTool === "rasteranalysis") return (
      <Embed>
        <RasterAnalysisPanel layers={layers} onAddLayer={addLayer} onAddImageLayer={addImageLayer} />
      </Embed>
    );

    // ── Vectorisation raster : polygones + contours ───────────
    if (activeTool === "rastervec") return (
      <Embed>
        <RasterVectorPanel layers={layers} onAddLayer={addLayer} />
      </Embed>
    );

    // ── Statistiques spatiales : Moran + hotspots Gi* ─────────
    if (activeTool === "spatialstats") return (
      <Embed>
        <SpatialStatsPanel layers={layers} onAddLayer={addLayer} />
      </Embed>
    );

    // ── Chaleur & clusters (densité de points) ────────────────
    if (activeTool === "vectorviz") return (
      <Embed>
        <VectorVizPanel layers={layers} onAdd={addLayerSilent} />
      </Embed>
    );

    // ── Analyse de visibilité (viewshed) ──────────────────────
    if (activeTool === "viewshed") return (
      <Embed>
        <ViewshedPanel viewshedClickRef={viewshedClickRef} onAddImageLayer={addImageLayer} layers={layers} onObsChange={setViewshedPtGJ} />
      </Embed>
    );

    // ── Géoréférenceur : caler une image par points d'appui ───
    if (activeTool === "georef") return (
      <Embed>
        <GeorefPanel georefClickRef={georefClickRef} onAddImageLayer={addImageLayer} onGcpsChange={setGeorefGcpGJ} />
      </Embed>
    );

    // ── Système solaire : globes 3D texturés (Three.js) ───────
    if (activeTool === "solarsystem") return (
      <Embed>
        <SolarSystemPanel body={solarBody} onBody={setSolarBody} />
      </Embed>
    );

    // ── Base de données ─────────────────────────────────────
    if (activeTool === "database") return (
      <Embed>
        <DBPanel onAddLayer={addLayer} />
      </Embed>
    );

    // ── Google Earth Engine ──────────────────────────────────
    if (activeTool === "gee") return (
      <Embed>
        <GEEPanel mapRef={mapRef} onAddRasterLayer={addRasterLayer} layers={layers} />
      </Embed>
    );

    // ── Vue 3D / Globe (deck.gl : LiDAR, 3D Tiles, glTF, splats) ──
    if (activeTool === "lidar") return (
      <Embed>
        <LidarPanel mapRef={mapRef} onAddLayer={addLayer} onAddImageLayer={addImageLayer} onAddPointcloudLayer={addPointcloudLayer} />
      </Embed>
    );
    if (activeTool === "scene3d") return (
      <Embed>
        <Scene3DPanel mapRef={mapRef} onAddLayer={addLayer} />
      </Embed>
    );

    // ── Services OGC ────────────────────────────────────────
    if (activeTool === "ogc") return (
      <Embed>
        <OGCPanel mapRef={mapRef} onAddLayer={addLayer} onAddRasterLayer={addRasterLayer} />
      </Embed>
    );

    // ── Profil altimétrique ─────────────────────────────────
    if (activeTool === "profil") return (
      <Embed>
        <ProfilPanel
          mapRef={mapRef}
          layers={layers}
          drawPoints={drawProfilPts}
          onClearDraw={() => setDrawProfilPts([])}
          onDrawModeChange={m => setProfilDrawMode(m)}
          onSetProfilLayer={gj => {
            setProfilSlopeLayer(gj);
            addLayer(gj, "Profil — pente", "profil");
          }}
        />
      </Embed>
    );

    // ── Détection de changement GEE ─────────────────────────
    if (activeTool === "change") return (
      <Embed>
        <ChangeDetectionPanel
          mapRef={mapRef}
          layers={layers}
          onAddRasterLayer={addRasterLayer}
        />
      </Embed>
    );

    // ── Comparateur A/B ──────────────────────────────────────
    if (activeTool === "compare") return (
      <Embed>
        <ComparatorPanel
          layers={layers}
          vs={vs}
          mapStyle={MAP_STYLES[mapSt]}
        />
      </Embed>
    );

    // ── Timelapse GEE ────────────────────────────────────────
    if (activeTool === "timelapse") return (
      <Embed>
        <TimelapsePanel mapRef={mapRef} layers={layers} />
      </Embed>
    );

    // ── Éditeur vectoriel ────────────────────────────────────
    if (activeTool === "editor") return (
      <Embed>
        <EditorPanel
          mapRef={mapRef}
          layers={layers}
          onSaveLayer={(gj, name, existingId) => {
            if (existingId) {
              // Mettre à jour une couche existante
              setLayers(prev => prev.map(l =>
                l.id === existingId
                  ? { ...l, geojson: gj, name, featureCount: gj.features?.length || 0 }
                  : l
              ));
            } else {
              // Créer une nouvelle couche
              addLayer(gj, name, "edit");
            }
          }}
        />
      </Embed>
    );

    // ── Import OSM ───────────────────────────────────────────
    if (activeTool === "osm") return (
      <Embed>
        <OsmPanel layers={layers} onAddLayer={addLayer} mapRef={mapRef} />
      </Embed>
    );

    return null;
  };

  const activeLabel = ALL_ITEMS.find(i => i.id === activeTool)?.label || "";

  // ── Handle resize visuel ──────────────────────────────────
  const rh = { width:5, flexShrink:0, background:"transparent", cursor:"col-resize", alignSelf:"stretch", zIndex:5, transition:"background .15s" };

  // ════════════════════════════════════════════════════════════
  //  RENDU
  // ════════════════════════════════════════════════════════════
  return (
    <ThemeContext.Provider value={C}>
    <div
      style={{ fontFamily:F, background:C.bg, color:C.txt, height:"100vh", width:"100%", display:"flex", flexDirection:"column", overflow:"hidden" }}
      onDragOver={e=>{e.preventDefault();setDragOver(true);}}
      onDragLeave={e=>{e.preventDefault();setDragOver(false);}}
      onDrop={e=>{e.preventDefault();setDragOver(false);if(e.dataTransfer?.files?.[0])doImport(e.dataTransfer.files[0]);}}>

      <style>{`
        html,body,#root{margin:0;padding:0;width:100%;height:100%;overflow:hidden}
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
        .rh:hover{background:${C.acc}50 !important}
        .rib:hover{background:${C.hover} !important}
        ::-webkit-scrollbar{width:4px;height:4px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:${C.bdr};border-radius:4px}
        ::-webkit-scrollbar-thumb:hover{background:${C.mut}}
      `}</style>

      <input ref={fileRef} type="file" accept=".geojson,.json,.csv,.tsv,.gpx,.kml,.zip,.shp,.tif,.tiff" style={{display:"none"}}
        onChange={e=>{if(e.target.files?.[0])doImport(e.target.files[0]);e.target.value="";}} />

      {dragOver && (
        <div style={{position:"fixed",inset:0,zIndex:9999,background:`${C.acc}18`,border:`3px dashed ${C.acc}`,display:"flex",alignItems:"center",justifyContent:"center",pointerEvents:"none"}}>
          <div style={{background:C.card,padding:"20px 36px",borderRadius:12,fontSize:14,fontWeight:500,color:C.acc}}>Déposez votre fichier</div>
        </div>
      )}

      {/* ══════ HEADER ══════ */}
      <header style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"0 12px",height:42,background:C.card,borderBottom:`0.5px solid ${C.bdr}`,flexShrink:0,gap:8}}>

        {/* ── Gauche : logo + nom + sur mobile thème & chat ── */}
        <div style={{display:"flex",alignItems:"center",gap:6,flexShrink:0}}>
          <a href="/" title="Accueil OpenMapAgents" style={{display:"flex",alignItems:"center",gap:6,textDecoration:"none"}}>
            <svg width="26" height="26" viewBox="0 0 26 26" fill="none" xmlns="http://www.w3.org/2000/svg" style={{borderRadius:6}}>
                <rect width="26" height="26" rx="6" fill={C.acc}/>
                {/* Globe */}
                <circle cx="13" cy="13" r="8" stroke="#fff" strokeWidth="1.2" fill="none" opacity="0.95"/>
                {/* Meridians */}
                <ellipse cx="13" cy="13" rx="3.5" ry="8" stroke="#fff" strokeWidth="1" fill="none" opacity="0.7"/>
                <line x1="5" y1="13" x2="21" y2="13" stroke="#fff" strokeWidth="1" opacity="0.7"/>
                {/* Parallels */}
                <ellipse cx="13" cy="10" rx="6.5" ry="2" stroke="#fff" strokeWidth="0.9" fill="none" opacity="0.55"/>
                <ellipse cx="13" cy="16" rx="6.5" ry="2" stroke="#fff" strokeWidth="0.9" fill="none" opacity="0.55"/>
              </svg>
            <div>
              <div style={{fontSize:13,fontWeight:600,color:C.txt,lineHeight:1}}>OpenMapAgents</div>
              <div style={{fontSize:9,color:C.dim,marginTop:1}}>Overture Maps · DuckDB · LiteLLM</div>
            </div>
          </a>
          <Link to="/doc" title="Documentation — indicateurs et outils" className="rib"
            style={{display:"flex",alignItems:"center",gap:5,textDecoration:"none",fontFamily:F,fontSize:11,
              padding:isMobile?"5px 7px":"4px 10px",borderRadius:6,border:`0.5px solid ${C.bdr}`,
              color:C.dim,background:"transparent",marginLeft:2}}>
            <IcInfo size={13}/>{!isMobile&&"Documentation"}
          </Link>
        </div>

        {/* ── Centre : styles carte + globe — desktop uniquement ── */}
        {/* Un SEUL enfant d'en-tête pour tout ce groupe : le <header> est en
            justify-content:space-between, une section supplémentaire serait
            écartée des autres au lieu de rester collée à « Projections ». */}
        <div style={{display:"flex",gap:3,alignItems:"center"}}>
          {!isMobile&&<>
          {Object.keys(MAP_STYLES).filter(k=>!PLANET_KEYS.includes(k)).map(k=>(
            <button key={k} onClick={()=>setMapSt(k)} className="rib"
              style={{fontFamily:F,fontSize:10,padding:"3px 9px",borderRadius:5,border:`0.5px solid ${mapSt===k?C.acc+"55":C.bdr}`,background:mapSt===k?C.acc+"15":"transparent",color:mapSt===k?C.acc:C.dim,cursor:"pointer"}}>
              {k.charAt(0).toUpperCase() + k.slice(1)}
            </button>
          ))}
          <div style={{width:1,height:16,background:C.bdr,margin:"0 3px"}}/>
          {/* Sélecteur de planète (liste déroulante) : une planète se voit en globe */}
          <select value={currentPlanet} onChange={e=>selectPlanet(e.target.value)} className="rib"
            title="Choisir un corps céleste (vue globe)"
            style={{fontFamily:F,fontSize:10,padding:"3px 6px",borderRadius:5,cursor:"pointer",
              border:`0.5px solid ${currentPlanet!=="earth"?C.acc+"55":C.bdr}`,
              background:currentPlanet!=="earth"?C.acc+"12":"transparent",
              color:currentPlanet!=="earth"?C.acc:C.dim,outline:"none"}}>
            {PLANETS.map(p=>(<option key={p.key} value={p.key}>{p.icon} {p.label}</option>))}
          </select>
          <div style={{width:1,height:16,background:C.bdr,margin:"0 3px"}}/>
          {/* Sélecteur de projection de la carte live : seuls Plan (Mercator) et Globe sont possibles sous MapLibre */}
          <div style={{display:"flex",border:`0.5px solid ${C.bdr}`,borderRadius:5,overflow:"hidden"}}>
            {[["Plan",false],["Globe",true]].map(([l,g])=>(
              <button key={l} onClick={()=>setProjectionMode(g)} className="rib" title={g?"Globe 3D (MapLibre)":"Carte plate — projection Mercator"}
                style={{fontFamily:F,fontSize:10,padding:"3px 9px",border:"none",borderLeft:g?`0.5px solid ${C.bdr}`:"none",background:globeOn===g?C.acc+"18":"transparent",color:globeOn===g?C.acc:C.dim,cursor:"pointer",display:"flex",alignItems:"center",gap:4}}>
                {g&&<IcGlobe size={12}/>}{l}
              </button>
            ))}
          </div>
          {/* Explorateur pédagogique : Robinson, Peters, Mollweide… (hors carte live) */}
          <button onClick={()=>setProjOpen(true)} className="rib" title="Explorateur de projections — Le pouvoir des cartes (Robinson, Peters, Mollweide…)"
            style={{fontFamily:F,fontSize:10,padding:"3px 9px",borderRadius:5,border:`0.5px solid ${C.bdr}`,background:"transparent",color:C.dim,cursor:"pointer",display:"flex",alignItems:"center",gap:4}}>
            <IcMap size={12}/> Projections
          </button>
          </>}

          {/* Relief 3D + Ambiance : hors du `!isMobile` mais DANS le même groupe,
              donc toujours collés a « Projections ». Sur telephone, seuls ces
              deux boutons restent, en icones. */}
          <button onClick={toggleTerrain} className="rib" title={terrain3D?"Désactiver le relief 3D":"Relief 3D (terrain) — monter/baisser la hauteur"}
            style={{fontFamily:F,fontSize:10,padding:isMobile?"5px 7px":"3px 9px",borderRadius:5,border:`0.5px solid ${terrain3D?C.acc+"55":C.bdr}`,background:terrain3D?C.acc+"15":"transparent",color:terrain3D?C.acc:C.dim,cursor:"pointer",display:"flex",alignItems:"center",gap:4}}>
            <IcMountain size={12}/>{!isMobile&&" Relief 3D"}
          </button>
          {/* Ambiance 3D — un seul bouton, réglages en LISTE dépliante. Six
              contrôles alignés débordaient de l'en-tête sur mobile. */}
          {terrain3D&&<div style={{position:"relative"}}>
            <button onClick={()=>setAmb3DOpen(v=>!v)} className="rib" title="Ambiance : exagération, moment de la journée, météo"
              style={{fontFamily:F,fontSize:10,padding:isMobile?"5px 7px":"3px 9px",borderRadius:5,border:`0.5px solid ${amb3DOpen?C.acc+"55":C.bdr}`,background:amb3DOpen?C.acc+"15":"transparent",color:amb3DOpen?C.acc:C.dim,cursor:"pointer",display:"flex",alignItems:"center",gap:4}}>
              {weather==="rain"?<IcCloudRain size={12}/>:weather==="snow"?<IcSnowflake size={12}/>:<IcSun size={12}/>}
              {!isMobile&&"Ambiance"}
              <IcChevronDown size={11}/>
            </button>
            {amb3DOpen&&(<>
              {/* Voile de fermeture : un clic hors du panneau le referme */}
              <div onClick={()=>setAmb3DOpen(false)} style={{position:"fixed",inset:0,zIndex:2800}}/>
              <div style={{position:"absolute",top:"calc(100% + 6px)",right:0,zIndex:2801,width:210,maxWidth:"calc(100vw - 24px)",
                           background:C.card,border:`0.5px solid ${C.bdr}`,borderRadius:9,boxShadow:"0 10px 30px rgba(0,0,0,.35)",
                           padding:10,display:"flex",flexDirection:"column",gap:11}}>
                <div>
                  <div style={{display:"flex",justifyContent:"space-between",fontSize:9,color:C.dim,textTransform:"uppercase",letterSpacing:".05em",marginBottom:5}}>
                    <span>Exagération</span><span style={{fontFamily:M,color:C.mut}}>×{terrainExag}</span>
                  </div>
                  <input type="range" min="0.5" max="6" step="0.5" value={terrainExag}
                    onChange={e=>changeExag(parseFloat(e.target.value))} style={{width:"100%",height:3}}/>
                </div>
                <div>
                  <div style={{fontSize:9,color:C.dim,textTransform:"uppercase",letterSpacing:".05em",marginBottom:5}}>Moment</div>
                  <div style={{display:"flex",flexDirection:"column",gap:2}}>
                    {Object.entries(SKY_MOMENTS).map(([k,p])=>(
                      <button key={k} onClick={()=>changeMoment(k)}
                        style={{fontFamily:F,fontSize:11,padding:"6px 9px",borderRadius:6,border:"none",textAlign:"left",
                                background:skyMoment===k?C.acc+"18":"transparent",color:skyMoment===k?C.acc:C.mut,cursor:"pointer"}}>
                        {p.label}
                      </button>
                    ))}
                  </div>
                </div>
                <div>
                  <div style={{fontSize:9,color:C.dim,textTransform:"uppercase",letterSpacing:".05em",marginBottom:5}}>Météo</div>
                  <div style={{display:"flex",flexDirection:"column",gap:2}}>
                    {Object.entries(WEATHER).map(([k,p])=>{
                      const Ico = k==="rain"?IcCloudRain:k==="snow"?IcSnowflake:IcSun;
                      return (
                        <button key={k} onClick={()=>changeWeather(k)}
                          style={{fontFamily:F,fontSize:11,padding:"6px 9px",borderRadius:6,border:"none",textAlign:"left",
                                  background:weather===k?C.acc+"18":"transparent",color:weather===k?C.acc:C.mut,cursor:"pointer",
                                  display:"flex",alignItems:"center",gap:7}}>
                          <Ico size={13}/> {p.label}
                        </button>
                      );
                    })}
                  </div>
                </div>
              </div>
            </>)}
          </div>}
        </div>

        {/* ── Droite : actions ── */}
        <div style={{display:"flex",alignItems:"center",gap:4}}>
          {!isMobile&&<div style={{fontSize:10,color:C.dim,padding:"2px 8px",borderRadius:5,background:C.hover,border:`0.5px solid ${C.bdr}`,display:"flex",alignItems:"center",gap:4,flexShrink:0}}>
            <span style={{width:5,height:5,borderRadius:"50%",background:C.acc,display:"inline-block"}}/>DuckDB
          </div>}
          {!isMobile&&<button className="rib" onClick={toggleTheme} style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
            {themeName==="dark"?<IcSun/>:<IcMoon/>}
          </button>}
          {!isMobile&&<button className="rib" onClick={shareLink} title="Partager le lien" style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
            <IcShare/>
          </button>}
          {!isMobile&&<button className="rib" onClick={()=>fileRef.current?.click()} title="Importer un fichier" style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
            <IcUpload/>
          </button>}
          {!isMobile&&<button className="rib" onClick={()=>setChatOpen(o=>!o)}
            style={{fontFamily:F,fontSize:11,padding:"5px 10px",borderRadius:6,border:`0.5px solid ${chatOpen?C.acc+"44":C.bdr}`,background:chatOpen?C.acc+"18":"transparent",color:chatOpen?C.acc:C.mut,cursor:"pointer",display:"flex",alignItems:"center",gap:5}}>
            <IcChat/> Chat
          </button>}
          {user ? (<>
            <button className="rib" onClick={() => {
              // Capture miniature : attendre render complet WebGL
              const map = mapRef.current?.getMap?.();
              if (!map) { setShowSave(true); return; }

              const capture = () => {
                // triggerRepaint force un cycle WebGL complet
                map.triggerRepaint();
                // Attendre 3 frames pour que le GPU ait fini
                requestAnimationFrame(() => requestAnimationFrame(() => requestAnimationFrame(() => {
                  try {
                    const canvas = map.getCanvas();
                    const data = canvas.toDataURL("image/jpeg", 0.85);
                    // < 5ko = canvas vide/noir → ne pas utiliser
                    if (data && data.length > 5000) setSaveThumb(data);
                    else setSaveThumb("");
                  } catch { setSaveThumb(""); }
                  setShowSave(true);
                })));
              };

              // Si la carte est en mouvement, attendre idle
              if (map.isMoving() || map.isZooming() || map.isRotating()) {
                map.once("idle", capture);
              } else {
                capture();
              }
            }} title="Sauvegarder la carte"
              style={{fontFamily:F,fontSize:11,padding:"5px 10px",borderRadius:6,
                      border:`0.5px solid ${currentMap ? C.acc+"44" : C.bdr}`,
                      background: currentMap ? C.acc+"18" : "transparent",
                      color: currentMap ? C.acc : C.mut,
                      cursor:"pointer",display:"flex",alignItems:"center",gap:4}}>
              💾
            </button>
            <button className="rib" onClick={() => setShowDash(true)}
              title={`Mon espace — ${user.username}`}
              style={{fontFamily:F,fontSize:11,padding:"5px 10px",borderRadius:6,
                      border:`0.5px solid ${C.acc}44`,background:C.acc+"18",
                      color:C.acc,cursor:"pointer",display:"flex",alignItems:"center",gap:5}}>
              👤 {user.username}
            </button>
          </>) : (
            null /* bouton connexion désactivé temporairement */
          )}
          {isMobile&&<>
            <button className="rib" onClick={()=>setChatOpen(o=>!o)}
              style={{fontFamily:F,fontSize:11,padding:"5px 10px",borderRadius:6,border:`0.5px solid ${chatOpen?C.acc+"44":C.bdr}`,background:chatOpen?C.acc+"18":"transparent",color:chatOpen?C.acc:C.mut,cursor:"pointer",display:"flex",alignItems:"center",gap:5}}>
              <IcChat/> Chat
            </button>
            <button className="rib" onClick={toggleTheme} style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
              {themeName==="dark"?<IcSun/>:<IcMoon/>}
            </button>
            <button className="rib" onClick={shareLink} title="Partager" style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
              <IcShare/>
            </button>
          </>}

        </div>
      </header>

      {/* ══════ BODY ══════ */}
      <div style={{flex:1,display:"flex",overflow:"hidden"}}>

        {/* ── RAIL THÉMATIQUE UNIFIÉ (piloté par menuTree.js) ── */}
        <ThematicMenu
          C={C} activeTool={activeTool} onActivate={activateItem} onIndicator={(id) => openModal({ type: "indicator", indKey: id })}
          layersCount={layers.length} openPanels={openPanels} panelIds={PANEL_IDS}
          onImport={() => fileRef.current?.click()} onPrint={() => activateItem("print")}
          onOpenSearch={() => setSearchOpen(true)} isMobile={isMobile}
        />

        {/* ── Palette de recherche globale (Ctrl+K) ── */}
        {searchOpen && (
          <SearchPalette
            onClose={() => setSearchOpen(false)}
            onSelect={(r) => { if (r.kind === "indicator") openModal({ type: "indicator", indKey: r.id }); else activateItem(r.id); }}
          />
        )}

        {/* ── Explorateur de projections (Le pouvoir des cartes) ── */}
        {projOpen && <ProjectionExplorer layers={layers} mapStyle={mapSt} onClose={() => setProjOpen(false)} />}
        {false && (
        <div style={{width:56,background:C.card,borderRight:`0.5px solid ${C.bdr}`,display:"flex",flexDirection:"column",alignItems:"center",padding:"4px 0",flexShrink:0,overflowY:"auto",overflowX:"hidden"}}>
          {RAIL_GROUPS.map((group, gi) => {
            // groupe "nav" toujours visible, pas de header cliquable
            const isCollapsible = !!group.label;
            const isOpen        = !isCollapsible || openGroup.has(group.id);
            // un item du groupe est-il actif ?
            const groupHasActive = group.items.some(it =>
              activeTool === it.id || (PANEL_IDS.has(it.id) && openPanels.has(it.id))
            );
            return (
              <div key={group.id} style={{width:"100%",display:"flex",flexDirection:"column",alignItems:"center",borderBottom:gi<RAIL_GROUPS.length-1?`0.5px solid ${C.bdr}`:"none"}}>

                {/* ── Header cliquable du groupe ── */}
                {isCollapsible && (
                  <button
                    onClick={() => setOpenGroup(prev => { const next = new Set(prev); next.has(group.id) ? next.delete(group.id) : next.add(group.id); return next; })}
                    title={group.label}
                    style={{
                      width:"100%", padding:"5px 0 4px", border:"none", background:"transparent",
                      cursor:"pointer", display:"flex", flexDirection:"column", alignItems:"center", gap:2,
                    }}
                  >
                    {/* Pastille active */}
                    <div style={{
                      fontSize:8, fontWeight:700, letterSpacing:"0.06em", textTransform:"uppercase",
                      color: groupHasActive ? C.acc : isOpen ? C.mut : C.dim,
                      lineHeight:1, userSelect:"none", position:"relative",
                    }}>
                      {group.label}
                      {groupHasActive && !isOpen && (
                        <span style={{position:"absolute",top:-1,right:-6,width:4,height:4,borderRadius:"50%",background:C.acc}}/>
                      )}
                    </div>
                    {/* Chevron */}
                    <svg width="8" height="5" viewBox="0 0 8 5" fill="none" style={{transition:"transform .2s", transform: isOpen ? "rotate(180deg)" : "rotate(0deg)"}}>
                      <path d="M1 1l3 3 3-3" stroke={groupHasActive ? C.acc : C.dim} strokeWidth="1.5" strokeLinecap="round"/>
                    </svg>
                  </button>
                )}

                {/* ── Items (visibles si groupe ouvert) ── */}
                {isOpen && (
                  <div style={{width:"100%",display:"flex",flexDirection:"column",alignItems:"center",gap:1,paddingBottom:4}}>
                    {!isCollapsible && <div style={{height:4}}/>}
                    {group.items.map(({id, label, sub, Icon: Ic2}) => {
                      const isPanelActive = PANEL_IDS.has(id) && openPanels.has(id);
                      const isActive      = activeTool === id;
                      const color         = isActive || isPanelActive ? C.acc : C.mut;
                      return (
                        <button key={id} className="rib" title={label} onClick={()=>activateItem(id)}
                          style={{width:"100%",padding:"4px 4px 3px",borderRadius:7,border:`0.5px solid ${isPanelActive?C.acc+"44":"transparent"}`,display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",gap:1,background:isPanelActive?C.acc+"20":isActive&&!PANEL_IDS.has(id)?C.acc+"15":"transparent",color,cursor:"pointer",transition:"all .12s"}}>
                          <div style={{position:"relative",display:"inline-flex"}}>
                            <Ic2/>
                            {id==="layers"&&layers.length>0&&<span style={{position:"absolute",top:-4,right:-6,background:C.acc,color:"#fff",borderRadius:8,fontSize:8,padding:"0 3px",fontWeight:700,lineHeight:"14px",minWidth:13,textAlign:"center"}}>{layers.length}</span>}
                          </div>
                          <span style={{fontSize:7,fontWeight:500,letterSpacing:"0.02em",lineHeight:1,opacity: isActive||isPanelActive ? 1 : 0.7}}>{sub}</span>
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
          {/* ── Import + Imprimer — juste après les groupes ── */}
          <div style={{width:"100%",display:"flex",flexDirection:"column",alignItems:"center",gap:1,padding:"5px 0",borderTop:`0.5px solid ${C.bdr}`}}>
            <button className="rib" title="Importer un fichier" onClick={()=>fileRef.current?.click()}
              style={{width:"100%",padding:"4px 4px 3px",borderRadius:7,border:"none",background:"transparent",color:C.dim,cursor:"pointer",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",gap:1}}>
              <IcUpload/>
              <span style={{fontSize:7,fontWeight:500,lineHeight:1,opacity:0.6}}>Import</span>
            </button>
            <button className="rib" title="Exporter carte" onClick={()=>activateItem("print")}
              style={{width:"100%",padding:"4px 4px 3px",borderRadius:7,border:`0.5px solid ${activeTool==="print"?C.acc+"44":"transparent"}`,background:activeTool==="print"?C.acc+"20":"transparent",color:activeTool==="print"?C.acc:C.dim,cursor:"pointer",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",gap:1}}>
              <IcPrint/>
              <span style={{fontSize:7,fontWeight:500,lineHeight:1,opacity:activeTool==="print"?1:0.6}}>Imprimer</span>
            </button>
          </div>
        </div>
        )}

        {/* Sidebar étendue retirée — modules dans FloatingPanels sur la carte */}

        {/* ── CARTE ── */}
        <div style={{flex:1,position:"relative",minWidth:0,background:globeOn?"#010206":undefined}}>
          {/* Fond spatial (étoiles + filantes) visible dans l'espace autour du globe */}
          {globeOn && <StarField />}
          <Map ref={mapRef} {...vs} onMove={e=>setVs(e.viewState)}
            style={{width:"100%",height:"100%",background:"transparent"}} mapStyle={typeof MAP_STYLES[mapSt]==="string"?MAP_STYLES[mapSt]:MAP_STYLES["positron"]}
            maplibreLogo={false} attributionControl={false} preserveDrawingBuffer={true}
            /* MapLibre plafonne l'inclinaison à 60° par défaut : impossible d'amener
               la ligne d'horizon dans le champ, donc de voir le ciel. 85° = maximum
               supporté, on regarde alors quasiment à l'horizontale. */
            maxPitch={85}
            onClick={handleMapClick} interactiveLayerIds={activeTool==="pointer"?intIds:[]}
            cursor={(activeTool!=="pointer"||routePickMode)?"crosshair":"grab"}
            onDoubleClick={(e)=>{
              // Double-clic sur la carte en mode polyline → ne rien faire (le dernier point a déjà été ajouté par onClick)
              // On empêche juste le zoom double-clic natif MapLibre
              e.preventDefault?.();
            }}
            onContextMenu={async(e)=>{
              e.preventDefault();const{lng,lat}=e.lngLat;
              try{const res=await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json`,{headers:{"User-Agent":"OpenMapAgents/1.0"}});const d=await res.json();setPopup({lng,lat,properties:{adresse:d.display_name||"Inconnu",lat:lat.toFixed(5),lon:lng.toFixed(5)},layerName:"geocode"});}
              catch{setPopup({lng,lat,properties:{lat:lat.toFixed(5),lon:lng.toFixed(5)},layerName:"coords"});}
            }}>

            {/* Geocoder flottant collé au NavigationControl */}
            <GeocoderControl mapRef={mapRef} C={C} />
            <NavigationControl position="top-right"/>
            <ScaleControl position="bottom-left"/>

            {/* Layers — inchangés */}
            {/* ── Notification éphémère (erreurs GEE, etc.) ── */}
            {/* ── Modale Timelapse (chat LLM) ── */}
            {timelapse && (
              <TimelapseModal
                timelapse={timelapse}
                onClose={() => setTimelapse(null)}
              />
            )}

            
            {/* ── Auth ── */}
            {showAuth && <AuthModal onClose={() => setShowAuth(false)} />}

            {/* ── Fenêtres flottantes empilables (menu thématique) ── */}
            {modals.map(m => m.type === "bivariate" ? (
              <BivariateModal
                key={m.id} mapRef={mapRef} layers={layers}
                onAddRasterLayer={addRasterLayer}
                z={m.z} initialPos={m.pos}
                onFocus={() => focusModal(m.id)} onClose={() => closeModal(m.id)}
              />
            ) : (
              <IndicatorModal
                key={m.id} indKey={m.indKey} mapRef={mapRef} layers={layers}
                onAddRasterLayer={addRasterLayer} onAnimate={setTlLayerId}
                z={m.z} initialPos={m.pos}
                onFocus={() => focusModal(m.id)} onClose={() => closeModal(m.id)}
              />
            ))}

            {/* ── Sauvegarder ── */}
            {showSave && (
              <SaveMapModal
                mapRef={mapRef}
                layers={layers}
                viewport={vs}
                mapStyle={mapSt}
                existingMap={currentMap}
                initialThumb={saveThumb}
                onClose={() => { setShowSave(false); setSaveThumb(""); }}
                onSaved={m => setCurrentMap(m)}
              />
            )}

            {/* ── Dashboard ── */}
            {showDash && (
              <Dashboard
                onOpenMap={m => {
                  // 1. Fermer le dashboard
                  setShowDash(false);
                  setCurrentMap(m);

                  try {
                    const st = typeof m.state_json === "string"
                      ? JSON.parse(m.state_json)
                      : (m.state_json || {});

                    // 2. Reset complet
                    setLayers([]);
                    setRouteLayer(null);
                    setIsoLayer(null);
                    setRouteMarkers(null);
                    setMeasurePts([]);
                    setMeasureRes(null);
                    setBufferLayer(null);
                    setDrawPts([]);
                    setDrawProfilPts([]);
                    setNotification(null);
                    setPopup(null);

                    // 3. Changer le fond de carte via state React
                    if (st.mapStyle) setMapSt(st.mapStyle);

                    // 4. Restaurer les couches SANS zoom intermédiaire (addLayerSilent)
                    //    puis calculer l'emprise globale de toutes les couches
                    const restoredLayers = (st.layers || []).filter(l => l.geojson);
                    if (restoredLayers.length) {
                      setTimeout(() => {
                        restoredLayers.forEach(l => {
                          addLayerSilent(l.geojson, l.name, l.theme || "data", {
                            color:    l.color,
                            opacity:  l.opacity,
                            radius:   l.radius,
                            classCfg: l.classCfg,
                          });
                        });
                      }, 100);
                    }

                    // 5. Zoom sur l'emprise globale des couches,
                    //    ou sur le viewport sauvegardé si pas de couches
                    const allFeatures = restoredLayers.flatMap(l => l.geojson?.features || []);
                    setPendingRestore({
                      features:  allFeatures.length ? allFeatures : null,
                      viewport:  st.viewport || null,
                      mapStyle:  st.mapStyle || null,
                    });
                  } catch(e) { console.warn("Restauration carte:", e); }
                }}
                onUpdateMap={m => {
                  // Ouvrir SaveMapModal directement sur cette carte pour la mettre à jour
                  setShowDash(false);
                  setCurrentMap(m);
                  setShowSave(true);
                }}
                onClose={() => setShowDash(false)}
              />
            )}

            {notification && (
              <div style={{
                position: "absolute", top: 60, left: "50%", transform: "translateX(-50%)",
                zIndex: 9999, maxWidth: 480, padding: "10px 16px", borderRadius: 8,
                background: notification.msgType === "info" ? "#0d2137" : "#1a1a2e",
                color: "#fff", fontSize: 13,
                border: `1px solid ${notification.msgType === "info" ? "rgba(29,158,117,0.6)" : "rgba(255,200,0,0.4)"}`,
                boxShadow: "0 4px 20px rgba(0,0,0,0.4)",
                display: "flex", alignItems: "center", gap: 10,
              }}>
                <span style={{ fontSize: 18 }}>
                  {notification.msgType === "info" ? "📅" : "⚠️"}
                </span>
                <span style={{ flex: 1 }}>{notification.message}</span>
                <button onClick={() => setNotification(null)} style={{
                  marginLeft: "auto", background: "none", border: "none",
                  color: "#aaa", cursor: "pointer", fontSize: 16, padding: "0 4px",
                }}>✕</button>
              </div>
            )}

            {layers.map(l=>{if(!l.isRaster)return null;if(l.kind==="image")return(<Source key={`${l.id}-${l.styleV||0}`} id={l.id} type="image" url={l.imageUrl} coordinates={l.coordinates}><Layer id={`${l.id}-layer`} type="raster" layout={{visibility:l.visible?"visible":"none"}} paint={{"raster-opacity":l.opacity??0.85,"raster-fade-duration":0}}/></Source>);if(l.theme==="vector")return(<Source key={l.id} id={l.id} type="vector" tiles={[l.tileUrl]} minzoom={0} maxzoom={22}><Layer id={`${l.id}-fill`} type="fill" layout={{visibility:l.visible?"visible":"none"}} filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":l.color||C.acc,"fill-opacity":l.opacity??0.3}}/><Layer id={`${l.id}-line`} type="line" layout={{visibility:l.visible?"visible":"none"}} filter={["any",["==",["geometry-type"],"LineString"],["==",["geometry-type"],"Polygon"]]} paint={{"line-color":l.color||C.acc,"line-width":1.5,"line-opacity":l.opacity??1}}/><Layer id={`${l.id}-circle`} type="circle" layout={{visibility:l.visible?"visible":"none"}} filter={["==",["geometry-type"],"Point"]} paint={{"circle-color":l.color||C.acc,"circle-radius":4,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-opacity":l.opacity??1}}/></Source>);return(<Source key={l.id} id={l.id} type="raster" tiles={[l.tileUrl]} tileSize={256}><Layer id={`${l.id}-layer`} type="raster" layout={{visibility:(l.visible&&l.id!==tlLayerId)?"visible":"none"}} paint={{"raster-opacity":l.opacity??0.85}}/></Source>);})}
            {layers.map(l=>l.visible&&!l.isRaster&&l.geojson&&!l.heatmap&&!l.extrude&&!l.cluster&&(<Source key={l.id} id={l.id} type="geojson" data={l.geojson}><Layer id={`${l.id}-fill`} type="fill" filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={getPaint(l,"fill")}/><Layer id={`${l.id}-outline`} type="line" filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={getPaint(l,"line")}/><Layer id={`${l.id}-road`} type="line" filter={["==",["geometry-type"],"LineString"]} paint={getPaint(l,"line")}/><Layer id={`${l.id}-circle`} type="circle" filter={["==",["geometry-type"],"Point"]} paint={getPaint(l,"circle")}/>{l.labels&&<Layer id={`${l.id}-label`} type="symbol" layout={{"text-field":["get",l.labelAttr||"name"],"text-size":11,"text-offset":[0,1.2],"text-anchor":"top","text-max-width":10}} paint={{"text-color":l.color,"text-halo-color":"#fff","text-halo-width":1}}/>}{l.classResult?.type==="symbol"&&(()=>{const cr=l.classResult;const map=mapRef.current?.getMap?.();if(cr.symbolMode==="image"&&cr.customImage?.id)return<Layer id={`${l.id}-icon`} type="symbol" filter={["==",["geometry-type"],"Point"]} layout={{"icon-image":cr.customImage.id,"icon-size":cr.imageSize||1,"icon-allow-overlap":true,"icon-anchor":"center"}} paint={{"icon-opacity":l.opacity}}/>;if(cr.symbolMode==="maki"&&cr.makiName&&map){const _mkSize=parseInt(cr.makiSize)||30;void makiTick;const imgId=loadMakiIcon(map,cr.makiName,cr.makiColor||"#ffffff",_mkSize);if(!imgId)return<Layer id={`${l.id}-sym-fb`} type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":5,"circle-color":cr.makiColor||l.color,"circle-opacity":l.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff"}}/>;return<Layer id={`${l.id}-sym`} type="symbol" filter={["==",["geometry-type"],"Point"]} layout={{"icon-image":imgId,"icon-size":1,"icon-allow-overlap":true,"icon-ignore-placement":true,"icon-anchor":"center"}} paint={{"icon-opacity":l.opacity}}/>;}return null;})()}</Source>))}
            {layers.map(l=>l.visible&&!l.isRaster&&l.extrude&&(<Source key={`${l.id}-3d`} id={`${l.id}-3d`} type="geojson" data={l.geojson}><Layer id={`${l.id}-extrude`} type="fill-extrusion" filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={{"fill-extrusion-color":l.classResult?.expression||l.color,"fill-extrusion-height":l.extrudeAttr?["*",["to-number",["get",l.extrudeAttr],5],l.extrudeScale||1]:["*",["to-number",["get","height"],5],l.extrudeScale||1],"fill-extrusion-base":0,"fill-extrusion-opacity":l.opacity*0.85}}/>{l.labels&&<Layer id={`${l.id}-3dlabel`} type="symbol" layout={{"text-field":["get",l.labelAttr||"name"],"text-size":10,"text-anchor":"center"}} paint={{"text-color":"#fff","text-halo-color":"#000","text-halo-width":1}}/>}</Source>))}
            {layers.map(l=>l.visible&&!l.isRaster&&l.cluster&&(<Source key={`${l.id}-cl`} id={`${l.id}-cl`} type="geojson" data={l.geojson} cluster={true} clusterMaxZoom={14} clusterRadius={l.clusterRadius||50}><Layer id={`${l.id}-clusters`} type="circle" filter={["has","point_count"]} paint={{"circle-color":["step",["get","point_count"],l.color,10,C.amb,50,C.red],"circle-radius":["step",["get","point_count"],18,10,24,50,32],"circle-opacity":0.85,"circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id={`${l.id}-cluster-count`} type="symbol" filter={["has","point_count"]} layout={{"text-field":"{point_count_abbreviated}","text-size":12}} paint={{"text-color":"#fff"}}/><Layer id={`${l.id}-unclustered`} type="circle" filter={["!",["has","point_count"]]} paint={{"circle-radius":l.radius||5,"circle-color":l.color,"circle-opacity":l.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.3}}/></Source>))}
            {layers.map(l=>l.visible&&!l.isRaster&&l.heatmap&&(<Source key={`${l.id}-hm`} id={`${l.id}-hm`} type="geojson" data={l.geojson}><Layer id={`${l.id}-heat`} type="heatmap" paint={{"heatmap-weight":l.heatmapField?["max",0,["to-number",["get",l.heatmapField],1]]:1,"heatmap-intensity":["interpolate",["linear"],["zoom"],0,1,15,l.heatmapIntensity||3],"heatmap-color":["interpolate",["linear"],["heatmap-density"],0,"rgba(0,0,0,0)",.2,C.acc,.4,C.amb,.6,"#D85A30",.8,C.red,1,"#fff"],"heatmap-radius":["interpolate",["linear"],["zoom"],0,4,15,l.heatmapRadius||30],"heatmap-opacity":l.opacity}}/></Source>))}

            {/* Graphiques par entite (camembert, barres, treemap...) */}
            {layers.map(l => l.visible && !l.isRaster && l.chartCfg && (
              <ChartLayer key={`${l.id}-chart`} layer={l} mapRef={mapRef} />
            ))}

            {/* Overlays mesure/dessin/buffer */}
            {viewshedPtGJ&&<Source id="viewshed-pt" type="geojson" data={viewshedPtGJ}><Layer id="viewshed-pt-circ" type="circle" paint={{"circle-radius":8,"circle-color":"#f59e0b","circle-stroke-width":3,"circle-stroke-color":"#fff"}}/><Layer id="viewshed-pt-lbl" type="symbol" layout={{"text-field":["get","label"],"text-size":14,"text-allow-overlap":true,"text-offset":[0,-1.4]}} paint={{"text-halo-color":"#fff","text-halo-width":1.5}}/></Source>}
            {georefGcpGJ&&<Source id="georef-gcp" type="geojson" data={georefGcpGJ}><Layer id="georef-gcp-circ" type="circle" paint={{"circle-radius":10,"circle-color":"#ff2d78","circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id="georef-gcp-lbl" type="symbol" layout={{"text-field":["get","label"],"text-size":12,"text-allow-overlap":true}} paint={{"text-color":"#fff"}}/></Source>}
            {measureGJ&&<Source id="measure" type="geojson" data={measureGJ}><Layer id="mpts" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":5,"circle-color":"#fff","circle-stroke-width":2,"circle-stroke-color":C.amb}}/><Layer id="mline" type="line" filter={["==",["geometry-type"],"LineString"]} paint={{"line-color":C.amb,"line-width":2,"line-dasharray":[4,2]}}/><Layer id="mpoly" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":C.amb,"fill-opacity":.15}}/></Source>}
            {bufferLayer&&<Source id="buffer" type="geojson" data={bufferLayer}><Layer id="bfill" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":C.pnk,"fill-opacity":.15}}/><Layer id="bline" type="line" filter={["==",["geometry-type"],"Polygon"]} paint={{"line-color":C.pnk,"line-width":2,"line-dasharray":[4,2]}}/><Layer id="bpt" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":6,"circle-color":C.pnk,"circle-stroke-width":2,"circle-stroke-color":"#fff"}}/></Source>}
            {drawGJ&&<Source id="draw" type="geojson" data={drawGJ}><Layer id="dpts" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":5,"circle-color":"#fff","circle-stroke-width":2,"circle-stroke-color":C.blu}}/><Layer id="dline" type="line" filter={["any",["==",["geometry-type"],"LineString"],["==",["geometry-type"],"Polygon"]]} paint={{"line-color":C.blu,"line-width":2}}/><Layer id="dfill" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":C.blu,"fill-opacity":.1}}/></Source>}

            {/* Route / iso layers */}
            {routeLayer&&<Source id="route" type="geojson" data={routeLayer}><Layer id="rline" type="line" filter={["==",["geometry-type"],"LineString"]} paint={{"line-color":"#378ADD","line-width":4,"line-opacity":0.85}}/><Layer id="rpts" type="circle" filter={["all",["==",["geometry-type"],"Point"],["==",["get","type"],"waypoint"]]} paint={{"circle-radius":8,"circle-color":"#378ADD","circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id="rlbl" type="symbol" filter={["all",["==",["geometry-type"],"Point"],["==",["get","type"],"waypoint"]]} layout={{"text-field":["get","label"],"text-size":11,"text-offset":[0,1.5],"text-anchor":"top"}} paint={{"text-color":"#378ADD","text-halo-color":"#fff","text-halo-width":1.5}}/></Source>}
            {isoLayer&&<Source id="isochrone" type="geojson" data={isoLayer}><Layer id="ifill" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":["step",["get","time_min"],C.acc,10,C.amb,15,"#D85A30",20,C.red],"fill-opacity":0.2}}/><Layer id="iline" type="line" filter={["==",["geometry-type"],"Polygon"]} paint={{"line-color":["step",["get","time_min"],C.acc,10,C.amb,15,"#D85A30",20,C.red],"line-width":2,"line-dasharray":[4,2]}}/><Layer id="icenter" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":8,"circle-color":C.acc,"circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id="ilbl" type="symbol" filter={["==",["geometry-type"],"Polygon"]} layout={{"text-field":["get","label"],"text-size":12,"text-anchor":"center"}} paint={{"text-color":"#333","text-halo-color":"#fff","text-halo-width":1.5}}/></Source>}
            {routeMarkers&&<Source id="rmarkers" type="geojson" data={routeMarkers}><Layer id="rmcirc" type="circle" paint={{"circle-radius":10,"circle-color":["match",["get","type"],"origin","#378ADD","dest","#E24B4A","#1D9E75"],"circle-stroke-width":3,"circle-stroke-color":"#fff"}}/><Layer id="rmlbl" type="symbol" layout={{"text-field":["get","label"],"text-size":14,"text-anchor":"center","text-allow-overlap":true}} paint={{"text-color":"#fff"}}/></Source>}
            {/* Marqueurs profil altimétrique — début vert, fin rouge, intermédiaires gris */}
            {drawProfilPts.length>0&&<Source id="profil-pts" type="geojson" data={{type:"FeatureCollection",features:drawProfilPts.map((p,i)=>({type:"Feature",geometry:{type:"Point",coordinates:p},properties:{type:i===0?"start":i===drawProfilPts.length-1?"end":"mid",label:i===0?"A":i===drawProfilPts.length-1?"B":String(i+1)}}))}}>
              <Layer id="profil-pts-circ" type="circle" paint={{"circle-radius":["match",["get","type"],"start",10,"end",10,6],"circle-color":["match",["get","type"],"start","#1D9E75","end","#E24B4A","#9a9a90"],"circle-stroke-width":2,"circle-stroke-color":"#fff","circle-opacity":0.95}}/>
              <Layer id="profil-pts-lbl" type="symbol" layout={{"text-field":["get","label"],"text-size":11,"text-anchor":"center","text-allow-overlap":true}} paint={{"text-color":"#fff","text-halo-color":"rgba(0,0,0,0.3)","text-halo-width":0.5}}/>
              {/* Ligne de tracé en pointillés entre les points */}
              {drawProfilPts.length>=2&&<><Source id="profil-line" type="geojson" data={{type:"Feature",geometry:{type:"LineString",coordinates:drawProfilPts}}}><Layer id="profil-line-layer" type="line" paint={{"line-color":"#1D9E75","line-width":2,"line-dasharray":[4,3],"line-opacity":0.7}}/></Source></>}
            </Source>}

            {/* Popup */}
            {popup&&(<Popup longitude={popup.lng} latitude={popup.lat} anchor="bottom" onClose={()=>setPopup(null)} closeButton closeOnClick={false}><div style={{fontFamily:F,padding:"2px 0",minWidth:160,maxWidth:280}}>{(()=>{const fields=getPopupFields(popup.properties);const nf=fields.find(f=>f.isName);return<><div style={{fontSize:12,fontWeight:600,color:"#222",marginBottom:4}}>{nf?nf.value:"Sans nom"}</div>{fields.filter(f=>!f.isName).map(f=><div key={f.key} style={{fontSize:11,color:"#555",padding:"1px 0"}}><span style={{color:"#888"}}>{f.key}:</span> {f.value}</div>)}</>;})()}</div></Popup>)}
          </Map>

          {/* Badge mesure */}
          {measureRes&&(
            <div style={{position:"absolute",top:10,left:"50%",transform:"translateX(-50%)",background:C.card,border:`0.5px solid ${C.bdr}`,borderRadius:8,padding:"5px 12px",fontSize:13,fontWeight:600,color:C.amb,pointerEvents:"none",whiteSpace:"nowrap",boxShadow:"0 2px 8px rgba(0,0,0,.2)"}}>
              {activeTool==="measure_dist"?"↔ ":"⬡ "}{measureRes}
              <button onClick={()=>{setMeasurePts([]);setMeasureRes(null);}} style={{marginLeft:8,background:"none",border:"none",color:C.dim,cursor:"pointer",fontSize:12,pointerEvents:"all"}}>✕</button>
            </div>
          )}

          {/* Buffer widget */}
          {activeTool==="buffer"&&(
            <div style={{position:"absolute",top:10,left:"50%",transform:"translateX(-50%)",background:C.card,border:`0.5px solid ${C.bdr}`,borderRadius:8,padding:"6px 14px",display:"flex",alignItems:"center",gap:10,boxShadow:"0 2px 8px rgba(0,0,0,.2)"}}>
              <span style={{fontSize:11,color:C.dim}}>Rayon</span>
              <input type="range" min={50} max={5000} step={50} value={bufferRadius} onChange={e=>setBufferRadius(Number(e.target.value))} style={{width:100}}/>
              <span style={{fontFamily:M,fontSize:11,color:C.txt,minWidth:44}}>{bufferRadius<1000?`${bufferRadius}m`:`${(bufferRadius/1000).toFixed(1)}km`}</span>
              {bufferLayer&&<button onClick={()=>{addLayer(bufferLayer,"Zone tampon","buffer");setBufferLayer(null);}} style={{fontFamily:F,fontSize:10,padding:"3px 8px",borderRadius:5,background:C.acc,color:"#fff",border:"none",cursor:"pointer"}}>Sauver</button>}
            </div>
          )}

          {/* PrintPanel */}
          {activeTool==="print"&&<PrintPanel mapRef={mapRef} layers={layers} viewState={vs} onClose={()=>activateItem("pointer")}/>}

          {/* FloatingPanels — un par module ouvert */}
          {[...openPanels].map((pid, idx) => {
            const lbl = ALL_ITEMS.find(i => i.id === pid)?.label || pid;
            return (
              <FloatingPanel key={pid} id={pid} title={lbl} onClose={()=>closePanel(pid)} offset={idx}>
                {renderPanelContent(pid)}
              </FloatingPanel>
            );
          })}

          {/* Précipitations : APRÈS </Map> pour se peindre sur les tuiles, AVANT la
              légende et les panneaux pour rester dessous. */}
          {terrain3D && weather !== "none" && <PrecipLayer type={weather} />}

          <Legend layers={layers}/>
          {!isMobile&&<MiniMap center={[vs.longitude,vs.latitude]} zoom={vs.zoom} mapStyle={MAP_STYLES[mapSt]}/>}

          {layers.length===0&&activeTool==="pointer"&&(
            <div style={{position:"absolute",top:"50%",left:"50%",transform:"translate(-50%,-50%)",textAlign:"center",color:C.dim,fontSize:13,pointerEvents:"none"}}>
              <div style={{fontSize:28,marginBottom:8,opacity:.3}}>🗺</div>
              <div style={{color:C.mut,fontWeight:500}}>Carte vide</div>
              <div style={{fontSize:11,marginTop:4}}>Ouvrez le Chat ou sélectionnez un outil à gauche</div>
            </div>
          )}

          {/* ── Vue planète plein écran (remplace la carte) — revient à la carte
              dès qu'on ouvre un autre module ou qu'on ajoute une donnée ── */}
          {planet3D && (
            <div style={{position:"absolute",inset:0,zIndex:40,background:"#05060a",display:"flex",flexDirection:"column"}}>
              <button onClick={()=>setPlanet3D(null)}
                style={{position:"absolute",top:10,right:10,zIndex:2,fontFamily:F,fontSize:12,fontWeight:500,padding:"6px 12px",borderRadius:8,border:`0.5px solid ${C.bdr}`,background:C.card,color:C.txt,cursor:"pointer",boxShadow:"0 2px 8px rgba(0,0,0,.35)",display:"flex",alignItems:"center",gap:5}}>
                <IcMap size={13}/> Retour à la carte
              </button>
              <SolarSystemPanel body={planet3D} onBody={setPlanet3D} />
            </div>
          )}
        </div>

        {/* ── CHAT DROIT redimensionnable ── */}
        {chatOpen&&(
          <>
            <div className="rh" onMouseDown={startChResize} style={{...rh,borderLeft:`0.5px solid ${C.bdr}`}}/>
            <div style={{width:chatWidth,flexShrink:0,background:C.card,borderLeft:`0.5px solid ${C.bdr}`,display:"flex",flexDirection:"column",overflow:"hidden"}}>
              <div style={{padding:"8px 12px",borderBottom:`0.5px solid ${C.bdr}`,display:"flex",alignItems:"center",justifyContent:"space-between",flexShrink:0}}>
                <div style={{display:"flex",alignItems:"center",gap:6}}><IcChat/><span style={{fontSize:12,fontWeight:600,color:C.txt}}>Chat IA</span></div>
                <button onClick={()=>setChatOpen(false)} style={{background:"none",border:"none",color:C.dim,cursor:"pointer",display:"flex",alignItems:"center"}}><IcX/></button>
              </div>
              <div style={{flex:1,minHeight:0,overflow:"hidden"}}>
                <ChatPanel onToolResult={handleToolResult} mapContext={mapCtx}/>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
    </ThemeContext.Provider>
  );
}
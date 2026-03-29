import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import Map, { Source, Layer, Popup, NavigationControl, ScaleControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import * as turf from "@turf/turf";

import { useTheme, ThemeContext, useThemeContext } from "./theme";
import { F, M, API, MAP_STYLES, LAYER_COLORS, EXPORT_FORMATS } from "./config";
import { buildClassification } from "./utils/classification";
import { encodePermalink, decodePermalink, importFile, computeBounds, getPopupFields } from "./utils/helpers";
import { executeSpatialOp } from "./utils/spatial";
import { computeRoute, computeIsochrone } from "./utils/routing";
import Legend from "./components/Legend";
import LayerPanel from "./components/LayerPanel";
import ChatPanel from "./components/ChatPanel";
import BottomPanel from "./components/BottomPanel";
import MiniMap from "./components/MiniMap";
import PrintPanel from "./components/PrintPanel";
import SpatialPanel from "./components/SpatialPanel";
import DBPanel from "./components/DBPanel";
import OGCPanel from "./components/OGCPanel";
import GEEPanel from "./components/GEEPanel";
import { loadMakiIcon } from "./utils/makiLoader";


import { useEffect } from "react";
import { useLocation } from "react-router-dom";

function useAnalytics() {
  const location = useLocation();

  useEffect(() => {
    window.gtag("config", "G-TV8HRDDDTN", {
      page_path: location.pathname + location.search,
    });
  }, [location]);
}

// ─── Icônes SVG — cohérentes et sémantiques ──────────────────
// Règle : une icône = une signification unique dans toute l'appli
const Ic = ({ d, d2, size = 15, sw = 1.8 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
    stroke="currentColor" strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round">
    <path d={d} />
    {d2 && <path d={d2} />}
  </svg>
);

// Navigation
const IcArrow     = () => <Ic d="M5 3l14 9-7 1-4 7z" />;               // pointer / sélection
// Mesure & dessin
const IcRulerTool = () => <Ic d="M22 11.08V12a10 10 0 11-5.93-9.14" d2="M22 4L12 14.01l-3-3" />; // distance (check = confirmer)
const IcHexagon   = () => <Ic d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z" />; // surface
const IcCircleDot = () => <Ic d="M12 2a10 10 0 100 20A10 10 0 0012 2zM12 8v4l3 3" />;             // buffer (cercle = zone)
const IcPencil    = () => <Ic d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4z" />; // dessin libre
// Routing
const IcNavigation= () => <Ic d="M3 11l19-9-9 19-2-8-8-2z" />;         // itinéraire (flèche de navigation)
const IcRadar     = () => <Ic d="M22 12h-4M6 12H2M12 6V2M12 22v-4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M19.07 4.93l-2.83 2.83M7.76 16.24l-2.83 2.83" />; // isochrone (rayonnement)
// Données & vues
const IcStack     = () => <Ic d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />;   // couches (layers)
const IcBarChart  = () => <Ic d="M18 20V10M12 20V4M6 20v-6" />;         // statistiques
const IcArrowDown = () => <Ic d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3" />; // export/télécharger
// Analyse
const IcVenn      = () => <Ic d="M9 3a6 6 0 100 18A6 6 0 009 3zm6 0a6 6 0 100 18A6 6 0 0015 3z" />; // analyse spatiale (intersection)
const IcDatabase  = () => <Ic d="M12 2C7 2 3 4 3 6v12c0 2 4 4 9 4s9-2 9-4V6c0-2-4-4-9-4z" d2="M3 6c0 2 4 4 9 4s9-2 9-4M3 12c0 2 4 4 9 4s9-2 9-4" />; // base de données
const IcSatellite = () => <Ic d="M12 12m-1 0a1 1 0 102 0 1 1 0 10-2 0M4.5 4.5c4.5-4.5 11.5-4.5 16 0M7.5 7.5c3-3 7-3 9 0M15 12a3 3 0 11-6 0 3 3 0 016 0" />; // GEE / satellite
const IcServer    = () => <Ic d="M2 6a2 2 0 012-2h16a2 2 0 012 2v2a2 2 0 01-2 2H4a2 2 0 01-2-2V6zM2 14a2 2 0 012-2h16a2 2 0 012 2v2a2 2 0 01-2 2H4a2 2 0 01-2-2v-2z" />; // OGC / services web
// Actions
const IcPrint     = () => <Ic d="M6 9V2h12v7M6 18H4a2 2 0 01-2-2v-5a2 2 0 012-2h16a2 2 0 012 2v5a2 2 0 01-2 2h-2M6 14h12v8H6z" />;
const IcUpload    = () => <Ic d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M17 8l-5-5-5 5M12 3v12" />;
const IcShare     = () => <Ic d="M4 12v8a2 2 0 002 2h12a2 2 0 002-2v-8M16 6l-4-4-4 4M12 2v13" />;
const IcSun       = () => <Ic d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42M12 6a6 6 0 100 12A6 6 0 0012 6z" />;
const IcMoon      = () => <Ic d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z" />;
const IcChat      = () => <Ic d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z" />;
const IcX         = () => <Ic d="M18 6L6 18M6 6l12 12" />;

// ─── Configuration du rail — groupes logiques, icônes uniques ─
const RAIL_GROUPS = [
  {
    id: "nav",
    items: [{ id: "pointer", label: "Sélection", Icon: IcArrow, hasPanel: false }],
  },
  {
    id: "measure",
    items: [
      { id: "measure_dist", label: "Mesure distance",  Icon: IcRulerTool, hasPanel: false },
      { id: "measure_area", label: "Mesure surface",   Icon: IcHexagon,   hasPanel: false },
      { id: "buffer",       label: "Zone tampon",      Icon: IcCircleDot, hasPanel: false },
      { id: "draw",         label: "Dessin libre",     Icon: IcPencil,    hasPanel: false },
    ],
  },
  {
    id: "routing",
    items: [
      { id: "route",     label: "Itinéraire", Icon: IcNavigation, hasPanel: true },
      { id: "isochrone", label: "Isochrone",  Icon: IcRadar,      hasPanel: true },
    ],
  },
  {
    id: "view",
    items: [
      { id: "layers", label: "Couches",       Icon: IcStack,   hasPanel: true },
      { id: "stats",  label: "Statistiques",  Icon: IcBarChart, hasPanel: true },
      { id: "export", label: "Export",        Icon: IcArrowDown, hasPanel: true },
    ],
  },
  {
    id: "data",
    items: [
      { id: "spatial",  label: "Analyse spatiale", Icon: IcVenn,      hasPanel: true },
      { id: "database", label: "Base de données",  Icon: IcDatabase,  hasPanel: true },
      { id: "gee",      label: "Google Earth Eng", Icon: IcSatellite, hasPanel: true },
      { id: "ogc",      label: "Services OGC",     Icon: IcServer,    hasPanel: true },
    ],
  },
  {
    id: "misc",
    items: [
      { id: "print", label: "Exporter carte", Icon: IcPrint, hasPanel: false },
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
  export:    { w: 280, h: "auto" },
  spatial:   { w: 520, h: 480 },
  database:  { w: 380, h: 480 },
  gee:       { w: 360, h: 500 },
  ogc:       { w: 360, h: 480 },
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
    <div ref={panelRef} style={{
      position: "fixed",
      ...(pos.x !== null ? { left: pos.x, top: pos.y } : { top: "50%", left: "50%", transform: "translate(-50%,-50%)" }),
      width: size.w, height: size.h, zIndex: 300,
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
        <div style={{ fontSize: 12, fontWeight: 600, color: C.acc, display: "flex", alignItems: "center", gap: 6, userSelect: "none" }}>
          <span style={{ fontSize: 11, color: C.dim, letterSpacing: 2 }}>⠿</span>
          {title}
        </div>
        <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", fontSize: 16 }}>✕</button>
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
export default function App() {
  const { name: themeName, C, toggle: toggleTheme } = useTheme();

  // ── Map ───────────────────────────────────────────────────
  const [layers, setLayers] = useState([]);
  const [mapSt,  setMapSt]  = useState("positron");
  const [vs,     setVs]     = useState({ longitude: -1.55, latitude: 47.22, zoom: 12, pitch: 0, bearing: 0 });
  const [popup,  setPopup]  = useState(null);

  // ── Sidebar gauche ────────────────────────────────────────
  const [activeTool,   setActiveTool]   = useState("pointer");
  const [sidebarOpen,  setSidebarOpen]  = useState(false); // conservé pour compat (tools sans panel)
  const [openPanels,   setOpenPanels]   = useState(new Set()); // ids des panneaux ouverts
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
  const mapRef  = useRef(null);
  const fileRef = useRef(null);
  const lctr    = useRef(0);

  // ── Changement de style carte via API MapLibre ────────────
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    const style = MAP_STYLES[mapSt];
    try { map.setStyle(style); } catch (_) {}
  }, [mapSt]);

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
  }, []);

  const closePanel = useCallback((id) => {
    setOpenPanels(prev => { const next = new Set(prev); next.delete(id); return next; });
  }, []);

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
    const ci = lctr.current % LAYER_COLORS.length; lctr.current++;
    setLayers(p => [...p, {
      id:info.id, name:info.name, theme:info.type||"wms", isRaster:true,
      tileUrl:info.tileUrl, geojson:null, visible:true,
      color:LAYER_COLORS[ci], opacity:info.opacity??0.85,
      featureCount:"raster", classCfg:null, classResult:null,
      bbox:     info.bbox      || null,
      visParams:  info.visParams  || null,  // palette/min/max GEE → RasterStylePanel
      _geeParams: info.geeParams  || null,  // params pour restyle sans rechargement
    }]);
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
      const feats = l.geojson?.features || [];
      const geomTypes = [...new Set(feats.map(f => f.geometry?.type).filter(Boolean))];
      const b = computeBounds(feats);
      const bbox = l.geojson?.metadata?.bbox || (b ? [b[0][0],b[0][1],b[1][0],b[1][1]] : null);
      return { id:l.id, name:l.name, featureCount:l.featureCount, visible:l.visible, theme:l.theme, geomTypes, bbox };
    }),
    center: [vs.longitude, vs.latitude], zoom: vs.zoom,
  }), [layers, vs]);

  const fitFeatures = useCallback((feats) => {
    const b = computeBounds(feats); if (!b) return;
    const m = mapRef.current?.getMap?.();
    if (m) setTimeout(() => m.fitBounds(b, {
      padding: { top:60, bottom:60, left: 66, right: chatOpen ? chatWidth+12 : 60 },
      maxZoom:17, duration:1200,
    }), 100);
  }, [sidebarOpen, sidebarWidth, chatOpen, chatWidth]);

  const addLayer = useCallback((geojson, name, theme = "data") => {
    const ci = lctr.current % LAYER_COLORS.length;
    const lid = `layer_${Date.now()}_${lctr.current++}`;
    setLayers(p => [...p, { id:lid, name, theme, geojson, visible:true, color:LAYER_COLORS[ci], opacity:0.8, radius:6, featureCount:geojson.features?.length||0, classCfg:null, classResult:null, heatmap:false, extrude:false, extrudeAttr:"", extrudeScale:1, cluster:false, labels:false, labelAttr:"name" }]);
    if (geojson.features?.length) fitFeatures(geojson.features);
  }, [fitFeatures]);

  const layersRef = useRef(layers);
  useEffect(() => { layersRef.current = layers; }, [layers]);

  const clipToPolygonLayer = useCallback((gj, polyLayer) => {
    const polys = (polyLayer.geojson?.features || []).filter(f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon");
    if (!polys.length) return gj;
    const mask = polys[0];
    const clipped = (gj.features || []).filter(f => {
      try {
        if (f.geometry?.type === "Point") return turf.booleanPointInPolygon(f, mask);
        if (f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon") return !!turf.intersect(turf.featureCollection([f, mask]));
        return true;
      } catch { return false; }
    });
    return { ...gj, features: clipped, metadata: { ...gj.metadata, clipped: true, clip_layer: polyLayer.name } };
  }, []);

  const handleToolResult = useCallback((action) => {
    if (action.type === "add_layer") {
      let gj = action.data;
      const params = gj.metadata?.query_params || {};
      const theme = gj.metadata?.theme || "data";
      const cur = layersRef.current;
      const polyL = cur.find(l => l.visible && (l.geojson?.features||[]).some(f => f.geometry?.type==="Polygon"||f.geometry?.type==="MultiPolygon"));
      const hasPoints = gj.features?.some(f => f.geometry?.type === "Point");
      if (polyL && hasPoints) {
        const pb = turf.bbox(polyL.geojson);
        const qb = gj.metadata?.bbox;
        const ok = qb && qb[0]<=pb[0] && qb[1]<=pb[1] && qb[2]>=pb[2] && qb[3]>=pb[3];
        if (!ok) {
          const pad = 0.005;
          fetch(`${API}/query?theme=${params.theme||theme}&xmin=${pb[0]-pad}&ymin=${pb[1]-pad}&xmax=${pb[2]+pad}&ymax=${pb[3]+pad}&limit=1000${params.category?`&category=${params.category}`:""}`)
            .then(r=>r.json()).then(fg => { addLayer(clipToPolygonLayer(fg.features?.length?fg:gj, polyL), params.category?`${params.category} (clipped)`:`${theme} (clipped)`, theme); })
            .catch(() => addLayer(clipToPolygonLayer(gj, polyL), params.category?`${params.category} (${theme})`:theme, theme));
          return;
        }
        gj = clipToPolygonLayer(gj, polyL);
      }
      addLayer(gj, params.category ? `${params.category} (${theme})` : theme, theme);
    } else if (action.type === "fly_to") {
      mapRef.current?.getMap?.()?.flyTo({ center:[action.longitude,action.latitude], zoom:action.zoom||14, pitch:action.pitch||0, duration:1500 });
    } else if (action.type === "remove_layer") {
      if (action.layer_id === "all") setLayers([]); else setLayers(p => p.filter(l => l.id !== action.layer_id));
    } else if (action.type === "spatial_analysis") {
      try {
        const lA = layers.find(l => l.name === action.layer_a_name);
        const lB = action.layer_b_name ? layers.find(l => l.name === action.layer_b_name) : null;
        if (!lA) return;
        const r = executeSpatialOp(action.operation, lA, lB, action.params || {});
        if (r?.features?.length) addLayer(r, action.result_name || `${action.operation}_result`, "analysis");
      } catch (e) { console.error(e); }
    } else if (action.type === "compute_route") {
      computeRoute(action.waypoints, action.profile || "foot")
        .then(gj => { setRouteLayer(gj); if (gj.features?.length) fitFeatures(gj.features); addLayer(gj, `Route ${action.profile||"foot"}`, "route"); })
        .catch(console.error);
    } else if (action.type === "compute_isochrone") {
      computeIsochrone(action.center, action.time_minutes || 10, action.profile || "foot")
        .then(gj => { setIsoLayer(gj); if (gj.features?.length) fitFeatures(gj.features); addLayer(gj, `Isochrone ${action.time_minutes||10}min`, "isochrone"); })
        .catch(console.error);
    }
  }, [addLayer, layers, fitFeatures, clipToPolygonLayer]);

  // Layer ops
  const toggleL = id => setLayers(p => p.map(l => {
    if (l.id !== id) return l; const nv = !l.visible;
    if (l.isRaster) { try { const map = mapRef.current?.getMap?.(); if (map) [`${id}-layer`,`${id}-fill`,`${id}-line`,`${id}-circle`].forEach(lid => { if (map.getLayer(lid)) map.setLayoutProperty(lid, "visibility", nv?"visible":"none"); }); } catch(_){} }
    return { ...l, visible: nv };
  }));
  const removeL = id => {
    const l = layers.find(x => x.id === id);
    if (l?.isRaster) { try { const map = mapRef.current?.getMap?.(); if (map) { [`${id}-layer`,`${id}-fill`,`${id}-line`,`${id}-circle`].forEach(lid => { if (map.getLayer(lid)) map.removeLayer(lid); }); if (map.getSource(id)) map.removeSource(id); } } catch(_){} }
    setLayers(p => p.filter(x => x.id !== id));
  };
  const styleL = (id, s) => setLayers(p => p.map(l => {
    if (l.id !== id) return l;
    if (l.isRaster && s.opacity !== undefined) { try { const map = mapRef.current?.getMap?.(); if (map) { if (map.getLayer(`${id}-layer`)) map.setPaintProperty(`${id}-layer`,"raster-opacity",s.opacity); if (map.getLayer(`${id}-fill`)) map.setPaintProperty(`${id}-fill`,"fill-opacity",s.opacity); if (map.getLayer(`${id}-line`)) map.setPaintProperty(`${id}-line`,"line-opacity",s.opacity); if (map.getLayer(`${id}-circle`)) map.setPaintProperty(`${id}-circle`,"circle-opacity",s.opacity); } } catch(_){} }
    return { ...l, ...s };
  }));
  const renameL   = (id, name) => setLayers(p => p.map(l => l.id === id ? {...l, name} : l));
  const classifyL = useCallback((id, cfg) => {
    setLayers(p => p.map(l => { if (l.id !== id) return l; const r = cfg ? buildClassification(l, cfg) : null; return {...l, classCfg:cfg, classResult:r}; }));
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
    try { const gj = await importFile(file); if (gj?.features?.length) addLayer(gj, file.name.replace(/\.[^.]+$/,""), "import"); else alert("Fichier vide ou format non reconnu."); }
    catch (e) { alert("Erreur import: " + e.message); }
  }, [addLayer]);

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
    if (gt==="circle") { if (cr?.type==="symbol") return {"circle-radius":0,"circle-opacity":0}; if (cr?.type==="proportional"&&cr.radiusExpression) return {"circle-radius":cr.radiusExpression,"circle-color":layer.color,"circle-opacity":layer.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.4}; return {"circle-radius":layer.radius||5,"circle-color":ce,"circle-opacity":layer.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.3}; }
    return {};
  }, []);

  // Map click — gère aussi la sélection de point pour route/iso
  const handleMapClick = useCallback((e) => {
    const lng = e.lngLat.lng, lat = e.lngLat.lat;
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
    else { if (!e.features?.length){setPopup(null);return;} const f=e.features[0]; setPopup({lng,lat,properties:f.properties,layerName:f.layer?.id||""}); }
  }, [activeTool, measurePts, bufferRadius, routePickMode, routeMarkers]);

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
      addLayer(gj, `Isochrone ${isoTime}min ${routeProfile}`, "isochrone");
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
                <button onClick={()=>addLayer(isoLayer,`Isochrone ${isoTime}min`,"isochrone")} style={{fontFamily:F,flex:1,fontSize:10,padding:"5px 0",borderRadius:6,border:`0.5px solid ${C.acc}55`,background:C.acc+"18",color:C.acc,cursor:"pointer"}}>Ajouter couche</button>
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
        />
      </Embed>
    );

    // ── Statistiques ────────────────────────────────────────
    if (activeTool === "stats") return (
      <Embed>
        <BottomPanel layers={layers.filter(l=>l.visible)} activeTab={null} onTab={()=>{}} onZoom={zoomFeat} onAddLayer={addLayer} />
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
    if (activeTool === "spatial") return (
      <Embed>
        <SpatialPanel layers={layers.filter(l=>l.visible&&!l.isRaster)} onAddLayer={addLayer} />
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

    // ── Services OGC ────────────────────────────────────────
    if (activeTool === "ogc") return (
      <Embed>
        <OGCPanel mapRef={mapRef} onAddLayer={addLayer} onAddRasterLayer={addRasterLayer} />
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
      style={{ fontFamily:F, background:C.bg, color:C.txt, height:"100vh", display:"flex", flexDirection:"column", overflow:"hidden" }}
      onDragOver={e=>{e.preventDefault();setDragOver(true);}}
      onDragLeave={e=>{e.preventDefault();setDragOver(false);}}
      onDrop={e=>{e.preventDefault();setDragOver(false);if(e.dataTransfer?.files?.[0])doImport(e.dataTransfer.files[0]);}}>

      <style>{`
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
        .rh:hover{background:${C.acc}50 !important}
        .rib:hover{background:${C.hover} !important}
        ::-webkit-scrollbar{width:4px;height:4px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:${C.bdr};border-radius:4px}
        ::-webkit-scrollbar-thumb:hover{background:${C.mut}}
      `}</style>

      <input ref={fileRef} type="file" accept=".geojson,.json,.csv,.tsv,.zip,.shp" style={{display:"none"}}
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
          {isMobile&&<>
            <button className="rib" onClick={toggleTheme} style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
              {themeName==="dark"?<IcSun/>:<IcMoon/>}
            </button>
            <button className="rib" onClick={()=>setChatOpen(o=>!o)}
              style={{fontFamily:F,fontSize:11,padding:"5px 10px",borderRadius:6,border:`0.5px solid ${chatOpen?C.acc+"44":C.bdr}`,background:chatOpen?C.acc+"18":"transparent",color:chatOpen?C.acc:C.mut,cursor:"pointer",display:"flex",alignItems:"center",gap:5}}>
              <IcChat/> Chat
            </button>
          </>}
        </div>

        {/* ── Centre : styles carte — desktop uniquement ── */}
        {!isMobile&&<div style={{display:"flex",gap:3}}>
          {Object.keys(MAP_STYLES).map(k=>(
            <button key={k} onClick={()=>setMapSt(k)} className="rib"
              style={{fontFamily:F,fontSize:10,padding:"3px 9px",borderRadius:5,border:`0.5px solid ${mapSt===k?C.acc+"55":C.bdr}`,background:mapSt===k?C.acc+"15":"transparent",color:mapSt===k?C.acc:C.dim,cursor:"pointer"}}>
              {k.charAt(0).toUpperCase() + k.slice(1)}
            </button>
          ))}
        </div>}

        {/* ── Droite : actions ── */}
        <div style={{display:"flex",alignItems:"center",gap:4}}>
          {!isMobile&&<div style={{fontSize:10,color:C.dim,padding:"2px 8px",borderRadius:5,background:C.hover,border:`0.5px solid ${C.bdr}`,display:"flex",alignItems:"center",gap:4,flexShrink:0}}>
            <span style={{width:5,height:5,borderRadius:"50%",background:C.acc,display:"inline-block"}}/>DuckDB
          </div>}
          {!isMobile&&<button className="rib" onClick={toggleTheme} style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
            {themeName==="dark"?<IcSun/>:<IcMoon/>}
          </button>}
          <button className="rib" onClick={()=>fileRef.current?.click()} style={{background:"transparent",border:`0.5px solid ${C.bdr}`,borderRadius:6,color:C.mut,cursor:"pointer",padding:"5px 7px",display:"flex",alignItems:"center"}}>
            <IcUpload/>
          </button>
          {!isMobile&&<button className="rib" onClick={()=>setChatOpen(o=>!o)}
            style={{fontFamily:F,fontSize:11,padding:"5px 10px",borderRadius:6,border:`0.5px solid ${chatOpen?C.acc+"44":C.bdr}`,background:chatOpen?C.acc+"18":"transparent",color:chatOpen?C.acc:C.mut,cursor:"pointer",display:"flex",alignItems:"center",gap:5}}>
            <IcChat/> Chat
          </button>}
          {layers.length>0&&(
            <button className="rib" onClick={()=>activateItem("layers")}
              style={{fontFamily:F,fontSize:11,padding:"5px 8px",borderRadius:6,border:`0.5px solid ${openPanels.has("layers")?C.acc+"44":C.bdr}`,background:openPanels.has("layers")?C.acc+"18":"transparent",color:openPanels.has("layers")?C.acc:C.mut,cursor:"pointer",display:"flex",alignItems:"center",gap:5,position:"relative"}}>
              <IcStack/>
              <span style={{background:C.acc,color:"#fff",borderRadius:8,fontSize:9,padding:"0 5px",fontWeight:600,minWidth:14,textAlign:"center"}}>{layers.length}</span>
            </button>
          )}
        </div>
      </header>

      {/* ══════ BODY ══════ */}
      <div style={{flex:1,display:"flex",overflow:"hidden"}}>

        {/* ── RAIL GAUCHE 46px ── */}
        <div style={{width:46,background:C.card,borderRight:`0.5px solid ${C.bdr}`,display:"flex",flexDirection:"column",alignItems:"center",padding:"6px 0",flexShrink:0}}>
          {RAIL_GROUPS.map((group, gi) => (
            <div key={group.id} style={{width:"100%",display:"flex",flexDirection:"column",alignItems:"center",gap:1,padding:"4px 0",borderBottom:gi<RAIL_GROUPS.length-1?`0.5px solid ${C.bdr}`:"none"}}>
              {group.items.map(({id, label, Icon: Ic2}) => {
                const isPanelActive = PANEL_IDS.has(id) && openPanels.has(id);
                const isActive      = activeTool===id;
                return (
                  <button key={id} className="rib" title={label} onClick={()=>activateItem(id)}
                    style={{width:34,height:32,borderRadius:7,border:`0.5px solid ${isPanelActive?C.acc+"44":"transparent"}`,display:"flex",alignItems:"center",justifyContent:"center",background:isPanelActive?C.acc+"20":isActive&&!PANEL_IDS.has(id)?C.acc+"15":"transparent",color:isActive?C.acc:C.mut,cursor:"pointer",padding:0,transition:"all .12s"}}>
                    <Ic2/>
                  </button>
                );
              })}
            </div>
          ))}
          <div style={{marginTop:"auto",width:"100%",display:"flex",flexDirection:"column",alignItems:"center",padding:"5px 0",borderTop:`0.5px solid ${C.bdr}`}}>
            <button className="rib" title="Importer un fichier" onClick={()=>fileRef.current?.click()}
              style={{width:34,height:32,borderRadius:7,border:"none",background:"transparent",color:C.dim,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center"}}>
              <IcUpload/>
            </button>
          </div>
        </div>

        {/* Sidebar étendue retirée — modules dans FloatingPanels sur la carte */}

        {/* ── CARTE ── */}
        <div style={{flex:1,position:"relative",minWidth:0}}>
          <Map ref={mapRef} {...vs} onMove={e=>setVs(e.viewState)}
            style={{width:"100%",height:"100%"}} mapStyle={typeof MAP_STYLES[mapSt]==="string"?MAP_STYLES[mapSt]:MAP_STYLES["positron"]}
            maplibreLogo={false} attributionControl={false} preserveDrawingBuffer={true}
            onClick={handleMapClick} interactiveLayerIds={activeTool==="pointer"?intIds:[]}
            cursor={(activeTool!=="pointer"||routePickMode)?"crosshair":"grab"}
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
            {layers.map(l=>{if(!l.isRaster)return null;if(l.theme==="vector")return(<Source key={l.id} id={l.id} type="vector" tiles={[l.tileUrl]} minzoom={0} maxzoom={22}><Layer id={`${l.id}-fill`} type="fill" layout={{visibility:l.visible?"visible":"none"}} filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":l.color||C.acc,"fill-opacity":l.opacity??0.3}}/><Layer id={`${l.id}-line`} type="line" layout={{visibility:l.visible?"visible":"none"}} filter={["any",["==",["geometry-type"],"LineString"],["==",["geometry-type"],"Polygon"]]} paint={{"line-color":l.color||C.acc,"line-width":1.5,"line-opacity":l.opacity??1}}/><Layer id={`${l.id}-circle`} type="circle" layout={{visibility:l.visible?"visible":"none"}} filter={["==",["geometry-type"],"Point"]} paint={{"circle-color":l.color||C.acc,"circle-radius":4,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-opacity":l.opacity??1}}/></Source>);return(<Source key={l.id} id={l.id} type="raster" tiles={[l.tileUrl]} tileSize={256}><Layer id={`${l.id}-layer`} type="raster" layout={{visibility:l.visible?"visible":"none"}} paint={{"raster-opacity":l.opacity??0.85}}/></Source>);})}
            {layers.map(l=>l.visible&&!l.isRaster&&!l.heatmap&&!l.extrude&&!l.cluster&&(<Source key={l.id} id={l.id} type="geojson" data={l.geojson}><Layer id={`${l.id}-fill`} type="fill" filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={getPaint(l,"fill")}/><Layer id={`${l.id}-outline`} type="line" filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={getPaint(l,"line")}/><Layer id={`${l.id}-road`} type="line" filter={["==",["geometry-type"],"LineString"]} paint={getPaint(l,"line")}/><Layer id={`${l.id}-circle`} type="circle" filter={["==",["geometry-type"],"Point"]} paint={getPaint(l,"circle")}/>{l.labels&&<Layer id={`${l.id}-label`} type="symbol" layout={{"text-field":["get",l.labelAttr||"name"],"text-size":11,"text-offset":[0,1.2],"text-anchor":"top","text-max-width":10}} paint={{"text-color":l.color,"text-halo-color":"#fff","text-halo-width":1}}/>}{l.classResult?.type==="symbol"&&(()=>{const cr=l.classResult;const map=mapRef.current?.getMap?.();if(cr.symbolMode==="image"&&cr.customImage?.id)return<Layer id={`${l.id}-icon`} type="symbol" filter={["==",["geometry-type"],"Point"]} layout={{"icon-image":cr.customImage.id,"icon-size":cr.imageSize||1,"icon-allow-overlap":true,"icon-anchor":"center"}} paint={{"icon-opacity":l.opacity}}/>;if(cr.symbolMode==="maki"&&cr.makiName&&map){const imgId=loadMakiIcon(map,cr.makiName,cr.makiColor||"#ffffff",cr.makiSize||30);if(!imgId)return<Layer id={`${l.id}-sym-fb`} type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":5,"circle-color":cr.makiColor||l.color,"circle-opacity":l.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff"}}/>;return<Layer id={`${l.id}-sym`} type="symbol" filter={["==",["geometry-type"],"Point"]} layout={{"icon-image":imgId,"icon-size":1,"icon-allow-overlap":true,"icon-ignore-placement":true,"icon-anchor":"center"}} paint={{"icon-opacity":l.opacity}}/>;}return null;})()}</Source>))}
            {layers.map(l=>l.visible&&!l.isRaster&&l.extrude&&(<Source key={`${l.id}-3d`} id={`${l.id}-3d`} type="geojson" data={l.geojson}><Layer id={`${l.id}-extrude`} type="fill-extrusion" filter={["any",["==",["geometry-type"],"Polygon"],["==",["geometry-type"],"MultiPolygon"]]} paint={{"fill-extrusion-color":l.classResult?.expression||l.color,"fill-extrusion-height":l.extrudeAttr?["*",["to-number",["get",l.extrudeAttr],5],l.extrudeScale||1]:["*",["to-number",["get","height"],5],l.extrudeScale||1],"fill-extrusion-base":0,"fill-extrusion-opacity":l.opacity*0.85}}/>{l.labels&&<Layer id={`${l.id}-3dlabel`} type="symbol" layout={{"text-field":["get",l.labelAttr||"name"],"text-size":10,"text-anchor":"center"}} paint={{"text-color":"#fff","text-halo-color":"#000","text-halo-width":1}}/>}</Source>))}
            {layers.map(l=>l.visible&&!l.isRaster&&l.cluster&&(<Source key={`${l.id}-cl`} id={`${l.id}-cl`} type="geojson" data={l.geojson} cluster={true} clusterMaxZoom={14} clusterRadius={50}><Layer id={`${l.id}-clusters`} type="circle" filter={["has","point_count"]} paint={{"circle-color":["step",["get","point_count"],l.color,10,C.amb,50,C.red],"circle-radius":["step",["get","point_count"],18,10,24,50,32],"circle-opacity":0.85,"circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id={`${l.id}-cluster-count`} type="symbol" filter={["has","point_count"]} layout={{"text-field":"{point_count_abbreviated}","text-size":12}} paint={{"text-color":"#fff"}}/><Layer id={`${l.id}-unclustered`} type="circle" filter={["!",["has","point_count"]]} paint={{"circle-radius":l.radius||5,"circle-color":l.color,"circle-opacity":l.opacity,"circle-stroke-width":1,"circle-stroke-color":"#fff","circle-stroke-opacity":0.3}}/></Source>))}
            {layers.map(l=>l.visible&&!l.isRaster&&l.heatmap&&(<Source key={`${l.id}-hm`} id={`${l.id}-hm`} type="geojson" data={l.geojson}><Layer id={`${l.id}-heat`} type="heatmap" paint={{"heatmap-weight":1,"heatmap-intensity":["interpolate",["linear"],["zoom"],0,1,15,3],"heatmap-color":["interpolate",["linear"],["heatmap-density"],0,"rgba(0,0,0,0)",.2,C.acc,.4,C.amb,.6,"#D85A30",.8,C.red,1,"#fff"],"heatmap-radius":["interpolate",["linear"],["zoom"],0,4,15,30],"heatmap-opacity":l.opacity}}/></Source>))}

            {/* Overlays mesure/dessin/buffer */}
            {measureGJ&&<Source id="measure" type="geojson" data={measureGJ}><Layer id="mpts" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":5,"circle-color":"#fff","circle-stroke-width":2,"circle-stroke-color":C.amb}}/><Layer id="mline" type="line" filter={["==",["geometry-type"],"LineString"]} paint={{"line-color":C.amb,"line-width":2,"line-dasharray":[4,2]}}/><Layer id="mpoly" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":C.amb,"fill-opacity":.15}}/></Source>}
            {bufferLayer&&<Source id="buffer" type="geojson" data={bufferLayer}><Layer id="bfill" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":C.pnk,"fill-opacity":.15}}/><Layer id="bline" type="line" filter={["==",["geometry-type"],"Polygon"]} paint={{"line-color":C.pnk,"line-width":2,"line-dasharray":[4,2]}}/><Layer id="bpt" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":6,"circle-color":C.pnk,"circle-stroke-width":2,"circle-stroke-color":"#fff"}}/></Source>}
            {drawGJ&&<Source id="draw" type="geojson" data={drawGJ}><Layer id="dpts" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":5,"circle-color":"#fff","circle-stroke-width":2,"circle-stroke-color":C.blu}}/><Layer id="dline" type="line" filter={["any",["==",["geometry-type"],"LineString"],["==",["geometry-type"],"Polygon"]]} paint={{"line-color":C.blu,"line-width":2}}/><Layer id="dfill" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":C.blu,"fill-opacity":.1}}/></Source>}

            {/* Route / iso layers */}
            {routeLayer&&<Source id="route" type="geojson" data={routeLayer}><Layer id="rline" type="line" filter={["==",["geometry-type"],"LineString"]} paint={{"line-color":"#378ADD","line-width":4,"line-opacity":0.85}}/><Layer id="rpts" type="circle" filter={["all",["==",["geometry-type"],"Point"],["==",["get","type"],"waypoint"]]} paint={{"circle-radius":8,"circle-color":"#378ADD","circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id="rlbl" type="symbol" filter={["all",["==",["geometry-type"],"Point"],["==",["get","type"],"waypoint"]]} layout={{"text-field":["get","label"],"text-size":11,"text-offset":[0,1.5],"text-anchor":"top"}} paint={{"text-color":"#378ADD","text-halo-color":"#fff","text-halo-width":1.5}}/></Source>}
            {isoLayer&&<Source id="isochrone" type="geojson" data={isoLayer}><Layer id="ifill" type="fill" filter={["==",["geometry-type"],"Polygon"]} paint={{"fill-color":["step",["get","time_min"],C.acc,10,C.amb,15,"#D85A30",20,C.red],"fill-opacity":0.2}}/><Layer id="iline" type="line" filter={["==",["geometry-type"],"Polygon"]} paint={{"line-color":["step",["get","time_min"],C.acc,10,C.amb,15,"#D85A30",20,C.red],"line-width":2,"line-dasharray":[4,2]}}/><Layer id="icenter" type="circle" filter={["==",["geometry-type"],"Point"]} paint={{"circle-radius":8,"circle-color":C.acc,"circle-stroke-width":2,"circle-stroke-color":"#fff"}}/><Layer id="ilbl" type="symbol" filter={["==",["geometry-type"],"Polygon"]} layout={{"text-field":["get","label"],"text-size":12,"text-anchor":"center"}} paint={{"text-color":"#333","text-halo-color":"#fff","text-halo-width":1.5}}/></Source>}
            {routeMarkers&&<Source id="rmarkers" type="geojson" data={routeMarkers}><Layer id="rmcirc" type="circle" paint={{"circle-radius":10,"circle-color":["match",["get","type"],"origin","#378ADD","dest","#E24B4A","#1D9E75"],"circle-stroke-width":3,"circle-stroke-color":"#fff"}}/><Layer id="rmlbl" type="symbol" layout={{"text-field":["get","label"],"text-size":14,"text-anchor":"center","text-allow-overlap":true}} paint={{"text-color":"#fff"}}/></Source>}

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

          <Legend layers={layers}/>
          {!isMobile&&<MiniMap center={[vs.longitude,vs.latitude]} zoom={vs.zoom} mapStyle={MAP_STYLES[mapSt]}/>}

          {layers.length===0&&activeTool==="pointer"&&(
            <div style={{position:"absolute",top:"50%",left:"50%",transform:"translate(-50%,-50%)",textAlign:"center",color:C.dim,fontSize:13,pointerEvents:"none"}}>
              <div style={{fontSize:28,marginBottom:8,opacity:.3}}>🗺</div>
              <div style={{color:C.mut,fontWeight:500}}>Carte vide</div>
              <div style={{fontSize:11,marginTop:4}}>Ouvrez le Chat ou sélectionnez un outil à gauche</div>
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

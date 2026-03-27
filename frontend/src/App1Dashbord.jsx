import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import * as d3 from "d3";
import Map, { Source, Layer, Popup, NavigationControl, ScaleControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";

const MAP_STYLES = {
  dark: "https://tiles.openfreemap.org/styles/dark",
  liberty: "https://tiles.openfreemap.org/styles/liberty",
  bright: "https://tiles.openfreemap.org/styles/bright",
  positron: "https://tiles.openfreemap.org/styles/positron",
};

const OVERTURE_RELEASE = "2026-03-18.0";
const S3_BASE = `s3://overturemaps-us-west-2/release/${OVERTURE_RELEASE}`;

const THEMES = {
  places: { label: "Places", icon: "◉", color: "#EF9F27", types: ["place"], desc: "POI, commerces, services" },
  buildings: { label: "Buildings", icon: "⬒", color: "#378ADD", types: ["building"], desc: "Emprises bâtiments + hauteurs" },
  transportation: { label: "Transport", icon: "═", color: "#1D9E75", types: ["segment", "connector"], desc: "Réseau routier" },
  divisions: { label: "Divisions", icon: "◫", color: "#D4537E", types: ["division", "division_area"], desc: "Limites admin" },
  base: { label: "Base", icon: "▤", color: "#639922", types: ["land", "land_cover", "water"], desc: "Occupation du sol" },
  addresses: { label: "Addresses", icon: "⌂", color: "#D85A30", types: ["address"], desc: "Adresses géocodées" },
};

const BBOX_PRESETS = {
  "Nantes Métropole": { bbox: [-1.72, 47.15, -1.42, 47.32], center: [-1.55, 47.22], zoom: 12 },
  "Loire-Atlantique": { bbox: [-2.56, 46.86, -0.92, 47.84], center: [-1.74, 47.35], zoom: 9 },
  "Dakar": { bbox: [-17.55, 14.63, -17.33, 14.82], center: [-17.44, 14.72], zoom: 12 },
  "Île-de-France": { bbox: [1.44, 48.12, 3.56, 49.24], center: [2.35, 48.86], zoom: 10 },
  "France": { bbox: [-5.14, 41.33, 9.56, 51.09], center: [2.21, 46.23], zoom: 6 },
};
const EXPORT_FORMATS = ["GeoJSON", "GeoPackage", "CSV", "GeoParquet", "FlatGeobuf", "Shapefile"];

const SAMPLE_PLACES = Array.from({ length: 200 }, (_, i) => ({
  id: `place_${i}`,
  name: ["Boulangerie Dupont", "Café de la Gare", "Restaurant Le Nantais", "Pharmacie Centrale", "Librairie Verne", "Hotel Atlantic", "Garage Auto+", "École Primaire", "Mairie Annexe", "Supermarché Bio"][i % 10],
  category: ["bakery", "cafe", "restaurant", "pharmacy", "bookstore", "hotel", "auto_repair", "school", "government", "supermarket"][i % 10],
  confidence: 0.5 + Math.random() * 0.5,
  lat: 47.15 + Math.random() * 0.17, lng: -1.72 + Math.random() * 0.30,
}));
const SAMPLE_BUILDINGS = Array.from({ length: 150 }, (_, i) => ({
  id: `bldg_${i}`, name: i % 5 === 0 ? `Bâtiment ${i}` : null,
  height: Math.round(3 + Math.random() * 45), area: Math.round(50 + Math.random() * 2000),
  lat: 47.18 + Math.random() * 0.10, lng: -1.62 + Math.random() * 0.18,
}));
const CATEGORY_COLORS = { bakery: "#EF9F27", cafe: "#BA7517", restaurant: "#D85A30", pharmacy: "#378ADD", bookstore: "#7F77DD", hotel: "#D4537E", auto_repair: "#888780", school: "#1D9E75", government: "#534AB7", supermarket: "#639922" };

const font = "'DM Sans', 'Inter', system-ui, sans-serif";
const mono = "'JetBrains Mono', 'Fira Code', monospace";
const colors = { bg: "#0c0e12", bgCard: "#13161c", bgHover: "#1a1e26", bgInput: "#181c24", border: "rgba(255,255,255,0.06)", borderHover: "rgba(255,255,255,0.12)", text: "#e8e6e1", textMuted: "#8a8880", textDim: "#5a5850", accent: "#1D9E75", accentDim: "#0F6E56", accentBg: "rgba(29,158,117,0.08)", amber: "#EF9F27", amberBg: "rgba(239,159,39,0.08)", danger: "#E24B4A", dangerBg: "rgba(226,75,74,0.08)", blue: "#378ADD", pink: "#D4537E" };

function MetricCard({ label, value, sub, color = colors.accent }) {
  return (<div style={{ background: colors.bgCard, borderRadius: 10, padding: "14px 18px", border: `0.5px solid ${colors.border}`, minWidth: 0 }}><div style={{ fontSize: 12, color: colors.textMuted, marginBottom: 4, letterSpacing: "0.04em", textTransform: "uppercase" }}>{label}</div><div style={{ fontSize: 26, fontWeight: 600, color, fontFamily: mono, letterSpacing: "-0.02em" }}>{value}</div>{sub && <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>{sub}</div>}</div>);
}
function Badge({ children, color = colors.accent }) {
  return <span style={{ display: "inline-block", fontSize: 11, padding: "2px 10px", borderRadius: 6, background: color + "18", color, fontWeight: 500 }}>{children}</span>;
}
function Button({ children, onClick, variant = "default", size = "md", active = false, style: s = {} }) {
  const base = { fontFamily: font, fontSize: size === "sm" ? 12 : 13, fontWeight: 500, padding: size === "sm" ? "5px 12px" : "8px 18px", borderRadius: 8, border: "none", cursor: "pointer", transition: "all 0.15s" };
  const v = { default: { background: active ? colors.accentBg : colors.bgCard, color: active ? colors.accent : colors.text, border: `0.5px solid ${active ? colors.accent + "44" : colors.border}` }, primary: { background: colors.accent, color: "#fff" }, ghost: { background: "transparent", color: colors.textMuted }, danger: { background: colors.dangerBg, color: colors.danger, border: `0.5px solid ${colors.danger}33` } };
  return <button onClick={onClick} style={{ ...base, ...v[variant], ...s }}>{children}</button>;
}
function Select({ value, onChange, options, style: s = {} }) {
  return <select value={value} onChange={e => onChange(e.target.value)} style={{ fontFamily: font, fontSize: 13, padding: "7px 12px", borderRadius: 8, background: colors.bgInput, color: colors.text, border: `0.5px solid ${colors.border}`, outline: "none", cursor: "pointer", ...s }}>{options.map(o => <option key={typeof o === "string" ? o : o.value} value={typeof o === "string" ? o : o.value}>{typeof o === "string" ? o : o.label}</option>)}</select>;
}
function TextInput({ value, onChange, placeholder, style: s = {} }) {
  return <input type="text" value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder} style={{ fontFamily: font, fontSize: 13, padding: "7px 12px", borderRadius: 8, width: "100%", background: colors.bgInput, color: colors.text, border: `0.5px solid ${colors.border}`, outline: "none", boxSizing: "border-box", ...s }} />;
}
function Toggle({ checked, onChange, label }) {
  return (<label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontSize: 13, color: colors.text }}><div onClick={() => onChange(!checked)} style={{ width: 36, height: 20, borderRadius: 10, position: "relative", transition: "background 0.2s", background: checked ? colors.accent : colors.bgInput, border: `0.5px solid ${checked ? colors.accent : colors.border}` }}><div style={{ width: 16, height: 16, borderRadius: "50%", background: "#fff", position: "absolute", top: 2, left: checked ? 18 : 2, transition: "left 0.2s" }} /></div>{label}</label>);
}
function CodeBlock({ code }) {
  return <pre style={{ fontFamily: mono, fontSize: 12, lineHeight: 1.6, padding: 16, borderRadius: 10, background: "#0a0c10", border: `0.5px solid ${colors.border}`, overflowX: "auto", color: colors.textMuted, margin: 0 }}><code>{code}</code></pre>;
}

function CollapsibleSidebar({ children, defaultOpen = true, width = 260 }) {
  const [open, setOpen] = useState(defaultOpen);
  return (<div style={{ display: "flex", height: "100%", flexShrink: 0 }}>
    <button onClick={() => setOpen(o => !o)} style={{ width: 24, background: colors.bgCard, border: "none", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: colors.textDim, fontSize: 16, padding: 0, flexShrink: 0, borderRight: `0.5px solid ${colors.border}`, borderLeft: `0.5px solid ${colors.border}`, transition: "color 0.15s", borderRadius: 0 }} onMouseEnter={e => { e.currentTarget.style.color = colors.accent; e.currentTarget.style.background = colors.bgHover; }} onMouseLeave={e => { e.currentTarget.style.color = colors.textDim; e.currentTarget.style.background = colors.bgCard; }}>{open ? "‹" : "›"}</button>
    <div style={{ width: open ? width : 0, overflow: "hidden", transition: "width 0.25s cubic-bezier(0.4,0,0.2,1)", flexShrink: 0 }}>
      <div style={{ width, height: "100%", overflowY: "auto", padding: open ? "12px 12px 12px 8px" : 0, display: "flex", flexDirection: "column", gap: 12 }}>{children}</div>
    </div>
  </div>);
}

function MapView({ points, colorBy = "category", selectedId, onSelect, mapStyle = "dark", center, zoom, height = 400, children }) {
  const [hoveredId, setHoveredId] = useState(null);
  const [popupInfo, setPopupInfo] = useState(null);
  const [viewState, setViewState] = useState({ longitude: center?.[0] || -1.55, latitude: center?.[1] || 47.22, zoom: zoom || 12, pitch: 0, bearing: 0 });
  useEffect(() => { if (center) setViewState(prev => ({ ...prev, longitude: center[0], latitude: center[1], zoom: zoom || prev.zoom })); }, [center, zoom]);
  const geojsonData = useMemo(() => ({ type: "FeatureCollection", features: (points || []).map(p => ({ type: "Feature", id: p.id, geometry: { type: "Point", coordinates: [p.lng, p.lat] }, properties: { id: p.id, name: p.name || p.id, category: p.category || "unknown", confidence: p.confidence || 0, height: p.height || 0, color: colorBy === "category" ? (CATEGORY_COLORS[p.category] || colors.textMuted) : colorBy === "height" ? d3.interpolateViridis(Math.min((p.height || 0) / 50, 1)) : colors.accent } })) }), [points, colorBy]);
  const pointLayer = { id: "overture-points", type: "circle", paint: { "circle-radius": ["case", ["==", ["get", "id"], selectedId || ""], 8, 5], "circle-color": ["get", "color"], "circle-opacity": 0.8, "circle-stroke-width": ["case", ["==", ["get", "id"], selectedId || ""], 2, 0.5], "circle-stroke-color": ["case", ["==", ["get", "id"], selectedId || ""], "#ffffff", "rgba(255,255,255,0.3)"] } };
  const handleClick = useCallback((e) => { const f = e.features?.[0]; if (f) { const pt = (points || []).find(p => p.id === f.properties.id); if (pt) { onSelect?.(pt); setPopupInfo(pt); } } else { onSelect?.(null); setPopupInfo(null); } }, [points, onSelect]);
  return (<div style={{ borderRadius: 12, overflow: "hidden", border: `0.5px solid ${colors.border}`, height, position: "relative", flex: typeof height === "string" ? 1 : undefined }}>
    <Map {...viewState} onMove={e => setViewState(e.viewState)} style={{ width: "100%", height: "100%" }} mapStyle={MAP_STYLES[mapStyle] || MAP_STYLES.dark} onClick={handleClick} onMouseEnter={useCallback(e => { if (e.features?.[0]) setHoveredId(e.features[0].properties.id); }, [])} onMouseLeave={useCallback(() => setHoveredId(null), [])} interactiveLayerIds={["overture-points"]} cursor={hoveredId ? "pointer" : "grab"}>
      <NavigationControl position="top-right" /><ScaleControl position="bottom-left" />
      {points && points.length > 0 && <Source id="overture-data" type="geojson" data={geojsonData}><Layer {...pointLayer} /></Source>}
      {popupInfo && <Popup longitude={popupInfo.lng} latitude={popupInfo.lat} anchor="bottom" onClose={() => setPopupInfo(null)} closeButton closeOnClick={false}><div style={{ fontFamily: font, padding: "4px 2px", minWidth: 140 }}><div style={{ fontSize: 13, fontWeight: 600, color: "#111", marginBottom: 4 }}>{popupInfo.name || popupInfo.id}</div>{popupInfo.category && <div style={{ fontSize: 11, color: "#555", marginBottom: 2 }}>{popupInfo.category}</div>}{popupInfo.confidence != null && <div style={{ fontSize: 11, color: "#555" }}>Confiance : {(popupInfo.confidence * 100).toFixed(0)}%</div>}{popupInfo.height > 0 && <div style={{ fontSize: 11, color: "#555" }}>Hauteur : {popupInfo.height}m</div>}<div style={{ fontSize: 10, color: "#888", fontFamily: mono, marginTop: 4 }}>{popupInfo.lat.toFixed(5)}, {popupInfo.lng.toFixed(5)}</div></div></Popup>}
      {children}
    </Map>
  </div>);
}

function BasemapSelector({ value, onChange }) {
  return <div style={{ display: "flex", gap: 3 }}>{Object.keys(MAP_STYLES).map(k => <button key={k} onClick={() => onChange(k)} style={{ fontFamily: font, fontSize: 11, padding: "4px 10px", borderRadius: 6, border: `0.5px solid ${value === k ? colors.accent + "55" : colors.border}`, background: value === k ? colors.accentBg : "transparent", color: value === k ? colors.accent : colors.textDim, cursor: "pointer", textTransform: "capitalize" }}>{k}</button>)}</div>;
}

function BarChart({ data, width = "100%", height = 220, color = colors.accent }) {
  if (!data.length) return null;
  const max = Math.max(...data.map(d => d.value)); const W = 680, H = height, barW = Math.min(40, (W - 100) / data.length - 4);
  return (<svg width={width} viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>{data.map((d, i) => { const bh = (d.value / max) * (H - 60); const x = 60 + i * ((W - 100) / data.length); return (<g key={d.label}><rect x={x} y={H - 30 - bh} width={barW} height={bh} rx="3" fill={d.color || color} opacity="0.85" /><text x={x + barW / 2} y={H - 30 - bh - 6} textAnchor="middle" fill={colors.textMuted} fontSize="10" fontFamily={mono}>{d.value}</text><text x={x + barW / 2} y={H - 12} textAnchor="middle" fill={colors.textDim} fontSize="9" fontFamily={font} transform={`rotate(-35 ${x + barW / 2} ${H - 12})`}>{d.label}</text></g>); })}</svg>);
}

function DonutChart({ data, size = 130 }) {
  const total = data.reduce((s, d) => s + d.value, 0); const r = size / 2 - 10, cx = size / 2, cy = size / 2; let sa = -Math.PI / 2;
  return (<svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>{data.map((d, i) => { const a = (d.value / total) * Math.PI * 2; const x1 = cx + r * Math.cos(sa), y1 = cy + r * Math.sin(sa), x2 = cx + r * Math.cos(sa + a), y2 = cy + r * Math.sin(sa + a); const lg = a > Math.PI ? 1 : 0; const p = `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${lg} 1 ${x2} ${y2} Z`; sa += a; return <path key={i} d={p} fill={d.color || colors.accent} opacity="0.85" />; })}<circle cx={cx} cy={cy} r={r * 0.55} fill={colors.bgCard} /><text x={cx} y={cy - 4} textAnchor="middle" fill={colors.text} fontSize="18" fontWeight="600" fontFamily={mono}>{total}</text><text x={cx} y={cy + 12} textAnchor="middle" fill={colors.textDim} fontSize="10" fontFamily={font}>total</text></svg>);
}

// ═══════════════════════════ PAGE: EXPLORER ═══════════════════
function PageExplorer() {
  const [selectedPreset, setSelectedPreset] = useState("Nantes Métropole");
  const [activeTheme, setActiveTheme] = useState("places");
  const [selectedPoint, setSelectedPoint] = useState(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [colorBy, setColorBy] = useState("category");
  const [mapStyleKey, setMapStyleKey] = useState("dark");
  const points = activeTheme === "places" ? SAMPLE_PLACES : SAMPLE_BUILDINGS.map(b => ({ ...b, category: "building" }));
  const filtered = points.filter(p => !searchQuery || (p.name || "").toLowerCase().includes(searchQuery.toLowerCase()));
  const categoryStats = useMemo(() => { const c = {}; filtered.forEach(p => { c[p.category] = (c[p.category] || 0) + 1; }); return Object.entries(c).map(([label, value]) => ({ label, value, color: CATEGORY_COLORS[label] || colors.textMuted })).sort((a, b) => b.value - a.value); }, [filtered]);
  return (
    <div style={{ display: "flex", height: "calc(100vh - 110px)" }}>
      <CollapsibleSidebar defaultOpen={true} width={260}>
        <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Zone</div><Select value={selectedPreset} onChange={setSelectedPreset} options={Object.keys(BBOX_PRESETS)} style={{ width: "100%" }} /></div>
        <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Thème Overture</div><div style={{ display: "flex", flexDirection: "column", gap: 3 }}>{Object.entries(THEMES).map(([k, t]) => (<button key={k} onClick={() => setActiveTheme(k)} style={{ fontFamily: font, fontSize: 12, padding: "8px 12px", borderRadius: 8, border: `0.5px solid ${activeTheme === k ? t.color + "55" : colors.border}`, background: activeTheme === k ? t.color + "12" : "transparent", color: activeTheme === k ? t.color : colors.textMuted, cursor: "pointer", textAlign: "left", display: "flex", alignItems: "center", gap: 8 }}><span style={{ fontSize: 14 }}>{t.icon}</span><span>{t.label}</span><span style={{ marginLeft: "auto", fontSize: 10, opacity: 0.5 }}>{t.types.join(", ")}</span></button>))}</div></div>
        <TextInput value={searchQuery} onChange={setSearchQuery} placeholder="Filtrer par nom..." />
        <div>
          <div style={{ fontSize: 11, color: colors.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Colorier par</div>
          <Select value={colorBy} onChange={setColorBy} options={[{ value: "category", label: "Catégorie" }, { value: "confidence", label: "Confiance" }, { value: "height", label: "Hauteur" }]} style={{ width: "100%", marginBottom: 10 }} />
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
            <DonutChart data={categoryStats.slice(0, 6)} size={120} />
            <div style={{ marginTop: 6, fontSize: 10, display: "flex", flexWrap: "wrap", gap: 4, justifyContent: "center" }}>{categoryStats.slice(0, 6).map(c => (<span key={c.label} style={{ display: "flex", alignItems: "center", gap: 3 }}><span style={{ width: 7, height: 7, borderRadius: 2, background: c.color, display: "inline-block" }} /><span style={{ color: colors.textMuted }}>{c.label}</span></span>))}</div>
          </div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}><MetricCard label="Features" value={filtered.length.toLocaleString()} /><MetricCard label="Catégories" value={categoryStats.length} color={colors.amber} /></div>
        {selectedPoint && (<div style={{ background: colors.bgCard, borderRadius: 10, padding: 14, border: `0.5px solid ${colors.accent}33` }}><div style={{ fontSize: 11, color: colors.accent, marginBottom: 6, textTransform: "uppercase" }}>Sélection</div><div style={{ fontSize: 14, fontWeight: 500, color: colors.text, marginBottom: 4 }}>{selectedPoint.name || selectedPoint.id}</div><div style={{ fontSize: 12, color: colors.textMuted }}>{selectedPoint.category && <div>Catégorie : {selectedPoint.category}</div>}{selectedPoint.confidence && <div>Confiance : {(selectedPoint.confidence * 100).toFixed(0)}%</div>}{selectedPoint.height && <div>Hauteur : {selectedPoint.height}m</div>}<div style={{ fontFamily: mono, fontSize: 10, marginTop: 4, color: colors.textDim }}>{selectedPoint.lat.toFixed(5)}, {selectedPoint.lng.toFixed(5)}</div></div></div>)}
      </CollapsibleSidebar>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8, padding: "0 0 0 12px", minWidth: 0 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}><div style={{ fontSize: 11, color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>{filtered.length} features — {activeTheme}</div><BasemapSelector value={mapStyleKey} onChange={setMapStyleKey} /></div>
        <MapView points={filtered} colorBy={colorBy} selectedId={selectedPoint?.id} onSelect={setSelectedPoint} mapStyle={mapStyleKey} center={BBOX_PRESETS[selectedPreset]?.center} zoom={BBOX_PRESETS[selectedPreset]?.zoom} height="100%" />
      </div>
    </div>
  );
}

// ═══════════════════════════ PAGE: ADMIN ═══════════════════════
function PageAdmin() {
  const [imports, setImports] = useState([{ id: 1, theme: "places", zone: "Nantes Métropole", status: "done", features: 12847, size: "34 MB", date: "2026-03-20" }, { id: 2, theme: "buildings", zone: "Nantes Métropole", status: "done", features: 89234, size: "210 MB", date: "2026-03-20" }, { id: 3, theme: "transportation", zone: "Loire-Atlantique", status: "running", features: null, size: null, date: "2026-03-24" }]);
  const [newTheme, setNewTheme] = useState("places"); const [newZone, setNewZone] = useState("Nantes Métropole");
  const [cacheEnabled, setCacheEnabled] = useState(true); const [maxMemory, setMaxMemory] = useState("4"); const [threads, setThreads] = useState("4");
  return (
    <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1fr) 300px", gap: 20, height: "calc(100vh - 110px)", overflowY: "auto" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}>
          <div style={{ fontSize: 15, fontWeight: 500, color: colors.text, marginBottom: 16 }}>Nouvel import</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr auto", gap: 10, alignItems: "end" }}>
            <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Thème</div><Select value={newTheme} onChange={setNewTheme} options={Object.entries(THEMES).map(([k, v]) => ({ value: k, label: v.label }))} style={{ width: "100%" }} /></div>
            <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Zone</div><Select value={newZone} onChange={setNewZone} options={Object.keys(BBOX_PRESETS)} style={{ width: "100%" }} /></div>
            <Button variant="primary" onClick={() => setImports(prev => [...prev, { id: Date.now(), theme: newTheme, zone: newZone, status: "pending", features: null, size: null, date: new Date().toISOString().slice(0, 10) }])}>Lancer</Button>
          </div>
          <div style={{ marginTop: 12 }}><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 6 }}>Requête DuckDB :</div><CodeBlock code={`LOAD spatial; LOAD httpfs;\nSET s3_region='us-west-2';\nCOPY(\n  SELECT id, names.primary AS name, categories.primary AS category, confidence, geometry\n  FROM read_parquet('${S3_BASE}/theme=${newTheme}/type=${THEMES[newTheme]?.types[0]}/*')\n  WHERE bbox.xmin BETWEEN ${BBOX_PRESETS[newZone]?.bbox[0]} AND ${BBOX_PRESETS[newZone]?.bbox[2]}\n    AND bbox.ymin BETWEEN ${BBOX_PRESETS[newZone]?.bbox[1]} AND ${BBOX_PRESETS[newZone]?.bbox[3]}\n) TO '${newTheme}_export.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');`} /></div>
        </div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}>
          <div style={{ fontSize: 15, fontWeight: 500, color: colors.text, marginBottom: 12 }}>Historique</div>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}><thead><tr style={{ borderBottom: `0.5px solid ${colors.border}` }}>{["Thème", "Zone", "Status", "Features", "Taille", "Date", ""].map(h => <th key={h} style={{ textAlign: "left", padding: "8px 10px", color: colors.textDim, fontSize: 11, fontWeight: 400, textTransform: "uppercase" }}>{h}</th>)}</tr></thead>
          <tbody>{imports.map(imp => (<tr key={imp.id} style={{ borderBottom: `0.5px solid ${colors.border}` }}><td style={{ padding: 10, color: THEMES[imp.theme]?.color }}>{THEMES[imp.theme]?.icon} {THEMES[imp.theme]?.label}</td><td style={{ padding: 10, color: colors.text }}>{imp.zone}</td><td style={{ padding: 10 }}><Badge color={imp.status === "done" ? colors.accent : imp.status === "running" ? colors.amber : colors.textDim}>{imp.status === "done" ? "Terminé" : imp.status === "running" ? "En cours..." : "Attente"}</Badge></td><td style={{ padding: 10, fontFamily: mono }}>{imp.features?.toLocaleString() || "—"}</td><td style={{ padding: 10, color: colors.textMuted }}>{imp.size || "—"}</td><td style={{ padding: 10, color: colors.textDim }}>{imp.date}</td><td style={{ padding: 10 }}><Button size="sm" variant="danger" onClick={() => setImports(prev => prev.filter(i => i.id !== imp.id))}>×</Button></td></tr>))}</tbody></table>
        </div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 15, fontWeight: 500, color: colors.text, marginBottom: 16 }}>Config DuckDB</div><div style={{ display: "flex", flexDirection: "column", gap: 14 }}><div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Mémoire</div><Select value={maxMemory} onChange={setMaxMemory} options={["2","4","8","16"].map(v => ({ value: v, label: `${v} GB` }))} style={{ width: "100%" }} /></div><div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Threads</div><Select value={threads} onChange={setThreads} options={["1","2","4","8"].map(v => ({ value: v, label: `${v}` }))} style={{ width: "100%" }} /></div><Toggle checked={cacheEnabled} onChange={setCacheEnabled} label="Cache activé" /></div></div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}><div style={{ fontFamily: mono, fontSize: 22, fontWeight: 600, color: colors.accent }}>{OVERTURE_RELEASE}</div><div style={{ fontSize: 12, color: colors.textMuted, marginTop: 4 }}>Release Overture</div></div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Extensions</div>{["spatial", "httpfs", "h3", "parquet", "json"].map(ext => <div key={ext} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: `0.5px solid ${colors.border}`, fontSize: 13 }}><span style={{ fontFamily: mono, color: colors.text }}>{ext}</span><Badge color={colors.accent}>ok</Badge></div>)}</div>
      </div>
    </div>
  );
}

// ═══════════════════════════ PAGE: DATAVIZ (carte MapLibre + charts) ════
function PageDataViz() {
  const [vizType, setVizType] = useState("distribution");
  const [mapStyleKey, setMapStyleKey] = useState("dark");
  const heightBuckets = useMemo(() => { const b = { "0-5m": 0, "5-10m": 0, "10-20m": 0, "20-30m": 0, "30-50m": 0 }; SAMPLE_BUILDINGS.forEach(bl => { if (bl.height < 5) b["0-5m"]++; else if (bl.height < 10) b["5-10m"]++; else if (bl.height < 20) b["10-20m"]++; else if (bl.height < 30) b["20-30m"]++; else b["30-50m"]++; }); return Object.entries(b).map(([l, v]) => ({ label: l, value: v })); }, []);
  const confidenceBuckets = useMemo(() => { const b = {}; SAMPLE_PLACES.forEach(p => { const k = (Math.round(p.confidence * 10) / 10).toFixed(1); b[k] = (b[k] || 0) + 1; }); return Object.entries(b).sort((a, c) => a[0] - c[0]).map(([l, v]) => ({ label: l, value: v })); }, []);
  const categoryStats = useMemo(() => { const c = {}; SAMPLE_PLACES.forEach(p => { c[p.category] = (c[p.category] || 0) + 1; }); return Object.entries(c).map(([l, v]) => ({ label: l, value: v, color: CATEGORY_COLORS[l] || colors.textMuted })).sort((a, b) => b.value - a.value); }, []);
  const avgH = Math.round(SAMPLE_BUILDINGS.reduce((s, b) => s + b.height, 0) / SAMPLE_BUILDINGS.length);
  const avgC = (SAMPLE_PLACES.reduce((s, p) => s + p.confidence, 0) / SAMPLE_PLACES.length * 100).toFixed(0);
  return (
    <div style={{ display: "flex", height: "calc(100vh - 110px)" }}>
      <CollapsibleSidebar defaultOpen={true} width={280}>
        <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Visualisation</div><div style={{ display: "flex", flexDirection: "column", gap: 3 }}>{[{ key: "distribution", label: "Distribution", icon: "▥" }, { key: "density", label: "Densité H3", icon: "⬡" }, { key: "temporal", label: "Temporel", icon: "◷" }, { key: "cross", label: "Croisement", icon: "⊞" }].map(t => (<button key={t.key} onClick={() => setVizType(t.key)} style={{ fontFamily: font, fontSize: 12, padding: "8px 12px", borderRadius: 8, border: `0.5px solid ${vizType === t.key ? colors.accent + "55" : colors.border}`, background: vizType === t.key ? colors.accentBg : "transparent", color: vizType === t.key ? colors.accent : colors.textMuted, cursor: "pointer", textAlign: "left", display: "flex", alignItems: "center", gap: 8 }}><span>{t.icon}</span><span>{t.label}</span></button>))}</div></div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}><MetricCard label="Buildings" value={SAMPLE_BUILDINGS.length} sub="Nantes" color={colors.blue} /><MetricCard label="Haut. moy." value={`${avgH}m`} color={colors.amber} /><MetricCard label="Places" value={SAMPLE_PLACES.length} sub="10 catégs" color={colors.accent} /><MetricCard label="Conf. moy." value={`${avgC}%`} color={colors.pink} /></div>
        <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.06em" }}>Fond de carte</div><BasemapSelector value={mapStyleKey} onChange={setMapStyleKey} /></div>
      </CollapsibleSidebar>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 12, padding: "0 0 0 12px", minWidth: 0, overflowY: "auto" }}>
        <MapView points={vizType === "distribution" ? SAMPLE_PLACES : SAMPLE_BUILDINGS.map(b => ({ ...b, category: "building" }))} colorBy={vizType === "distribution" ? "category" : "height"} mapStyle={mapStyleKey} center={BBOX_PRESETS["Nantes Métropole"].center} zoom={12} height={300} />
        {vizType === "distribution" && (<div style={{ display: "grid", gridTemplateColumns: "minmax(0,1fr) minmax(0,1fr)", gap: 12 }}><div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Distribution par catégorie</div><BarChart data={categoryStats.slice(0, 10)} /></div><div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Hauteurs des bâtiments</div><BarChart data={heightBuckets} color={colors.blue} /></div></div>)}
        {vizType === "density" && (<div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 8 }}>Densité H3 — résolution 8</div><svg width="100%" viewBox="0 0 680 260">{Array.from({ length: 80 }, (_, i) => { const col = i % 10, row = Math.floor(i / 10); const cx = 60 + col * 64 + (row % 2 ? 32 : 0); const cy = 24 + row * 30; const val = Math.random(); const fill = d3.interpolateViridis(val); const r = 16; const hex = Array.from({ length: 6 }, (_, j) => { const a = (Math.PI / 3) * j - Math.PI / 6; return `${cx + r * Math.cos(a)},${cy + r * Math.sin(a)}`; }).join(" "); return <polygon key={i} points={hex} fill={fill} opacity="0.8" stroke={colors.bg} strokeWidth="1" />; })}</svg></div>)}
        {vizType === "temporal" && (<div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 8 }}>Évolution releases Overture</div><BarChart data={[{ label: "2024-04", value: 2100000 }, { label: "2024-07", value: 2350000 }, { label: "2024-11", value: 2600000 }, { label: "2025-04", value: 2850000 }, { label: "2025-09", value: 3100000 }, { label: "2026-03", value: 3400000 }]} height={240} color={colors.accent} /></div>)}
        {vizType === "cross" && (<div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 8 }}>Hauteur × Surface</div><svg width="100%" viewBox="0 0 680 280"><line x1="60" y1="250" x2="660" y2="250" stroke={colors.border} strokeWidth="0.5" /><line x1="60" y1="250" x2="60" y2="20" stroke={colors.border} strokeWidth="0.5" /><text x="360" y="276" textAnchor="middle" fill={colors.textDim} fontSize="10" fontFamily={font}>Surface (m²)</text><text x="16" y="140" textAnchor="middle" fill={colors.textDim} fontSize="10" fontFamily={font} transform="rotate(-90 16 140)">Hauteur (m)</text>{SAMPLE_BUILDINGS.map((b, i) => <circle key={i} cx={60 + (b.area / 2000) * 580} cy={250 - (b.height / 50) * 220} r={3} fill={colors.blue} opacity="0.5" />)}</svg></div>)}
      </div>
    </div>
  );
}

// ═══════════════════════════ PAGE: STYLE EDITOR (carte MapLibre live) ════
function PageStyleEditor() {
  const [activeLayer, setActiveLayer] = useState("buildings");
  const [fillColor, setFillColor] = useState("#378ADD"); const [fillOpacity, setFillOpacity] = useState(0.7);
  const [strokeColor, setStrokeColor] = useState("#ffffff"); const [strokeWidth, setStrokeWidth] = useState(0.5);
  const [extruded, setExtruded] = useState(true); const [heightField, setHeightField] = useState("height");
  const [baseStyle, setBaseStyle] = useState("dark");
  const points = activeLayer === "buildings" ? SAMPLE_BUILDINGS.map(b => ({ ...b, category: "building" })) : SAMPLE_PLACES;
  const styleJson = useMemo(() => JSON.stringify({ version: 8, name: "Overture Custom Style", sources: { overture: { type: "vector", url: `pmtiles://https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/${OVERTURE_RELEASE}/${activeLayer}.pmtiles` } }, layers: [{ id: `${activeLayer}-fill`, type: extruded ? "fill-extrusion" : "fill", source: "overture", "source-layer": activeLayer, paint: extruded ? { "fill-extrusion-color": fillColor, "fill-extrusion-opacity": fillOpacity, "fill-extrusion-height": ["get", heightField] } : { "fill-color": fillColor, "fill-opacity": fillOpacity } }, { id: `${activeLayer}-line`, type: "line", source: "overture", "source-layer": activeLayer, paint: { "line-color": strokeColor, "line-width": strokeWidth } }] }, null, 2), [activeLayer, fillColor, fillOpacity, strokeColor, strokeWidth, extruded, heightField]);
  return (
    <div style={{ display: "flex", height: "calc(100vh - 110px)" }}>
      <CollapsibleSidebar defaultOpen={true} width={270}>
        <div style={{ background: colors.bgCard, borderRadius: 10, padding: 14, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Couche</div><Select value={activeLayer} onChange={setActiveLayer} options={Object.entries(THEMES).map(([k, v]) => ({ value: k, label: v.label }))} style={{ width: "100%" }} /></div>
        <div style={{ background: colors.bgCard, borderRadius: 10, padding: 14, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Remplissage</div><div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}><input type="color" value={fillColor} onChange={e => setFillColor(e.target.value)} style={{ width: 36, height: 28, border: "none", borderRadius: 4, cursor: "pointer", background: "none" }} /><span style={{ fontFamily: mono, fontSize: 12, color: colors.textMuted }}>{fillColor}</span></div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4 }}>Opacité : {fillOpacity.toFixed(1)}</div><input type="range" min="0" max="1" step="0.1" value={fillOpacity} onChange={e => setFillOpacity(parseFloat(e.target.value))} style={{ width: "100%" }} /></div>
        <div style={{ background: colors.bgCard, borderRadius: 10, padding: 14, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Contour</div><div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}><input type="color" value={strokeColor} onChange={e => setStrokeColor(e.target.value)} style={{ width: 36, height: 28, border: "none", borderRadius: 4, cursor: "pointer", background: "none" }} /><span style={{ fontFamily: mono, fontSize: 12, color: colors.textMuted }}>{strokeColor}</span></div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4 }}>Épaisseur : {strokeWidth}px</div><input type="range" min="0" max="3" step="0.5" value={strokeWidth} onChange={e => setStrokeWidth(parseFloat(e.target.value))} style={{ width: "100%" }} /></div>
        <div style={{ background: colors.bgCard, borderRadius: 10, padding: 14, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>3D</div><Toggle checked={extruded} onChange={setExtruded} label="Extrusion" />{extruded && <div style={{ marginTop: 10 }}><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4 }}>Champ hauteur</div><Select value={heightField} onChange={setHeightField} options={["height", "num_floors", "area"]} style={{ width: "100%" }} /></div>}</div>
        <div style={{ background: colors.bgCard, borderRadius: 10, padding: 14, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Fond de carte</div><BasemapSelector value={baseStyle} onChange={setBaseStyle} /></div>
      </CollapsibleSidebar>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 12, padding: "0 0 0 12px", minWidth: 0, overflowY: "auto" }}>
        <MapView points={points} colorBy={activeLayer === "buildings" ? "height" : "category"} mapStyle={baseStyle} center={BBOX_PRESETS["Nantes Métropole"].center} zoom={13} height={360} />
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text }}>Style JSON</div><div style={{ display: "flex", gap: 6 }}><Button size="sm" onClick={() => navigator.clipboard?.writeText(styleJson)}>Copier</Button><Button size="sm" variant="primary" onClick={() => { const b = new Blob([styleJson], { type: "application/json" }); const a = document.createElement("a"); a.href = URL.createObjectURL(b); a.download = `style_${activeLayer}.json`; a.click(); }}>Export .json</Button></div></div><pre style={{ fontFamily: mono, fontSize: 11, lineHeight: 1.5, padding: 14, borderRadius: 8, background: "#0a0c10", border: `0.5px solid ${colors.border}`, maxHeight: 200, overflowY: "auto", color: colors.textMuted, margin: 0, whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{styleJson}</pre></div>
      </div>
    </div>
  );
}

// ═══════════════════════════ PAGE: EXPORT ═══════════════════════
function PageExport() {
  const [selectedTheme, setSelectedTheme] = useState("places"); const [selectedZone, setSelectedZone] = useState("Nantes Métropole");
  const [selectedFormat, setSelectedFormat] = useState("GeoJSON"); const [maxFeatures, setMaxFeatures] = useState("10000");
  const [selectedCols, setSelectedCols] = useState(["id", "name", "category", "geometry"]);
  const allCols = { places: ["id", "name", "category", "confidence", "addresses", "websites", "phones", "geometry"], buildings: ["id", "name", "height", "num_floors", "class", "geometry"], transportation: ["id", "class", "subtype", "names", "geometry"], divisions: ["id", "name", "subtype", "country", "geometry"], base: ["id", "class", "subtype", "geometry"], addresses: ["id", "number", "street", "postcode", "city", "geometry"] };
  const exportQuery = `LOAD spatial; LOAD httpfs;\nSET s3_region='us-west-2';\n\nCOPY(\n  SELECT ${selectedCols.join(", ")}\n  FROM read_parquet('${S3_BASE}/theme=${selectedTheme}/type=${THEMES[selectedTheme]?.types[0]}/*')\n  WHERE bbox.xmin BETWEEN ${BBOX_PRESETS[selectedZone]?.bbox[0]} AND ${BBOX_PRESETS[selectedZone]?.bbox[2]}\n    AND bbox.ymin BETWEEN ${BBOX_PRESETS[selectedZone]?.bbox[1]} AND ${BBOX_PRESETS[selectedZone]?.bbox[3]}\n  LIMIT ${maxFeatures}\n) TO 'export.${selectedFormat === "GeoJSON" ? "geojson" : "gpkg"}'\nWITH (FORMAT GDAL, DRIVER '${selectedFormat === "GeoPackage" ? "GPKG" : selectedFormat}');`;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "300px minmax(0,1fr)", gap: 20, height: "calc(100vh - 110px)", overflowY: "auto" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 18, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 15, fontWeight: 500, color: colors.text, marginBottom: 14 }}>Paramètres</div>
          <div style={{ marginBottom: 12 }}><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Thème</div><Select value={selectedTheme} onChange={v => { setSelectedTheme(v); setSelectedCols(["id", "name", "geometry"]); }} options={Object.entries(THEMES).map(([k, v]) => ({ value: k, label: `${v.icon} ${v.label}` }))} style={{ width: "100%" }} /></div>
          <div style={{ marginBottom: 12 }}><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Zone</div><Select value={selectedZone} onChange={setSelectedZone} options={Object.keys(BBOX_PRESETS)} style={{ width: "100%" }} /></div>
          <div style={{ marginBottom: 12 }}><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Format</div><div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>{EXPORT_FORMATS.map(f => <Button key={f} size="sm" active={selectedFormat === f} onClick={() => setSelectedFormat(f)}>{f}</Button>)}</div></div>
          <div><div style={{ fontSize: 11, color: colors.textDim, marginBottom: 4, textTransform: "uppercase" }}>Max features</div><Select value={maxFeatures} onChange={setMaxFeatures} options={["1000","5000","10000","50000","100000"].map(v => ({ value: v, label: v }))} style={{ width: "100%" }} /></div>
        </div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 18, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Colonnes</div>{(allCols[selectedTheme] || []).map(col => <label key={col} style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontSize: 12, color: colors.text, padding: "4px 0" }}><input type="checkbox" checked={selectedCols.includes(col)} onChange={e => { if (e.target.checked) setSelectedCols(p => [...p, col]); else setSelectedCols(p => p.filter(c => c !== col)); }} style={{ accentColor: colors.accent }} /><span style={{ fontFamily: mono }}>{col}</span></label>)}</div>
        <Button variant="primary" style={{ width: "100%", padding: "12px 0", fontSize: 14 }}>Lancer l'export</Button>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 18, border: `0.5px solid ${colors.border}` }}><div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text }}>Requête DuckDB</div><Button size="sm" onClick={() => navigator.clipboard?.writeText(exportQuery)}>Copier</Button></div><CodeBlock code={exportQuery} /></div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10 }}><MetricCard label="Format" value={selectedFormat} /><MetricCard label="Colonnes" value={selectedCols.length} color={colors.amber} /><MetricCard label="Limite" value={parseInt(maxFeatures).toLocaleString()} color={colors.blue} /></div>
      </div>
    </div>
  );
}

// ═══════════════════════════ PAGE: MCP ═══════════════════════════
function PageMCP() {
  const tools = [{ name: "query_places", desc: "POI par bbox, catégorie, nom", params: "bbox, category?, name?" }, { name: "query_buildings", desc: "Bâtiments par bbox, hauteur", params: "bbox, min_height?" }, { name: "query_transport", desc: "Réseau routier", params: "bbox, class?" }, { name: "spatial_stats", desc: "Stats H3", params: "theme, bbox, resolution?" }, { name: "export_data", desc: "Export multi-format", params: "theme, bbox, format" }, { name: "style_generate", desc: "Style MapLibre", params: "layers, colors" }, { name: "raw_duckdb", desc: "SQL brut", params: "sql" }];
  const logs = [{ time: "14:32:05", tool: "query_places", status: "ok", dur: "1.2s", q: "restaurants near Nantes" }, { time: "14:31:48", tool: "query_buildings", status: "ok", dur: "2.8s", q: "buildings >20m" }, { time: "14:31:12", tool: "export_data", status: "ok", dur: "4.5s", q: "export geojson" }];
  return (
    <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1fr) 320px", gap: 20, height: "calc(100vh - 110px)", overflowY: "auto" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 15, fontWeight: 500, color: colors.text, marginBottom: 14 }}>MCP tools</div><div style={{ display: "flex", flexDirection: "column", gap: 6 }}>{tools.map(t => <div key={t.name} style={{ padding: "12px 14px", borderRadius: 8, background: colors.bgHover, border: `0.5px solid ${colors.border}`, display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}><div><div style={{ fontFamily: mono, fontSize: 13, color: colors.accent, fontWeight: 500 }}>{t.name}</div><div style={{ fontSize: 12, color: colors.textMuted, marginTop: 2 }}>{t.desc}</div><div style={{ fontSize: 11, color: colors.textDim, marginTop: 4, fontFamily: mono }}>params: {t.params}</div></div><Badge color={colors.accent}>actif</Badge></div>)}</div></div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 20, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Config Claude Desktop</div><CodeBlock code={`{\n  "mcpServers": {\n    "overture-maps": {\n      "command": "python",\n      "args": ["-m", "overture_mcp_server"],\n      "env": { "OVERTURE_RELEASE": "${OVERTURE_RELEASE}", "DUCKDB_MEMORY": "4GB" }\n    }\n  }\n}`} /></div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}><MetricCard label="Tools" value={tools.length} /><MetricCard label="Requêtes" value={logs.length} sub="dernière h" color={colors.amber} /></div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 10 }}>Logs</div>{logs.map((l, i) => <div key={i} style={{ padding: "10px 12px", borderRadius: 6, background: "#0a0c10", border: `0.5px solid ${colors.border}`, marginBottom: 6, fontSize: 12 }}><div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}><span style={{ fontFamily: mono, color: colors.accent }}>{l.tool}</span><span style={{ color: colors.textDim }}>{l.time}</span></div><div style={{ color: colors.textMuted, marginBottom: 2 }}>{l.q}</div><div style={{ display: "flex", gap: 8 }}><Badge color={colors.accent}>{l.status}</Badge><span style={{ fontFamily: mono, fontSize: 11, color: colors.textDim }}>{l.dur}</span></div></div>)}</div>
        <div style={{ background: colors.bgCard, borderRadius: 12, padding: 16, border: `0.5px solid ${colors.border}` }}><div style={{ fontSize: 13, fontWeight: 500, color: colors.text, marginBottom: 8 }}>Status</div>{[{ l: "MCP Server", ok: true }, { l: "DuckDB", ok: true }, { l: "S3", ok: true }, { l: "FastAPI", ok: true }].map(s => <div key={s.l} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "4px 0", fontSize: 12 }}><span style={{ color: colors.text }}>{s.l}</span><div style={{ display: "flex", alignItems: "center", gap: 6 }}><div style={{ width: 7, height: 7, borderRadius: "50%", background: s.ok ? colors.accent : colors.danger }} /><span style={{ color: s.ok ? colors.accent : colors.danger, fontSize: 11 }}>{s.ok ? "online" : "off"}</span></div></div>)}</div>
      </div>
    </div>
  );
}

// ═══════════════════════════ MAIN APP ═══════════════════════════
const PAGES = [{ key: "explorer", label: "Explorer", icon: "◉" }, { key: "admin", label: "Admin", icon: "⚙" }, { key: "dataviz", label: "DataViz", icon: "▥" }, { key: "style", label: "Styles", icon: "◧" }, { key: "export", label: "Export", icon: "↓" }, { key: "mcp", label: "MCP", icon: "⬡" }];

export default function App() {
  const [activePage, setActivePage] = useState("explorer");
  return (
    <div style={{ fontFamily: font, background: colors.bg, color: colors.text, height: "100vh", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <header style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "10px 20px", borderBottom: `0.5px solid ${colors.border}`, background: colors.bgCard, flexShrink: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}><div style={{ width: 30, height: 30, borderRadius: 8, background: colors.accent, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 15, fontWeight: 700, color: "#fff" }}>O</div><div><div style={{ fontSize: 14, fontWeight: 600, letterSpacing: "-0.02em" }}>Overture Maps Explorer</div><div style={{ fontSize: 10, color: colors.textDim }}>v{OVERTURE_RELEASE}</div></div></div>
        <nav style={{ display: "flex", gap: 2, background: colors.bg, borderRadius: 10, padding: 3 }}>{PAGES.map(p => <button key={p.key} onClick={() => setActivePage(p.key)} style={{ fontFamily: font, fontSize: 12, fontWeight: 500, padding: "6px 14px", borderRadius: 8, border: "none", cursor: "pointer", transition: "all 0.15s", background: activePage === p.key ? colors.accent : "transparent", color: activePage === p.key ? "#fff" : colors.textMuted, display: "flex", alignItems: "center", gap: 5 }}><span style={{ fontSize: 13 }}>{p.icon}</span>{p.label}</button>)}</nav>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}><Badge color={colors.accent}>DuckDB</Badge><Badge color={colors.amber}>S3</Badge></div>
      </header>
      <main style={{ flex: 1, padding: "12px 16px", overflow: "hidden" }}>
        {activePage === "explorer" && <PageExplorer />}
        {activePage === "admin" && <PageAdmin />}
        {activePage === "dataviz" && <PageDataViz />}
        {activePage === "style" && <PageStyleEditor />}
        {activePage === "export" && <PageExport />}
        {activePage === "mcp" && <PageMCP />}
      </main>
    </div>
  );
}

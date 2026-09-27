/**
 * WhiteboxPanel.jsx — Grand panneau d'analyse raster (Whitebox/GEE).
 * =============================================================================
 * Gauche : arborescence dépliable Catégories → Outils (+ recherche, badges).
 * Droite : vue d'ensemble de l'architecture (par défaut) OU détail d'un outil
 *          (définition markdown + formulaire de paramètres dynamique).
 * Les outils `implemented:false` s'affichent avec un badge « Bientôt » et un
 * bouton Exécuter désactivé — on branche le backend au fur et à mesure.
 * =============================================================================
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import {
  WHITEBOX_TOOLS, WHITEBOX_TOOLS_BY_ID, getToolsByCategory, getCategories,
} from "../utils/whiteboxTools";
import {
  IcChevronDown, IcMap, IcGlobe, IcStack, IcMountain, IcDroplets, IcGrid,
  IcWrench, IcBoxes, IcBarChart, IcImage, IcNavigation, IcSparkles, IcSearch,
} from "../icons";

// Base API relative en prod (nginx proxie /api), localhost en dev. Ne PAS importer
// le API="/api" de config.js ici : on préfixe déjà /api → sinon /api/api (404).
const API = import.meta.env.VITE_API_URL || (import.meta.env.DEV ? "http://localhost:8000" : "");

// Mapping icône de catégorie (string du catalogue → composant)
const CAT_ICONS = {
  mountain: IcMountain, droplets: IcDroplets, grid: IcGrid, wrench: IcWrench,
  boxes: IcBoxes, chart: IcBarChart, image: IcImage, navigation: IcNavigation,
  sliders: IcGrid, sparkles: IcSparkles,
};

// Calcul bbox [west, south, east, north] depuis un GeoJSON
function geojsonBbox(gj) {
  if (!gj?.features?.length) return null;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const walk = (c) => {
    if (typeof c[0] === "number") {
      minX = Math.min(minX, c[0]); maxX = Math.max(maxX, c[0]);
      minY = Math.min(minY, c[1]); maxY = Math.max(maxY, c[1]);
    } else c.forEach(walk);
  };
  gj.features.forEach(f => { if (f.geometry?.coordinates) walk(f.geometry.coordinates); });
  if (!isFinite(minX)) return null;
  return [minX, minY, maxX, maxY];
}

// Rendu markdown minimal : **gras**, listes, sauts de ligne
function renderDefinition(text, C) {
  if (!text) return null;
  return text.split("\n").map(l => l.trim()).map((line, i) => {
    if (!line) return <div key={i} style={{ height: 8 }} />;
    const bold = line.match(/^\*\*(.+?)\*\*(.*)$/);
    if (bold) return (
      <div key={i} style={{ marginTop: i > 0 ? 10 : 0, marginBottom: 4 }}>
        <span style={{ fontWeight: 700, color: C.txt }}>{bold[1]}</span><span>{bold[2]}</span>
      </div>
    );
    if (line.startsWith("- ")) return (
      <div key={i} style={{ display: "flex", gap: 6, marginBottom: 2, paddingLeft: 4 }}>
        <span style={{ color: C.acc }}>•</span><span>{line.slice(2)}</span>
      </div>
    );
    return <div key={i} style={{ marginBottom: 2 }}>{line}</div>;
  });
}

export default function WhiteboxPanel({ onAddRasterLayer, mapRef, layers = [], filterCategories = null }) {
  const C = useThemeContext();
  const allCategories = getCategories();
  const categories = filterCategories ? allCategories.filter(c => filterCategories.includes(c.key)) : allCategories;
  const isMobile = typeof window !== "undefined" && window.innerWidth < 640;

  const [selectedToolId, setSelectedToolId] = useState(null); // null = vue d'ensemble
  const [expanded, setExpanded] = useState({ morphologie: true }); // catégories dépliées
  const [search, setSearch] = useState("");
  const [params, setParams] = useState({});
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [activeTab, setActiveTab] = useState("reglages");
  const [zoneMode, setZoneMode] = useState("map");
  const [bbox, setBbox] = useState({ south: "", west: "", north: "", east: "" });
  const [selectedLayerId, setSelectedLayerId] = useState("");
  const [sidebarOpen, setSidebarOpen] = useState(!isMobile);

  const selectedTool = selectedToolId ? WHITEBOX_TOOLS_BY_ID[selectedToolId] : null;
  const q = search.trim().toLowerCase();

  const toggleCat = (key) => setExpanded(e => ({ ...e, [key]: !e[key] }));

  const handleSelectTool = (toolId) => {
    setSelectedToolId(toolId);
    setError(null); setSuccess(null); setActiveTab("reglages");
    const tool = WHITEBOX_TOOLS_BY_ID[toolId];
    const np = {};
    (tool.params || []).forEach(p => { np[p.id] = p.default; });
    setParams(np);
    if (isMobile) setSidebarOpen(false);
  };

  const handleParamChange = (id, v) => setParams(p => ({ ...p, [id]: v }));

  const fillBboxFromView = () => {
    const map = mapRef?.current?.getMap?.();
    if (!map || !map.isStyleLoaded?.()) return;
    try {
      const b = map.getBounds?.();
      if (b) setBbox({ south: b.getSouth().toFixed(4), west: b.getWest().toFixed(4), north: b.getNorth().toFixed(4), east: b.getEast().toFixed(4) });
    } catch (e) { console.warn("getBounds:", e); }
  };

  const handleExecute = async () => {
    if (!selectedTool) return;
    if (!selectedTool.implemented) {
      setError("Outil pas encore disponible (implémentation en cours).");
      return;
    }
    setError(null); setSuccess(null); setRunning(true);
    try {
      let bounds = [-180, -90, 180, 90];
      if (zoneMode === "map") {
        const map = mapRef?.current?.getMap?.();
        if (map && map.isStyleLoaded?.()) {
          const b = map.getBounds?.();
          if (b) bounds = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
        }
      } else if (zoneMode === "layer") {
        if (!selectedLayerId) { setError("Sélectionnez une couche."); setRunning(false); return; }
        const layer = layers.find(l => l.id === selectedLayerId);
        if (layer?.bbox) bounds = layer.bbox;
        else if (layer?.geojson) { const bb = geojsonBbox(layer.geojson); if (bb) bounds = bb; }
      }

      const demSource = params.dem || "SRTM_30m";
      const toolParams = { ...params }; delete toolParams.dem;

      const res = await fetch(`${API}/api/whitebox/run`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tool: selectedTool.id, bbox: bounds, dem_source: demSource, params: toolParams }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);

      const zoneLabel = zoneMode === "map" ? "zone visible" : zoneMode === "monde" ? "zone mondiale" : "couche";
      onAddRasterLayer?.({
        id: `wbx_${selectedTool.id}_${Date.now()}`,
        name: `${selectedTool.name} (${zoneLabel})`,
        type: "wms", tileUrl: data.tile_url, opacity: 0.85,
        bbox: zoneMode === "monde" ? null : bounds, visParams: data.vis_params || null,
      });
      setSuccess(`✓ ${selectedTool.name} calculé (${zoneLabel})`);
      setRunning(false);
    } catch (e) { setError(`Erreur : ${e.message}`); setRunning(false); }
  };

  // ── Styles ──
  const inputStyle = { width: "100%", padding: "8px 12px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: M, fontSize: 13, marginBottom: 8, boxSizing: "border-box" };
  const labelStyle = { fontSize: 11, fontWeight: 600, color: C.txt, textTransform: "uppercase", marginBottom: 6, letterSpacing: "0.5px" };
  const paramGroupStyle = { marginBottom: 16 };
  const msgStyle = (t) => ({ padding: "10px 12px", borderRadius: 6, fontSize: 12, marginBottom: 12, border: `0.5px solid ${t === "error" ? "#f0a8a8" : "#a8f0c8"}`, background: t === "error" ? "rgba(240,168,168,0.1)" : "rgba(168,240,200,0.1)", color: t === "error" ? "#d85a30" : "#0F6E56" });
  const badge = (bg, col) => ({ fontSize: 9, fontWeight: 700, padding: "1px 6px", borderRadius: 10, background: bg, color: col, textTransform: "uppercase", letterSpacing: "0.3px" });

  // ── Sidebar : arborescence catégories → outils ──
  const renderSidebar = () => (
    <div style={{
      width: isMobile ? "100%" : (sidebarOpen ? 250 : 46),
      borderRight: isMobile ? "none" : `0.5px solid ${C.bdr}`,
      borderBottom: isMobile ? `0.5px solid ${C.bdr}` : "none",
      maxHeight: isMobile ? (sidebarOpen ? "40vh" : 46) : "none",
      overflow: "auto", flexShrink: 0, transition: "width 0.2s",
      display: "flex", flexDirection: "column",
    }}>
      {/* Barre recherche + toggle */}
      <div style={{ padding: sidebarOpen ? "12px 12px 8px" : "12px 6px 8px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", gap: 6, alignItems: "center" }}>
        {sidebarOpen && (
          <div style={{ flex: 1, display: "flex", alignItems: "center", gap: 6, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "5px 8px" }}>
            <IcSearch size={13} style={{ color: C.dim, flexShrink: 0 }} />
            <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Rechercher un outil…"
              style={{ border: "none", background: "transparent", color: C.txt, fontFamily: F, fontSize: 12, width: "100%", outline: "none" }} />
          </div>
        )}
        <button onClick={() => setSidebarOpen(o => !o)} title={sidebarOpen ? "Replier" : "Déplier"}
          style={{ border: "none", background: "transparent", color: C.mut, cursor: "pointer", padding: 4, display: "flex", transform: sidebarOpen ? "rotate(90deg)" : "rotate(-90deg)", transition: "transform 0.2s" }}>
          <IcChevronDown size={16} />
        </button>
      </div>

      {sidebarOpen && (
        <div style={{ padding: "8px 0" }}>
          {categories.map(cat => {
            const CatIcon = CAT_ICONS[cat.icon] || IcGrid;
            let tools = getToolsByCategory(cat.key);
            if (q) tools = tools.filter(t => t.name.toLowerCase().includes(q) || (t.description || "").toLowerCase().includes(q));
            if (q && !tools.length) return null;
            const isOpen = q ? true : !!expanded[cat.key];
            return (
              <div key={cat.key}>
                <button onClick={() => toggleCat(cat.key)}
                  style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "8px 12px", border: "none", background: "transparent", color: C.txt, cursor: "pointer", fontFamily: F, fontSize: 12.5, fontWeight: 600 }}>
                  <IcChevronDown size={13} style={{ color: C.dim, transform: isOpen ? "none" : "rotate(-90deg)", transition: "transform 0.15s", flexShrink: 0 }} />
                  <CatIcon size={15} style={{ color: C.acc, flexShrink: 0 }} />
                  <span style={{ flex: 1, textAlign: "left" }}>{cat.name}</span>
                  <span style={{ fontSize: 10, color: C.dim }}>{cat.ready}/{cat.total}</span>
                </button>
                {isOpen && tools.map(tool => {
                  const active = selectedToolId === tool.id;
                  return (
                    <button key={tool.id} onClick={() => handleSelectTool(tool.id)} title={tool.description}
                      style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "7px 12px 7px 34px", border: "none", borderLeft: `2px solid ${active ? C.acc : "transparent"}`, background: active ? C.acc + "14" : "transparent", color: active ? C.acc : C.mut, cursor: "pointer", fontFamily: F, fontSize: 12, fontWeight: active ? 600 : 400, textAlign: "left" }}>
                      <span style={{ flex: 1 }}>{tool.name}</span>
                      {!tool.implemented && <span style={badge(C.bdr, C.dim)}>Bientôt</span>}
                    </button>
                  );
                })}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );

  // ── Vue d'ensemble (architecture) — affichée quand aucun outil sélectionné ──
  const renderOverview = () => {
    const totalTools = categories.reduce((s, c) => s + c.total, 0);
    const totalReady = categories.reduce((s, c) => s + c.ready, 0);
    return (
      <div style={{ padding: 24, overflow: "auto" }}>
        <h3 style={{ fontSize: 17, fontWeight: 700, color: C.txt, margin: "0 0 4px" }}>Analyse raster — Whitebox / GEE</h3>
        <p style={{ fontSize: 12.5, color: C.dim, margin: "0 0 18px", lineHeight: 1.5 }}>
          {totalReady} outil{totalReady > 1 ? "s" : ""} opérationnel{totalReady > 1 ? "s" : ""} sur {totalTools} planifiés.
          Sélectionnez un outil à gauche. Les outils <b>Bientôt</b> sont préparés et branchés au fur et à mesure.
        </p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(210px, 1fr))", gap: 12 }}>
          {categories.map(cat => {
            const CatIcon = CAT_ICONS[cat.icon] || IcGrid;
            const pct = cat.total ? Math.round((cat.ready / cat.total) * 100) : 0;
            return (
              <button key={cat.key} onClick={() => { toggleCat(cat.key); setExpanded(e => ({ ...e, [cat.key]: true })); }}
                style={{ textAlign: "left", border: `0.5px solid ${C.bdr}`, borderRadius: 10, padding: 14, background: C.input, cursor: "pointer", fontFamily: F, display: "flex", flexDirection: "column", gap: 8 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <CatIcon size={17} style={{ color: C.acc }} />
                  <span style={{ fontSize: 13, fontWeight: 600, color: C.txt }}>{cat.name}</span>
                </div>
                <span style={{ fontSize: 11, color: C.dim, lineHeight: 1.4 }}>{cat.description}</span>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 2 }}>
                  <div style={{ flex: 1, height: 5, borderRadius: 3, background: C.bdr, overflow: "hidden" }}>
                    <div style={{ width: `${pct}%`, height: "100%", background: cat.ready ? C.acc : C.dim }} />
                  </div>
                  <span style={{ fontSize: 10, color: C.dim, fontFamily: M }}>{cat.ready}/{cat.total}</span>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    );
  };

  // ── Détail d'un outil ──
  const renderToolDetail = () => (
    <div style={{ flex: 1, padding: 24, overflow: "auto", display: "flex", flexDirection: "column", minWidth: 0 }}>
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
          <h3 style={{ fontSize: 16, fontWeight: 600, color: C.txt, margin: 0 }}>{selectedTool.name}</h3>
          {!selectedTool.implemented && <span style={badge(C.bdr, C.dim)}>Bientôt</span>}
          <span style={badge(C.acc + "22", C.acc)}>{selectedTool.engine}</span>
        </div>
        <p style={{ fontSize: 12, color: C.dim, margin: 0 }}>{selectedTool.description}</p>
      </div>

      {/* Onglets */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, borderBottom: `0.5px solid ${C.bdr}`, paddingBottom: 8 }}>
        {["reglages", "definition"].map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)}
            style={{ padding: "6px 12px", border: "none", background: activeTab === tab ? C.acc : "transparent", color: activeTab === tab ? "#fff" : C.mut, borderRadius: 4, fontFamily: F, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
            {tab === "reglages" ? "Réglages" : "Définition"}
          </button>
        ))}
      </div>

      {error && <div style={msgStyle("error")}>{error}</div>}
      {success && <div style={msgStyle("success")}>{success}</div>}

      {!selectedTool.implemented && (
        <div style={{ ...msgStyle("success"), color: C.mut, borderColor: C.bdr, background: C.input }}>
          Cet outil est planifié ({selectedTool.engine}). Le formulaire est un aperçu ; le calcul sera branché prochainement.
        </div>
      )}

      {activeTab === "definition" && (
        <div style={{ background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 10, padding: 14, fontSize: 12, color: C.txt, lineHeight: 1.6, wordWrap: "break-word" }}>
          {renderDefinition(selectedTool.definition || "Documentation à venir.", C)}
        </div>
      )}

      {activeTab === "reglages" && (
        <form onSubmit={(e) => { e.preventDefault(); handleExecute(); }}>
          {(selectedTool.inputs || []).map(input => (
            <div key={input.id} style={paramGroupStyle}>
              <label style={labelStyle}>{input.label} {input.required && "*"}</label>
              <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px 0" }}>{input.description}</p>
              <select style={inputStyle} value={params[input.id] ?? "SRTM_30m"} onChange={e => handleParamChange(input.id, e.target.value)}>
                {input.type === "raster" && input.id === "dem" ? (
                  <>
                    <option value="SRTM_30m">SRTM 30 m (mondial)</option>
                    <option value="COPDEM_30m">Copernicus DEM GLO-30</option>
                  </>
                ) : (
                  <>
                    <option value="">Sélectionner…</option>
                    {layers.filter(l => l.isRaster).map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
                  </>
                )}
              </select>
            </div>
          ))}

          {(selectedTool.params || []).map(param => (
            <div key={param.id} style={paramGroupStyle}>
              <label style={labelStyle}>{param.label}</label>
              <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px 0" }}>{param.description}</p>
              {param.type === "select" ? (
                <select style={inputStyle} value={params[param.id] ?? param.default} onChange={e => handleParamChange(param.id, e.target.value)}>
                  {param.options.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
              ) : param.type === "number" ? (
                <input type="number" style={inputStyle} value={params[param.id] ?? param.default} min={param.min} max={param.max} step={param.step || "0.1"} onChange={e => handleParamChange(param.id, parseFloat(e.target.value))} />
              ) : (
                <input type="text" style={inputStyle} value={params[param.id] ?? param.default} onChange={e => handleParamChange(param.id, e.target.value)} />
              )}
            </div>
          ))}

          {/* Emprise */}
          <div style={paramGroupStyle}>
            <label style={labelStyle}>Emprise d'analyse</label>
            <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px 0" }}>Zone d'intérêt</p>
            <div style={{ display: "flex", gap: 6 }}>
              {[{ id: "map", label: "Vue carte", Icon: IcMap }, { id: "monde", label: "Zone mondiale", Icon: IcGlobe }, { id: "layer", label: "Couche", Icon: IcStack }].map(({ id, label, Icon }) => (
                <button key={id} type="button" onClick={() => { setZoneMode(id); if (id === "map") fillBboxFromView(); }}
                  style={{ flex: 1, padding: "8px 4px", border: `0.5px solid ${zoneMode === id ? C.acc : C.bdr}`, background: zoneMode === id ? C.acc + "18" : "transparent", color: zoneMode === id ? C.acc : C.mut, borderRadius: 6, fontFamily: F, fontSize: 10, fontWeight: 600, cursor: "pointer", display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
                  <Icon size={15} />{label}
                </button>
              ))}
            </div>
            {zoneMode === "map" && bbox.south && <p style={{ fontSize: 10, color: C.dim, margin: "6px 0 0 0" }}>S:{bbox.south}° W:{bbox.west}° N:{bbox.north}° E:{bbox.east}°</p>}
            {zoneMode === "layer" && (
              <select style={{ ...inputStyle, marginTop: 6 }} value={selectedLayerId} onChange={e => setSelectedLayerId(e.target.value)}>
                <option value="">Sélectionner une couche…</option>
                {layers.map(l => <option key={l.id} value={l.id}>{l.name || l.id}</option>)}
              </select>
            )}
          </div>

          <button type="submit" disabled={running || !selectedTool.implemented}
            style={{ padding: "10px 16px", marginTop: 8, background: (running || !selectedTool.implemented) ? C.dim : C.acc, color: (running || !selectedTool.implemented) ? C.txt : "#04120a", border: "none", borderRadius: 8, fontFamily: F, fontSize: 13, fontWeight: 600, cursor: (running || !selectedTool.implemented) ? "not-allowed" : "pointer", opacity: (running || !selectedTool.implemented) ? 0.6 : 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8, width: "100%" }}>
            {running ? (<><span style={{ animation: "spin 1s linear infinite" }}>⟳</span>Calcul en cours…</>) : (selectedTool.implemented ? "Exécuter" : "Bientôt disponible")}
          </button>
        </form>
      )}
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: isMobile ? "column" : "row", height: "100%", minHeight: 0 }}>
      {renderSidebar()}
      <div style={{ flex: 1, minWidth: 0, overflow: "auto", display: "flex", flexDirection: "column" }}>
        {selectedTool ? renderToolDetail() : renderOverview()}
      </div>
      <style>{`@keyframes spin { from { transform: rotate(0); } to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

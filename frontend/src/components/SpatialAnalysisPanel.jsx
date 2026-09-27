/**
 * SpatialAnalysisPanel.jsx — Hub d'analyse spatiale unifié.
 * =============================================================================
 * UNE sidebar dépliable (Vecteur / Raster / Avancé) au style uniforme, avec
 * barre de recherche en bas. Le détail à droite :
 *   - opération vecteur (turf)      → formulaire inline
 *   - outil raster (Whitebox/GEE)   → formulaire inline + POST /api/whitebox/run
 *   - module complexe               → composant existant (stats spatiales,
 *     chaleur/clusters, jointure, analyse zonale, vectorisation, classif, SQL)
 * =============================================================================
 */
import { useState, useMemo, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { useSpatialSection } from "../utils/spatialNav";
import { SPATIAL_OPS, SPATIAL_GROUPS, executeSpatialOp } from "../utils/spatial";
import { getLayerAttrs } from "../utils/classification";
import { getToolsByCategory, WHITEBOX_TOOLS_BY_ID, getCategories } from "../utils/whiteboxTools";
import { Sel, Lbl } from "./ui";
import SpatialStatsPanel from "./SpatialStatsPanel";
import VectorVizPanel from "./VectorVizPanel";
import JoinPanel from "./JoinPanel";
import RasterAnalysisPanel from "./RasterAnalysisPanel";
import RasterVectorPanel from "./RasterVectorPanel";
import ClassifSupPanel from "./ClassifSupPanel";
import SqlPanel from "./SqlPanel";
import {
  IcChevronDown, IcSearch, IcMap, IcGlobe, IcStack, IcVenn, IcMountain, IcSparkles,
} from "../icons";

const API = import.meta.env.VITE_API_URL || (import.meta.env.DEV ? "http://localhost:8000" : "");

function geojsonBbox(gj) {
  if (!gj?.features?.length) return null;
  let a = Infinity, b = Infinity, c = -Infinity, d = -Infinity;
  const walk = (x) => { if (typeof x[0] === "number") { a = Math.min(a, x[0]); c = Math.max(c, x[0]); b = Math.min(b, x[1]); d = Math.max(d, x[1]); } else x.forEach(walk); };
  gj.features.forEach(f => { if (f.geometry?.coordinates) walk(f.geometry.coordinates); });
  return isFinite(a) ? [a, b, c, d] : null;
}

function renderDefinition(text, C) {
  if (!text) return null;
  return text.split("\n").map(l => l.trim()).map((line, i) => {
    if (!line) return <div key={i} style={{ height: 8 }} />;
    const bold = line.match(/^\*\*(.+?)\*\*(.*)$/);
    if (bold) return <div key={i} style={{ marginTop: i > 0 ? 10 : 0, marginBottom: 4 }}><span style={{ fontWeight: 700, color: C.txt }}>{bold[1]}</span><span>{bold[2]}</span></div>;
    if (line.startsWith("- ")) return <div key={i} style={{ display: "flex", gap: 6, marginBottom: 2, paddingLeft: 4 }}><span style={{ color: C.acc }}>•</span><span>{line.slice(2)}</span></div>;
    return <div key={i} style={{ marginBottom: 2 }}>{line}</div>;
  });
}

// Construit l'arborescence : sections → groupes → outils
function buildSections() {
  const cats = getCategories();
  const rasterGroups = cats.filter(c => c.key !== "avance").map(c => ({
    key: `ras_${c.key}`, name: c.name,
    tools: getToolsByCategory(c.key).map(t => ({ id: t.id, name: t.name, kind: "raster", implemented: t.implemented, desc: t.description })),
  }));
  const avanceGroups = cats.filter(c => c.key === "avance").map(c => ({
    key: `ras_${c.key}`, name: c.name,
    tools: getToolsByCategory(c.key).map(t => ({ id: t.id, name: t.name, kind: "raster", implemented: t.implemented, desc: t.description })),
  }));
  return [
    { id: "vecteur", label: "Vecteur", Icon: IcVenn, groups: [
      ...SPATIAL_GROUPS.map(g => ({ key: `vec_${g}`, name: g, tools: SPATIAL_OPS.filter(o => o.group === g).map(o => ({ id: o.id, name: o.name, kind: "vector", implemented: true, desc: o.desc })) })),
      { key: "mod_spatialstats", name: "Stats spatiales", tools: [{ id: "spatialstats", name: "Moran & hotspots", kind: "module", module: "spatialstats", implemented: true, desc: "Autocorrélation, points chauds/froids" }] },
      { key: "mod_vectorviz", name: "Chaleur & clusters", tools: [{ id: "vectorviz", name: "Chaleur & clusters", kind: "module", module: "vectorviz", implemented: true, desc: "Densité et regroupement de points" }] },
      { key: "mod_join", name: "Jointure attributaire", tools: [{ id: "join", name: "Jointure CSV → couche", kind: "module", module: "join", implemented: true, desc: "Rapatrie des colonnes d'un CSV" }] },
    ]},
    { id: "raster", label: "Raster", Icon: IcMountain, groups: [
      ...rasterGroups,
      { key: "mod_rasteranalysis", name: "Analyse zonale + calc", tools: [{ id: "rasteranalysis", name: "Zonal + map algebra", kind: "module", module: "rasteranalysis", implemented: true, desc: "Stats zonales et calculatrice" }] },
      { key: "mod_rastervec", name: "Vectorisation raster", tools: [{ id: "rastervec", name: "Polygones + contours", kind: "module", module: "rastervec", implemented: true, desc: "Raster → polygones/contours" }] },
    ]},
    { id: "avance", label: "Avancé", Icon: IcSparkles, groups: [
      ...avanceGroups,
      { key: "mod_classif", name: "Classification supervisée", tools: [{ id: "classif", name: "Classif. supervisée", kind: "module", module: "classif", implemented: true, desc: "Entraîne un modèle sur échantillons" }] },
      { key: "mod_sql", name: "SQL Workspace", tools: [{ id: "sql", name: "SQL spatial (DuckDB)", kind: "module", module: "sql", implemented: true, desc: "Requêtes SQL sur vos couches" }] },
    ]},
  ];
}

export default function SpatialAnalysisPanel({
  layers = [], onAddLayer, onAddRasterLayer, mapRef,
  addLayerSilent, addImageLayer, updateRasterLayer, classifClickRef,
}) {
  const C = useThemeContext();
  const sections = useMemo(buildSections, []);
  const isMobile = typeof window !== "undefined" && window.innerWidth < 640;

  const navSection = useSpatialSection(); // section demandée depuis le menu latéral
  const [selected, setSelected] = useState(null); // { kind, id, module? }
  const [expandedSections, setExpandedSections] = useState({ vecteur: true }); // Vecteur déplié par défaut
  const [expanded, setExpanded] = useState({}); // catégories (niveau 2)
  const [search, setSearch] = useState("");
  const [sidebarOpen, setSidebarOpen] = useState(!isMobile);

  // Déplie la famille demandée depuis le menu latéral (Vecteur/Raster/Avancé)
  useEffect(() => {
    if (navSection) setExpandedSections(e => ({ ...e, [navSection]: true }));
  }, [navSection]);

  // ── Formulaire VECTEUR (turf) ──
  const [vLayerA, setVLayerA] = useState("");
  const [vLayerB, setVLayerB] = useState("");
  const [vParams, setVParams] = useState({});
  const [vName, setVName] = useState("");
  const [vErr, setVErr] = useState(null);
  const [vBusy, setVBusy] = useState(false);
  const [vOk, setVOk] = useState(null);

  // ── Formulaire RASTER (whitebox/gee) ──
  const [rParams, setRParams] = useState({});
  const [rTab, setRTab] = useState("reglages");
  const [zoneMode, setZoneMode] = useState("map");
  const [rBusy, setRBusy] = useState(false);
  const [rErr, setRErr] = useState(null);
  const [rOk, setROk] = useState(null);
  const [rLayerId, setRLayerId] = useState("");

  const q = search.trim().toLowerCase();
  const toggleSection = (id) => setExpandedSections(e => ({ ...e, [id]: !e[id] }));
  const toggleGroup = (k) => setExpanded(e => ({ ...e, [k]: !e[k] }));

  const selectTool = (tool) => {
    setSelected(tool);
    setVErr(null); setVOk(null); setRErr(null); setROk(null); setRTab("reglages");
    if (tool.kind === "vector") { setVParams({}); setVName(""); }
    if (tool.kind === "raster") {
      const wt = WHITEBOX_TOOLS_BY_ID[tool.id];
      const np = {}; (wt?.params || []).forEach(p => { np[p.id] = p.default; }); setRParams(np);
    }
    if (isMobile) setSidebarOpen(false);
  };

  // ── Exécution VECTEUR ──
  const vectorLayers = layers.filter(l => l.visible && !l.isRaster);
  const opV = selected?.kind === "vector" ? SPATIAL_OPS.find(o => o.id === selected.id) : null;
  const needsB = opV?.inputs?.length > 1;
  const layerA = layers.find(l => l.id === vLayerA);
  const attrsA = useMemo(() => layerA ? getLayerAttrs(layerA) : { num: [], cat: [] }, [layerA]);

  const runVector = () => {
    setVErr(null); setVBusy(true); setVOk(null);
    try {
      if (!layerA) throw new Error("Sélectionnez la couche A");
      const layerB = layers.find(l => l.id === vLayerB);
      if (needsB && !layerB) throw new Error("Sélectionnez la couche B");
      const p = {};
      (opV.params || []).forEach(pr => { const v = vParams[pr.id] ?? pr.default; p[pr.id] = pr.type === "number" ? (parseFloat(v) || pr.default) : v; });
      const result = executeSpatialOp(opV.id, layerA, needsB ? layerB : null, p);
      if (!result?.features?.length) { setVErr("Aucun résultat — les couches ne se chevauchent peut-être pas"); setVBusy(false); return; }
      onAddLayer(result, vName || `${opV.name}_${layerA.name.slice(0, 12)}`, "analysis");
      setVOk(`${result.features.length} entités ajoutées`);
    } catch (e) { setVErr(e.message); }
    setVBusy(false);
  };

  // ── Exécution RASTER ──
  const runRaster = async () => {
    const wt = WHITEBOX_TOOLS_BY_ID[selected.id];
    if (!wt?.implemented) { setRErr("Outil pas encore disponible."); return; }
    setRErr(null); setROk(null); setRBusy(true);
    try {
      let bounds = [-180, -90, 180, 90];
      if (zoneMode === "map") {
        const map = mapRef?.current?.getMap?.();
        if (map?.isStyleLoaded?.()) { const b = map.getBounds?.(); if (b) bounds = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
      } else if (zoneMode === "layer") {
        const layer = layers.find(l => l.id === rLayerId);
        if (layer?.bbox) bounds = layer.bbox; else if (layer?.geojson) { const bb = geojsonBbox(layer.geojson); if (bb) bounds = bb; }
      }
      const demSource = rParams.dem || "SRTM_30m";
      const tp = { ...rParams }; delete tp.dem;
      const res = await fetch(`${API}/api/whitebox/run`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ tool: wt.id, bbox: bounds, dem_source: demSource, params: tp }) });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      const zl = zoneMode === "map" ? "zone visible" : zoneMode === "monde" ? "zone mondiale" : "couche";
      onAddRasterLayer?.({ id: `wbx_${wt.id}_${Date.now()}`, name: `${wt.name} (${zl})`, type: "wms", tileUrl: data.tile_url, opacity: 0.85, bbox: zoneMode === "monde" ? null : bounds, visParams: data.vis_params || null });
      setROk(`✓ ${wt.name} calculé (${zl})`);
    } catch (e) { setRErr(`Erreur : ${e.message}`); }
    setRBusy(false);
  };

  // ── Rendu module composant ──
  const renderModule = (m) => {
    switch (m) {
      case "spatialstats": return <SpatialStatsPanel layers={layers} onAddLayer={onAddLayer} />;
      case "vectorviz": return <VectorVizPanel layers={layers} onAdd={addLayerSilent || onAddLayer} />;
      case "join": return <JoinPanel layers={layers.filter(l => !l.isRaster && l.geojson)} onAddLayer={onAddLayer} />;
      case "rasteranalysis": return <RasterAnalysisPanel layers={layers} onAddLayer={onAddLayer} onAddImageLayer={addImageLayer} />;
      case "rastervec": return <RasterVectorPanel layers={layers} onAddLayer={onAddLayer} />;
      case "classif": return <ClassifSupPanel mapRef={mapRef} layers={layers} addRasterLayer={onAddRasterLayer} updateRasterLayer={updateRasterLayer} classifClickRef={classifClickRef} />;
      case "sql": return <SqlPanel onAddLayer={onAddLayer} layers={layers} />;
      default: return null;
    }
  };

  // ── Styles communs ──
  const inputStyle = { width: "100%", padding: "8px 12px", border: `0.5px solid ${C.bdr}`, borderRadius: 7, background: C.input, color: C.txt, fontFamily: M, fontSize: 13, marginBottom: 8, boxSizing: "border-box" };
  const labelStyle = { fontSize: 11, fontWeight: 600, color: C.txt, textTransform: "uppercase", marginBottom: 6, letterSpacing: "0.5px" };
  const grp = { marginBottom: 16 };
  const msg = (t) => ({ padding: "10px 12px", borderRadius: 6, fontSize: 12, marginBottom: 12, border: `0.5px solid ${t === "error" ? "#f0a8a8" : "#a8f0c8"}`, background: t === "error" ? "rgba(240,168,168,0.1)" : "rgba(168,240,200,0.1)", color: t === "error" ? "#d85a30" : "#0F6E56" });
  const badge = (bg, col) => ({ fontSize: 9, fontWeight: 700, padding: "1px 6px", borderRadius: 10, background: bg, color: col, textTransform: "uppercase" });

  // ── SIDEBAR : arbre à 3 niveaux (Section → Catégorie → Outil) ──
  const renderSidebar = () => (
    <div style={{ width: isMobile ? "100%" : (sidebarOpen ? 260 : 46), borderRight: isMobile ? "none" : `0.5px solid ${C.bdr}`, borderBottom: isMobile ? `0.5px solid ${C.bdr}` : "none", maxHeight: isMobile ? (sidebarOpen ? "45vh" : 46) : "none", display: "flex", flexDirection: "column", flexShrink: 0, transition: "width 0.2s", minHeight: 0 }}>
      {/* Recherche EN HAUT + toggle */}
      <div style={{ display: "flex", gap: 6, alignItems: "center", padding: sidebarOpen ? "10px 10px 8px" : "10px 6px", borderBottom: `0.5px solid ${C.bdr}`, flexShrink: 0 }}>
        {sidebarOpen && (
          <div style={{ flex: 1, display: "flex", alignItems: "center", gap: 6, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "6px 8px" }}>
            <IcSearch size={13} style={{ color: C.dim, flexShrink: 0 }} />
            <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Rechercher un outil…" style={{ border: "none", background: "transparent", color: C.txt, fontFamily: F, fontSize: 12, width: "100%", outline: "none" }} />
          </div>
        )}
        <button onClick={() => setSidebarOpen(o => !o)} title={sidebarOpen ? "Replier" : "Déplier"} style={{ border: "none", background: "transparent", color: C.mut, cursor: "pointer", padding: 4, display: "flex", transform: sidebarOpen ? "rotate(90deg)" : "rotate(-90deg)" }}>
          <IcChevronDown size={16} />
        </button>
      </div>

      {/* Arbre */}
      {sidebarOpen && (
        <div style={{ flex: 1, overflow: "auto", padding: "6px 0" }}>
          {sections.map(sec => {
            // Filtre recherche : ne garder que les groupes/outils qui matchent
            let groups = sec.groups;
            if (q) groups = groups.map(g => ({ ...g, tools: g.tools.filter(t => t.name.toLowerCase().includes(q) || (t.desc || "").toLowerCase().includes(q)) })).filter(g => g.tools.length);
            if (q && !groups.length) return null;
            const secOpen = q ? true : !!expandedSections[sec.id];
            const secCount = groups.reduce((n, g) => n + g.tools.length, 0);
            return (
              <div key={sec.id}>
                {/* Niveau 1 : Section */}
                <button onClick={() => toggleSection(sec.id)} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "9px 12px", border: "none", background: secOpen ? C.acc + "0c" : "transparent", color: C.txt, cursor: "pointer", fontFamily: F, fontSize: 13, fontWeight: 700, borderLeft: `2px solid ${secOpen ? C.acc : "transparent"}` }}>
                  <IcChevronDown size={13} style={{ color: C.dim, transform: secOpen ? "none" : "rotate(-90deg)", flexShrink: 0 }} />
                  <sec.Icon size={16} style={{ color: C.acc, flexShrink: 0 }} />
                  <span style={{ flex: 1, textAlign: "left" }}>{sec.label}</span>
                  <span style={{ fontSize: 9, color: C.dim }}>{secCount}</span>
                </button>
                {/* Niveau 2 : Catégories */}
                {secOpen && groups.map(g => {
                  const gOpen = q ? true : (expanded[g.key] ?? false);
                  return (
                    <div key={g.key}>
                      <button onClick={() => toggleGroup(g.key)} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "6px 12px 6px 26px", border: "none", background: "transparent", color: C.mut, cursor: "pointer", fontFamily: F, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                        <IcChevronDown size={11} style={{ color: C.dim, transform: gOpen ? "none" : "rotate(-90deg)", flexShrink: 0 }} />
                        <span style={{ flex: 1, textAlign: "left" }}>{g.name}</span>
                        <span style={{ fontSize: 9, color: C.dim }}>{g.tools.length}</span>
                      </button>
                      {/* Niveau 3 : Outils */}
                      {gOpen && g.tools.map(tool => {
                        const active = selected?.id === tool.id && selected?.kind === tool.kind;
                        return (
                          <button key={tool.id} onClick={() => selectTool(tool)} title={tool.desc} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "6px 12px 6px 44px", border: "none", borderLeft: `2px solid ${active ? C.acc : "transparent"}`, background: active ? C.acc + "14" : "transparent", color: active ? C.acc : C.mut, cursor: "pointer", fontFamily: F, fontSize: 12, fontWeight: active ? 600 : 400, textAlign: "left" }}>
                            <span style={{ flex: 1 }}>{tool.name}</span>
                            {tool.implemented === false && <span style={badge(C.bdr, C.dim)}>Bientôt</span>}
                          </button>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );

  // ── DÉTAIL VECTEUR ──
  const renderVectorForm = () => (
    <div style={{ padding: 20, overflow: "auto" }}>
      <div style={{ marginBottom: 14 }}>
        <h3 style={{ fontSize: 15, fontWeight: 600, color: C.txt, margin: 0 }}>{opV?.name}</h3>
        <p style={{ fontSize: 12, color: C.dim, margin: "4px 0 0" }}>{opV?.desc}</p>
      </div>
      {vErr && <div style={msg("error")}>{vErr}</div>}
      {vOk && <div style={msg("success")}>{vOk}</div>}
      <div style={grp}><Lbl>Couche A {opV?.inputs?.[0] ? `— ${opV.inputs[0]}` : ""}</Lbl>
        <Sel value={vLayerA} onChange={setVLayerA} options={[{ value: "", label: "-- Choisir --" }, ...vectorLayers.map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} /></div>
      {needsB && <div style={grp}><Lbl>Couche B {opV?.inputs?.[1] ? `— ${opV.inputs[1]}` : ""}</Lbl>
        <Sel value={vLayerB} onChange={setVLayerB} options={[{ value: "", label: "-- Choisir --" }, ...vectorLayers.filter(l => l.id !== vLayerA).map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} /></div>}
      {(opV?.params || []).map(param => (
        <div key={param.id} style={grp}><Lbl>{param.label}</Lbl>
          {param.type === "number" ? <input type="number" value={vParams[param.id] ?? param.default} onChange={e => setVParams(p => ({ ...p, [param.id]: e.target.value }))} style={inputStyle} />
            : param.type === "attribute" ? <Sel value={vParams[param.id] ?? param.default} onChange={v => setVParams(p => ({ ...p, [param.id]: v }))} options={[{ value: "", label: "-- Aucun --" }, ...[...attrsA.cat, ...attrsA.num].map(a => ({ value: a, label: a }))]} />
              : <input value={vParams[param.id] ?? param.default} onChange={e => setVParams(p => ({ ...p, [param.id]: e.target.value }))} style={inputStyle} />}
        </div>
      ))}
      <div style={grp}><Lbl>Nom du résultat</Lbl><input value={vName} onChange={e => setVName(e.target.value)} placeholder={opV?.name} style={inputStyle} /></div>
      {!vectorLayers.length && <p style={{ fontSize: 11, color: C.dim, marginBottom: 10 }}>Aucune couche vectorielle chargée — importez-en une pour exécuter.</p>}
      <button onClick={runVector} disabled={vBusy || !vLayerA} style={{ width: "100%", padding: "10px", background: vLayerA ? C.acc : C.dim, color: vLayerA ? "#04120a" : C.txt, border: "none", borderRadius: 8, fontFamily: F, fontSize: 13, fontWeight: 600, cursor: vLayerA ? "pointer" : "not-allowed", opacity: vBusy ? 0.6 : 1 }}>{vBusy ? "⏳ Calcul…" : "Exécuter"}</button>
    </div>
  );

  // ── DÉTAIL RASTER ──
  const renderRasterForm = () => {
    const wt = WHITEBOX_TOOLS_BY_ID[selected.id];
    if (!wt) return null;
    return (
      <div style={{ padding: 20, overflow: "auto" }}>
        <div style={{ marginBottom: 14 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <h3 style={{ fontSize: 15, fontWeight: 600, color: C.txt, margin: 0 }}>{wt.name}</h3>
            {!wt.implemented && <span style={badge(C.bdr, C.dim)}>Bientôt</span>}
            <span style={badge(C.acc + "22", C.acc)}>{wt.engine}</span>
          </div>
          <p style={{ fontSize: 12, color: C.dim, margin: "4px 0 0" }}>{wt.description}</p>
        </div>
        <div style={{ display: "flex", gap: 8, marginBottom: 14, borderBottom: `0.5px solid ${C.bdr}`, paddingBottom: 8 }}>
          {["reglages", "definition"].map(t => <button key={t} onClick={() => setRTab(t)} style={{ padding: "6px 12px", border: "none", background: rTab === t ? C.acc : "transparent", color: rTab === t ? "#fff" : C.mut, borderRadius: 4, fontFamily: F, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>{t === "reglages" ? "Réglages" : "Définition"}</button>)}
        </div>
        {rErr && <div style={msg("error")}>{rErr}</div>}
        {rOk && <div style={msg("success")}>{rOk}</div>}
        {rTab === "definition" && <div style={{ background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 10, padding: 14, fontSize: 12, color: C.txt, lineHeight: 1.6 }}>{renderDefinition(wt.definition || "Documentation à venir.", C)}</div>}
        {rTab === "reglages" && (
          <form onSubmit={e => { e.preventDefault(); runRaster(); }}>
            {(wt.inputs || []).map(input => (
              <div key={input.id} style={grp}><label style={labelStyle}>{input.label} {input.required && "*"}</label>
                <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px" }}>{input.description}</p>
                <select style={inputStyle} value={rParams[input.id] ?? "SRTM_30m"} onChange={e => setRParams(p => ({ ...p, [input.id]: e.target.value }))}>
                  {input.id === "dem" ? <><option value="SRTM_30m">SRTM 30 m (mondial)</option><option value="COPDEM_30m">Copernicus DEM GLO-30</option></>
                    : <><option value="">Sélectionner…</option>{layers.filter(l => l.isRaster).map(l => <option key={l.id} value={l.id}>{l.name}</option>)}</>}
                </select>
              </div>
            ))}
            {(wt.params || []).map(param => (
              <div key={param.id} style={grp}><label style={labelStyle}>{param.label}</label>
                <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px" }}>{param.description}</p>
                {param.type === "select" ? <select style={inputStyle} value={rParams[param.id] ?? param.default} onChange={e => setRParams(p => ({ ...p, [param.id]: e.target.value }))}>{param.options.map(o => <option key={o} value={o}>{o}</option>)}</select>
                  : param.type === "number" ? <input type="number" style={inputStyle} value={rParams[param.id] ?? param.default} min={param.min} max={param.max} step={param.step || "0.1"} onChange={e => setRParams(p => ({ ...p, [param.id]: parseFloat(e.target.value) }))} />
                    : <input type="text" style={inputStyle} value={rParams[param.id] ?? param.default} onChange={e => setRParams(p => ({ ...p, [param.id]: e.target.value }))} />}
              </div>
            ))}
            <div style={grp}><label style={labelStyle}>Emprise d'analyse</label>
              <div style={{ display: "flex", gap: 6 }}>
                {[{ id: "map", label: "Vue carte", Icon: IcMap }, { id: "monde", label: "Zone mondiale", Icon: IcGlobe }, { id: "layer", label: "Couche", Icon: IcStack }].map(({ id, label, Icon }) => (
                  <button key={id} type="button" onClick={() => setZoneMode(id)} style={{ flex: 1, padding: "8px 4px", border: `0.5px solid ${zoneMode === id ? C.acc : C.bdr}`, background: zoneMode === id ? C.acc + "18" : "transparent", color: zoneMode === id ? C.acc : C.mut, borderRadius: 6, fontFamily: F, fontSize: 10, fontWeight: 600, cursor: "pointer", display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}><Icon size={15} />{label}</button>
                ))}
              </div>
              {zoneMode === "layer" && <select style={{ ...inputStyle, marginTop: 6 }} value={rLayerId} onChange={e => setRLayerId(e.target.value)}><option value="">Sélectionner une couche…</option>{layers.map(l => <option key={l.id} value={l.id}>{l.name || l.id}</option>)}</select>}
            </div>
            <button type="submit" disabled={rBusy || !wt.implemented} style={{ width: "100%", padding: "10px", marginTop: 4, background: (rBusy || !wt.implemented) ? C.dim : C.acc, color: (rBusy || !wt.implemented) ? C.txt : "#04120a", border: "none", borderRadius: 8, fontFamily: F, fontSize: 13, fontWeight: 600, cursor: (rBusy || !wt.implemented) ? "not-allowed" : "pointer", opacity: (rBusy || !wt.implemented) ? 0.6 : 1 }}>{rBusy ? "Calcul en cours…" : wt.implemented ? "Exécuter" : "Bientôt disponible"}</button>
          </form>
        )}
      </div>
    );
  };

  // ── Vue d'ensemble (les 3 sections) ──
  const renderOverview = () => (
    <div style={{ padding: 24, overflow: "auto" }}>
      <h3 style={{ fontSize: 16, fontWeight: 700, color: C.txt, margin: "0 0 4px" }}>Analyse spatiale</h3>
      <p style={{ fontSize: 12.5, color: C.dim, margin: "0 0 18px", lineHeight: 1.5 }}>Dépliez une famille (Vecteur, Raster, Avancé) dans l'arborescence à gauche, puis choisissez un outil. La recherche est en haut.</p>
      {sections.map(sec => (
        <div key={sec.id} style={{ marginBottom: 18 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
            <sec.Icon size={16} style={{ color: C.acc }} />
            <span style={{ fontSize: 13.5, fontWeight: 700, color: C.txt }}>{sec.label}</span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(190px, 1fr))", gap: 8 }}>
            {sec.groups.map(g => (
              <button key={g.key} onClick={() => { setExpandedSections(e => ({ ...e, [sec.id]: true })); setExpanded(e => ({ ...e, [g.key]: true })); }} style={{ textAlign: "left", border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: 11, background: C.input, cursor: "pointer", fontFamily: F }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: C.txt }}>{g.name}</div>
                <div style={{ fontSize: 10.5, color: C.dim, marginTop: 3 }}>{g.tools.length} outil{g.tools.length > 1 ? "s" : ""}</div>
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  );

  const renderDetail = () => {
    if (!selected) return renderOverview();
    if (selected.kind === "module") return <div style={{ height: "100%", overflow: "auto" }}>{renderModule(selected.module)}</div>;
    if (selected.kind === "vector") return renderVectorForm();
    if (selected.kind === "raster") return renderRasterForm();
    return renderOverview();
  };

  return (
    <div style={{ display: "flex", flexDirection: isMobile ? "column" : "row", height: "100%", minHeight: 0 }}>
      {renderSidebar()}
      <div style={{ flex: 1, minWidth: 0, overflow: "auto", display: "flex", flexDirection: "column" }}>{renderDetail()}</div>
    </div>
  );
}

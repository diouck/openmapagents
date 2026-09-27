/**
 * WhiteboxPanel.jsx — Module Whitebox unifié (Morphologie en MVP)
 * =============================================================================
 * Gauche : liste d'outils de la catégorie
 * Droite : définition de l'outil + formulaire paramètres dynamique
 * =============================================================================
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { WHITEBOX_TOOLS, WHITEBOX_TOOLS_BY_ID, getToolsByCategory, getCategoryName } from "../utils/whiteboxTools";
import { IcLoader, IcCheck, IcX } from "../icons";

export default function WhiteboxPanel({ category = "morphologie", onAddLayer, mapRef }) {
  const C = useThemeContext();
  const [selectedToolId, setSelectedToolId] = useState("slope");
  const [params, setParams] = useState({});
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [activeTab, setActiveTab] = useState("reglages"); // "reglages" ou "definition"
  const [zoneMode, setZoneMode] = useState("map"); // "map" ou "monde"
  const [bbox, setBbox] = useState({ south: "", west: "", north: "", east: "" });

  const tools = getToolsByCategory(category);
  const selectedTool = WHITEBOX_TOOLS_BY_ID[selectedToolId];

  if (!tools.length) {
    return (
      <div style={{ padding: 20, fontSize: 12, color: C.dim, textAlign: "center" }}>
        Aucun outil disponible pour cette catégorie
      </div>
    );
  }

  // Initialiser params pour l'outil sélectionné
  const handleSelectTool = (toolId) => {
    setSelectedToolId(toolId);
    setError(null);
    setSuccess(null);
    // Reset params
    const tool = WHITEBOX_TOOLS_BY_ID[toolId];
    const newParams = {};
    (tool.params || []).forEach(p => {
      newParams[p.id] = p.default;
    });
    setParams(newParams);
  };

  // Mettre à jour un paramètre
  const handleParamChange = (paramId, value) => {
    setParams(p => ({ ...p, [paramId]: value }));
  };

  // Récupérer bounds de la carte
  const fillBboxFromView = () => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const b = map.getBounds();
    setBbox({ south: b.getSouth().toFixed(4), west: b.getWest().toFixed(4), north: b.getNorth().toFixed(4), east: b.getEast().toFixed(4) });
  };

  // Exécuter l'outil (mock pour tester)
  const handleExecute = async () => {
    if (!selectedTool) return;
    setError(null);
    setSuccess(null);
    setRunning(true);

    try {
      // Déterminer la bbox à utiliser
      let bounds = null;
      if (zoneMode === "map") {
        const map = mapRef?.current?.getMap?.();
        if (map) {
          const b = map.getBounds();
          bounds = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
        }
      } else {
        // Zone mondiale
        bounds = [-180, -90, 180, 90];
      }

      // TODO: appel vrai à POST /api/whitebox/run
      // Pour MVP : mock delay + succès avec bbox réelle
      await new Promise(r => setTimeout(r, 2000));

      // Simulation : retourner un GeoJSON raster (TIF) ou vecteur
      const mockResult = {
        type: "FeatureCollection",
        features: [
          {
            type: "Feature",
            geometry: {
              type: "Polygon",
              coordinates: [[
                [bounds[0], bounds[1]], [bounds[2], bounds[1]],
                [bounds[2], bounds[3]], [bounds[0], bounds[3]],
                [bounds[0], bounds[1]]
              ]]
            },
            properties: { tool: selectedTool.id, zone: zoneMode }
          }
        ]
      };

      // Simuler onAddLayer
      if (onAddLayer) {
        onAddLayer(mockResult, `${selectedTool.name}_result`, "whitebox");
      }

      setSuccess(`✓ ${selectedTool.name} exécuté (${zoneMode === "map" ? "zone visible" : "zone mondiale"})`);
      setRunning(false);
    } catch (e) {
      setError(`Erreur : ${e.message}`);
      setRunning(false);
    }
  };

  const toolListStyle = {
    width: 240,
    borderRight: `0.5px solid ${C.bdr}`,
    overflow: "auto",
    padding: "16px 0",
    flexShrink: 0
  };

  const toolBtnStyle = (isActive) => ({
    width: "100%",
    padding: "10px 16px",
    margin: "4px 0",
    textAlign: "left",
    border: `0.5px solid ${isActive ? C.acc : C.bdr}`,
    borderRadius: 7,
    background: isActive ? C.acc + "18" : "transparent",
    color: isActive ? C.acc : C.mut,
    fontWeight: isActive ? 600 : 400,
    cursor: "pointer",
    fontFamily: F,
    fontSize: 13,
    transition: "all 0.2s"
  });

  const moduleViewStyle = {
    flex: 1,
    padding: "24px",
    overflow: "auto",
    display: "flex",
    flexDirection: "column",
    minWidth: 0
  };

  const defBoxStyle = {
    background: C.input,
    border: `0.5px solid ${C.bdr}`,
    borderRadius: 10,
    padding: "14px",
    marginBottom: "20px",
    fontSize: 12,
    color: C.txt,
    lineHeight: "1.6",
    whiteSpace: "pre-wrap",
    wordWrap: "break-word"
  };

  const paramGroupStyle = {
    marginBottom: "16px"
  };

  const labelStyle = {
    fontSize: 11,
    fontWeight: 600,
    color: C.txt,
    textTransform: "uppercase",
    marginBottom: "6px",
    letterSpacing: "0.5px"
  };

  const inputStyle = {
    width: "100%",
    padding: "8px 12px",
    border: `0.5px solid ${C.bdr}`,
    borderRadius: 7,
    background: C.input,
    color: C.txt,
    fontFamily: M,
    fontSize: 13,
    marginBottom: "8px",
    boxSizing: "border-box"
  };

  const btnExecuteStyle = {
    padding: "10px 16px",
    marginTop: "20px",
    background: running ? C.dim : C.acc,
    color: running ? C.txt : "#04120a",
    border: "none",
    borderRadius: 8,
    fontFamily: F,
    fontSize: 13,
    fontWeight: 600,
    cursor: running ? "not-allowed" : "pointer",
    opacity: running ? 0.6 : 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "8px"
  };

  const msgStyle = (type) => ({
    padding: "10px 12px",
    borderRadius: 6,
    fontSize: 12,
    marginBottom: "12px",
    border: `0.5px solid ${type === "error" ? "#f0a8a8" : "#a8f0c8"}`,
    background: type === "error" ? "rgba(240, 168, 168, 0.1)" : "rgba(168, 240, 200, 0.1)",
    color: type === "error" ? "#d85a30" : "#0F6E56"
  });

  return (
    <div style={{
      display: "flex",
      flexDirection: window.innerWidth < 500 ? "column" : "row",
      height: "100%",
      minHeight: 0
    }}>
      {/* ── GAUCHE : Liste d'outils ─ */}
      <div style={{
        ...toolListStyle,
        width: window.innerWidth < 500 ? "100%" : 240,
        borderRight: window.innerWidth < 500 ? "none" : `0.5px solid ${C.bdr}`,
        borderBottom: window.innerWidth < 500 ? `0.5px solid ${C.bdr}` : "none",
        maxHeight: window.innerWidth < 500 ? "150px" : "auto"
      }}>
        <div style={{ padding: "0 16px 12px", borderBottom: `0.5px solid ${C.bdr}`, marginBottom: "12px" }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: C.txt }}>
            {getCategoryName(category)}
          </div>
          <div style={{ fontSize: 11, color: C.dim, marginTop: "3px" }}>
            {tools.length} outil{tools.length > 1 ? "s" : ""}
          </div>
        </div>
        {tools.map(tool => (
          <button
            key={tool.id}
            onClick={() => handleSelectTool(tool.id)}
            style={{
              ...toolBtnStyle(selectedToolId === tool.id),
              marginLeft: "8px",
              marginRight: "8px"
            }}
          >
            {tool.name}
          </button>
        ))}
      </div>

      {/* ── DROITE : Détail + Formulaire ─ */}
      <div style={moduleViewStyle}>
        {selectedTool ? (
          <>
            {/* En-tête */}
            <div style={{ marginBottom: "16px" }}>
              <h3 style={{ fontSize: 16, fontWeight: 600, color: C.txt, margin: 0, marginBottom: "4px" }}>
                {selectedTool.name}
              </h3>
              <p style={{ fontSize: 12, color: C.dim, margin: 0 }}>
                {selectedTool.description}
              </p>
            </div>

            {/* Onglets */}
            <div style={{ display: "flex", gap: "8px", marginBottom: "16px", borderBottom: `0.5px solid ${C.bdr}`, paddingBottom: "8px" }}>
              <button
                onClick={() => setActiveTab("reglages")}
                style={{
                  padding: "6px 12px",
                  border: "none",
                  background: activeTab === "reglages" ? C.acc : "transparent",
                  color: activeTab === "reglages" ? "#fff" : C.mut,
                  borderRadius: 4,
                  fontFamily: F,
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: "pointer",
                  transition: "all 0.2s"
                }}
              >
                Réglages
              </button>
              <button
                onClick={() => setActiveTab("definition")}
                style={{
                  padding: "6px 12px",
                  border: "none",
                  background: activeTab === "definition" ? C.acc : "transparent",
                  color: activeTab === "definition" ? "#fff" : C.mut,
                  borderRadius: 4,
                  fontFamily: F,
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: "pointer",
                  transition: "all 0.2s"
                }}
              >
                Définition
              </button>
            </div>

            {/* Messages */}
            {error && <div style={msgStyle("error")}>{error}</div>}
            {success && <div style={msgStyle("success")}>{success}</div>}

            {/* Définition (onglet) */}
            {activeTab === "definition" && (
              <div style={defBoxStyle}>
                {selectedTool.definition}
              </div>
            )}

            {/* Formulaire paramètres (onglet Réglages) */}
            {activeTab === "reglages" && (
            <form onSubmit={(e) => { e.preventDefault(); handleExecute(); }}>
              {/* Entrée raster */}
              {selectedTool.inputs && selectedTool.inputs.map(input => (
                <div key={input.id} style={paramGroupStyle}>
                  <label style={labelStyle}>
                    {input.label} {input.required && "*"}
                  </label>
                  <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px 0" }}>
                    {input.description}
                  </p>
                  <select
                    style={inputStyle}
                    defaultValue="GEBCO_2024"
                    onChange={(e) => handleParamChange(input.id, e.target.value)}
                  >
                    <option value="">Sélectionner un raster...</option>
                    <option value="GEBCO_2024">GEBCO 2024 (monde, 15 arcsec)</option>
                    <option value="SRTM_30m">SRTM 30m (zone trouale)</option>
                    <option value="custom_dem">MNT personnalisé importé</option>
                  </select>
                </div>
              ))}

              {/* Paramètres */}
              {selectedTool.params && selectedTool.params.map(param => (
                <div key={param.id} style={paramGroupStyle}>
                  <label style={labelStyle}>{param.label}</label>
                  <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px 0" }}>
                    {param.description}
                  </p>
                  {param.type === "select" ? (
                    <select
                      style={inputStyle}
                      value={params[param.id] ?? param.default}
                      onChange={(e) => handleParamChange(param.id, e.target.value)}
                    >
                      {param.options.map(opt => (
                        <option key={opt} value={opt}>
                          {opt}
                        </option>
                      ))}
                    </select>
                  ) : param.type === "number" ? (
                    <input
                      type="number"
                      style={inputStyle}
                      value={params[param.id] ?? param.default}
                      onChange={(e) => handleParamChange(param.id, parseFloat(e.target.value))}
                      min={param.min}
                      max={param.max}
                      step={param.step || "0.1"}
                    />
                  ) : (
                    <input
                      type="text"
                      style={inputStyle}
                      value={params[param.id] ?? param.default}
                      onChange={(e) => handleParamChange(param.id, e.target.value)}
                      placeholder={param.default}
                    />
                  )}
                </div>
              ))}

              {/* Zone (Emprise) */}
              <div style={paramGroupStyle}>
                <label style={labelStyle}>Emprise d'analyse</label>
                <p style={{ fontSize: 11, color: C.dim, margin: "0 0 6px 0" }}>
                  Sélectionner la zone d'intérêt
                </p>
                <div style={{ display: "flex", gap: "6px" }}>
                  <button
                    type="button"
                    onClick={() => { setZoneMode("map"); fillBboxFromView(); }}
                    style={{
                      flex: 1,
                      padding: "8px",
                      border: `0.5px solid ${zoneMode === "map" ? C.acc : C.bdr}`,
                      background: zoneMode === "map" ? C.acc + "18" : "transparent",
                      color: zoneMode === "map" ? C.acc : C.mut,
                      borderRadius: 6,
                      fontFamily: F,
                      fontSize: 11,
                      fontWeight: 600,
                      cursor: "pointer",
                      transition: "all 0.2s"
                    }}
                  >
                    Zone sur la carte
                  </button>
                  <button
                    type="button"
                    onClick={() => setZoneMode("monde")}
                    style={{
                      flex: 1,
                      padding: "8px",
                      border: `0.5px solid ${zoneMode === "monde" ? C.acc : C.bdr}`,
                      background: zoneMode === "monde" ? C.acc + "18" : "transparent",
                      color: zoneMode === "monde" ? C.acc : C.mut,
                      borderRadius: 6,
                      fontFamily: F,
                      fontSize: 11,
                      fontWeight: 600,
                      cursor: "pointer",
                      transition: "all 0.2s"
                    }}
                  >
                    Zone mondiale
                  </button>
                </div>
                {zoneMode === "map" && bbox.south && (
                  <p style={{ fontSize: 10, color: C.dim, margin: "6px 0 0 0" }}>
                    S:{bbox.south}° W:{bbox.west}° N:{bbox.north}° E:{bbox.east}°
                  </p>
                )}
              </div>

              {/* Nom sortie */}
              <div style={paramGroupStyle}>
                <label style={labelStyle}>Nom fichier résultat</label>
                <input
                  type="text"
                  style={inputStyle}
                  defaultValue={selectedTool.outputs?.filename || "output"}
                  placeholder="Nom fichier"
                />
              </div>

              {/* Bouton exécuter */}
              <button
                type="submit"
                disabled={running}
                style={btnExecuteStyle}
              >
                {running ? (
                  <>
                    <span style={{ animation: "spin 1s linear infinite", display: "inline-block" }}>⟳</span>
                    Exécution en cours...
                  </>
                ) : (
                  "Exécuter"
                )}
              </button>
            </form>
            )}
          </>
        ) : (
          <div style={{ padding: 20, fontSize: 12, color: C.dim }}>Sélectionnez un outil</div>
        )}
      </div>

      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}

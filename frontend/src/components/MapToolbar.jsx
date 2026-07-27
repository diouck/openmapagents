import { useThemeContext } from "../theme";
import { F, M } from "../config";

const TOOL_GROUPS = [
  { label: "Sélection", tools: [
    { key: "pointer", label: "Sélection", icon: "▷" },
  ]},
  { label: "Mesure", tools: [
    { key: "measure_dist", label: "Distance", icon: "↔" },
    { key: "measure_area", label: "Surface", icon: "⬡" },
  ]},
  { label: "Dessin", tools: [
    { key: "buffer", label: "Buffer / zone tampon", icon: "◎" },
    { key: "draw", label: "Dessiner polygone", icon: "✎" },
  ]},
  { label: "Routing", tools: [
    { key: "route", label: "Itinéraire", icon: "⤳" },
    { key: "isochrone", label: "Isochrone", icon: "◉" },
  ]},
  { label: "Analyse", tools: [
    { key: "spatial", label: "Analyse spatiale", icon: "📊" },
    { key: "gee", label: "Google Earth Engine", icon: "🛰️" },
  ]},
  { label: "Données", tools: [
    { key: "ogc", label: "Services OGC (WMS/WFS/WMTS)", icon: "📡" },
    { key: "database", label: "Base de données", icon: "🗄" },
  ]},
  { label: "Export", tools: [
    { key: "print", label: "Impression PDF", icon: "⎙" },
  ]},
];

const ALL_TOOLS = TOOL_GROUPS.flatMap(g => g.tools);

export default function MapToolbar({
  activeTool,
  onTool,
  measureResult,
  bufferRadius,
  onBufferRadius,
}) {
  const C = useThemeContext();
  const activeDef = ALL_TOOLS.find(t => t.key === activeTool);

  return (
    <div style={{
      position: "absolute",
      top: 10,
      left: 10,
      zIndex: 20,
      width: 240,
      display: "flex",
      flexDirection: "column",
      gap: 6
    }}>

      {/* Tool panel */}
      <div style={{
        background: C.card + "ee",
        borderRadius: 10,
        border: `0.5px solid ${C.bdr}`,
        boxShadow: "0 6px 20px rgba(0,0,0,0.25)",
        backdropFilter: "blur(10px)",
        padding: 8,
      }}>

        {/* Title */}
        <div style={{
          fontSize: 11,
          fontFamily: F,
          color: C.dim,
          marginBottom: 6,
          textTransform: "uppercase",
          letterSpacing: "0.05em"
        }}>
          Outils
        </div>

        {/* Groups */}
        {TOOL_GROUPS.map(group => (
          <div key={group.label} style={{ marginBottom: 8 }}>

            {/* Group label */}
            <div style={{
              fontSize: 10,
              color: C.dim,
              marginBottom: 4,
              textTransform: "uppercase",
            }}>
              {group.label}
            </div>

            {/* Tools */}
            <div style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 4
            }}>
              {group.tools.map(t => (
                <button
                  key={t.key}
                  onClick={() => onTool(t.key)}
                  style={{
                    fontFamily: F,
                    fontSize: 11,
                    padding: "6px 8px",
                    borderRadius: 6,
                    border: `0.5px solid ${activeTool === t.key ? C.acc : C.bdr}`,
                    background: activeTool === t.key ? C.acc + "18" : C.bg,
                    color: activeTool === t.key ? C.acc : C.txt,
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "center",
                    gap: 4,
                    transition: "all 0.15s ease"
                  }}
                  onMouseEnter={e => {
                    if (activeTool !== t.key) e.currentTarget.style.background = C.hover;
                  }}
                  onMouseLeave={e => {
                    e.currentTarget.style.background =
                      activeTool === t.key ? C.acc + "18" : C.bg;
                  }}
                >
                  <span style={{ fontSize: 13 }}>{t.icon}</span>
                  <span>{t.label}</span>
                </button>
              ))}
            </div>
          </div>
        ))}

      </div>

      {/* Active tool info */}
      {activeTool !== "pointer" && activeDef && (
        <div style={{
          fontFamily: F,
          fontSize: 11,
          padding: "5px 10px",
          borderRadius: 6,
          background: C.acc + "18",
          color: C.acc,
          border: `0.5px solid ${C.acc}44`,
          backdropFilter: "blur(8px)",
          display: "flex",
          alignItems: "center",
          gap: 6,
        }}>
          <span>{activeDef.icon}</span>
          <span>{activeDef.label}</span>
          <button
            onClick={() => onTool("pointer")}
            style={{
              marginLeft: "auto",
              fontSize: 10,
              background: "none",
              border: "none",
              color: C.acc,
              cursor: "pointer",
              fontFamily: F,
            }}
          >
            ✕
          </button>
        </div>
      )}

      {/* Buffer controls */}
      {activeTool === "buffer" && (
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          padding: "6px 8px",
          background: C.card + "ee",
          borderRadius: 6,
          border: `0.5px solid ${C.bdr}`,
          backdropFilter: "blur(8px)"
        }}>
          <input
            type="range"
            min="100"
            max="5000"
            step="100"
            value={bufferRadius}
            onChange={e => onBufferRadius(parseInt(e.target.value))}
            style={{ width: 90 }}
          />
          <span style={{
            fontSize: 10,
            color: C.acc,
            fontFamily: M
          }}>
            {bufferRadius >= 1000
              ? `${bufferRadius / 1000} km`
              : `${bufferRadius} m`}
          </span>
        </div>
      )}

      {/* Measure result */}
      {measureResult && (
        <div style={{
          padding: "5px 10px",
          fontSize: 11,
          color: C.amb,
          fontFamily: M,
          background: C.card + "ee",
          borderRadius: 6,
          border: `0.5px solid ${C.bdr}`,
          backdropFilter: "blur(8px)"
        }}>
          {measureResult}
        </div>
      )}

    </div>
  );
}
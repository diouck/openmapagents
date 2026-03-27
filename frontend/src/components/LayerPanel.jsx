import { useState } from "react";
import { getTheme } from "../theme";
import { F, M, EXPORT_FORMATS } from "../config";
import { Badge, Btn } from "./ui";
import ClassPanel from "./ClassPanel";

export default function LayerPanel({ layers, onToggle, onRemove, onStyle, onExport, onClassify, onExportFmt, onRename, onMoveUp, onMoveDown, onZoomExtent }) {
  const C = getTheme();
  const [exp, setExp] = useState(null);
  const [editName, setEditName] = useState(null);

  if (!layers.length) return null;

  return (
    <div style={{ position: "absolute", top: 50, left: 10, zIndex: 10, width: 290, maxHeight: "55vh", overflowY: "auto", background: C.card + "ee", borderRadius: 10, border: `0.5px solid ${C.bdr}`, backdropFilter: "blur(8px)" }}>
      <div style={{ padding: "8px 12px", borderBottom: `0.5px solid ${C.bdr}`, fontSize: 12, fontWeight: 500, color: C.txt }}>
        Couches ({layers.length})
      </div>
      {layers.map(l => (
        <div key={l.id} style={{ borderBottom: `0.5px solid ${C.bdr}` }}>
          <div style={{ padding: "8px 12px", display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}
            onClick={() => setExp(exp === l.id ? null : l.id)}>
            <div style={{ width: 10, height: 10, borderRadius: 3, background: l.color, opacity: l.visible ? 1 : 0.3 }} />

            {/* Editable name */}
            {editName === l.id ? (
              <input autoFocus value={l.name}
                onChange={e => onRename(l.id, e.target.value)}
                onBlur={() => setEditName(null)}
                onKeyDown={e => e.key === "Enter" && setEditName(null)}
                onClick={e => e.stopPropagation()}
                style={{ fontFamily: F, fontSize: 12, padding: "2px 6px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.acc}`, outline: "none", flex: 1, minWidth: 0 }} />
            ) : (
              <span onDoubleClick={e => { e.stopPropagation(); setEditName(l.id); }}
                style={{ fontSize: 12, color: l.visible ? C.txt : C.dim, flex: 1, cursor: "text" }}
                title="Double-clic pour renommer">{l.name}</span>
            )}

            {/* Zoom sur emprise */}
            <button onClick={e => { e.stopPropagation(); onZoomExtent?.(l.id); }}
              title="Zoomer sur la couche"
              style={{ background: "none", border: "none", cursor: "pointer", fontSize: 12, padding: "0 2px", color: C.dim, lineHeight: 1, flexShrink: 0 }}>🔍</button>

            {/* Réordonner */}
            <button onClick={e => { e.stopPropagation(); onMoveUp?.(l.id); }}
              title="Monter"
              style={{ background: "none", border: "none", cursor: "pointer", fontSize: 11, padding: "0 1px", color: C.dim, lineHeight: 1, flexShrink: 0 }}>▲</button>
            <button onClick={e => { e.stopPropagation(); onMoveDown?.(l.id); }}
              title="Descendre"
              style={{ background: "none", border: "none", cursor: "pointer", fontSize: 11, padding: "0 1px", color: C.dim, lineHeight: 1, flexShrink: 0 }}>▼</button>

            <Badge color={C.mut}>{l.featureCount}</Badge>
            {l.classResult && <Badge color={C.acc}>classif.</Badge>}
            <Btn small onClick={e => { e.stopPropagation(); onToggle(l.id); }}>
              {l.visible ? "masquer" : "afficher"}
            </Btn>
          </div>

          {exp === l.id && (
            <div style={{ padding: "6px 10px 10px", display: "flex", flexDirection: "column", gap: 8 }}>
              {/* Couche raster WMS/WMTS : uniquement opacité + suppression */}
              {l.isRaster ? (
                <>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                    <span style={{ color: C.dim }}>Opacité</span>
                    <input type="range" min="0" max="1" step="0.05" value={l.opacity}
                      onChange={e => {
                        const op = parseFloat(e.target.value);
                        onStyle(l.id, { opacity: op });
                        // Appliquer directement sur la source MapLibre
                      }} style={{ flex: 1, height: 3 }} />
                    <span style={{ color: C.dim, fontFamily: M }}>{Math.round((l.opacity ?? 0.85) * 100)}%</span>
                  </div>
                  <div style={{ fontSize: 10, color: C.dim, fontFamily: M, wordBreak: "break-all", lineHeight: 1.4 }}>
                    {l.tileUrl?.slice(0, 80)}…
                  </div>
                  <div style={{ display: "flex", gap: 4 }}>
                    <Btn small color={C.red} onClick={() => onRemove(l.id)}>Suppr.</Btn>
                  </div>
                </>
              ) : (
              <>
              {/* Color + opacity */}
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                <span style={{ color: C.dim }}>Couleur</span>
                <input type="color" value={l.color} onChange={e => onStyle(l.id, { color: e.target.value })}
                  style={{ width: 24, height: 18, border: "none", borderRadius: 3, cursor: "pointer", background: "none" }} />
                <span style={{ color: C.dim }}>Opacité</span>
                <input type="range" min="0" max="1" step="0.1" value={l.opacity}
                  onChange={e => onStyle(l.id, { opacity: parseFloat(e.target.value) })} style={{ flex: 1, height: 3 }} />
              </div>
              {/* Size */}
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                <span style={{ color: C.dim }}>Taille</span>
                <input type="range" min="2" max="15" step="1" value={l.radius || 5}
                  onChange={e => onStyle(l.id, { radius: parseInt(e.target.value) })} style={{ flex: 1, height: 3 }} />
                <span style={{ color: C.dim, fontFamily: M }}>{l.radius || 5}px</span>
              </div>

              {/* Classification */}
              <ClassPanel layer={l} classification={l.classCfg} onChange={cfg => onClassify(l.id, cfg)} />

              {/* Actions row 1 — render modes */}
              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                <Btn small color={C.amb} active={l.heatmap} onClick={() => onStyle(l.id, { heatmap: !l.heatmap, extrude: false })}>
                  Heatmap
                </Btn>
                <Btn small color={C.blu} active={l.extrude} onClick={() => onStyle(l.id, { extrude: !l.extrude, heatmap: false })}>
                  3D
                </Btn>
                <Btn small color={C.pnk} active={l.cluster} onClick={() => onStyle(l.id, { cluster: !l.cluster })}>
                  Cluster
                </Btn>
                <Btn small color={C.mut} active={l.labels} onClick={() => onStyle(l.id, { labels: !l.labels })}>
                  Labels
                </Btn>
              </div>

              {/* 3D height attribute selector */}
              {l.extrude && (() => {
                const numAttrs = (l.geojson?.features || []).slice(0, 10).reduce((acc, f) => {
                  Object.entries(f.properties || {}).forEach(([k, v]) => {
                    if (typeof v === "number" && v > 0 && !["id"].includes(k)) acc.add(k);
                  });
                  return acc;
                }, new Set());
                return numAttrs.size > 0 ? (
                  <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
                    <span style={{ color: C.dim }}>Hauteur</span>
                    <select value={l.extrudeAttr || ""} onChange={e => onStyle(l.id, { extrudeAttr: e.target.value })}
                      style={{ fontFamily: F, fontSize: 10, padding: "3px 6px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", flex: 1 }}>
                      <option value="">auto (height)</option>
                      {[...numAttrs].map(a => <option key={a} value={a}>{a}</option>)}
                    </select>
                    <span style={{ color: C.dim }}>x</span>
                    <input type="range" min="1" max="20" step="1" value={l.extrudeScale || 1}
                      onChange={e => onStyle(l.id, { extrudeScale: parseInt(e.target.value) })} style={{ width: 50, height: 3 }} />
                    <span style={{ color: C.dim, fontFamily: M }}>{l.extrudeScale || 1}x</span>
                  </div>
                ) : null;
              })()}

              {/* Label attribute selector */}
              {l.labels && (() => {
                const txtAttrs = (l.geojson?.features || []).slice(0, 10).reduce((acc, f) => {
                  Object.entries(f.properties || {}).forEach(([k, v]) => {
                    if (v != null && v !== "" && !["id", "geom_json"].includes(k)) acc.add(k);
                  });
                  return acc;
                }, new Set());
                return (
                  <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
                    <span style={{ color: C.dim }}>Etiquette</span>
                    <select value={l.labelAttr || "name"} onChange={e => onStyle(l.id, { labelAttr: e.target.value })}
                      style={{ fontFamily: F, fontSize: 10, padding: "3px 6px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", flex: 1 }}>
                      {[...txtAttrs].map(a => <option key={a} value={a}>{a}</option>)}
                    </select>
                  </div>
                );
              })()}

              {/* Actions row 2 — export */}
              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                <Btn small color={C.acc} onClick={() => onExport(l.id)}>Export GeoJSON</Btn>
                <Btn small color={C.red} onClick={() => onRemove(l.id)}>Suppr.</Btn>
              </div>

              {/* Multi-format export */}
              <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
                {EXPORT_FORMATS.filter(f => f !== "GeoJSON").map(fmt => (
                  <Btn key={fmt} small onClick={() => onExportFmt(l.id, fmt)}>{fmt}</Btn>
                ))}
              </div>
            </>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

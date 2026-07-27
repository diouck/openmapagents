/**
 * EditorPanel.jsx — Éditeur vectoriel
 * - Créer une nouvelle couche (points / lignes / polygones) avec schéma de champs
 * - Éditer une couche existante (géométries + attributs)
 * - Table attributs inline éditable
 * - Modale attributs auto après chaque dessin (style QGIS)
 * - Ctrl+Z / Redo
 * - Export GeoJSON
 *
 * Dessin : @mapbox/mapbox-gl-draw pour les géométries
 *
 * Props :
 *   mapRef       — ref MapLibre
 *   layers       — couches vectorielles disponibles
 *   onSaveLayer  — (geojson, name, id?) → met à jour ou crée la couche
 *   onClose      — fermeture du panel
 */

import { useState, useEffect, useRef, useCallback } from "react";
import { createPortal } from "react-dom";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX, IcTrash, IcClipboard, IcSave, IcEdit, IcPlus, IcMapPin, IcSpline,
  IcHexagon, IcSearch, IcFile, IcArrow, IcUndo, IcRedo } from "../icons";

const FIELD_TYPES = ["text", "integer", "float", "boolean", "date"];

// ── Séparateur section ────────────────────────────────────────
function Sep({ children }) {
  const C = useThemeContext();
  return (
    <div style={{
      fontSize: 9, color: C.dim, textTransform: "uppercase",
      letterSpacing: ".06em", fontWeight: 600,
      borderBottom: `0.5px solid ${C.bdr}`, paddingBottom: 4, marginTop: 2,
    }}>{children}</div>
  );
}

// ── Éditeur de schéma de champs ───────────────────────────────
function SchemaEditor({ fields, onChange }) {
  const C = useThemeContext();
  const inp = {
    fontFamily: M, fontSize: 10, padding: "4px 6px", borderRadius: 4,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none",
  };
  const addField = () => onChange([...fields, { name: `champ${fields.length + 1}`, type: "text" }]);
  const removeField = (i) => onChange(fields.filter((_, fi) => fi !== i));
  const updateField = (i, key, val) => onChange(fields.map((f, fi) => fi === i ? { ...f, [key]: val } : f));
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      {fields.map((f, i) => (
        <div key={i} style={{ display: "flex", gap: 4, alignItems: "center" }}>
          <input value={f.name}
            onChange={e => updateField(i, "name", e.target.value.replace(/\s+/g, "_").toLowerCase())}
            placeholder="nom_champ" style={{ ...inp, flex: 2 }} />
          <select value={f.type} onChange={e => updateField(i, "type", e.target.value)} style={{ ...inp, flex: 1 }}>
            {FIELD_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <button onClick={() => removeField(i)} style={{
            padding: "4px 7px", borderRadius: 4,
            background: "transparent", border: `0.5px solid ${C.bdr}`,
            color: C.red, cursor: "pointer", flexShrink: 0, display: "flex",
          }}><IcX size={12}/></button>
        </div>
      ))}
      <button onClick={addField} style={{
        fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5,
        background: "transparent", border: `0.5px solid ${C.acc}55`,
        color: C.acc, cursor: "pointer", width: "100%",
      }}>+ Ajouter un champ</button>
    </div>
  );
}

// ── Table attributs éditable ──────────────────────────────────
function AttrTable({ features, fields, selectedId, onSelect, onDelete, onUpdate }) {
  const C = useThemeContext();
  if (!features.length) return (
    <div style={{ fontSize: 10, color: C.dim, padding: "8px", textAlign: "center" }}>
      Dessinez des entités sur la carte pour les voir ici
    </div>
  );
  const inp = {
    fontFamily: M, fontSize: 10, padding: "2px 5px", borderRadius: 3,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };
  return (
    <div style={{ overflowX: "auto", overflowY: "auto", maxHeight: 220 }}>
      <table style={{ borderCollapse: "collapse", fontSize: 10, width: "100%", minWidth: fields.length * 90 + 90 }}>
        <thead>
          <tr style={{ position: "sticky", top: 0, background: C.card, zIndex: 1 }}>
            <th style={{ padding: "4px 6px", borderBottom: `0.5px solid ${C.bdr}`, color: C.dim, fontWeight: 500, fontSize: 9, textAlign: "left", minWidth: 28 }}>#</th>
            <th style={{ padding: "4px 6px", borderBottom: `0.5px solid ${C.bdr}`, color: C.dim, fontWeight: 500, fontSize: 9, textAlign: "left" }}>Type</th>
            {fields.map(f => (
              <th key={f.name} style={{ padding: "4px 6px", borderBottom: `0.5px solid ${C.bdr}`, color: C.dim, fontWeight: 500, fontSize: 9, textAlign: "left", whiteSpace: "nowrap" }}>
                {f.name}<span style={{ color: C.dim, opacity: 0.6, marginLeft: 4 }}>{f.type}</span>
              </th>
            ))}
            <th style={{ padding: "4px 6px", borderBottom: `0.5px solid ${C.bdr}`, minWidth: 28 }} />
          </tr>
        </thead>
        <tbody>
          {features.map((feat, i) => {
            const isSelected = feat.id === selectedId;
            return (
              <tr key={feat.id} onClick={() => onSelect(feat.id)}
                style={{
                  borderBottom: `0.5px solid ${C.bdr}`,
                  background: isSelected ? C.acc + "18" : "transparent",
                  cursor: "pointer",
                }}
                onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = C.hover; }}
                onMouseLeave={e => { e.currentTarget.style.background = isSelected ? C.acc + "18" : "transparent"; }}>
                <td style={{ padding: "3px 6px", color: C.dim, fontFamily: M }}>{i + 1}</td>
                <td style={{ padding: "3px 6px", color: C.dim, fontSize: 9 }}>
                  {feat.geometry?.type?.replace("Multi", "M") || "?"}
                </td>
                {fields.map(f => (
                  <td key={f.name} style={{ padding: "2px 4px" }}>
                    <input value={feat.properties?.[f.name] ?? ""}
                      onClick={e => e.stopPropagation()}
                      onChange={e => onUpdate(feat.id, f.name, e.target.value)}
                      style={inp} />
                  </td>
                ))}
                <td style={{ padding: "2px 4px", textAlign: "center" }}>
                  <button
                    onClick={e => { e.stopPropagation(); onDelete(feat.id); }}
                    title="Supprimer cette entité"
                    style={{
                      padding: "3px 6px", borderRadius: 4,
                      background: "transparent", border: `0.5px solid ${C.bdr}`,
                      color: C.red, cursor: "pointer", display: "inline-flex",
                    }}><IcTrash size={12}/></button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
export default function EditorPanel({ mapRef, layers = [], onSaveLayer, onClose }) {
  const C = useThemeContext();

  const [mode,        setMode]        = useState("new");
  const [editLayerId, setEditLayerId] = useState("");
  const [layerName,   setLayerName]   = useState("Ma couche");
  const [geomType,    setGeomType]    = useState("Point");
  const [fields,      setFields]      = useState([
    { name: "id",          type: "integer" },
    { name: "categorie",   type: "text"    },
    { name: "description", type: "text"    },
  ]);

  const [features,   setFeatures]   = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [history,    setHistory]    = useState([]);
  const [future,     setFuture]     = useState([]);
  const [drawActive, setDrawActive] = useState(false);
  // drawMode : null | "drawing" | "select"
  const [drawMode,   setDrawMode]   = useState(null);
  const [status,     setStatus]     = useState(null);
  const [attrForm,   setAttrForm]   = useState(null); // { id, properties }

  const drawRef    = useRef(null);
  const drawLoaded = useRef(false);
  const geomTypeRef  = useRef(geomType);
  useEffect(() => { geomTypeRef.current = geomType; }, [geomType]);
  // Helper qui met à jour state ET ref de façon synchrone pour éviter les stale closures
  const setGeomTypeSync = useCallback((gt) => {
    geomTypeRef.current = gt;
    setGeomType(gt);
  }, []);
  const fieldsRef    = useRef(fields);
  useEffect(() => { fieldsRef.current = fields; }, [fields]);
  const featuresRef  = useRef(features);
  useEffect(() => { featuresRef.current = features; }, [features]);

  // ── Prochain ID auto-incrémental ──────────────────────────
  const nextAutoId = useCallback(() => {
    const idField = fieldsRef.current.find(f => f.name === "id" && f.type === "integer");
    if (!idField) return null;
    const max = featuresRef.current.reduce((acc, f) => {
      const v = parseInt(f.properties?.id, 10);
      return isNaN(v) ? acc : Math.max(acc, v);
    }, 0);
    return String(max + 1);
  }, []);

  // ── Charger couche à éditer ────────────────────────────────
  useEffect(() => {
    if (mode === "edit" && editLayerId) {
      const layer = layers.find(l => l.id === editLayerId);
      if (!layer?.geojson) return;
      const feats = (layer.geojson.features || []).map((f, i) => ({
        ...f, id: f.id || `feat_${i}_${Date.now()}`,
      }));
      setFeatures(feats);
      // Inférer le schéma depuis les propriétés existantes
      const sample = feats[0]?.properties || {};
      const inferred = Object.entries(sample).map(([name, val]) => ({
        name,
        type: typeof val === "number" ? (Number.isInteger(val) ? "integer" : "float") : "text",
      }));
      if (inferred.length) setFields(inferred);
      setLayerName(layer.name);
      // ── Détecter automatiquement le type de géométrie ────
      const rawGt = feats[0]?.geometry?.type || "Point";
      const detectedGt = rawGt.replace("Multi", "");
      setGeomTypeSync(detectedGt);
      // Passer en mode sélection et charger les features dans Draw
      setDrawMode("select");
      if (drawRef.current) {
        try {
          drawRef.current.set({ type: "FeatureCollection", features: feats });
          drawRef.current.changeMode("simple_select");
        } catch (_) {}
      }
    }
  }, [mode, editLayerId, layers]);

  // ── Init MapboxDraw ────────────────────────────────────────
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map || drawLoaded.current) return;

    import("@mapbox/mapbox-gl-draw").then(({ default: MapboxDraw }) => {
      if (drawLoaded.current || !map) return;

      const draw = new MapboxDraw({
        displayControlsDefault: false,
        snap: true,
        snapOptions: {
          snapPx: 10,
          snapToMidPoints: true,
          snapVertexPriorityDistance: 0.0025,
        },
        styles: [
          { id: "gl-draw-point",          type: "circle", filter: ["all", ["==", "$type", "Point"],      ["==", "meta", "feature"]],  paint: { "circle-radius": 6, "circle-color": ["case", ["==", ["get", "active"], "true"], "#1D9E75", "#378ADD"], "circle-stroke-width": 2, "circle-stroke-color": "#fff" } },
          { id: "gl-draw-line",           type: "line",   filter: ["all", ["==", "$type", "LineString"], ["!=", "mode", "static"]],   paint: { "line-color": ["case", ["==", ["get", "active"], "true"], "#1D9E75", "#378ADD"], "line-width": 2 } },
          { id: "gl-draw-polygon-fill",   type: "fill",   filter: ["all", ["==", "$type", "Polygon"],    ["!=", "mode", "static"]],   paint: { "fill-color": "#1D9E75", "fill-opacity": 0.15 } },
          { id: "gl-draw-polygon-stroke", type: "line",   filter: ["all", ["==", "$type", "Polygon"],    ["!=", "mode", "static"]],   paint: { "line-color": "#1D9E75", "line-width": 2 } },
          { id: "gl-draw-vertex",         type: "circle", filter: ["all", ["==", "meta", "vertex"],      ["==", "$type", "Point"]],   paint: { "circle-radius": 5, "circle-color": "#fff", "circle-stroke-color": "#1D9E75", "circle-stroke-width": 2 } },
          { id: "gl-draw-midpoint",       type: "circle", filter: ["all", ["==", "$type", "Point"],      ["==", "meta", "midpoint"]], paint: { "circle-radius": 3, "circle-color": "#EF9F27" } },
        ],
      });

      map.addControl(draw, "top-right");
      drawRef.current = draw;
      drawLoaded.current = true;

      // draw.create → ouvre la modale, NE supprime PAS la géométrie
      const onCreate = (e) => {
        const all = draw.getAll();
        const flds = fieldsRef.current;
        const newFeats = all.features.map(f => ({
          ...f,
          id: f.id || `feat_${Date.now()}_${Math.random().toString(36).slice(2)}`,
          properties: { ...Object.fromEntries(flds.map(fl => [fl.name, f.properties?.[fl.name] ?? ""])), ...(f.properties || {}) },
        }));
        setHistory(h => [...h, newFeats]);
        setFuture([]);
        setFeatures(newFeats);
        const created = e.features?.[0];
        if (created) {
          try { draw.changeMode("simple_select"); } catch (_) {}
          setDrawMode("select");
          const flds = fieldsRef.current;
          const autoId = nextAutoId();
          const initProps = Object.fromEntries(
            flds.map(fl => [fl.name, fl.name === "id" && fl.type === "integer" && autoId !== null ? autoId : (created.properties?.[fl.name] ?? "")])
          );
          setAttrForm({ id: created.id, properties: initProps });
        }
      };

      // draw.update → sync géométries
      const onUpdate = () => {
        const all = draw.getAll();
        const flds = fieldsRef.current;
        setFeatures(prev => all.features.map(f => {
          const existing = prev.find(p => p.id === f.id);
          return {
            ...f,
            properties: existing?.properties ?? Object.fromEntries(flds.map(fl => [fl.name, ""])),
          };
        }));
      };

      // draw.delete (touche Suppr native Draw)
      const onDeleteDraw = () => {
        const all = draw.getAll();
        setFeatures(all.features);
        setSelectedId(null);
      };

      map.on("draw.create", onCreate);
      map.on("draw.update", onUpdate);
      map.on("draw.delete", onDeleteDraw);
      map.on("draw.selectionchange", e => {
        setSelectedId(e.features?.[0]?.id || null);
      });

      // Lire l'état courant via refs (couche peut avoir été sélectionnée avant que Draw soit prêt)
      const currentFeatures = featuresRef.current;
      if (currentFeatures.length) {
        draw.set({ type: "FeatureCollection", features: currentFeatures });
      }
      setDrawActive(true);
    }).catch(() => {
      setStatus({ type: "error", msg: "Installez @mapbox/mapbox-gl-draw : npm install @mapbox/mapbox-gl-draw" });
    });

    return () => {
      const m = mapRef.current?.getMap?.();
      if (m && drawRef.current) {
        try { m.removeControl(drawRef.current); } catch (_) {}
        drawRef.current = null;
        drawLoaded.current = false;
      }
    };
  }, [mapRef]); // eslint-disable-line


  // ── Activer dessin ─────────────────────────────────────────
  const startDrawing = useCallback(() => {
    if (!drawRef.current) return;
    const gt = geomTypeRef.current;
    setDrawMode("drawing");
    if (gt === "Point")           drawRef.current.changeMode("draw_point");
    else if (gt === "LineString") drawRef.current.changeMode("draw_line_string");
    else                          drawRef.current.changeMode("draw_polygon");
  }, []);

  // ── Activer sélection ──────────────────────────────────────
  const startSelect = useCallback(() => {
    if (!drawRef.current) return;
    setDrawMode("select");
    try { drawRef.current.changeMode("simple_select"); } catch (_) {}
  }, []);

  // ── Supprimer une entité ───────────────────────────────────
  const deleteFeature = useCallback((idOverride) => {
    if (!drawRef.current) return;
    const targetId = idOverride || selectedId;
    if (!targetId) return;
    setHistory(h => [...h, features]);
    setFuture([]);
    drawRef.current.delete([targetId]);
    const all = drawRef.current.getAll();
    setFeatures(all.features);
    setSelectedId(null);
  }, [selectedId, features]);

  // ── Undo / Redo ────────────────────────────────────────────
  const undo = useCallback(() => {
    if (!history.length || !drawRef.current) return;
    const prev = history[history.length - 1];
    setFuture(f => [features, ...f]);
    setHistory(h => h.slice(0, -1));
    setFeatures(prev);
    drawRef.current.set({ type: "FeatureCollection", features: prev });
  }, [history, features]);

  const redo = useCallback(() => {
    if (!future.length || !drawRef.current) return;
    const next = future[0];
    setHistory(h => [...h, features]);
    setFuture(f => f.slice(1));
    setFeatures(next);
    drawRef.current.set({ type: "FeatureCollection", features: next });
  }, [future, features]);

  useEffect(() => {
    const handler = (e) => {
      if (attrForm) return;
      if ((e.ctrlKey || e.metaKey) && e.key === "z" && !e.shiftKey) { e.preventDefault(); undo(); }
      if ((e.ctrlKey || e.metaKey) && (e.key === "y" || (e.key === "z" && e.shiftKey))) { e.preventDefault(); redo(); }
      if ((e.key === "Delete" || e.key === "Backspace") && selectedId) { deleteFeature(); }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [undo, redo, deleteFeature, selectedId, attrForm]);

  // ── Mettre à jour un attribut ──────────────────────────────
  const updateAttr = useCallback((featId, fieldName, value) => {
    setFeatures(prev => prev.map(f => {
      if (f.id !== featId) return f;
      if (drawRef.current) {
        try { drawRef.current.setFeatureProperty(featId, fieldName, value); } catch (_) {}
      }
      return { ...f, properties: { ...f.properties, [fieldName]: value } };
    }));
  }, []);

  // ── Modale : Enregistrer → garde géom + attributs, reprend dessin ──
  const confirmAttrForm = useCallback(() => {
    if (!attrForm) return;
    const { id, properties } = attrForm;
    setFeatures(prev => prev.map(f => {
      if (f.id !== id) return f;
      if (drawRef.current) {
        Object.entries(properties).forEach(([k, v]) => {
          try { drawRef.current.setFeatureProperty(id, k, v); } catch (_) {}
        });
      }
      return { ...f, properties: { ...f.properties, ...properties } };
    }));
    setAttrForm(null);
    startDrawing();
  }, [attrForm, startDrawing]);

  // ── Modale : Passer → garde géom sans attributs, reprend dessin ──
  const skipAttrForm = useCallback(() => {
    if (!attrForm) return;
    setAttrForm(null);
    startDrawing();
  }, [attrForm, startDrawing]);

  // ── Modale : Annuler → supprime la géométrie ──────────────
  const cancelAttrForm = useCallback(() => {
    if (!attrForm) return;
    const { id } = attrForm;
    if (drawRef.current) {
      try { drawRef.current.delete([id]); } catch (_) {}
    }
    setFeatures(prev => prev.filter(f => f.id !== id));
    setAttrForm(null);
    setDrawMode("select");
  }, [attrForm]);

  // ── Sauvegarder comme couche ───────────────────────────────
  const save = useCallback(() => {
    if (!features.length) { setStatus({ type: "error", msg: "Aucune entité à enregistrer" }); return; }
    const normalized = features.map(f => ({
      type: "Feature", id: f.id, geometry: f.geometry,
      properties: {
        ...Object.fromEntries(fields.map(fl => [fl.name, f.properties?.[fl.name] ?? ""])),
        ...f.properties,
      },
    }));
    const gj = { type: "FeatureCollection", features: normalized };
    onSaveLayer?.(gj, layerName, mode === "edit" ? editLayerId : undefined);
    setStatus({ type: "ok", msg: `${normalized.length} entité${normalized.length > 1 ? "s" : ""} enregistrée${normalized.length > 1 ? "s" : ""} dans "${layerName}"` });
  }, [features, fields, layerName, mode, editLayerId, onSaveLayer]);

  // ── Export GeoJSON ─────────────────────────────────────────
  const exportGeoJSON = useCallback(() => {
    if (!features.length) return;
    const gj = JSON.stringify({ type: "FeatureCollection", features }, null, 2);
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([gj], { type: "application/json" }));
    a.download = `${layerName.replace(/\s+/g, "_")}.geojson`;
    a.click();
  }, [features, layerName]);

  // ── Zoom sur sélection ─────────────────────────────────────
  const zoomToSelected = useCallback(() => {
    if (!selectedId) return;
    const feat = features.find(f => f.id === selectedId);
    if (!feat?.geometry) return;
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    import("@turf/turf").then(({ bbox }) => {
      const b = bbox({ type: "FeatureCollection", features: [feat] });
      map.fitBounds([[b[0], b[1]], [b[2], b[3]]], { padding: 80, maxZoom: 18, duration: 600 });
    }).catch(() => {});
  }, [selectedId, features, mapRef]);

  // ── Dérivés ────────────────────────────────────────────────
  const editLayers  = layers.filter(l => !l.isRaster && l.geojson?.features?.length);
  const hasFeatures = features.length > 0;
  const canDelete   = !!selectedId;

  const inp = {
    fontFamily: M, fontSize: 10, padding: "5px 7px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };

  const drawStatusLabel = !drawActive ? "Chargement…"
    : drawMode === "drawing" ? "● Dessin en cours"
    : drawMode === "select"  ? "● Mode sélection"
    : "Prêt";
  const drawStatusColor = drawMode === "drawing" ? C.acc
    : drawMode === "select" ? C.amb : C.dim;

  // ── Modale attributs ──────────────────────────────────────
  const AttrFormModal = attrForm ? createPortal((() => {
    const fieldInp = {
      fontFamily: M, fontSize: 11, padding: "5px 8px", borderRadius: 5,
      background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
      outline: "none", width: "100%", boxSizing: "border-box",
    };
    return (
      <div style={{
        position: "fixed", inset: 0, zIndex: 99999,
        display: "flex", alignItems: "center", justifyContent: "center",
        background: "rgba(0,0,0,0.5)", backdropFilter: "blur(2px)",
      }}>
        <div style={{
          background: C.card, border: `1px solid ${C.bdr}`,
          borderRadius: 10, padding: "20px 22px", minWidth: 280, maxWidth: 400, width: "90%",
          boxShadow: "0 8px 32px rgba(0,0,0,.4)",
          display: "flex", flexDirection: "column", gap: 12,
        }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}><IcClipboard size={14}/> Attributs de l'entité</div>
            <button onClick={cancelAttrForm} title="Annuler et supprimer la géométrie" style={{
              fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
              background: C.red + "18", border: `0.5px solid ${C.red}44`,
              color: C.red, cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 4,
            }}><IcX size={11}/> Annuler</button>
          </div>
          <div style={{ fontSize: 10, color: C.dim, marginTop: -6 }}>
            Renseignez les attributs. <strong style={{ color: C.mut }}>Enregistrer</strong> reprend le dessin automatiquement.
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {fields.map((f, fi) => (
              <div key={f.name}>
                <div style={{ fontSize: 9, color: C.dim, marginBottom: 3, display: "flex", gap: 4 }}>
                  <span style={{ fontWeight: 600, color: C.mut }}>{f.name}</span>
                  <span style={{ opacity: 0.5 }}>{f.type}</span>
                </div>
                <input
                  autoFocus={fi === 0}
                  type={f.type === "integer" || f.type === "float" ? "number" : f.type === "date" ? "date" : "text"}
                  step={f.type === "float" ? "any" : undefined}
                  value={attrForm.properties[f.name] ?? ""}
                  onChange={e => setAttrForm(prev => ({
                    ...prev,
                    properties: { ...prev.properties, [f.name]: e.target.value },
                  }))}
                  onKeyDown={e => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) confirmAttrForm(); }}
                  placeholder={`Valeur ${f.type}…`}
                  style={fieldInp}
                />
              </div>
            ))}
          </div>
          <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
            <button onClick={confirmAttrForm} style={{
              fontFamily: F, flex: 2, fontSize: 11, fontWeight: 600,
              padding: "9px 0", borderRadius: 6, cursor: "pointer",
              background: C.acc, color: "#fff", border: "none",
              display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6,
            }}><IcSave size={13}/> Enregistrer</button>
            <button onClick={skipAttrForm} title="Garder la géométrie sans attributs" style={{
              fontFamily: F, flex: 1, fontSize: 11,
              padding: "9px 0", borderRadius: 6, cursor: "pointer",
              background: "transparent", color: C.dim,
              border: `0.5px solid ${C.bdr}`,
            }}>Passer</button>
          </div>
          <div style={{ fontSize: 9, color: C.dim, textAlign: "center", marginTop: -6 }}>
            Ctrl+Entrée · <span style={{ color: C.red + "bb", display: "inline-flex", alignItems: "center", gap: 3 }}><IcX size={10}/> Annuler supprime la géométrie</span>
          </div>
        </div>
      </div>
    );
  })(), document.body) : null;

  // ══════════════════════════════════════════════════════════
  return (
    <div style={{ display: "flex", flexDirection: "column", width: "100%", height: "100%", minHeight: 0, overflow: "hidden" }}>
      {AttrFormModal}

      {/* Header */}
      <div style={{
        padding: "5px 12px", borderBottom: `0.5px solid ${C.bdr}`,
        fontSize: 10, color: C.dim, flexShrink: 0,
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 5 }}><IcEdit size={12}/> Éditeur vectoriel</span>
        {drawActive && (
          <span style={{ fontSize: 9, color: drawStatusColor, fontWeight: 500 }}>{drawStatusLabel}</span>
        )}
      </div>

      <div style={{ flex: 1, minHeight: 0, overflowY: "auto", overflowX: "hidden", padding: "12px 14px", display: "flex", flexDirection: "column", gap: 10 }}>

        {/* Mode */}
        <div style={{ display: "flex", gap: 4 }}>
          {[["new", IcPlus, "Nouvelle couche"], ["edit", IcEdit, "Éditer couche"]].map(([m, Icon, lbl]) => (
            <button key={m} onClick={() => { setMode(m); setFeatures([]); setHistory([]); setFuture([]); setDrawMode(null); }} style={{
              fontFamily: F, flex: 1, fontSize: 10, padding: "6px 0", borderRadius: 5, cursor: "pointer",
              background: mode === m ? C.acc + "18" : "transparent",
              border: `0.5px solid ${mode === m ? C.acc + "55" : C.bdr}`,
              color: mode === m ? C.acc : C.dim, fontWeight: mode === m ? 600 : 400,
              display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 5,
            }}><Icon size={12}/> {lbl}</button>
          ))}
        </div>

        {/* Couche à éditer */}
        {mode === "edit" && (
          <>
            <Sep>Couche à éditer</Sep>
            {editLayers.length === 0
              ? <div style={{ fontSize: 9, color: C.amb, padding: "5px 8px", background: C.amb + "12", borderRadius: 5 }}>Aucune couche vectorielle disponible.</div>
              : <select value={editLayerId} onChange={e => setEditLayerId(e.target.value)}
                  style={{ fontFamily: F, fontSize: 10, padding: "5px 8px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${editLayerId ? C.acc + "66" : C.bdr}`, outline: "none", width: "100%" }}>
                  <option value="">— Choisir une couche —</option>
                  {editLayers.map(l => <option key={l.id} value={l.id}>{l.name} ({l.featureCount})</option>)}
                </select>
            }
          </>
        )}

        {/* Configuration */}
        <Sep>Configuration</Sep>
        <div>
          <div style={{ fontSize: 9, color: C.dim, marginBottom: 3 }}>Nom de la couche</div>
          <input value={layerName} onChange={e => setLayerName(e.target.value)} style={inp} />
        </div>

        {/* Type de géométrie — config seulement, n'active PAS le dessin */}
        {mode === "new" && (
          <div>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 3 }}>Type de géométrie</div>
            <div style={{ display: "flex", gap: 4 }}>
              {[["Point", IcMapPin, "Point"], ["LineString", IcSpline, "Ligne"], ["Polygon", IcHexagon, "Polygone"]].map(([t, Icon, lbl]) => (
                <button key={t} onClick={() => {
                  setGeomType(t);
                  // Repasse en sélection si on change de type en cours de dessin
                  if (drawMode === "drawing" && drawRef.current) {
                    try { drawRef.current.changeMode("simple_select"); } catch (_) {}
                    setDrawMode("select");
                  }
                }} style={{
                  fontFamily: F, fontSize: 10, flex: 1, padding: "6px 0", borderRadius: 5, cursor: "pointer",
                  background: geomType === t ? C.acc + "18" : "transparent",
                  border: `0.5px solid ${geomType === t ? C.acc + "55" : C.bdr}`,
                  color: geomType === t ? C.acc : C.dim, fontWeight: geomType === t ? 600 : 400,
                  display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 4,
                }}><Icon size={12}/> {lbl}</button>
              ))}
            </div>
          </div>
        )}

        {/* Schéma de champs */}
        <Sep>Champs / Attributs</Sep>
        <SchemaEditor fields={fields} onChange={setFields} />

        {/* Outils de dessin */}
        <Sep>Outils</Sep>
        {!drawActive ? (
          <div style={{ fontSize: 9, color: C.amb, padding: "5px 8px", background: C.amb + "12", borderRadius: 5 }}>
            Chargement de MapboxDraw…
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>

            {/* Ligne 1 : Dessiner + Sélectionner */}
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={startDrawing} style={{
                fontFamily: F, fontSize: 10, flex: 2, padding: "8px 0", borderRadius: 6, cursor: "pointer",
                background: drawMode === "drawing" ? C.acc + "20" : C.acc,
                border: `0.5px solid ${C.acc}`,
                color: drawMode === "drawing" ? C.acc : "#fff",
                fontWeight: 600,
                display: "flex", alignItems: "center", justifyContent: "center", gap: 5,
              }}>
                <IcEdit size={13}/>
                <span>{drawMode === "drawing"
                  ? "En cours…"
                  : `Dessiner ${geomType === "Point" ? "point" : geomType === "LineString" ? "ligne" : "polygone"}`
                }</span>
              </button>
              <button onClick={startSelect} style={{
                fontFamily: F, fontSize: 10, flex: 1, padding: "8px 0", borderRadius: 6, cursor: "pointer",
                background: drawMode === "select" ? C.amb + "20" : "transparent",
                border: `0.5px solid ${drawMode === "select" ? C.amb + "88" : C.bdr}`,
                color: drawMode === "select" ? C.amb : C.mut,
                display: "flex", alignItems: "center", justifyContent: "center", gap: 4,
              }}>
                <IcArrow size={12}/><span>Sélect.</span>
              </button>
            </div>

            {/* Ligne 2 : Supprimer sélection + Undo/Redo */}
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={() => deleteFeature()} disabled={!canDelete} title="Supprimer l'entité sélectionnée (Suppr)" style={{
                fontFamily: F, fontSize: 10, flex: 1, padding: "6px 0", borderRadius: 5,
                cursor: canDelete ? "pointer" : "default",
                background: canDelete ? C.red + "12" : "transparent",
                border: `0.5px solid ${canDelete ? C.red + "55" : C.bdr + "44"}`,
                color: canDelete ? C.red : C.dim,
                display: "flex", alignItems: "center", justifyContent: "center", gap: 5,
              }}>
                <IcTrash size={12}/> Supprimer{canDelete ? " la sélection" : ""}
              </button>
              <button onClick={undo} disabled={!history.length} title="Ctrl+Z" style={{
                flex: "0 0 36px", padding: "6px 0", borderRadius: 5,
                background: "transparent", border: `0.5px solid ${history.length ? C.bdr : C.bdr + "44"}`,
                color: history.length ? C.mut : C.dim, cursor: history.length ? "pointer" : "default",
                display: "flex", alignItems: "center", justifyContent: "center",
              }}><IcUndo size={14}/></button>
              <button onClick={redo} disabled={!future.length} title="Ctrl+Y" style={{
                flex: "0 0 36px", padding: "6px 0", borderRadius: 5,
                background: "transparent", border: `0.5px solid ${future.length ? C.bdr : C.bdr + "44"}`,
                color: future.length ? C.mut : C.dim, cursor: future.length ? "pointer" : "default",
                display: "flex", alignItems: "center", justifyContent: "center",
              }}><IcRedo size={14}/></button>
            </div>

            {/* Hint contextuel */}
            <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.5 }}>
              {drawMode === "drawing"
                ? geomType === "Point"      ? "Cliquez pour placer un point"
                : geomType === "LineString" ? "Cliquez pour tracer · double-clic pour terminer"
                :                            "Cliquez pour dessiner · double-clic pour fermer"
                : drawMode === "select"
                  ? "Cliquez une entité pour la sélectionner · Suppr pour supprimer"
                  : `${features.length} entité${features.length !== 1 ? "s" : ""} · Cliquez Dessiner pour commencer`
              }
            </div>
            {/* Badge snap actif */}
            <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 9, color: C.acc }}>
              <span style={{
                width: 6, height: 6, borderRadius: "50%", background: C.acc, display: "inline-block", flexShrink: 0,
              }} />
              Accrochage actif — s'accroche aux sommets et milieux existants
            </div>
          </div>
        )}

        {/* Table attributs */}
        {hasFeatures && (
          <>
            <Sep>Attributs — {features.length} entité{features.length > 1 ? "s" : ""}</Sep>
            {selectedId && (
              <button onClick={zoomToSelected} style={{
                fontFamily: F, fontSize: 9, padding: "3px 8px", borderRadius: 4, alignSelf: "flex-start",
                background: C.acc + "12", border: `0.5px solid ${C.acc}44`, color: C.acc, cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 5,
              }}><IcSearch size={11}/> Zoomer sur la sélection</button>
            )}
            <AttrTable
              features={features}
              fields={fields}
              selectedId={selectedId}
              onSelect={id => {
                setSelectedId(id);
                setDrawMode("select");
                if (drawRef.current) try { drawRef.current.changeMode("simple_select", { featureIds: [id] }); } catch (_) {}
              }}
              onDelete={deleteFeature}
              onUpdate={updateAttr}
            />
          </>
        )}

        {/* Status */}
        {status && (
          <div style={{
            fontSize: 10, padding: "6px 8px", borderRadius: 5, lineHeight: 1.5,
            background: (status.type === "ok" ? C.acc : C.red) + "15",
            border: `0.5px solid ${(status.type === "ok" ? C.acc : C.red)}44`,
            color: status.type === "ok" ? C.acc : C.red,
          }}>{status.msg}</div>
        )}

        {/* Actions */}
        <div style={{ marginTop: "auto", display: "flex", flexDirection: "column", gap: 6, paddingTop: 4 }}>
          <button onClick={save} disabled={!hasFeatures} style={{
            fontFamily: F, fontSize: 12, fontWeight: 600, padding: "10px 0", borderRadius: 7, width: "100%",
            background: hasFeatures ? C.acc : C.hover,
            color: hasFeatures ? "#fff" : C.dim,
            border: "none", cursor: hasFeatures ? "pointer" : "default",
            display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
          }}><IcSave size={14}/> Enregistrer comme couche</button>
          <button onClick={exportGeoJSON} disabled={!hasFeatures} style={{
            fontFamily: F, fontSize: 10, padding: "6px 0", borderRadius: 6, width: "100%",
            background: "transparent", border: `0.5px solid ${hasFeatures ? C.bdr : C.bdr + "44"}`,
            color: hasFeatures ? C.mut : C.dim, cursor: hasFeatures ? "pointer" : "default",
            display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
          }}><IcFile size={12}/> Exporter GeoJSON</button>
        </div>

        <div style={{ height: 8 }} />
      </div>
    </div>
  );
}

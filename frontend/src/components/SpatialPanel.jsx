import { useState, useMemo } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";
import { SPATIAL_OPS, SPATIAL_GROUPS, executeSpatialOp } from "../utils/spatial";
import { getLayerAttrs } from "../utils/classification";
import { Sel, Lbl } from "./ui";

// Icônes par groupe
const GROUP_ICONS = {
  "Overlay":       "⧉",
  "Proximité":     "◎",
  "Géométrie":     "△",
  "Mesure":        "📐",
  "Statistiques":  "📊",
  "Mobilité/Flux": "⤳",
  "Génération":    "✦",
};

export default function SpatialPanel({ layers, onAddLayer }) {
  const C = getTheme();
  const [opId,        setOpId]        = useState("intersection");
  const [layerAId,    setLayerAId]    = useState("");
  const [layerBId,    setLayerBId]    = useState("");
  const [params,      setParams]      = useState({});
  const [resultName,  setResultName]  = useState("");
  const [error,       setError]       = useState(null);
  const [running,     setRunning]     = useState(false);
  const [lastResult,  setLastResult]  = useState(null);
  // Groupes dépliés — tous ouverts par défaut
  const [openGroups, setOpenGroups]   = useState(() =>
    Object.fromEntries(SPATIAL_GROUPS.map(g => [g, false]))
  );

  const op      = SPATIAL_OPS.find(o => o.id === opId);
  const needsB  = op?.inputs?.length > 1;
  const layerA  = layers.find(l => l.id === layerAId);
  const layerB  = layers.find(l => l.id === layerBId);
  const attrsA  = useMemo(() => layerA ? getLayerAttrs(layerA) : { num: [], cat: [] }, [layerA]);

  const toggleGroup = (g) => setOpenGroups(p => ({ ...p, [g]: !p[g] }));

  const execute = () => {
    setError(null); setRunning(true); setLastResult(null);
    try {
      if (!layerA) throw new Error("Sélectionnez la couche A");
      if (needsB && !layerB) throw new Error("Sélectionnez la couche B");
      const p = {};
      (op.params || []).forEach(param => {
        const val = params[param.id] ?? param.default;
        p[param.id] = param.type === "number" ? (parseFloat(val) || param.default) : val;
      });
      const result = executeSpatialOp(opId, layerA, needsB ? layerB : null, p);
      if (!result?.features?.length) {
        setError("Aucun résultat — les couches ne se chevauchent peut-être pas");
        setRunning(false); return;
      }
      const name = resultName || `${op.name}_${layerA.name.slice(0, 12)}`;
      setLastResult({ count: result.features.length });
      onAddLayer(result, name, "analysis");
    } catch (e) { setError(e.message); }
    setRunning(false);
  };

  const inp = {
    fontFamily: M, fontSize: 10, padding: "4px 7px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };

  if (!layers.length) return (
    <div style={{ padding: 20, fontSize: 12, color: C.dim, textAlign: "center" }}>
      Chargez des couches vectorielles pour utiliser l'analyse spatiale
    </div>
  );

  return (
    <div style={{ display: "flex", height: "100%", width: "100%", minHeight: 0, overflow: "hidden" }}>

      {/* ── LISTE GAUCHE — groupes dépliables ─────────────── */}
      <div style={{
        width: 168, flexShrink: 0,
        overflowY: "auto", overflowX: "hidden",
        borderRight: `0.5px solid ${C.bdr}`,
      }}>
        {SPATIAL_GROUPS.map(group => {
          const ops   = SPATIAL_OPS.filter(o => o.group === group);
          const open  = openGroups[group];
          const hasSelected = ops.some(o => o.id === opId);
          return (
            <div key={group}>
              {/* En-tête groupe — cliquable */}
              <div
                onClick={() => toggleGroup(group)}
                style={{
                  display: "flex", alignItems: "center", gap: 5,
                  padding: "5px 8px 4px",
                  cursor: "pointer",
                  background: hasSelected ? C.acc + "0a" : "transparent",
                  borderLeft: hasSelected ? `2px solid ${C.acc}` : "2px solid transparent",
                  userSelect: "none",
                }}
              >
                <span style={{ fontSize: 11 }}>{GROUP_ICONS[group] || "•"}</span>
                <span style={{
                  flex: 1, fontSize: 9, fontWeight: 600, color: hasSelected ? C.acc : C.dim,
                  textTransform: "uppercase", letterSpacing: ".06em",
                }}>{group}</span>
                <span style={{ fontSize: 9, color: C.dim, opacity: 0.6 }}>{open ? "▾" : "▸"}</span>
              </div>

              {/* Opérations du groupe */}
              {open && ops.map(o => (
                <div
                  key={o.id}
                  onClick={() => { setOpId(o.id); setError(null); setLastResult(null); setParams({}); }}
                  style={{
                    padding: "3px 8px 3px 20px",
                    fontSize: 10, cursor: "pointer",
                    background: opId === o.id ? C.acc + "18" : "transparent",
                    color: opId === o.id ? C.acc : C.mut,
                    fontWeight: opId === o.id ? 500 : 400,
                    borderLeft: opId === o.id ? `2px solid ${C.acc}` : "2px solid transparent",
                    whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
                    lineHeight: 1.5,
                  }}
                  title={o.desc}
                >{o.name}</div>
              ))}
            </div>
          );
        })}
      </div>

      {/* ── PANNEAU DROIT — config + exécution ────────────── */}
      <div style={{
        flex: 1, overflowY: "auto", padding: "10px 12px",
        display: "flex", flexDirection: "column", gap: 8, minWidth: 0,
      }}>

        {/* Titre opération */}
        <div>
          <div style={{ fontSize: 12, fontWeight: 600, color: C.txt }}>{op?.name}</div>
          <div style={{ fontSize: 10, color: C.mut, marginTop: 1, lineHeight: 1.4 }}>{op?.desc}</div>
        </div>

        {/* Couche A */}
        <div>
          <Lbl>Couche A {op?.inputs?.[0] ? `— ${op.inputs[0]}` : ""}</Lbl>
          <Sel value={layerAId} onChange={v => { setLayerAId(v); setLastResult(null); }}
            options={[{ value: "", label: "-- Choisir --" }, ...layers.map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} />
        </div>

        {/* Couche B */}
        {needsB && (
          <div>
            <Lbl>Couche B {op?.inputs?.[1] ? `— ${op.inputs[1]}` : ""}</Lbl>
            <Sel value={layerBId} onChange={v => { setLayerBId(v); setLastResult(null); }}
              options={[{ value: "", label: "-- Choisir --" }, ...layers.filter(l => l.id !== layerAId).map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} />
          </div>
        )}

        {/* Paramètres */}
        {op?.params?.map(param => (
          <div key={param.id}>
            <Lbl>{param.label}</Lbl>
            {param.type === "number" ? (
              <input type="number" value={params[param.id] ?? param.default}
                onChange={e => setParams(p => ({ ...p, [param.id]: e.target.value }))}
                style={inp} />
            ) : param.type === "attribute" ? (
              <Sel value={params[param.id] ?? param.default}
                onChange={v => setParams(p => ({ ...p, [param.id]: v }))}
                options={[{ value: "", label: "-- Aucun --" }, ...[...attrsA.cat, ...attrsA.num].map(a => ({ value: a, label: a }))]} />
            ) : (
              <input value={params[param.id] ?? param.default}
                onChange={e => setParams(p => ({ ...p, [param.id]: e.target.value }))}
                style={inp} />
            )}
          </div>
        ))}

        {/* Nom résultat */}
        <div>
          <Lbl>Nom du résultat</Lbl>
          <input value={resultName} onChange={e => setResultName(e.target.value)}
            placeholder={`${op?.name || "résultat"}`}
            style={inp} />
        </div>

        {/* Erreur */}
        {error && (
          <div style={{ fontSize: 10, color: C.red, background: C.red + "12", padding: "5px 8px", borderRadius: 5, border: `0.5px solid ${C.red}33`, lineHeight: 1.5 }}>
            {error}
          </div>
        )}

        {/* Résultat */}
        {lastResult && (
          <div style={{ fontSize: 10, color: C.acc }}>
            ✓ {lastResult.count} features ajoutées
          </div>
        )}

        {/* Bouton exécuter — sticky en bas */}
        <div style={{ marginTop: "auto", paddingTop: 6 }}>
          <button onClick={execute} disabled={running || !layerAId} style={{
            fontFamily: F, fontSize: 11, fontWeight: 600, padding: "8px 0",
            borderRadius: 6, width: "100%",
            background: layerAId ? C.acc : C.hover,
            color: layerAId ? "#fff" : C.dim,
            border: "none", cursor: layerAId ? "pointer" : "default",
            opacity: running ? 0.6 : 1,
          }}>
            {running ? "⏳ Calcul…" : "▶ Exécuter"}
          </button>
        </div>
      </div>
    </div>
  );
}

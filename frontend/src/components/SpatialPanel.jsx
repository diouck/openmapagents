import { useState, useMemo } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";
import { SPATIAL_OPS, SPATIAL_GROUPS, executeSpatialOp } from "../utils/spatial";
import { getLayerAttrs } from "../utils/classification";
import { Sel, Lbl, Btn } from "./ui";

export default function SpatialPanel({ layers, onAddLayer }) {
  const C = getTheme();
  const [opId, setOpId] = useState("intersection");
  const [layerAId, setLayerAId] = useState("");
  const [layerBId, setLayerBId] = useState("");
  const [params, setParams] = useState({});
  const [resultName, setResultName] = useState("");
  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [lastResult, setLastResult] = useState(null);

  const op = SPATIAL_OPS.find(o => o.id === opId);
  const needsB = op?.inputs?.length > 1;
  const layerA = layers.find(l => l.id === layerAId);
  const layerB = layers.find(l => l.id === layerBId);

  // Auto-detect attributes for dissolve
  const attrsA = useMemo(() => layerA ? getLayerAttrs(layerA) : { num: [], cat: [] }, [layerA]);

  const execute = () => {
    setError(null); setRunning(true); setLastResult(null);
    try {
      if (!layerA) throw new Error("Sélectionnez la couche A");
      if (needsB && !layerB) throw new Error("Sélectionnez la couche B");

      // Build params
      const p = {};
      (op.params || []).forEach(param => {
        const val = params[param.id] ?? param.default;
        if (param.type === "number") p[param.id] = parseFloat(val) || param.default;
        else p[param.id] = val;
      });

      const result = executeSpatialOp(opId, layerA, needsB ? layerB : null, p);

      if (!result?.features?.length) {
        setError("Aucun résultat — les couches ne se chevauchent peut-être pas");
        setRunning(false);
        return;
      }

      const name = resultName || `${op.name}_${layerA.name.slice(0, 15)}`;
      setLastResult({ count: result.features.length });
      onAddLayer(result, name, "analysis");
    } catch (e) {
      setError(e.message);
    }
    setRunning(false);
  };

  if (!layers.length) return (
    <div style={{ padding: 16, fontSize: 12, color: C.dim, textAlign: "center" }}>
      Chargez des couches pour utiliser l'analyse spatiale
    </div>
  );

  return (
    <div style={{ display: "flex", gap: 0, height: "100%" }}>
      {/* Left: operation list */}
      <div style={{ width: 170, flexShrink: 0, overflowY: "auto", borderRight: `0.5px solid ${C.bdr}`, padding: "6px 0" }}>
        {SPATIAL_GROUPS.map(group => (
          <div key={group}>
            <div style={{ fontSize: 10, color: C.dim, fontWeight: 500, padding: "6px 10px 2px", textTransform: "uppercase", letterSpacing: "0.05em" }}>{group}</div>
            {SPATIAL_OPS.filter(o => o.group === group).map(o => (
              <div key={o.id} onClick={() => { setOpId(o.id); setError(null); setLastResult(null); }}
                style={{
                  padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 0,
                  background: opId === o.id ? C.acc + "15" : "transparent",
                  color: opId === o.id ? C.acc : C.mut,
                  fontWeight: opId === o.id ? 500 : 400,
                  borderLeft: opId === o.id ? `2px solid ${C.acc}` : "2px solid transparent",
                }}>{o.name}</div>
            ))}
          </div>
        ))}
      </div>

      {/* Right: config */}
      <div style={{ flex: 1, padding: 12, overflowY: "auto", display: "flex", flexDirection: "column", gap: 10 }}>
        {/* Op header */}
        <div>
          <div style={{ fontSize: 13, fontWeight: 500, color: C.txt }}>{op?.name}</div>
          <div style={{ fontSize: 11, color: C.mut, marginTop: 2 }}>{op?.desc}</div>
        </div>

        {/* Layer A */}
        <div>
          <Lbl>Couche A {op?.inputs?.[0] ? `(${op.inputs[0]})` : ""}</Lbl>
          <Sel value={layerAId} onChange={setLayerAId}
            options={[{ value: "", label: "-- Choisir --" }, ...layers.map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} />
        </div>

        {/* Layer B */}
        {needsB && (
          <div>
            <Lbl>Couche B {op?.inputs?.[1] ? `(${op.inputs[1]})` : ""}</Lbl>
            <Sel value={layerBId} onChange={setLayerBId}
              options={[{ value: "", label: "-- Choisir --" }, ...layers.filter(l => l.id !== layerAId).map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} />
          </div>
        )}

        {/* Params */}
        {op?.params?.map(param => (
          <div key={param.id}>
            <Lbl>{param.label}</Lbl>
            {param.type === "number" ? (
              <input type="number" value={params[param.id] ?? param.default}
                onChange={e => setParams(p => ({ ...p, [param.id]: e.target.value }))}
                style={{ fontFamily: M, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
            ) : param.type === "attribute" ? (
              <Sel value={params[param.id] ?? param.default}
                onChange={v => setParams(p => ({ ...p, [param.id]: v }))}
                options={[{ value: "", label: "-- Aucun (tous) --" }, ...[...attrsA.cat, ...attrsA.num].map(a => ({ value: a, label: a }))]} />
            ) : (
              <input value={params[param.id] ?? param.default}
                onChange={e => setParams(p => ({ ...p, [param.id]: e.target.value }))}
                style={{ fontFamily: F, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
            )}
          </div>
        ))}

        {/* Result name */}
        <div>
          <Lbl>Nom du résultat</Lbl>
          <input value={resultName} onChange={e => setResultName(e.target.value)}
            placeholder={`${op?.name}_résultat`}
            style={{ fontFamily: F, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
        </div>

        {/* Execute */}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <button onClick={execute} disabled={running || !layerAId}
            style={{
              fontFamily: F, fontSize: 12, fontWeight: 500, padding: "7px 20px", borderRadius: 6,
              background: layerAId ? C.acc : C.dim, color: "#fff", border: "none",
              cursor: layerAId ? "pointer" : "default", opacity: running ? 0.6 : 1,
            }}>
            {running ? "Calcul..." : "Exécuter"}
          </button>
          {lastResult && (
            <span style={{ fontSize: 11, color: C.acc }}>{lastResult.count} features ajoutées</span>
          )}
        </div>

        {/* Error */}
        {error && (
          <div style={{ fontSize: 11, color: C.red, background: C.red + "12", padding: "6px 10px", borderRadius: 6, border: `0.5px solid ${C.red}33` }}>
            {error}
          </div>
        )}
      </div>
    </div>
  );
}

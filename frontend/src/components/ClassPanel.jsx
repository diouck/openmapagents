import { useState, useMemo } from "react";
import { getTheme } from "../theme";
import { F, M, RAMPS } from "../config";
import { getLayerAttrs, getNumVals } from "../utils/classification";
import { Sel, Lbl } from "./ui";

export default function ClassPanel({ layer, classification, onChange }) {
  const C = getTheme();
  const attrs = useMemo(() => getLayerAttrs(layer), [layer]);
  const [type,   setType]   = useState(classification?.type   || "none");
  const [attr,   setAttr]   = useState(classification?.attribute || "");
  const [method, setMethod] = useState(classification?.method || "quantile");
  const [nc,     setNc]     = useState(classification?.nClasses || 5);
  const [ramp,   setRamp]   = useState(classification?.ramp   || "viridis");
  const [cb,     setCb]     = useState("");
  // Symboles proportionnels
  const [minSize, setMinSize] = useState(classification?.minSize ?? 3);
  const [maxSize, setMaxSize] = useState(classification?.maxSize ?? 30);

  const apply = () => {
    if (type === "none") { onChange(null); return; }
    onChange({
      type, attribute: attr, method, nClasses: parseInt(nc), ramp,
      customBreaks: cb ? cb.split(",").map(Number).filter(v => !isNaN(v)) : null,
      minSize: parseFloat(minSize) || 3,
      maxSize: parseFloat(maxSize) || 30,
    });
  };

  const allA = type === "categorized" ? attrs.cat : attrs.num;

  // Preview min/max pour l'attribut sélectionné
  const numStats = useMemo(() => {
    if (!attr || !layer) return null;
    const vals = getNumVals(layer, attr);
    if (!vals.length) return null;
    return { min: Math.min(...vals), max: Math.max(...vals) };
  }, [attr, layer]);

  const isProp = type === "proportional" || type === "proportional_line";

  return (
    <div style={{ background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>Classification</div>

      {/* Type */}
      <div>
        <Lbl>Type</Lbl>
        <Sel value={type} onChange={v => { setType(v); setAttr(""); if (v === "none") onChange(null); }} options={[
          { value: "none",               label: "Couleur unique" },
          { value: "categorized",        label: "Catégorisée" },
          { value: "graduated",          label: "Graduée (couleur)" },
          { value: "proportional",       label: "⬤ Symboles proportionnels" },
          { value: "proportional_line",  label: "━ Traits proportionnels" },
        ]} />
      </div>

      {/* Attribut */}
      {type !== "none" && (
        <div>
          <Lbl>Attribut {isProp ? "(numérique)" : ""}</Lbl>
          <Sel value={attr} onChange={setAttr}
            options={[{ value: "", label: "-- Choisir --" }, ...(isProp ? attrs.num : allA).map(a => ({ value: a, label: a }))]} />
        </div>
      )}

      {/* Options graduée */}
      {type === "graduated" && attr && (
        <>
          <div><Lbl>Méthode</Lbl><Sel value={method} onChange={setMethod} options={[
            { value: "quantile", label: "Quantile" }, { value: "jenks", label: "Jenks" },
            { value: "equal",    label: "Intervalles égaux" }, { value: "fixed", label: "Fixes" },
          ]} /></div>
          <div><Lbl>Classes</Lbl><Sel value={nc} onChange={setNc} options={[3,4,5,6,7,8,9,10].map(n => ({ value: String(n), label: `${n}` }))} /></div>
          {method === "fixed" && (
            <div><Lbl>Bornes</Lbl>
              <input value={cb} onChange={e => setCb(e.target.value)} placeholder="0,5,10,20"
                style={{ fontFamily: M, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
            </div>
          )}
        </>
      )}

      {/* Options symboles proportionnels */}
      {isProp && attr && (
        <>
          {numStats && (
            <div style={{ fontSize: 9, color: C.dim, background: C.hover, borderRadius: 4, padding: "3px 7px", lineHeight: 1.6 }}>
              Plage : <span style={{ color: C.txt, fontFamily: M }}>{numStats.min.toLocaleString("fr")} → {numStats.max.toLocaleString("fr")}</span>
            </div>
          )}
          <div style={{ display: "flex", gap: 6 }}>
            <div style={{ flex: 1 }}>
              <Lbl>{type === "proportional_line" ? "Épaisseur min (px)" : "Rayon min (px)"}</Lbl>
              <input type="number" min="1" max="20" value={minSize} onChange={e => setMinSize(e.target.value)}
                style={{ fontFamily: M, fontSize: 11, padding: "4px 7px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
            </div>
            <div style={{ flex: 1 }}>
              <Lbl>{type === "proportional_line" ? "Épaisseur max (px)" : "Rayon max (px)"}</Lbl>
              <input type="number" min="2" max="80" value={maxSize} onChange={e => setMaxSize(e.target.value)}
                style={{ fontFamily: M, fontSize: 11, padding: "4px 7px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
            </div>
          </div>

          {/* Preview visuel */}
          {numStats && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 0" }}>
              {type === "proportional" ? (
                <>
                  <svg width="60" height="40" viewBox="0 0 60 40">
                    <circle cx="10" cy="20" r={Math.max(2, parseFloat(minSize)||3)} fill={C.acc} opacity="0.8" />
                    <circle cx="38" cy="20" r={Math.max(2, Math.min(20, parseFloat(maxSize)||30) * 0.6)} fill={C.acc} opacity="0.8" />
                    <circle cx="58" cy="20" r={Math.max(2, Math.min(20, parseFloat(maxSize)||30))} fill={C.acc} opacity="0.8" />
                  </svg>
                  <span style={{ fontSize: 9, color: C.dim }}>min → max</span>
                </>
              ) : (
                <>
                  <svg width="60" height="40" viewBox="0 0 60 40">
                    <line x1="5" y1="20" x2="55" y2="20" stroke={C.acc} strokeWidth={Math.max(1, parseFloat(minSize)||1)} opacity="0.6" />
                    <line x1="5" y1="30" x2="55" y2="30" stroke={C.acc} strokeWidth={Math.max(1, (parseFloat(minSize)||1 + parseFloat(maxSize)||8) / 2)} opacity="0.75" />
                    <line x1="5" y1="38" x2="55" y2="38" stroke={C.acc} strokeWidth={Math.min(10, parseFloat(maxSize)||8)} opacity="0.9" />
                  </svg>
                  <span style={{ fontSize: 9, color: C.dim }}>min → max</span>
                </>
              )}
            </div>
          )}
        </>
      )}

      {/* Palette (couleur uniquement) */}
      {(type === "categorized" || type === "graduated") && attr && (
        <div><Lbl>Palette</Lbl>
          <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
            {Object.entries(RAMPS).map(([n, cols]) => (
              <button key={n} onClick={() => setRamp(n)} style={{
                width: 44, height: 12, borderRadius: 3,
                border: ramp === n ? `2px solid ${C.acc}` : `1px solid ${C.bdr}`,
                background: `linear-gradient(to right,${cols.slice(0, 5).join(",")})`,
                cursor: "pointer", padding: 0,
              }} />
            ))}
          </div>
        </div>
      )}

      {/* Appliquer */}
      {type !== "none" && attr && (
        <button onClick={apply} style={{
          fontFamily: F, fontSize: 11, fontWeight: 500, padding: "6px 12px", borderRadius: 6,
          background: C.acc, color: "#fff", border: "none", cursor: "pointer",
        }}>Appliquer</button>
      )}
    </div>
  );
}

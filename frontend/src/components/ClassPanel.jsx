import { useState, useMemo } from "react";
import { getTheme } from "../theme";
import { F, M, RAMPS } from "../config";
import { getLayerAttrs } from "../utils/classification";
import { Sel, Lbl } from "./ui";

export default function ClassPanel({ layer, classification, onChange }) {
  const C = getTheme();
  const attrs = useMemo(() => getLayerAttrs(layer), [layer]);
  const [type, setType] = useState(classification?.type || "none");
  const [attr, setAttr] = useState(classification?.attribute || "");
  const [method, setMethod] = useState(classification?.method || "quantile");
  const [nc, setNc] = useState(classification?.nClasses || 5);
  const [ramp, setRamp] = useState(classification?.ramp || "viridis");
  const [cb, setCb] = useState("");

  const apply = () => {
    if (type === "none") { onChange(null); return; }
    onChange({
      type, attribute: attr, method, nClasses: parseInt(nc), ramp,
      customBreaks: cb ? cb.split(",").map(Number).filter(v => !isNaN(v)) : null,
    });
  };

  const allA = type === "categorized" ? attrs.cat : attrs.num;

  return (
    <div style={{ background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>Classification</div>
      <div>
        <Lbl>Type</Lbl>
        <Sel value={type} onChange={v => { setType(v); setAttr(""); if (v === "none") onChange(null); }} options={[
          { value: "none", label: "Couleur unique" },
          { value: "categorized", label: "Catégorisée" },
          { value: "graduated", label: "Graduée" },
        ]} />
      </div>
      {type !== "none" && (
        <div><Lbl>Attribut</Lbl><Sel value={attr} onChange={setAttr} options={[{ value: "", label: "-- Choisir --" }, ...allA.map(a => ({ value: a, label: a }))]} /></div>
      )}
      {type === "graduated" && attr && (
        <>
          <div><Lbl>Méthode</Lbl><Sel value={method} onChange={setMethod} options={[
            { value: "quantile", label: "Quantile" }, { value: "jenks", label: "Jenks" },
            { value: "equal", label: "Intervalles égaux" }, { value: "fixed", label: "Fixes" },
          ]} /></div>
          <div><Lbl>Classes</Lbl><Sel value={nc} onChange={setNc} options={[3,4,5,6,7,8,9,10].map(n => ({ value: String(n), label: `${n}` }))} /></div>
          {method === "fixed" && (
            <div><Lbl>Bornes</Lbl><input value={cb} onChange={e => setCb(e.target.value)} placeholder="0,5,10,20"
              style={{ fontFamily: M, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} /></div>
          )}
        </>
      )}
      {type !== "none" && attr && (
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
      {type !== "none" && attr && (
        <button onClick={apply} style={{
          fontFamily: F, fontSize: 11, fontWeight: 500, padding: "6px 12px", borderRadius: 6,
          background: C.acc, color: "#fff", border: "none", cursor: "pointer",
        }}>Appliquer</button>
      )}
    </div>
  );
}

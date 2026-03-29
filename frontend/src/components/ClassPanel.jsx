import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, RAMPS } from "../config";
import { getLayerAttrs, getNumVals } from "../utils/classification";
import { MAKI_GROUPS, MAKI_PATHS } from "../utils/makiIcons";
import { makiToDataUrl, loadMakiIcon } from "../utils/makiLoader";
import { Sel, Lbl } from "./ui";

// Preview inline d'une icône Maki (SVG dans le DOM, sans map)
function MakiPreview({ name, color = "#1D9E75", size = 20 }) {
  const paths = MAKI_PATHS[name];
  if (!paths) return <span style={{ width: size, height: size, display: "inline-block" }} />;
  return (
    <svg width={size} height={size} viewBox="0 0 15 15" xmlns="http://www.w3.org/2000/svg"
      style={{ display: "inline-block", verticalAlign: "middle", flexShrink: 0 }}>
      {paths.map((d, i) => <path key={i} d={d} fill={color} />)}
    </svg>
  );
}

export default function ClassPanel({ layer, classification, onChange, mapRef }) {
  const C = useThemeContext();
  const attrs = useMemo(() => getLayerAttrs(layer), [layer]);

  const [type,    setType]    = useState(classification?.type    || "none");
  const [attr,    setAttr]    = useState(classification?.attribute || "");
  const [method,  setMethod]  = useState(classification?.method  || "quantile");
  const [nc,      setNc]      = useState(classification?.nClasses || 5);
  const [ramp,    setRamp]    = useState(classification?.ramp    || "viridis");
  const [cb,      setCb]      = useState("");
  const [minSize, setMinSize] = useState(classification?.minSize ?? 3);
  const [maxSize, setMaxSize] = useState(classification?.maxSize ?? 30);

  // Maki symbol
  const [makiName,  setMakiName]  = useState(classification?.makiName  || "marker");
  const [makiColor, setMakiColor] = useState(classification?.makiColor || "#ffffff");
  const [makiSize,  setMakiSize]  = useState(classification?.makiSize  || 30);
  const [makiGroup, setMakiGroup] = useState(Object.keys(MAKI_GROUPS)[0]);

  // Image custom upload
  const [customImage, setCustomImage] = useState(classification?.customImage || null);
  const [imageSize,   setImageSize]   = useState(classification?.imageSize   || 1);
  const [symbolMode,  setSymbolMode]  = useState(classification?.symbolMode  || "maki");

  const isProp   = type === "proportional" || type === "proportional_line";
  const isSymbol = type === "symbol";
  const allA     = type === "categorized" ? attrs.cat : attrs.num;

  const numStats = useMemo(() => {
    if (!attr || !layer) return null;
    const vals = getNumVals(layer, attr);
    return vals.length ? { min: Math.min(...vals), max: Math.max(...vals) } : null;
  }, [attr, layer]);

  // Charger l'icône Maki dans la map et retourner son imageId
  const getMakiImageId = useCallback(() => {
    if (!mapRef?.current) return null;
    const map = mapRef.current?.getMap?.();
    if (!map) return null;
    return loadMakiIcon(map, makiName, makiColor, makiSize);
  }, [mapRef, makiName, makiColor, makiSize]);

  const handleImageUpload = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const dataUrl = ev.target.result;
      const imgId   = `custom_${layer.id}_${Date.now()}`;
      const map = mapRef?.current?.getMap?.();
      if (map) {
        const img = new Image();
        img.onload = () => { if (map.hasImage(imgId)) map.removeImage(imgId); map.addImage(imgId, img); };
        img.src = dataUrl;
      }
      setCustomImage({ id: imgId, dataUrl });
    };
    reader.readAsDataURL(file);
  };

  const apply = () => {
    if (type === "none") { onChange(null); return; }
    let imageId = null;
    if (isSymbol && symbolMode === "maki") imageId = getMakiImageId();
    onChange({
      type, attribute: attr, method, nClasses: parseInt(nc), ramp,
      customBreaks: cb ? cb.split(",").map(Number).filter(v => !isNaN(v)) : null,
      minSize: parseFloat(minSize) || 3,
      maxSize: parseFloat(maxSize) || 30,
      // Symbol
      symbolMode,
      makiName, makiColor, makiSize: parseInt(makiSize),
      makiImageId: imageId,
      customImage, imageSize: parseFloat(imageSize) || 1,
    });
  };

  const inp = {
    fontFamily: M, fontSize: 11, padding: "5px 8px", borderRadius: 6,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };

  return (
    <div style={{ background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>Classification</div>

      {/* Type */}
      <div>
        <Lbl>Type</Lbl>
        <Sel value={type} onChange={v => { setType(v); setAttr(""); if (v === "none") onChange(null); }} options={[
          { value: "none",              label: "Couleur unique" },
          { value: "categorized",       label: "Catégorisée" },
          { value: "graduated",         label: "Graduée (couleur)" },
          { value: "proportional",      label: "⬤ Symboles proportionnels" },
          { value: "proportional_line", label: "━ Traits proportionnels" },
          { value: "symbol",            label: "🗺 Icône Maki / Image" },
        ]} />
      </div>

      {/* Attribut (non symbol) */}
      {type !== "none" && !isSymbol && (
        <div>
          <Lbl>Attribut {isProp ? "(numérique)" : ""}</Lbl>
          <Sel value={attr} onChange={setAttr}
            options={[{ value: "", label: "-- Choisir --" }, ...(isProp ? attrs.num : allA).map(a => ({ value: a, label: a }))]} />
        </div>
      )}

      {/* ══ SYMBOL / MAKI ══════════════════════════════════════ */}
      {isSymbol && (
        <>
          {/* Mode maki / image */}
          <div style={{ display: "flex", gap: 4 }}>
            {[["maki","🗺 Maki SVG"],["image","🖼 Image/PNG"]].map(([k,l]) => (
              <button key={k} onClick={() => setSymbolMode(k)} style={{
                fontFamily: F, fontSize: 10, padding: "4px 0", borderRadius: 4, flex: 1,
                background: symbolMode === k ? C.acc+"18" : "transparent",
                border: `0.5px solid ${symbolMode === k ? C.acc+"66" : C.bdr}`,
                color: symbolMode === k ? C.acc : C.dim, cursor: "pointer",
              }}>{l}</button>
            ))}
          </div>

          {/* ── Mode Maki ── */}
          {symbolMode === "maki" && (
            <>
              {/* Groupe */}
              <div>
                <Lbl>Catégorie</Lbl>
                <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
                  {Object.keys(MAKI_GROUPS).map(g => (
                    <button key={g} onClick={() => setMakiGroup(g)} style={{
                      fontFamily: F, fontSize: 9, padding: "2px 6px", borderRadius: 4,
                      background: makiGroup === g ? C.acc+"18" : "transparent",
                      border: `0.5px solid ${makiGroup === g ? C.acc+"55" : C.bdr}`,
                      color: makiGroup === g ? C.acc : C.dim, cursor: "pointer",
                    }}>{g}</button>
                  ))}
                </div>
              </div>

              {/* Grille d'icônes */}
              <div>
                <Lbl>Icône — <b style={{ color: C.acc }}>{makiName}</b></Lbl>
                <div style={{
                  display: "grid", gridTemplateColumns: "repeat(8, 1fr)", gap: 3,
                  background: C.hover, borderRadius: 6, padding: 6,
                  border: `0.5px solid ${C.bdr}`, maxHeight: 160, overflowY: "auto",
                }}>
                  {(MAKI_GROUPS[makiGroup] || []).map(name => (
                    <button key={name} onClick={() => setMakiName(name)}
                      title={name}
                      style={{
                        padding: 5, borderRadius: 4, cursor: "pointer",
                        background: makiName === name ? C.acc+"25" : "transparent",
                        border: makiName === name ? `1.5px solid ${C.acc}` : "1.5px solid transparent",
                        display: "flex", alignItems: "center", justifyContent: "center",
                      }}>
                      <MakiPreview name={name} color={makiColor} size={18} />
                    </button>
                  ))}
                </div>
              </div>

              {/* Couleur + taille */}
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <div>
                  <Lbl>Couleur</Lbl>
                  <input type="color" value={makiColor} onChange={e => setMakiColor(e.target.value)}
                    style={{ width: 36, height: 28, border: "none", borderRadius: 4, cursor: "pointer", background: "none", padding: 0 }} />
                </div>
                <div style={{ flex: 1 }}>
                  <Lbl>Taille : {makiSize}px</Lbl>
                  <input type="range" min="14" max="64" step="2" value={makiSize}
                    onChange={e => setMakiSize(e.target.value)}
                    style={{ width: "100%", height: 3 }} />
                </div>
                {/* Preview live */}
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
                  <div style={{ width: 40, height: 40, borderRadius: 6, background: C.hover, display: "flex", alignItems: "center", justifyContent: "center", border: `0.5px solid ${C.bdr}` }}>
                    <MakiPreview name={makiName} color={makiColor} size={24} />
                  </div>
                  <span style={{ fontSize: 8, color: C.dim }}>{makiName}</span>
                </div>
              </div>
            </>
          )}

          {/* ── Mode image custom ── */}
          {symbolMode === "image" && (
            <>
              <div>
                <Lbl>Fichier PNG / SVG / WebP</Lbl>
                <label style={{
                  display: "flex", alignItems: "center", gap: 8, padding: "7px 10px",
                  borderRadius: 6, background: C.hover, border: `0.5px dashed ${C.bdr}`,
                  cursor: "pointer", fontSize: 11, color: C.mut,
                }}>
                  {customImage
                    ? <><img src={customImage.dataUrl} style={{ width: 24, height: 24, objectFit: "contain" }} alt="" /> Remplacer l'icône</>
                    : "📂 Choisir un fichier"
                  }
                  <input type="file" accept=".svg,.png,.jpg,.jpeg,.webp" onChange={handleImageUpload}
                    style={{ display: "none" }} />
                </label>
              </div>
              {customImage && (
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <img src={customImage.dataUrl} style={{ width: 36, height: 36, objectFit: "contain", borderRadius: 4, border: `0.5px solid ${C.bdr}` }} alt="icon" />
                  <div style={{ flex: 1 }}>
                    <Lbl>Taille : {imageSize}x</Lbl>
                    <input type="range" min="0.2" max="3" step="0.1" value={imageSize}
                      onChange={e => setImageSize(e.target.value)}
                      style={{ width: "100%", height: 3 }} />
                  </div>
                </div>
              )}
            </>
          )}
        </>
      )}

      {/* ══ GRADUÉE ═════════════════════════════════════════════ */}
      {type === "graduated" && attr && (
        <>
          <div><Lbl>Méthode</Lbl><Sel value={method} onChange={setMethod} options={[
            { value: "quantile", label: "Quantile" }, { value: "jenks", label: "Jenks" },
            { value: "equal", label: "Intervalles égaux" }, { value: "fixed", label: "Fixes" },
          ]} /></div>
          <div><Lbl>Classes</Lbl><Sel value={nc} onChange={setNc} options={[3,4,5,6,7,8,9,10].map(n => ({ value: String(n), label: `${n}` }))} /></div>
          {method === "fixed" && (
            <div><Lbl>Bornes</Lbl><input value={cb} onChange={e => setCb(e.target.value)} placeholder="0,5,10,20" style={inp} /></div>
          )}
        </>
      )}

      {/* ══ PROPORTIONNEL ═══════════════════════════════════════ */}
      {isProp && attr && (
        <>
          {numStats && (
            <div style={{ fontSize: 9, color: C.dim, background: C.hover, borderRadius: 4, padding: "3px 7px", lineHeight: 1.6 }}>
              Plage : <span style={{ color: C.txt, fontFamily: M }}>{numStats.min.toLocaleString("fr")} → {numStats.max.toLocaleString("fr")}</span>
            </div>
          )}
          <div style={{ display: "flex", gap: 6 }}>
            <div style={{ flex: 1 }}>
              <Lbl>{type === "proportional_line" ? "Épais. min" : "Rayon min"} (px)</Lbl>
              <input type="number" min="1" max="20" value={minSize} onChange={e => setMinSize(e.target.value)} style={inp} />
            </div>
            <div style={{ flex: 1 }}>
              <Lbl>{type === "proportional_line" ? "Épais. max" : "Rayon max"} (px)</Lbl>
              <input type="number" min="2" max="80" value={maxSize} onChange={e => setMaxSize(e.target.value)} style={inp} />
            </div>
          </div>
          {numStats && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 0" }}>
              {type === "proportional" ? (
                <svg width="60" height="40" viewBox="0 0 60 40">
                  <circle cx="8"  cy="20" r={Math.max(2, parseFloat(minSize)||3)} fill={C.acc} opacity="0.8" />
                  <circle cx="32" cy="20" r={Math.max(2, Math.min(18, (parseFloat(minSize)||3 + parseFloat(maxSize)||30)/2))} fill={C.acc} opacity="0.8" />
                  <circle cx="54" cy="20" r={Math.max(2, Math.min(18, parseFloat(maxSize)||30))} fill={C.acc} opacity="0.8" />
                </svg>
              ) : (
                <svg width="60" height="36" viewBox="0 0 60 36">
                  <line x1="4" y1="10" x2="56" y2="10" stroke={C.acc} strokeWidth={Math.max(0.5, parseFloat(minSize)||1)} />
                  <line x1="4" y1="20" x2="56" y2="20" stroke={C.acc} strokeWidth={Math.max(0.5, (parseFloat(minSize)||1+parseFloat(maxSize)||8)/2)} />
                  <line x1="4" y1="30" x2="56" y2="30" stroke={C.acc} strokeWidth={Math.min(10, parseFloat(maxSize)||8)} />
                </svg>
              )}
              <span style={{ fontSize: 9, color: C.dim }}>min → max</span>
            </div>
          )}
        </>
      )}

      {/* ══ PALETTE COULEUR ════════════════════════════════════ */}
      {(type === "categorized" || type === "graduated") && attr && (
        <div><Lbl>Palette</Lbl>
          <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
            {Object.entries(RAMPS).map(([n, cols]) => (
              <button key={n} onClick={() => setRamp(n)} style={{
                width: 44, height: 12, borderRadius: 3,
                border: ramp === n ? `2px solid ${C.acc}` : `1px solid ${C.bdr}`,
                background: `linear-gradient(to right,${cols.slice(0,5).join(",")})`,
                cursor: "pointer", padding: 0,
              }} />
            ))}
          </div>
        </div>
      )}

      {/* Appliquer */}
      {type !== "none" && (isSymbol || attr) && (
        <button onClick={apply} style={{
          fontFamily: F, fontSize: 11, fontWeight: 500, padding: "6px 12px", borderRadius: 6,
          background: C.acc, color: "#fff", border: "none", cursor: "pointer",
        }}>Appliquer</button>
      )}
    </div>
  );
}

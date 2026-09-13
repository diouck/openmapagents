import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, RAMPS, RAMP_GROUPS, RAMP_NAMES, CAT_RAMPS } from "../config";
import { getLayerAttrs, getNumVals } from "../utils/classification";
import { MAKI_GROUPS, MAKI_PATHS } from "../utils/makiIcons";
import { makiToDataUrl, loadMakiIcon } from "../utils/makiLoader";
import { Sel, Lbl } from "./ui";
import ChartStyleBlock from "./ChartStyleBlock";
import { IcMap, IcImage, IcUpload, IcZap, IcTrash, IcPlus } from "../icons";

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

// dégradé CSS d'une rampe (catégorielle = bandes discrètes)
function rampCss(cols, key) {
  if (key && CAT_RAMPS.has(key)) {
    const n = cols.length;
    return `linear-gradient(90deg,${cols.map((c, i) => `${c} ${i / n * 100}%,${c} ${(i + 1) / n * 100}%`).join(",")})`;
  }
  return `linear-gradient(90deg,${cols.join(",")})`;
}

export default function ClassPanel({ layer, classification, onChange, onStyle, mapRef, chartCfg, onChartChange, onLayerOpacity }) {
  const C = useThemeContext();
  const attrs = useMemo(() => getLayerAttrs(layer), [layer]);
  const isVec = !layer?.isRaster && !!layer?.geojson;
  // Type de géométrie → contrôles adaptés (les points ont taille+marqueur ; les lignes
  // n'ont ni remplissage ni marqueur ; les polygones ont aplat + contour).
  const geomType = (layer?.geojson?.features || []).find(f => f?.geometry)?.geometry?.type || "";
  const isLine  = /LineString/i.test(geomType);
  const isPoly  = /Polygon/i.test(geomType);
  const isPointish = !isLine && !isPoly;   // point OU géométrie inconnue → contrôles point

  const [type,    setType]    = useState(classification?.type    || "none");
  const [attr,    setAttr]    = useState(classification?.attribute || "");
  const [method,  setMethod]  = useState(classification?.method  || "quantile");
  const [nc,      setNc]      = useState(classification?.nClasses || 5);
  const [ramp,    setRamp]    = useState(classification?.ramp    || classification?.palette || "viridis");
  const [invertRamp, setInvertRamp] = useState(classification?.invertRamp || false);
  const [cb,      setCb]      = useState("");
  const [minSize, setMinSize] = useState(classification?.minSize ?? 3);
  const [maxSize, setMaxSize] = useState(classification?.maxSize ?? 30);

  // Édition des classes : auto (méthode) | manual (édité)
  const [manualMode, setManualMode] = useState("auto");
  const [manual,     setManual]     = useState(null);

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

  const fmtN = n => (typeof n === "number" ? (Math.abs(n) >= 1000 ? Math.round(n).toLocaleString("fr") : (Math.round(n * 100) / 100).toLocaleString("fr")) : n);

  // Charger l'icône Maki dans la map et retourner son imageId
  const getMakiImageId = useCallback(() => {
    if (!mapRef?.current) return null;
    const map = mapRef.current?.getMap?.();
    if (!map) return null;
    if (!map.isStyleLoaded()) { console.warn("[ClassPanel] style pas encore chargé"); return null; }
    const imgId = loadMakiIcon(map, makiName, makiColor, parseInt(makiSize));
    if (!imgId) { console.warn("[ClassPanel] loadMakiIcon null pour", makiName); return null; }
    if (!map.hasImage(imgId)) return loadMakiIcon(map, makiName, makiColor, parseInt(makiSize));
    return imgId;
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

  // Construit la config de classification à partir de l'état (+ surcharges)
  const buildCfg = (extra = {}) => ({
    type, attribute: attr, method, nClasses: parseInt(nc), ramp,
    invertRamp,
    customBreaks: cb ? cb.split(",").map(Number).filter(v => !isNaN(v)) : null,
    minSize: parseFloat(minSize) || 3,
    maxSize: parseFloat(maxSize) || 30,
    symbolMode,
    makiName, makiColor, makiSize: parseInt(makiSize),
    customImage, imageSize: parseFloat(imageSize) || 1,
    manual: manualMode === "manual" ? manual : null,
    ...extra,
  });

  const applyCfg = (cfg) => {
    if (cfg.type === "none") { onChange(null); return; }
    if (cfg.type === "symbol" && cfg.symbolMode === "maki") {
      const map = mapRef?.current?.getMap?.();
      const tryApply = () => {
        const imgId = loadMakiIcon(map, cfg.makiName, cfg.makiColor, parseInt(cfg.makiSize));
        onChange({ ...cfg, makiImageId: imgId || null });
      };
      if (map && map.isStyleLoaded()) tryApply();
      else if (map) map.once("styledata", tryApply);
      else onChange({ ...cfg, makiImageId: null });
    } else {
      onChange(cfg);
    }
  };
  const apply  = () => applyCfg(buildCfg());
  // commit "live" pour les contrôles de classification (gradué / catégorisé)
  const commit = (extra = {}) => applyCfg(buildCfg(extra));

  // ── Édition manuelle des classes ────────────────────────────────
  const seedManual = () => {
    const cr = layer?.classResult;
    if (!cr) return null;
    if (cr.type === "graduated") {
      const cls = cr.classes || [];
      if (!cls.length) return null;
      return {
        type: "graduated",
        breaks: [cls[0].min, ...cls.map(c => c.max)],
        colors: cls.map(c => c.color),
        labels: cls.map((c, i) => c.label ?? `${fmtN(c.min)} – ${fmtN(c.max)}`),
      };
    }
    if (cr.type === "categorized") {
      return {
        type: "categorized",
        entries: (cr.entries || []).map(e => ({ value: e.value, color: e.color, label: e.label ?? String(e.value) })),
      };
    }
    return null;
  };
  const toManual = () => {
    const m = seedManual();
    if (!m) return;
    setManual(m); setManualMode("manual");
    commit({ manual: m });
  };
  const toAuto = () => {
    setManual(null); setManualMode("auto");
    commit({ manual: null });
  };
  const pushManual = (m) => { setManual(m); commit({ manual: m }); };

  const inp = {
    fontFamily: M, fontSize: 11, padding: "5px 8px", borderRadius: 6,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };
  // styles + fabriques du bloc « Symbole unique » (rendu façon maquette)
  const rowSt = { display: "flex", alignItems: "center", gap: 8, fontSize: 11 };
  const dimS  = { color: C.dim };
  const valS  = { color: C.dim, fontFamily: M };
  const swInp = { width: 26, height: 20, border: "none", borderRadius: 4, cursor: "pointer", background: "none", padding: 0, flexShrink: 0 };
  const fLbl  = { fontSize: 9.5, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4, fontWeight: 600 };
  // champ couleur : pastille (color-picker) + code hexa mono, comme la maquette
  const swatchField = (lbl, val, onCol) => (
    <div style={{ flex: 1, minWidth: 104 }}>
      <div style={fLbl}>{lbl}</div>
      <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
        <label style={{ position: "relative", width: 30, height: 24, borderRadius: 6, border: `0.5px solid ${C.bdr}`, background: val, cursor: "pointer", overflow: "hidden", flexShrink: 0 }}>
          <input type="color" value={val || "#000000"} onChange={e => onCol(e.target.value)} style={{ position: "absolute", inset: -6, width: "160%", height: "160%", opacity: 0, cursor: "pointer" }} />
        </label>
        <span style={{ fontFamily: M, fontSize: 10.5, color: C.mut }}>{(val || "").toUpperCase()}</span>
      </div>
    </div>
  );
  // champ numérique : curseur + valeur
  const stepField = (lbl, val, mn, mx, st, unit, onVal) => (
    <div style={{ flex: 1, minWidth: 104 }}>
      <div style={fLbl}>{lbl}</div>
      <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
        <input type="range" min={mn} max={mx} step={st} value={val} onChange={e => onVal(parseFloat(e.target.value))} style={{ flex: 1, height: 3 }} />
        <span style={{ fontFamily: M, fontSize: 11, color: C.mut, minWidth: 34, textAlign: "right" }}>{val}{unit}</span>
      </div>
    </div>
  );
  // aperçu du symbole (SVG) selon la géométrie
  const symbolPreview = () => {
    const fill = layer?.color || "#1D9E75", outline = layer?.outlineColor || "#000000", sw = layer?.strokeWidth ?? 1.5;
    if (isLine) return <svg width="52" height="52" viewBox="0 0 52 52"><line x1="7" y1="26" x2="45" y2="26" stroke={fill} strokeWidth={Math.max(1, layer?.strokeWidth ?? 2)} strokeLinecap="round" /></svg>;
    if (isPoly) return <svg width="52" height="52" viewBox="0 0 52 52"><rect x="8" y="12" width="36" height="28" rx="2" fill={fill} stroke={outline} strokeWidth={sw} /></svg>;
    const r = Math.max(6, (layer?.radius || 5) * 1.6), c = 26, shape = layer?.markerShape || "circle";
    let inner;
    if (shape === "square") inner = <rect x={c - r} y={c - r} width={r * 2} height={r * 2} />;
    else if (shape === "triangle") inner = <polygon points={`${c},${c - r} ${c + r * 0.92},${c + r * 0.8} ${c - r * 0.92},${c + r * 0.8}`} />;
    else if (shape === "diamond") inner = <polygon points={`${c},${c - r} ${c + r},${c} ${c},${c + r} ${c - r},${c}`} />;
    else inner = <circle cx={c} cy={c} r={r} />;
    return <svg width="52" height="52" viewBox="0 0 52 52"><g fill={fill} stroke={outline} strokeWidth={sw} strokeLinejoin="miter">{inner}</g></svg>;
  };
  const SHAPES = [["circle", "●", "Rond"], ["square", "■", "Carré"], ["triangle", "▲", "Triangle"], ["diamond", "◆", "Losange"]];
  // Forme des bords du trait/contour (bout + coin) : lignes & polygones
  const STROKE_SHAPES = [["butt", "miter", "Angles droits"], ["round", "round", "Arrondi"], ["square", "miter", "Carré"]];

  // types de rendu (segmenté façon QGIS)
  const RENDER_TYPES = [
    { v: "none",              label: "Couleur unique" },
    { v: "categorized",       label: "Catégorisée" },
    { v: "graduated",         label: "Graduée" },
    { v: "proportional",      label: "Proportionnels" },
    { v: "proportional_line", label: "Traits prop." },
    { v: "symbol",            label: "Icône" },
  ];

  // lignes de légende affichées (source de vérité = manual si édité, sinon classResult)
  const cr = layer?.classResult;
  const isGrad = type === "graduated";
  const isCat  = type === "categorized";
  const legendRows = (() => {
    if (manualMode === "manual" && manual) {
      if (manual.type === "graduated")
        return manual.colors.map((col, i) => ({ color: col, min: manual.breaks[i], max: manual.breaks[i + 1], label: manual.labels[i] }));
      return (manual.entries || []).map(e => ({ color: e.color, value: e.value, label: e.label }));
    }
    if (!cr) return [];
    return cr.type === "categorized" ? (cr.entries || []) : (cr.classes || []);
  })();
  const legIsCat = manualMode === "manual" && manual ? manual.type === "categorized" : cr?.type === "categorized";

  // ── UI atoms ────────────────────────────────────────────────────
  const Seg = ({ options, value, onPick, small, block }) => (
    <div style={{ display: block ? "flex" : "inline-flex", width: block ? "100%" : undefined, flexWrap: "wrap", gap: 2, padding: 3, borderRadius: 8,
      background: C.hover, border: `0.5px solid ${C.bdr}`, boxSizing: "border-box" }}>
      {options.map(o => {
        const on = value === o.v;
        return (
          <button key={o.v} onClick={() => onPick(o.v)} style={{
            fontFamily: F, fontSize: small ? 10.5 : 11.5, fontWeight: on ? 600 : 500,
            padding: small ? "4px 9px" : "5px 8px", borderRadius: 6, border: "none", cursor: "pointer",
            flex: block ? "1 1 auto" : undefined, whiteSpace: "nowrap",
            background: on ? C.bg : "transparent", color: on ? C.txt : C.dim,
            boxShadow: on ? "0 1px 2px rgba(0,0,0,.12)" : "none",
          }}>{o.label}</button>
        );
      })}
    </div>
  );

  const grammarBox = (children, title) => (
    <div style={{ border: `1px solid ${C.acc}55`, borderRadius: 10, overflow: "hidden", background: C.bg }}>
      <div style={{ background: C.acc + "18", padding: "6px 11px", fontSize: 10, fontWeight: 600, color: C.acc,
        display: "flex", alignItems: "center", gap: 6, letterSpacing: ".02em" }}>
        <span style={{ fontSize: 11 }}>◍</span> {title}
      </div>
      <div style={{ padding: 11, display: "flex", flexDirection: "column", gap: 10 }}>{children}</div>
    </div>
  );

  // grille de rampes groupées (Séquentiel / Divergent / Catégoriel)
  const rampGrid = (onlyCat) => (
    <div>
      <Lbl>Rampe de couleurs</Lbl>
      {/* aperçu palette active (avec inversion) */}
      {(() => {
        const cols = RAMPS[ramp] || [];
        const preview = invertRamp ? [...cols].reverse() : cols;
        return (
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
            <div style={{ flex: 1, height: 10, borderRadius: 4, border: `0.5px solid ${C.bdr}`, background: rampCss(preview, ramp) }} />
            <button onClick={() => { setInvertRamp(v => !v); commit({ invertRamp: !invertRamp }); }}
              title="Inverser la palette" style={{
                fontFamily: F, fontSize: 9.5, padding: "2px 7px", borderRadius: 4, cursor: "pointer",
                background: invertRamp ? C.acc + "22" : "transparent",
                border: `0.5px solid ${invertRamp ? C.acc + "88" : C.bdr}`, color: invertRamp ? C.acc : C.dim,
              }}>↕ {invertRamp ? "Inversée" : "Inverser"}</button>
          </div>
        );
      })()}
      <div style={{ maxHeight: 138, overflowY: "auto", border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: 8, background: C.hover }}>
        {Object.entries(RAMP_GROUPS)
          .filter(([g]) => !onlyCat || g === "Catégoriel")
          .map(([grp, keys]) => (
            <div key={grp} style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 8.5, textTransform: "uppercase", letterSpacing: ".06em", color: C.dim, fontWeight: 600, marginBottom: 5 }}>{grp}</div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {keys.filter(k => RAMPS[k]).map(k => (
                  <button key={k} onClick={() => { setRamp(k); commit({ ramp: k }); }} title={RAMP_NAMES[k] || k}
                    style={{ padding: 2, borderRadius: 6, cursor: "pointer", lineHeight: 0,
                      border: ramp === k ? `2px solid ${C.acc}` : "2px solid transparent" }}>
                    <span style={{ display: "block", width: 60, height: 14, borderRadius: 4,
                      border: "0.5px solid rgba(0,0,0,.12)", background: rampCss(RAMPS[k], k) }} />
                  </button>
                ))}
              </div>
            </div>
          ))}
      </div>
    </div>
  );

  // tableau de légende éditable (couleur / plage-valeur / étiquette)
  const legendTable = () => {
    if (!legendRows.length) return null;
    const editable = manualMode === "manual" && manual;
    return (
      <div style={{ border: `0.5px solid ${C.bdr}`, borderRadius: 8, overflow: "hidden" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 8.5, textTransform: "uppercase",
          letterSpacing: ".04em", color: C.dim, background: C.hover, padding: "5px 9px", fontWeight: 600 }}>
          <span style={{ width: 22 }} />
          <span style={{ flex: 1.1 }}>{legIsCat ? "Valeur" : editable ? "Borne haute" : "Plage"}</span>
          <span style={{ flex: 1.3 }}>Étiquette</span>
          <span style={{ width: 20 }} />
        </div>
        {legendRows.slice(0, 60).map((e, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 9px", borderTop: `0.5px solid ${C.bdr}` }}>
            {/* pastille couleur (éditable) */}
            <label style={{ position: "relative", width: 22, height: 16, borderRadius: 4, flexShrink: 0,
              border: `0.5px solid ${C.bdr}`, background: e.color, cursor: "pointer", overflow: "hidden" }}>
              <input type="color" value={e.color || "#888888"}
                onChange={ev => {
                  let m = editable ? manual : seedManual();
                  if (!m) return;
                  if (m.type === "graduated") m = { ...m, colors: m.colors.map((c, j) => j === i ? ev.target.value : c) };
                  else m = { ...m, entries: m.entries.map((en, j) => j === i ? { ...en, color: ev.target.value } : en) };
                  if (!editable) setManualMode("manual");
                  pushManual(m);
                }}
                style={{ position: "absolute", inset: -6, width: "160%", height: "160%", opacity: 0, cursor: "pointer" }} />
            </label>
            {/* valeur / plage / borne haute */}
            {legIsCat ? (
              <span style={{ flex: 1.1, fontFamily: M, fontSize: 10.5, color: C.txt, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {String(e.value ?? "—")}
              </span>
            ) : editable ? (
              <input defaultValue={fmtN(e.max)} key={`b${i}-${e.max}`}
                onBlur={ev => {
                  const v = parseFloat(ev.target.value.replace(/\s/g, "").replace(",", "."));
                  if (isNaN(v)) return;
                  const m = { ...manual, breaks: manual.breaks.map((b, j) => j === i + 1 ? v : b) };
                  pushManual(m);
                }}
                style={{ ...inp, flex: 1.1, padding: "3px 6px", fontSize: 10.5 }} />
            ) : (
              <span style={{ flex: 1.1, fontFamily: M, fontSize: 10.5, color: C.txt }}>
                {fmtN(e.min)} – {fmtN(e.max)}
              </span>
            )}
            {/* étiquette */}
            {editable ? (
              <input defaultValue={e.label ?? ""} key={`l${i}-${e.label}`}
                onBlur={ev => {
                  let m;
                  if (manual.type === "graduated") m = { ...manual, labels: manual.labels.map((l, j) => j === i ? ev.target.value : l) };
                  else m = { ...manual, entries: manual.entries.map((en, j) => j === i ? { ...en, label: ev.target.value } : en) };
                  pushManual(m);
                }}
                style={{ ...inp, flex: 1.3, padding: "3px 6px", fontSize: 11, color: C.mut }} />
            ) : (
              <span style={{ flex: 1.3, fontSize: 11, color: C.mut, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {e.label ?? (legIsCat ? String(e.value ?? "") : `Classe ${i + 1}`)}
              </span>
            )}
            {/* effectif ou suppression */}
            {editable ? (
              <button title="Supprimer la classe" onClick={() => {
                let m;
                if (manual.type === "graduated") {
                  if (manual.colors.length <= 2) return;
                  m = { ...manual,
                    colors: manual.colors.filter((_, j) => j !== i),
                    labels: manual.labels.filter((_, j) => j !== i),
                    breaks: manual.breaks.filter((_, j) => j !== i + 1) };
                } else {
                  if (manual.entries.length <= 1) return;
                  m = { ...manual, entries: manual.entries.filter((_, j) => j !== i) };
                }
                pushManual(m);
              }} style={{ width: 20, height: 20, border: "none", background: "transparent", color: C.dim, cursor: "pointer",
                borderRadius: 4, display: "grid", placeItems: "center" }}>
                <IcTrash size={12} />
              </button>
            ) : (
              <span style={{ width: 20, textAlign: "right", fontFamily: M, fontSize: 9, color: C.dim }}>
                {e.count != null ? e.count : ""}
              </span>
            )}
          </div>
        ))}
        {editable && !legIsCat && (
          <button onClick={() => {
            const last = manual.breaks[manual.breaks.length - 1];
            const cols = RAMPS[ramp] || RAMPS.viridis;
            const m = { ...manual,
              colors: [...manual.colors, cols[cols.length - 1]],
              labels: [...manual.labels, "Nouvelle classe"],
              breaks: [...manual.breaks, last] };
            pushManual(m);
          }} style={{ width: "100%", padding: "5px 0", border: "none", borderTop: `0.5px dashed ${C.bdr}`,
            background: "transparent", color: C.mut, cursor: "pointer", fontFamily: F, fontSize: 11, fontWeight: 600,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}>
            <IcPlus size={12} /> Ajouter une classe
          </button>
        )}
      </div>
    );
  };

  return (
    <div style={{ background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 10 }}>
      {/* ══ Type de rendu (segmenté, toujours visible) ══════════ */}
      <div>
        <Lbl>Type de rendu</Lbl>
        <Seg options={RENDER_TYPES} value={type} block
          onPick={v => {
            setType(v); setAttr(""); setManualMode("auto"); setManual(null);
            if (v === "categorized" && !CAT_RAMPS.has(ramp)) setRamp("categorial");
            else if ((v === "graduated" || v === "proportional") && CAT_RAMPS.has(ramp)) setRamp("viridis");
            if (v === "none") onChange(null);
          }} />
      </div>

      {/* ══ SYMBOLE UNIQUE (couleur unique) — rendu façon maquette, adapté géométrie ══ */}
      {type === "none" && isVec && onStyle && (
        <div style={{ display: "flex", flexDirection: "column", gap: 11 }}>
          <span style={{ fontSize: 10, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", fontWeight: 600 }}>
            Symbole {isLine ? "· ligne" : isPoly ? "· polygone" : "· point"}
          </span>
          {/* Aperçu + contrôles couleur / taille (couleur + épaisseur appairées) */}
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", padding: 12, border: `0.5px solid ${C.bdr}`, borderRadius: 10, background: C.hover }}>
            <div style={{ width: 64, height: 64, borderRadius: 9, background: C.bg, border: `0.5px solid ${C.bdr}`, display: "grid", placeItems: "center", flexShrink: 0 }}>
              {symbolPreview()}
            </div>
            <div style={{ flex: 1, minWidth: 150, display: "flex", flexDirection: "column", gap: 10 }}>
              {/* Ligne 1 : couleur principale + (point) taille / (ligne) épaisseur */}
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                {swatchField(isLine ? "Couleur" : "Remplissage", layer.color || "#1D9E75", v => onStyle(layer.id, { color: v }))}
                {isPointish && stepField("Taille", layer.radius || 5, 2, 15, 1, "px", v => onStyle(layer.id, { radius: parseInt(v) }))}
                {isLine && stepField("Épaisseur", layer.strokeWidth ?? 2, 0.5, 12, 0.5, "px", v => onStyle(layer.id, { strokeWidth: v }))}
              </div>
              {/* Ligne 2 : contour = couleur + épaisseur ensemble (point & polygone) */}
              {!isLine && (
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  {swatchField("Contour", layer.outlineColor || layer.color || "#000000", v => onStyle(layer.id, { outlineColor: v }))}
                  {stepField("Épaisseur", layer.strokeWidth ?? 1.5, 0, 10, 0.5, "px", v => onStyle(layer.id, { strokeWidth: v }))}
                </div>
              )}
            </div>
          </div>
          {/* Forme du marqueur — points uniquement */}
          {isPointish && (
            <div>
              <div style={fLbl}>Forme du marqueur</div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {SHAPES.map(([k, gl, lab]) => {
                  const on = (layer.markerShape || "circle") === k;
                  return (
                    <button key={k} onClick={() => onStyle(layer.id, { markerShape: k })} style={{
                      fontFamily: F, fontSize: 11.5, padding: "5px 11px", borderRadius: 8, cursor: "pointer",
                      display: "inline-flex", alignItems: "center", gap: 6,
                      background: on ? C.acc + "18" : "transparent",
                      border: `0.5px solid ${on ? C.acc : C.bdr}`, color: on ? C.acc : C.mut,
                    }}><span>{gl}</span> {lab}</button>
                  );
                })}
              </div>
            </div>
          )}
          {/* Forme des bords du trait/contour — lignes & polygones */}
          {!isPointish && (
            <div>
              <div style={fLbl}>{isLine ? "Bouts & coins du trait" : "Coins du contour"}</div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {STROKE_SHAPES.map(([cap, join, lab]) => {
                  const on = (layer.lineCap || "butt") === cap && (layer.lineJoin || "miter") === join;
                  return (
                    <button key={lab} onClick={() => onStyle(layer.id, { lineCap: cap, lineJoin: join })} style={{
                      fontFamily: F, fontSize: 11.5, padding: "5px 11px", borderRadius: 8, cursor: "pointer",
                      background: on ? C.acc + "18" : "transparent",
                      border: `0.5px solid ${on ? C.acc : C.bdr}`, color: on ? C.acc : C.mut,
                    }}>{lab}</button>
                  );
                })}
              </div>
            </div>
          )}
          {/* Légende explicative selon géométrie (comme la maquette) */}
          <p style={{ margin: 0, fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
            {isPoly ? <>Polygone : <b style={{ color: C.mut }}>Remplissage</b> = aplat, <b style={{ color: C.mut }}>Contour</b> = bord (couleur + épaisseur variables), coins en angles droits.</>
              : isLine ? <>Ligne : <b style={{ color: C.mut }}>Couleur</b> et <b style={{ color: C.mut }}>épaisseur</b> du trait.</>
              : <>Point : forme (rond/carré/triangle/losange), remplissage, contour et taille.</>}
          </p>
        </div>
      )}

      {/* Attribut — proportionnels uniquement (gradué/catégorisé l'ont dans la grammaire) */}
      {isProp && (
        <div>
          <Lbl>Attribut (numérique)</Lbl>
          <Sel value={attr} onChange={setAttr}
            options={[{ value: "", label: "-- Choisir --" }, ...attrs.num.map(a => ({ value: a, label: a }))]} />
        </div>
      )}

      {/* ══ GRADUÉE — grammaire QGIS ═══════════════════════════ */}
      {isGrad && grammarBox(
        <>
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            <div style={{ flex: 1, minWidth: 130 }}>
              <Lbl>Attribut</Lbl>
              <Sel value={attr}
                onChange={v => { setAttr(v); setManualMode("auto"); setManual(null); if (v) commit({ attribute: v, manual: null }); }}
                options={[{ value: "", label: "-- Choisir --" }, ...attrs.num.map(a => ({ value: a, label: a }))]} />
            </div>
            <div style={{ flex: 1 }}>
              <Lbl>Méthode</Lbl>
              <Sel value={method} onChange={v => { setMethod(v); if (attr && manualMode !== "manual") commit({ method: v }); }} options={[
                { value: "quantile", label: "Quantiles" }, { value: "jenks", label: "Ruptures naturelles" },
                { value: "equal", label: "Intervalles égaux" }, { value: "fixed", label: "Bornes fixes" },
              ]} />
            </div>
          </div>
          {attr && (
            <>
              {/* Classes : slider + effectif + Classer (façon maquette) */}
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 10, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Classes</span>
                <input type="range" min={2} max={10} value={nc}
                  onChange={e => { const n = parseInt(e.target.value); setNc(n); if (manualMode !== "manual") commit({ nClasses: n }); }}
                  style={{ flex: 1, height: 3 }} />
                <span style={{ fontFamily: M, fontSize: 12, fontWeight: 600, color: C.txt, minWidth: 16, textAlign: "center" }}>{nc}</span>
                <button onClick={() => { setManualMode("auto"); setManual(null); commit({ manual: null }); }} title="Recalculer les classes" style={{
                  fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "6px 11px", borderRadius: 8,
                  background: C.acc + "18", color: C.acc, border: `0.5px solid ${C.acc}66`, cursor: "pointer",
                  display: "flex", alignItems: "center", gap: 5, whiteSpace: "nowrap",
                }}><IcZap size={13} /> Classer</button>
              </div>
              {method === "fixed" && (
                <div><Lbl>Bornes</Lbl><input value={cb} onChange={e => setCb(e.target.value)} onBlur={() => commit()} placeholder="0,5,10,20" style={inp} /></div>
              )}
              {rampGrid(false)}
              {/* Auto / Manuel */}
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 10, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Édition</span>
                <Seg small value={manualMode} onPick={m => (m === "manual" ? toManual() : toAuto())}
                  options={[{ v: "auto", label: "Auto (méthode)" }, { v: "manual", label: "Manuel (édité)" }]} />
              </div>
              {legendTable()}
            </>
          )}
        </>,
        "Classification — vecteur (gradué) · même grammaire"
      )}

      {/* ══ CATÉGORISÉE — grammaire QGIS ═══════════════════════ */}
      {isCat && grammarBox(
        <>
          <div>
            <Lbl>Attribut</Lbl>
            <Sel value={attr}
              onChange={v => { setAttr(v); setManualMode("auto"); setManual(null); if (v) commit({ attribute: v, manual: null }); }}
              options={[{ value: "", label: "-- Choisir --" }, ...attrs.cat.map(a => ({ value: a, label: a }))]} />
          </div>
          {attr && (
            <>
              {rampGrid(true)}
              <button onClick={() => { setManualMode("auto"); setManual(null); commit({ manual: null }); }} style={{
                fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "6px 11px", borderRadius: 8, alignSelf: "flex-start",
                background: C.acc + "18", color: C.acc, border: `0.5px solid ${C.acc}66`, cursor: "pointer",
                display: "flex", alignItems: "center", gap: 5,
              }}><IcZap size={13} /> Classer les valeurs uniques</button>
              {/* Auto / Manuel */}
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 10, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Édition</span>
                <Seg small value={manualMode} onPick={m => (m === "manual" ? toManual() : toAuto())}
                  options={[{ v: "auto", label: "Auto" }, { v: "manual", label: "Manuel (édité)" }]} />
              </div>
              {legendTable()}
            </>
          )}
        </>,
        "Classification — vecteur (catégorisé) · même grammaire"
      )}

      {/* ══ SYMBOL / MAKI ══════════════════════════════════════ */}
      {isSymbol && (
        <>
          {/* Mode maki / image */}
          <div style={{ display: "flex", gap: 4 }}>
            {[["maki",IcMap,"Maki SVG"],["image",IcImage,"Image/PNG"]].map(([k,Icon,l]) => (
              <button key={k} onClick={() => setSymbolMode(k)} style={{
                fontFamily: F, fontSize: 10, padding: "4px 0", borderRadius: 4, flex: 1,
                background: symbolMode === k ? C.acc+"18" : "transparent",
                border: `0.5px solid ${symbolMode === k ? C.acc+"66" : C.bdr}`,
                color: symbolMode === k ? C.acc : C.dim, cursor: "pointer",
                display: "flex", alignItems: "center", justifyContent: "center", gap: 5,
              }}><Icon size={12}/> {l}</button>
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
                    : <><IcUpload size={13}/> Choisir un fichier</>
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
                  {(() => {
                    const cols = RAMPS[ramp] || RAMPS.viridis;
                    const c1 = cols[0] || C.acc;
                    const c2 = cols[Math.floor(cols.length/2)] || C.acc;
                    const c3 = cols[cols.length-1] || C.acc;
                    const rMin = Math.max(2, parseFloat(minSize)||3);
                    const rMax = Math.max(2, Math.min(18, parseFloat(maxSize)||30));
                    const rMid = Math.max(2, Math.min(18, (rMin+rMax)/2));
                    return <>
                      <circle cx="8"  cy="20" r={rMin} fill={c1} opacity="0.85" />
                      <circle cx="32" cy="20" r={rMid} fill={c2} opacity="0.85" />
                      <circle cx="54" cy="20" r={rMax} fill={c3} opacity="0.85" />
                    </>;
                  })()}
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
          {/* palette pour cercles proportionnels colorés */}
          {type === "proportional" && rampGrid(false)}
        </>
      )}

      {/* Appliquer — symbol / proportionnels (gradué & catégorisé s'appliquent en direct) */}
      {(isSymbol || (isProp && attr)) && (
        <button onClick={apply} style={{
          fontFamily: F, fontSize: 11, fontWeight: 600, padding: "7px 12px", borderRadius: 6,
          background: C.acc, color: "#fff", border: "none", cursor: "pointer",
        }}>Appliquer</button>
      )}

      {/* Graphiques par entité — mode à part, replié par défaut */}
      {onChartChange && (
        <ChartStyleBlock layer={layer} cfg={chartCfg} onChange={onChartChange} mapRef={mapRef}
          layerOpacity={layer?.opacity ?? 1} onLayerOpacity={onLayerOpacity} />
      )}
    </div>
  );
}

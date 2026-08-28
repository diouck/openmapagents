/**
 * GeorefPanel.jsx — Géoréférenceur (façon QGIS) : caler une image par points
 * d'appui (GCP).
 *
 * Visionneuse d'image ZOOMABLE/DÉPLAÇABLE (molette + glisser) montrant toute
 * l'image ; les points d'appui sont posés, sélectionnés, déplacés (glisser) ou
 * supprimés directement dessus. Chaque point complété apparaît AUSSI sur la
 * carte (via onGcpsChange). À ≥3 (affine) / ≥4 (projective) points →
 * POST /api/georef/warp → overlay reprojeté (addImageLayer).
 *
 * Clic carte capté via `georefClickRef` (armé par App, cf. handleMapClick).
 */
import { useState, useRef, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

export default function GeorefPanel({ georefClickRef, onAddImageLayer, onGcpsChange }) {
  const C = useThemeContext();
  const [img, setImg] = useState(null);          // { dataUrl, w, h }
  const [gcps, setGcps] = useState([]);          // [{px,py,lng,lat}]
  const [pending, setPending] = useState(null);  // {px,py} en attente d'un point carte
  const [arming, setArming] = useState(null);    // null | "new" | index (attente clic carte)
  const [selected, setSelected] = useState(null);// index sélectionné
  const [ttype, setTtype] = useState("affine");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [info, setInfo] = useState(null);
  const [view, setView] = useState({ s: 1, tx: 0, ty: 0 });

  const vpRef = useRef(null);
  const viewRef = useRef(view);   useEffect(() => { viewRef.current = view; }, [view]);
  const pendRef = useRef(pending); useEffect(() => { pendRef.current = pending; }, [pending]);
  const dragRef = useRef(null);

  useEffect(() => () => { if (georefClickRef) georefClickRef.current = null; onGcpsChange?.(null); }, [georefClickRef, onGcpsChange]);

  // Points géographiques (complétés) → carte
  useEffect(() => {
    if (!onGcpsChange) return;
    const feats = gcps.filter((g) => g.lng != null).map((g, i) => ({ type: "Feature", geometry: { type: "Point", coordinates: [g.lng, g.lat] }, properties: { label: String(i + 1) } }));
    onGcpsChange(feats.length ? { type: "FeatureCollection", features: feats } : null);
  }, [gcps, onGcpsChange]);

  const imgToView = (px, py, v = viewRef.current) => [px * v.s + v.tx, py * v.s + v.ty];
  const viewToImg = (vx, vy, v = viewRef.current) => [(vx - v.tx) / v.s, (vy - v.ty) / v.s];

  const fitView = useCallback(() => {
    const vp = vpRef.current; if (!vp || !img) return;
    const vw = vp.clientWidth, vh = vp.clientHeight;
    const s = Math.min(vw / img.w, vh / img.h) * 0.96;
    setView({ s, tx: (vw - img.w * s) / 2, ty: (vh - img.h * s) / 2 });
  }, [img]);
  useEffect(() => { if (img) { const t = requestAnimationFrame(fitView); return () => cancelAnimationFrame(t); } }, [img, fitView]);

  const onFile = (file) => {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const im = new Image();
      im.onload = () => { setImg({ dataUrl: reader.result, w: im.naturalWidth, h: im.naturalHeight }); setGcps([]); setPending(null); setSelected(null); setInfo(null); setErr(null); };
      im.onerror = () => setErr("Image illisible.");
      im.src = reader.result;
    };
    reader.readAsDataURL(file);
  };

  const hitMarker = (vx, vy) => {
    const R = 11;
    if (pendRef.current) { const [x, y] = imgToView(pendRef.current.px, pendRef.current.py); if (Math.hypot(vx - x, vy - y) <= R) return { type: "pending" }; }
    for (let i = gcps.length - 1; i >= 0; i--) { const [x, y] = imgToView(gcps[i].px, gcps[i].py); if (Math.hypot(vx - x, vy - y) <= R) return { type: "gcp", idx: i }; }
    return null;
  };

  const vpXY = (e) => { const r = vpRef.current.getBoundingClientRect(); return [e.clientX - r.left, e.clientY - r.top]; };

  const onWheel = (e) => {
    e.preventDefault();
    const [vx, vy] = vpXY(e); const v = viewRef.current;
    const ns = Math.max(0.02, Math.min(40, v.s * (e.deltaY < 0 ? 1.15 : 1 / 1.15)));
    const f = ns / v.s;
    setView({ s: ns, tx: vx - (vx - v.tx) * f, ty: vy - (vy - v.ty) * f });
  };
  const onPointerDown = (e) => {
    if (!img) return;
    const [vx, vy] = vpXY(e);
    const hit = hitMarker(vx, vy);
    dragRef.current = { sx: vx, sy: vy, moved: 0, mode: hit ? "marker" : "pan", hit, orig: { ...viewRef.current } };
    try { vpRef.current.setPointerCapture(e.pointerId); } catch (_) {}
  };
  const onPointerMove = (e) => {
    const d = dragRef.current; if (!d) return;
    const [vx, vy] = vpXY(e);
    d.moved = Math.max(d.moved, Math.hypot(vx - d.sx, vy - d.sy));
    if (d.mode === "pan") {
      setView((v) => ({ ...v, tx: d.orig.tx + (vx - d.sx), ty: d.orig.ty + (vy - d.sy) }));
    } else {
      const [ipx, ipy] = viewToImg(vx, vy, d.orig);
      const px = Math.max(0, Math.min(img.w, Math.round(ipx))), py = Math.max(0, Math.min(img.h, Math.round(ipy)));
      if (d.hit.type === "pending") setPending({ px, py });
      else setGcps((g) => g.map((x, i) => (i === d.hit.idx ? { ...x, px, py } : x)));
    }
  };
  const onPointerUp = (e) => {
    const d = dragRef.current; dragRef.current = null; if (!d || !img) return;
    if (d.moved < 4) {                       // clic (pas un glisser)
      if (d.mode === "marker" && d.hit.type === "gcp") { setSelected(d.hit.idx); return; }
      if (d.mode === "marker" && d.hit.type === "pending") return;
      const [vx, vy] = vpXY(e); const [ipx, ipy] = viewToImg(vx, vy);
      if (ipx >= 0 && ipx <= img.w && ipy >= 0 && ipy <= img.h) {
        if (georefClickRef) georefClickRef.current = null; setArming(null); setSelected(null);
        setPending({ px: Math.round(ipx), py: Math.round(ipy) });
      }
    }
  };

  const armNew = useCallback(() => {
    if (!pending || !georefClickRef) return;
    const p = pending; setArming("new");
    georefClickRef.current = (lng, lat) => {
      setGcps((g) => [...g, { px: p.px, py: p.py, lng: +lng.toFixed(6), lat: +lat.toFixed(6) }]);
      setPending(null); setArming(null); georefClickRef.current = null;
    };
  }, [pending, georefClickRef]);
  const armRedefine = (idx) => {
    if (!georefClickRef) return; setArming(idx);
    georefClickRef.current = (lng, lat) => {
      setGcps((g) => g.map((x, i) => (i === idx ? { ...x, lng: +lng.toFixed(6), lat: +lat.toFixed(6) } : x)));
      setArming(null); georefClickRef.current = null;
    };
  };
  const cancelArm = () => { if (georefClickRef) georefClickRef.current = null; setArming(null); };
  const delGcp = (i) => { setGcps((g) => g.filter((_, j) => j !== i)); setSelected(null); };

  const need = ttype === "projective" ? 4 : 3;
  const ready = img && gcps.length >= need;

  const run = useCallback(async () => {
    if (!ready) return;
    setBusy(true); setErr(null); setInfo(null);
    try {
      const r = await fetch(`${API}/georef/warp`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ image_b64: img.dataUrl, gcps, transform_type: ttype }) });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({ name: d.name, imageUrl: `data:image/png;base64,${d.png_b64}`, coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.85, rasterToken: null, bands: d.bands, fit: true });
      setInfo(`Calé (${ttype}, ${d.n_gcps} points) · RMSE ≈ ${d.rmse_m} m.`);
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [ready, img, gcps, ttype, onAddImageLayer]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const btn = (bg, col, extra) => ({ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "6px 12px", cursor: "pointer", background: bg, color: col, border: bg === "transparent" ? `1px solid ${col}66` : "none", borderRadius: 7, ...extra });
  const dot = (x, y, bg, sz, content, ring = "#fff") => (
    <span style={{ position: "absolute", left: x, top: y, transform: "translate(-50%,-50%)", width: sz, height: sz, borderRadius: "50%", background: bg, color: "#fff", fontFamily: M, fontSize: 10, fontWeight: 700, display: "flex", alignItems: "center", justifyContent: "center", border: `2px solid ${ring}`, boxShadow: "0 1px 4px rgba(0,0,0,.5)", pointerEvents: "none" }}>{content}</span>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      {!img ? (
        <label style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 8, padding: "28px 12px", border: `1px dashed ${C.bdr}`, borderRadius: 10, cursor: "pointer", color: C.mut, fontFamily: F, fontSize: 12.5 }}>
          <input type="file" accept="image/*" style={{ display: "none" }} onChange={(e) => onFile(e.target.files?.[0])} />
          <span style={{ fontSize: 26, opacity: 0.5 }}>🗺</span>
          Importer une image à caler (plan scanné, photo…)
        </label>
      ) : (
        <>
          <div style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.4, flexWrap: "wrap" }}>
            <span>Clic = poser un repère · glisser un repère = déplacer · molette = zoom.</span>
            <button onClick={fitView} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 9px", fontSize: 11 }}>Ajuster</button>
          </div>

          {/* Visionneuse image zoomable */}
          <div ref={vpRef} onWheel={onWheel} onPointerDown={onPointerDown} onPointerMove={onPointerMove} onPointerUp={onPointerUp}
            style={{ position: "relative", flex: 1, minHeight: 240, overflow: "hidden", borderRadius: 8, border: `0.5px solid ${C.bdr}`, background: "#0c0d12", cursor: dragRef.current?.mode === "pan" ? "grabbing" : "crosshair", touchAction: "none", userSelect: "none" }}>
            <img src={img.dataUrl} alt="" draggable={false}
              style={{ position: "absolute", left: 0, top: 0, width: img.w, height: img.h, transformOrigin: "0 0", transform: `translate(${view.tx}px,${view.ty}px) scale(${view.s})`, imageRendering: "auto", pointerEvents: "none" }} />
            {gcps.map((g, i) => { const [x, y] = imgToView(g.px, g.py); return <span key={i}>{dot(x, y, selected === i ? "#f59e0b" : C.acc, selected === i ? 22 : 18, i + 1, selected === i ? "#fff" : "#fff")}</span>; })}
            {pending && (() => { const [x, y] = imgToView(pending.px, pending.py); return dot(x, y, "#f59e0b", 16, "", "#fff"); })()}
          </div>

          {/* Point sélectionné → éditer */}
          {selected != null && gcps[selected] && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, background: "#f59e0b14", border: "0.5px solid #f59e0b55", borderRadius: 7, padding: "6px 10px", flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>Point {selected + 1} · px {gcps[selected].px},{gcps[selected].py} → {gcps[selected].lat.toFixed(4)}, {gcps[selected].lng.toFixed(4)}</span>
              {arming === selected ? (
                <><span style={{ fontFamily: F, fontSize: 11, color: "#f59e0b", fontWeight: 600 }}>Cliquez la carte…</span><button onClick={cancelArm} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 8px", fontSize: 11 }}>Annuler</button></>
              ) : (
                <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                  <button onClick={() => armRedefine(selected)} style={{ ...btn("transparent", C.acc), padding: "3px 9px", fontSize: 11 }}>Redéfinir sur la carte</button>
                  <button onClick={() => delGcp(selected)} style={{ ...btn("transparent", "#e11d1d"), padding: "3px 9px", fontSize: 11 }}>Supprimer</button>
                </div>
              )}
            </div>
          )}

          {/* Point en attente (posé sur l'image) → placer sur la carte */}
          {pending && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, background: "#f59e0b18", border: "0.5px solid #f59e0b66", borderRadius: 7, padding: "6px 10px", flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>Nouveau repère · pixel {pending.px}, {pending.py}</span>
              {arming === "new" ? (
                <><span style={{ fontFamily: F, fontSize: 11.5, color: "#f59e0b", fontWeight: 600 }}>Cliquez le même lieu sur la carte…</span><button onClick={cancelArm} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 8px", fontSize: 11 }}>Annuler</button></>
              ) : (
                <><button onClick={armNew} style={{ ...btn(C.acc, "#fff"), marginLeft: "auto", padding: "5px 10px" }}>Placer sur la carte</button><button onClick={() => setPending(null)} style={{ ...btn("transparent", C.mut), padding: "5px 8px", fontSize: 11 }}>✕</button></>
              )}
            </div>
          )}

          {/* Liste des GCP */}
          {gcps.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 110, overflowY: "auto" }}>
              {gcps.map((g, i) => (
                <div key={i} onClick={() => setSelected(i)} style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: M, fontSize: 10.5, color: C.mut, padding: "3px 5px", borderRadius: 5, cursor: "pointer", background: selected === i ? C.acc + "14" : "transparent" }}>
                  <span style={{ width: 16, height: 16, borderRadius: "50%", background: selected === i ? "#f59e0b" : C.acc, color: "#fff", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 9, fontWeight: 700, flexShrink: 0 }}>{i + 1}</span>
                  <span>px {g.px},{g.py}</span>
                  <span style={{ color: C.dim }}>→ {g.lat.toFixed(4)}, {g.lng.toFixed(4)}</span>
                  <button onClick={(e) => { e.stopPropagation(); delGcp(i); }} style={{ marginLeft: "auto", background: "none", border: "none", color: "#e11d1d", cursor: "pointer", fontSize: 12 }}>✕</button>
                </div>
              ))}
            </div>
          )}

          {/* Transformation + action */}
          <div style={{ display: "flex", alignItems: "flex-end", gap: 8, flexWrap: "wrap" }}>
            <div>
              <div style={lbl}>Transformation</div>
              <select value={ttype} onChange={(e) => setTtype(e.target.value)} style={{ fontFamily: F, fontSize: 12, padding: "5px 8px", borderRadius: 6, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt }}>
                <option value="affine">Affine (≥3 points)</option>
                <option value="projective">Projective (≥4 points)</option>
              </select>
            </div>
            <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
              <button onClick={() => { setImg(null); setGcps([]); setPending(null); setSelected(null); }} style={btn("transparent", C.mut)}>Changer d'image</button>
              <button onClick={run} disabled={!ready || busy} style={{ ...btn(C.acc, "#fff"), opacity: ready ? 1 : 0.5, cursor: (!ready || busy) ? "not-allowed" : "pointer", fontWeight: 600 }}>
                {busy ? "Calage…" : `Géoréférencer (${gcps.length}/${need})`}
              </button>
            </div>
          </div>

          {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
          {info && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{info}</div>}
        </>
      )}
    </div>
  );
}

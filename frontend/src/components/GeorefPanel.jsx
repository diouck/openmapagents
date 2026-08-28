/**
 * GeorefPanel.jsx — Géoréférenceur (façon QGIS) : caler une image par points
 * d'appui (GCP), puis exporter la couche géoréférencée (GeoTIFF).
 *
 * Visionneuse image zoomable/déplaçable (molette + glisser) ; repères en CROIX
 * bien visibles, éditables (glisser = déplacer, clic = sélectionner, « aller à »
 * pour recadrer). Chaque point complété apparaît aussi sur la carte
 * (onGcpsChange). À ≥3 (affine) / ≥4 (projective) → POST /api/georef/warp →
 * overlay (addImageLayer) ; bouton « Exporter GeoTIFF » (with_geotiff).
 * Onglets : Calage / Définition.
 */
import { useState, useRef, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

export default function GeorefPanel({ georefClickRef, onAddImageLayer, onGcpsChange }) {
  const C = useThemeContext();
  const [tab, setTab] = useState("go");
  const [img, setImg] = useState(null);
  const [gcps, setGcps] = useState([]);
  const [pending, setPending] = useState(null);
  const [arming, setArming] = useState(null);      // null | "new" | index
  const [selected, setSelected] = useState(null);
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
  useEffect(() => { if (img && tab === "go") { const t = requestAnimationFrame(fitView); return () => cancelAnimationFrame(t); } }, [img, tab, fitView]);

  const goTo = (px, py) => {
    const vp = vpRef.current; if (!vp) return;
    const s = Math.max(viewRef.current.s, 3);
    setView({ s, tx: vp.clientWidth / 2 - px * s, ty: vp.clientHeight / 2 - py * s });
  };

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
    const R = 12;
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
    const [vx, vy] = vpXY(e); const hit = hitMarker(vx, vy);
    dragRef.current = { sx: vx, sy: vy, moved: 0, mode: hit ? "marker" : "pan", hit, orig: { ...viewRef.current } };
    try { vpRef.current.setPointerCapture(e.pointerId); } catch (_) {}
  };
  const onPointerMove = (e) => {
    const d = dragRef.current; if (!d) return;
    const [vx, vy] = vpXY(e); d.moved = Math.max(d.moved, Math.hypot(vx - d.sx, vy - d.sy));
    if (d.mode === "pan") setView((v) => ({ ...v, tx: d.orig.tx + (vx - d.sx), ty: d.orig.ty + (vy - d.sy) }));
    else {
      const [ipx, ipy] = viewToImg(vx, vy, d.orig);
      const px = Math.max(0, Math.min(img.w, Math.round(ipx))), py = Math.max(0, Math.min(img.h, Math.round(ipy)));
      if (d.hit.type === "pending") setPending({ px, py });
      else setGcps((g) => g.map((x, i) => (i === d.hit.idx ? { ...x, px, py } : x)));
    }
  };
  const onPointerUp = (e) => {
    const d = dragRef.current; dragRef.current = null; if (!d || !img) return;
    if (d.moved < 4) {
      if (d.mode === "marker" && d.hit.type === "gcp") { setSelected(d.hit.idx); return; }
      if (d.mode === "marker") return;
      const [vx, vy] = vpXY(e); const [ipx, ipy] = viewToImg(vx, vy);
      if (ipx >= 0 && ipx <= img.w && ipy >= 0 && ipy <= img.h) { if (georefClickRef) georefClickRef.current = null; setArming(null); setSelected(null); setPending({ px: Math.round(ipx), py: Math.round(ipy) }); }
    }
  };

  const armNew = useCallback(() => {
    if (!pending || !georefClickRef) return; const p = pending; setArming("new");
    georefClickRef.current = (lng, lat) => { setGcps((g) => [...g, { px: p.px, py: p.py, lng: +lng.toFixed(6), lat: +lat.toFixed(6) }]); setPending(null); setArming(null); georefClickRef.current = null; };
  }, [pending, georefClickRef]);
  const armRedefine = (idx) => {
    if (!georefClickRef) return; setArming(idx);
    georefClickRef.current = (lng, lat) => { setGcps((g) => g.map((x, i) => (i === idx ? { ...x, lng: +lng.toFixed(6), lat: +lat.toFixed(6) } : x))); setArming(null); georefClickRef.current = null; };
  };
  const cancelArm = () => { if (georefClickRef) georefClickRef.current = null; setArming(null); };
  const delGcp = (i) => { setGcps((g) => g.filter((_, j) => j !== i)); setSelected(null); };

  const need = ttype === "projective" ? 4 : 3;
  const ready = img && gcps.length >= need;

  const callWarp = useCallback(async (withTiff) => {
    const r = await fetch(`${API}/georef/warp`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ image_b64: img.dataUrl, gcps, transform_type: ttype, with_geotiff: withTiff }) });
    if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
    return r.json();
  }, [img, gcps, ttype]);

  const run = useCallback(async () => {
    if (!ready) return; setBusy(true); setErr(null); setInfo(null);
    try {
      const d = await callWarp(false);
      onAddImageLayer?.({ name: d.name, imageUrl: `data:image/png;base64,${d.png_b64}`, coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.85, rasterToken: null, bands: d.bands, fit: true });
      setInfo(`Calé (${ttype}, ${d.n_gcps} points) · RMSE ≈ ${d.rmse_m} m.`);
    } catch (e) { setErr(e.message || String(e)); } finally { setBusy(false); }
  }, [ready, callWarp, ttype, onAddImageLayer]);

  const exportTiff = useCallback(async () => {
    if (!ready) return; setBusy(true); setErr(null); setInfo(null);
    try {
      const d = await callWarp(true);
      if (!d.geotiff_b64) throw new Error("GeoTIFF non produit.");
      const bin = atob(d.geotiff_b64); const bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      const url = URL.createObjectURL(new Blob([bytes], { type: "image/tiff" }));
      const a = document.createElement("a"); a.href = url; a.download = `${(d.name || "georef").replace(/[^\w.-]+/g, "_")}.tif`;
      document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 2000);
      setInfo(`GeoTIFF exporté (${ttype}, RMSE ≈ ${d.rmse_m} m).`);
    } catch (e) { setErr(e.message || String(e)); } finally { setBusy(false); }
  }, [ready, callWarp, ttype]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const btn = (bg, col, extra) => ({ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "6px 12px", cursor: "pointer", background: bg, color: col, border: bg === "transparent" ? `1px solid ${col}66` : "none", borderRadius: 7, ...extra });
  const tabBtn = (id, label) => (<button key={id} onClick={() => setTab(id)} style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer", background: "transparent", color: tab === id ? C.acc : C.mut, border: "none", borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>{label}</button>);

  // Repère en croix (bien visible) : lignes + anneau + badge numéro
  const Cross = ({ x, y, n, active }) => {
    const col = active ? "#f59e0b" : "#ff2d78";
    const line = { position: "absolute", background: col, boxShadow: "0 0 0 0.5px #fff" };
    return (
      <div style={{ position: "absolute", left: x, top: y, transform: "translate(-50%,-50%)", pointerEvents: "none", zIndex: 3 }}>
        <div style={{ ...line, left: -11, top: -0.75, width: 22, height: 1.5 }} />
        <div style={{ ...line, left: -0.75, top: -11, width: 1.5, height: 22 }} />
        <div style={{ position: "absolute", left: -5, top: -5, width: 10, height: 10, borderRadius: "50%", border: `1.5px solid ${col}`, boxShadow: "0 0 0 0.5px #fff" }} />
        {n != null && <div style={{ position: "absolute", left: 8, top: -17, background: col, color: "#fff", fontFamily: M, fontSize: 10, fontWeight: 700, padding: "0 4px", borderRadius: 4, border: "1px solid #fff", lineHeight: "15px" }}>{n}</div>}
      </div>
    );
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>{tabBtn("go", "Calage")}{tabBtn("def", "Définition")}</div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Cale une image sans coordonnées (plan scanné, photo, croquis) sur la carte à l'aide de <b>points d'appui (GCP)</b>.</p>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Poser un point</div>
            <p style={{ margin: 0, color: C.mut }}>1. Cliquez un repère identifiable <b>sur l'image</b> (un carrefour, un angle). 2. « Placer sur la carte », puis cliquez <b>le même lieu</b> sur la carte. Le point apparaît en croix sur l'image et sur la carte.</p></div>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Naviguer & corriger</div>
            <p style={{ margin: 0, color: C.mut }}>Molette = zoom, glisser le fond = déplacer, « Ajuster » = tout voir. Glissez une croix pour la déplacer ; cliquez-la (ou « aller à » dans la liste) pour la sélectionner, puis <b>Redéfinir sur la carte</b> ou <b>Supprimer</b>.</p></div>
          <div><div style={{ fontWeight: 600, marginBottom: 3 }}>Transformation & export</div>
            <p style={{ margin: 0, color: C.mut }}><b>Affine</b> (≥3 points) gère rotation/échelle/cisaillement ; <b>Projective</b> (≥4) corrige la perspective. Le <b>RMSE</b> (en mètres) mesure la qualité du calage. « Géoréférencer » ajoute l'overlay ; « <b>Exporter GeoTIFF</b> » télécharge la couche calée (EPSG:3857) réutilisable dans un SIG.</p></div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            Répartissez les points sur toute l'image (évitez de les aligner ou de les regrouper) pour un meilleur calage. Image jusqu'à 5 000 px de côté.
          </div>
        </div>
      ) : !img ? (
        <label style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 8, padding: "28px 12px", border: `1px dashed ${C.bdr}`, borderRadius: 10, cursor: "pointer", color: C.mut, fontFamily: F, fontSize: 12.5 }}>
          <input type="file" accept="image/*" style={{ display: "none" }} onChange={(e) => onFile(e.target.files?.[0])} />
          <span style={{ fontSize: 26, opacity: 0.5 }}>🗺</span>
          Importer une image à caler (plan scanné, photo…)
        </label>
      ) : (
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 10, paddingRight: 2 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: F, fontSize: 11.5, color: C.mut, flexWrap: "wrap" }}>
            <span>Clic = poser · glisser une croix = déplacer · molette = zoom.</span>
            <button onClick={fitView} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 9px", fontSize: 11 }}>Ajuster</button>
          </div>

          <div ref={vpRef} onWheel={onWheel} onPointerDown={onPointerDown} onPointerMove={onPointerMove} onPointerUp={onPointerUp}
            style={{ position: "relative", height: 340, flexShrink: 0, overflow: "hidden", borderRadius: 8, border: `0.5px solid ${C.bdr}`, background: "#0c0d12", cursor: dragRef.current?.mode === "pan" ? "grabbing" : "crosshair", touchAction: "none", userSelect: "none" }}>
            <img src={img.dataUrl} alt="" draggable={false} style={{ position: "absolute", left: 0, top: 0, width: img.w, height: img.h, transformOrigin: "0 0", transform: `translate(${view.tx}px,${view.ty}px) scale(${view.s})`, pointerEvents: "none" }} />
            {gcps.map((g, i) => { const [x, y] = imgToView(g.px, g.py); return <Cross key={i} x={x} y={y} n={i + 1} active={selected === i} />; })}
            {pending && (() => { const [x, y] = imgToView(pending.px, pending.py); return <Cross x={x} y={y} n={null} active />; })()}
          </div>

          {selected != null && gcps[selected] && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, background: "#f59e0b14", border: "0.5px solid #f59e0b55", borderRadius: 7, padding: "6px 10px", flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>Point {selected + 1} · {gcps[selected].lat.toFixed(4)}, {gcps[selected].lng.toFixed(4)}</span>
              {arming === selected ? (
                <><span style={{ fontFamily: F, fontSize: 11, color: "#f59e0b", fontWeight: 600 }}>Cliquez la carte…</span><button onClick={cancelArm} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 8px", fontSize: 11 }}>Annuler</button></>
              ) : (
                <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                  <button onClick={() => armRedefine(selected)} style={{ ...btn("transparent", C.acc), padding: "3px 9px", fontSize: 11 }}>Redéfinir carte</button>
                  <button onClick={() => delGcp(selected)} style={{ ...btn("transparent", "#e11d1d"), padding: "3px 9px", fontSize: 11 }}>Supprimer</button>
                </div>
              )}
            </div>
          )}

          {pending && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, background: "#f59e0b18", border: "0.5px solid #f59e0b66", borderRadius: 7, padding: "6px 10px", flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>Nouveau repère · px {pending.px},{pending.py}</span>
              {arming === "new" ? (
                <><span style={{ fontFamily: F, fontSize: 11.5, color: "#f59e0b", fontWeight: 600 }}>Cliquez le même lieu sur la carte…</span><button onClick={cancelArm} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 8px", fontSize: 11 }}>Annuler</button></>
              ) : (
                <><button onClick={armNew} style={{ ...btn(C.acc, "#fff"), marginLeft: "auto", padding: "5px 10px" }}>Placer sur la carte</button><button onClick={() => setPending(null)} style={{ ...btn("transparent", C.mut), padding: "5px 8px", fontSize: 11 }}>✕</button></>
              )}
            </div>
          )}

          {gcps.length > 0 && (
            <div>
              <div style={lbl}>Points d'appui ({gcps.length})</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 140, overflowY: "auto", border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: 3 }}>
                {gcps.map((g, i) => (
                  <div key={i} onClick={() => setSelected(i)} style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: M, fontSize: 10.5, color: C.mut, padding: "3px 5px", borderRadius: 5, cursor: "pointer", background: selected === i ? C.acc + "14" : "transparent" }}>
                    <span style={{ width: 16, height: 16, borderRadius: 4, background: selected === i ? "#f59e0b" : "#ff2d78", color: "#fff", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 9, fontWeight: 700, flexShrink: 0 }}>{i + 1}</span>
                    <span>px {g.px},{g.py}</span>
                    <span style={{ color: C.dim }}>→ {g.lat.toFixed(4)}, {g.lng.toFixed(4)}</span>
                    <button onClick={(e) => { e.stopPropagation(); goTo(g.px, g.py); setSelected(i); }} title="Recadrer sur ce point" style={{ marginLeft: "auto", ...btn("transparent", C.mut), padding: "1px 7px", fontSize: 10 }}>aller à</button>
                    <button onClick={(e) => { e.stopPropagation(); delGcp(i); }} style={{ background: "none", border: "none", color: "#e11d1d", cursor: "pointer", fontSize: 12 }}>✕</button>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div style={{ display: "flex", alignItems: "flex-end", gap: 8, flexWrap: "wrap" }}>
            <div>
              <div style={lbl}>Transformation</div>
              <select value={ttype} onChange={(e) => setTtype(e.target.value)} style={{ fontFamily: F, fontSize: 12, padding: "5px 8px", borderRadius: 6, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt }}>
                <option value="affine">Affine (≥3 points)</option>
                <option value="projective">Projective (≥4 points)</option>
              </select>
            </div>
            <button onClick={() => { setImg(null); setGcps([]); setPending(null); setSelected(null); }} style={{ ...btn("transparent", C.mut), marginLeft: "auto" }}>Changer d'image</button>
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={exportTiff} disabled={!ready || busy} style={{ ...btn("transparent", C.acc), flex: 1, opacity: ready ? 1 : 0.5, cursor: (!ready || busy) ? "not-allowed" : "pointer" }}>⬇ Exporter GeoTIFF</button>
            <button onClick={run} disabled={!ready || busy} style={{ ...btn(C.acc, "#fff"), flex: 1, opacity: ready ? 1 : 0.5, cursor: (!ready || busy) ? "not-allowed" : "pointer", fontWeight: 600 }}>{busy ? "…" : `Géoréférencer (${gcps.length}/${need})`}</button>
          </div>

          {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
          {info && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{info}</div>}
        </div>
      )}
    </div>
  );
}

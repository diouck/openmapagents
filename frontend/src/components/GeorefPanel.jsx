/**
 * GeorefPanel.jsx — Géoréférenceur : caler une image sur la carte par points
 * d'appui (GCP).
 *
 * Flux d'un point : (1) cliquer sur l'image → coordonnée pixel ; (2) « Placer
 * sur la carte » puis cliquer la carte → coordonnée géographique. À ≥3 points
 * (affine) ou ≥4 (projective), « Géoréférencer » envoie image + GCP à
 * POST /api/georef/warp ; l'overlay reprojeté est ajouté via addImageLayer.
 *
 * Le clic carte est capté via `georefClickRef` (armé par App, cf. handleMapClick).
 */
import { useState, useRef, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

export default function GeorefPanel({ georefClickRef, onAddImageLayer }) {
  const C = useThemeContext();
  const [img, setImg] = useState(null);        // { dataUrl, w, h }
  const [gcps, setGcps] = useState([]);        // [{px,py,lng,lat}]
  const [pending, setPending] = useState(null); // {px,py} en attente d'un point carte
  const [arming, setArming] = useState(false); // attente d'un clic carte
  const [ttype, setTtype] = useState("affine");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [info, setInfo] = useState(null);
  const imgRef = useRef(null);

  // Nettoyage du handler de clic carte au démontage / changement.
  useEffect(() => () => { if (georefClickRef) georefClickRef.current = null; }, [georefClickRef]);

  const onFile = (file) => {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const im = new Image();
      im.onload = () => { setImg({ dataUrl: reader.result, w: im.naturalWidth, h: im.naturalHeight }); setGcps([]); setPending(null); setInfo(null); setErr(null); };
      im.onerror = () => setErr("Image illisible.");
      im.src = reader.result;
    };
    reader.readAsDataURL(file);
  };

  // Clic sur l'image → coordonnée pixel (dans la résolution naturelle).
  const onImgClick = (e) => {
    const el = imgRef.current; if (!el || !img) return;
    if (georefClickRef) georefClickRef.current = null;   // annule une capture carte en cours
    setArming(false);
    const rect = el.getBoundingClientRect();
    const px = (e.clientX - rect.left) * (img.w / rect.width);
    const py = (e.clientY - rect.top) * (img.h / rect.height);
    setPending({ px: Math.round(px), py: Math.round(py) });
    setInfo(null);
  };

  // Arme la capture du prochain clic carte pour compléter le point en attente.
  const armMap = useCallback(() => {
    if (!pending || !georefClickRef) return;
    setArming(true);
    georefClickRef.current = (lng, lat) => {
      setGcps((g) => [...g, { px: pending.px, py: pending.py, lng: +lng.toFixed(6), lat: +lat.toFixed(6) }]);
      setPending(null); setArming(false);
      georefClickRef.current = null;
    };
  }, [pending, georefClickRef]);

  const cancelArm = () => { if (georefClickRef) georefClickRef.current = null; setArming(false); };
  const delGcp = (i) => setGcps((g) => g.filter((_, j) => j !== i));

  const need = ttype === "projective" ? 4 : 3;
  const ready = img && gcps.length >= need;

  const run = useCallback(async () => {
    if (!ready) return;
    setBusy(true); setErr(null); setInfo(null);
    try {
      const r = await fetch(`${API}/georef/warp`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ image_b64: img.dataUrl, gcps, transform_type: ttype }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({
        name: d.name, imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.85,
        rasterToken: null, bands: d.bands, fit: true,
      });
      setInfo(`Calé (${ttype}, ${d.n_gcps} points) · erreur RMSE ≈ ${d.rmse_m} m.`);
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [ready, img, gcps, ttype, onAddImageLayer]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const btn = (bg, col, extra) => ({ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "6px 12px", cursor: "pointer", background: bg, color: col, border: bg === "transparent" ? `1px solid ${col}66` : "none", borderRadius: 7, ...extra });

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
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.45 }}>
            1. Cliquez un repère sur l'image. 2. « Placer sur la carte », puis cliquez le même lieu sur la carte. Répétez ≥{need} fois.
          </div>

          {/* Image cliquable + repères */}
          <div style={{ position: "relative", width: "100%", overflow: "hidden", borderRadius: 8, border: `0.5px solid ${C.bdr}`, cursor: "crosshair" }}>
            <img ref={imgRef} src={img.dataUrl} alt="" onClick={onImgClick} draggable={false}
              style={{ display: "block", width: "100%", height: "auto", userSelect: "none" }} />
            {gcps.map((g, i) => (
              <span key={i} style={{ position: "absolute", left: `${(g.px / img.w) * 100}%`, top: `${(g.py / img.h) * 100}%`, transform: "translate(-50%,-50%)",
                width: 18, height: 18, borderRadius: "50%", background: C.acc, color: "#fff", fontFamily: M, fontSize: 10, fontWeight: 700,
                display: "flex", alignItems: "center", justifyContent: "center", border: "2px solid #fff", boxShadow: "0 1px 4px rgba(0,0,0,.4)" }}>{i + 1}</span>
            ))}
            {pending && (
              <span style={{ position: "absolute", left: `${(pending.px / img.w) * 100}%`, top: `${(pending.py / img.h) * 100}%`, transform: "translate(-50%,-50%)",
                width: 16, height: 16, borderRadius: "50%", background: "#f59e0b", border: "2px solid #fff", boxShadow: "0 1px 4px rgba(0,0,0,.4)" }} />
            )}
          </div>

          {/* Point en attente → placer sur la carte */}
          {pending && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, background: "#f59e0b18", border: "0.5px solid #f59e0b66", borderRadius: 7, padding: "6px 10px", flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 11, color: C.txt }}>pixel {pending.px}, {pending.py}</span>
              {arming ? (
                <>
                  <span style={{ fontFamily: F, fontSize: 11.5, color: "#f59e0b", fontWeight: 600 }}>Cliquez sur la carte…</span>
                  <button onClick={cancelArm} style={{ ...btn("transparent", C.mut), marginLeft: "auto", padding: "3px 8px", fontSize: 11 }}>Annuler</button>
                </>
              ) : (
                <button onClick={armMap} style={{ ...btn(C.acc, "#fff"), marginLeft: "auto", padding: "5px 10px" }}>Placer sur la carte</button>
              )}
            </div>
          )}

          {/* Liste des GCP */}
          {gcps.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 3, maxHeight: 130, overflowY: "auto" }}>
              {gcps.map((g, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: M, fontSize: 10.5, color: C.mut, padding: "2px 4px" }}>
                  <span style={{ width: 16, height: 16, borderRadius: "50%", background: C.acc, color: "#fff", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 9, fontWeight: 700, flexShrink: 0 }}>{i + 1}</span>
                  <span>px {g.px},{g.py}</span>
                  <span style={{ color: C.dim }}>→ {g.lat.toFixed(4)}, {g.lng.toFixed(4)}</span>
                  <button onClick={() => delGcp(i)} style={{ marginLeft: "auto", background: "none", border: "none", color: "#e11d1d", cursor: "pointer", fontSize: 12 }}>✕</button>
                </div>
              ))}
            </div>
          )}

          {/* Transformation + action */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            <div>
              <div style={lbl}>Transformation</div>
              <select value={ttype} onChange={(e) => setTtype(e.target.value)} style={{ fontFamily: F, fontSize: 12, padding: "5px 8px", borderRadius: 6, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt }}>
                <option value="affine">Affine (≥3 points)</option>
                <option value="projective">Projective (≥4 points)</option>
              </select>
            </div>
            <div style={{ marginLeft: "auto", alignSelf: "flex-end", display: "flex", gap: 8 }}>
              <button onClick={() => { setImg(null); setGcps([]); setPending(null); }} style={btn("transparent", C.mut)}>Changer d'image</button>
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

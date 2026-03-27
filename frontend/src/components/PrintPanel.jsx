import { useState, useCallback, useRef, useEffect } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";

// ── Helper : applique une ombre de texte forte pour lisibilité sans fond ──
function setShadow(ctx, blur) {
  ctx.shadowColor   = "#000";
  ctx.shadowBlur    = blur;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = 0;
}
function clearShadow(ctx) {
  ctx.shadowColor   = "transparent";
  ctx.shadowBlur    = 0;
  ctx.shadowOffsetX = 0;
  ctx.shadowOffsetY = 0;
}

// ── Dessine overlays sur canvas — sans aucun fond ────────────────
function drawOverlaysOnCanvas(ctx, W, H, opts) {
  const { layers, showLegend, legendPos, showNorth, showScale, vs } = opts;
  const PAD   = Math.round(W * 0.013);
  const FONT  = Math.round(W * 0.013);
  const SMALL = Math.round(W * 0.011);
  const SH    = Math.round(W * 0.008); // shadow blur

  // ── Légende — texte + pictos couleur, sans fond ───────────────
  if (showLegend) {
    const visLayers = layers.filter(l => l.visible);
    if (visLayers.length > 0) {
      let lines = [];
      visLayers.forEach(l => {
        lines.push({ type: "layer", layer: l });
        if (l.classResult?.type === "categorized")
          l.classResult.entries?.slice(0, 6).forEach(e =>
            lines.push({ type: "class", color: e.color, label: `${e.value} (${e.count})` })
          );
        if (l.classResult?.type === "graduated")
          l.classResult.classes?.forEach(c =>
            lines.push({ type: "class", color: c.color, label: `${c.min.toFixed(1)}–${c.max.toFixed(1)} (${c.count})` })
          );
      });

      const lineH  = FONT * 1.7;
      const boxW   = Math.round(W * 0.22);
      const boxH   = PAD + lines.length * lineH;
      const margin = PAD * 2.5;

      let bx, by;
      if (legendPos === "bottom-left")  { bx = margin;            by = H - margin - boxH; }
      if (legendPos === "bottom-right") { bx = W - margin - boxW; by = H - margin - boxH; }
      if (legendPos === "top-left")     { bx = margin;            by = margin; }
      if (legendPos === "top-right")    { bx = W - margin - boxW; by = margin; }

      ctx.save();
      let ly = by + FONT;
      lines.forEach(line => {
        if (line.type === "layer") {
          const l = line.layer;
          // Carré couleur avec ombre
          setShadow(ctx, SH * 1.5);
          ctx.fillStyle = l.color || "#888";
          ctx.beginPath();
          ctx.roundRect(bx, ly - FONT * 0.78, FONT * 0.85, FONT * 0.85, 2);
          ctx.fill();
          // Texte nom couche
          ctx.fillStyle = "#ffffff";
          ctx.font = `600 ${FONT}px sans-serif`;
          const name = l.name.length > 22 ? l.name.slice(0, 22) + "…" : l.name;
          ctx.fillText(`${name} (${l.featureCount})`, bx + FONT * 1.2, ly);
        } else {
          setShadow(ctx, SH);
          ctx.fillStyle = line.color || "#888";
          ctx.fillRect(bx + PAD * 1.2, ly - FONT * 0.68, FONT * 0.72, FONT * 0.72);
          ctx.fillStyle = "#ffffff";
          ctx.font = `${SMALL}px sans-serif`;
          const lbl = line.label.length > 26 ? line.label.slice(0, 26) + "…" : line.label;
          ctx.fillText(lbl, bx + PAD * 1.2 + FONT, ly);
        }
        ctx.font = `${FONT}px sans-serif`;
        ly += lineH;
      });
      clearShadow(ctx);
      ctx.restore();
    }
  }

  // ── Flèche Nord — picto simple sans cercle fond ───────────────
  if (showNorth) {
    const sz = Math.round(W * 0.016);
    const nx = W - PAD * 4 - sz;
    const ny = PAD * 4 + sz;
    ctx.save();
    setShadow(ctx, SH * 2);

    // Lettre N
    ctx.fillStyle = "#ffffff";
    ctx.font = `bold ${Math.round(sz * 1.1)}px sans-serif`;
    ctx.textAlign = "center";
    ctx.fillText("N", nx, ny - sz * 0.6);

    // Flèche
    ctx.fillStyle = "#ffffff";
    ctx.beginPath();
    ctx.moveTo(nx, ny - sz * 0.2);
    ctx.lineTo(nx - sz * 0.45, ny + sz * 0.8);
    ctx.lineTo(nx, ny + sz * 0.4);
    ctx.lineTo(nx + sz * 0.45, ny + sz * 0.8);
    ctx.closePath();
    ctx.fill();

    ctx.textAlign = "left";
    clearShadow(ctx);
    ctx.restore();
  }

  // ── Barre d'échelle — lignes + texte blancs, sans fond ────────
  if (showScale && vs) {
    const mpp    = 156543.03 * Math.cos(vs.latitude * Math.PI / 180) / Math.pow(2, vs.zoom);
    const barPx  = Math.round(W * 0.14);
    const scaleM = Math.round(mpp * barPx);
    const label  = scaleM >= 1000 ? `${(scaleM / 1000).toFixed(1)} km` : `${scaleM} m`;
    const margin = PAD * 2.5;
    const northOffset = showNorth ? Math.round(W * 0.075) : 0;
    const sx = W - margin - barPx - northOffset;
    const sy = H - margin;

    ctx.save();
    setShadow(ctx, SH * 2);

    ctx.strokeStyle = "#ffffff";
    ctx.lineWidth   = Math.max(2, Math.round(W * 0.003));
    ctx.lineCap     = "round";
    ctx.beginPath();
    ctx.moveTo(sx, sy);
    ctx.lineTo(sx + barPx, sy);
    ctx.stroke();
    ctx.lineWidth = Math.max(1.5, Math.round(W * 0.002));
    ctx.beginPath();
    ctx.moveTo(sx,          sy - SMALL * 0.45);
    ctx.lineTo(sx,          sy + SMALL * 0.45);
    ctx.moveTo(sx + barPx,  sy - SMALL * 0.45);
    ctx.lineTo(sx + barPx,  sy + SMALL * 0.45);
    ctx.stroke();

    ctx.fillStyle  = "#ffffff";
    ctx.font       = `${SMALL}px sans-serif`;
    ctx.textAlign  = "center";
    ctx.fillText(label, sx + barPx / 2, sy - SMALL * 0.65);
    ctx.textAlign  = "left";

    clearShadow(ctx);
    ctx.restore();
  }
}

// ── Génère un canvas "aperçu complet" avec titre/desc/sources + carte ──
async function buildPreviewCanvas(mapRef, layers, viewState, opts) {
  const { title, subtitle, sources, showLegend, legendPos, showNorth, showScale } = opts;

  // 1. Capturer la carte
  const mapDataUrl = await new Promise((resolve, reject) => {
    const map = mapRef.current?.getMap?.();
    if (!map) return reject(new Error("Carte non disponible"));

    const doCapture = () => {
      try {
        const container = map.getContainer();
        const canvases  = Array.from(container.querySelectorAll("canvas"));
        if (!canvases.length) return reject(new Error("Aucun canvas trouvé"));

        const main = canvases.reduce((a, b) =>
          a.width * a.height >= b.width * b.height ? a : b
        );
        const W = main.width, H = main.height;
        if (!W || !H) return reject(new Error(
          "Canvas vide — ajoutez preserveDrawingBuffer={true} sur <Map>"
        ));

        const out = document.createElement("canvas");
        out.width = W; out.height = H;
        const ctx = out.getContext("2d");
        ctx.fillStyle = "#1a1a1a";
        ctx.fillRect(0, 0, W, H);
        canvases.forEach(c => {
          if (c.width > 0 && c.height > 0) {
            try { ctx.drawImage(c, 0, 0, W, H); } catch (_) {}
          }
        });

        // Overlays sur la carte
        drawOverlaysOnCanvas(ctx, W, H, { layers, showLegend, legendPos, showNorth, showScale, vs: viewState });

        resolve({ dataUrl: out.toDataURL("image/png"), W, H });
      } catch (e) {
        reject(new Error("Erreur capture : " + e.message));
      }
    };

    map.once("render", doCapture);
    map.triggerRepaint();
  });

  // 2. Composer carte + textes sur un canvas final
  const { dataUrl: mapUrl, W: MW, H: MH } = mapDataUrl;
  const SCALE    = 1;
  const F_SIZE   = Math.round(MW * 0.022);   // titre
  const SF_SIZE  = Math.round(MW * 0.015);   // sous-titre
  const SM_SIZE  = Math.round(MW * 0.011);   // sources / coords
  const PAD      = Math.round(MW * 0.018);
  const headerH  = PAD + F_SIZE + (subtitle ? SF_SIZE * 1.8 : 0) + SM_SIZE * 2 + PAD;
  const footerH  = SM_SIZE * 2.5 + PAD;
  const TW       = MW;
  const TH       = MH + headerH + footerH;

  const final = document.createElement("canvas");
  final.width  = TW;
  final.height = TH;
  const ctx = final.getContext("2d");

  // Fond global
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, TW, TH);

  // ── Header ──
  let y = PAD + F_SIZE;
  ctx.fillStyle = "#111111";
  ctx.font      = `700 ${F_SIZE}px sans-serif`;
  ctx.fillText(title || "Carte", PAD, y);
  y += Math.round(F_SIZE * 0.4);

  if (subtitle) {
    y += SF_SIZE * 1.2;
    ctx.fillStyle = "#555555";
    ctx.font      = `400 ${SF_SIZE}px sans-serif`;
    ctx.fillText(subtitle, PAD, y);
  }

  y += SM_SIZE * 1.8;
  ctx.fillStyle = "#999999";
  ctx.font      = `400 ${SM_SIZE}px sans-serif`;
  const vs = viewState;
  ctx.fillText(
    `${vs.longitude.toFixed(4)}, ${vs.latitude.toFixed(4)}  |  zoom ${vs.zoom.toFixed(1)}  |  ${new Date().toLocaleDateString("fr-FR")} ${new Date().toLocaleTimeString("fr-FR")}`,
    PAD, y
  );

  // Ligne séparatrice
  y += PAD * 0.6;
  ctx.strokeStyle = "#e0e0e0";
  ctx.lineWidth   = 1;
  ctx.beginPath();
  ctx.moveTo(PAD, y);
  ctx.lineTo(TW - PAD, y);
  ctx.stroke();

  // ── Carte ──
  const mapImg = new Image();
  await new Promise(res => { mapImg.onload = res; mapImg.src = mapUrl; });
  ctx.drawImage(mapImg, 0, headerH, TW, MH);

  // ── Footer sources ──
  const fy = TH - PAD * 0.6;
  ctx.font      = `400 ${SM_SIZE}px sans-serif`;
  ctx.fillStyle = "#999999";
  ctx.textAlign = "left";
  ctx.fillText(sources || "Overture Maps Explorer", PAD, fy);
  ctx.textAlign = "right";
  ctx.fillText("OpenMapAgents", TW - PAD, fy);
  ctx.textAlign = "left";

  return final.toDataURL("image/jpeg", 0.93);
}

// ── Veil plein écran ────────────────────────────────────────────
function PreviewVeil({ dataUrl, legendPos, onLegendPos, onClose, previewing }) {
  const C = getTheme();
  const corners = [
    { key: "top-left",     style: { top: 8,    left: 8    } },
    { key: "top-right",    style: { top: 8,    right: 44  } }, // décalé à gauche du ✕
    { key: "bottom-left",  style: { bottom: 8, left: 8    } },
    { key: "bottom-right", style: { bottom: 8, right: 8   } },
  ];
  const ARROW = { "top-left": "↖", "top-right": "↗", "bottom-left": "↙", "bottom-right": "↘" };

  return (
    <div
      style={{ position: "fixed", inset: 0, zIndex: 200, background: "rgba(0,0,0,0.86)", display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={onClose}
    >
      <div onClick={e => e.stopPropagation()} style={{ position: "relative", maxWidth: "90vw", maxHeight: "88vh", borderRadius: 10, overflow: "hidden", boxShadow: "0 8px 40px rgba(0,0,0,0.7)", border: "1px solid #333" }}>
        {previewing
          ? <div style={{ width: 600, height: 400, background: "#111", display: "flex", alignItems: "center", justifyContent: "center", color: "#555", fontSize: 14 }}>⏳ Mise à jour…</div>
          : <img src={dataUrl} style={{ display: "block", maxWidth: "90vw", maxHeight: "88vh", objectFit: "contain" }} alt="Aperçu" />
        }
        {corners.map(c => (
          <button key={c.key} onClick={() => onLegendPos(c.key)} title="Déplacer légende ici"
            style={{
              position: "absolute", ...c.style, width: 34, height: 34, borderRadius: 6,
              background: legendPos === c.key ? "rgba(29,158,117,0.85)" : "rgba(20,20,20,0.75)",
              border: legendPos === c.key ? "2px solid #1D9E75" : "1px solid rgba(255,255,255,0.2)",
              color: legendPos === c.key ? "#fff" : "rgba(255,255,255,0.55)",
              cursor: "pointer", fontSize: 16,
              display: "flex", alignItems: "center", justifyContent: "center",
            }}>
            {legendPos === c.key ? "✓" : ARROW[c.key]}
          </button>
        ))}
        <button onClick={onClose} style={{ position: "absolute", top: 8, right: 8, width: 32, height: 32, borderRadius: "50%", background: "rgba(0,0,0,0.7)", border: "1px solid #555", color: "#fff", cursor: "pointer", fontSize: 15, display: "flex", alignItems: "center", justifyContent: "center" }}>✕</button>
        <div style={{ position: "absolute", bottom: 46, left: "50%", transform: "translateX(-50%)", background: "rgba(0,0,0,0.65)", borderRadius: 20, padding: "4px 12px", fontSize: 10, color: "rgba(255,255,255,0.6)", whiteSpace: "nowrap", pointerEvents: "none" }}>
          Cliquez un coin pour repositionner la légende
        </div>
      </div>
    </div>
  );
}

// ── Composant principal ─────────────────────────────────────────
export default function PrintPanel({ mapRef, layers, viewState, onClose }) {
  const C = getTheme();
  const [title, setTitle]             = useState("Carte Overture Maps");
  const [subtitle, setSubtitle]       = useState("");
  const [sources, setSources]         = useState("Overture Maps Foundation | OpenStreetMap | OpenFreeMap");
  const [showLegend, setShowLegend]   = useState(true);
  const [legendPos, setLegendPos]     = useState("bottom-left");
  const [showNorth, setShowNorth]     = useState(true);
  const [showScale, setShowScale]     = useState(true);
  const [format, setFormat]           = useState("A4");
  const [orientation, setOrientation] = useState("landscape");
  const [printing, setPrinting]       = useState(false);
  const [previewing, setPreviewing]   = useState(false);
  const [preview, setPreview]         = useState(null);
  const [veil, setVeil]               = useState(false);
  const [error, setError]             = useState(null);

  // ── Drag ────────────────────────────────────────────────────
  const [pos, setPos]     = useState({ x: null, y: null }); // null = position CSS par défaut
  const dragRef           = useRef({ dragging: false, ox: 0, oy: 0, px: 0, py: 0 });
  const panelRef          = useRef(null);

  const onDragStart = useCallback((e) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const rect = panelRef.current?.getBoundingClientRect();
    dragRef.current = {
      dragging: true,
      ox: e.clientX - (rect?.left ?? 0),
      oy: e.clientY - (rect?.top  ?? 0),
    };
    const onMove = (ev) => {
      if (!dragRef.current.dragging) return;
      setPos({ x: ev.clientX - dragRef.current.ox, y: ev.clientY - dragRef.current.oy });
    };
    const onUp = () => {
      dragRef.current.dragging = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup",   onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup",   onUp);
  }, []);

  const getOpts = (pos) => ({
    title, subtitle, sources,
    showLegend, legendPos: pos ?? legendPos,
    showNorth, showScale,
  });

  const doCapture = useCallback(async (pos) => {
    return await buildPreviewCanvas(mapRef, layers, viewState, getOpts(pos));
  }, [mapRef, layers, viewState, title, subtitle, sources, showLegend, legendPos, showNorth, showScale]);

  const generatePreview = useCallback(async () => {
    setError(null);
    setPreviewing(true);
    try {
      const url = await doCapture();
      setPreview(url);
    } catch (e) { setError(e.message); }
    setPreviewing(false);
  }, [doCapture]);

  const handleLegendPosChange = useCallback(async (newPos) => {
    setLegendPos(newPos);
    setPreviewing(true);
    try {
      const url = await doCapture(newPos);
      setPreview(url);
    } catch (e) { setError(e.message); }
    setPreviewing(false);
  }, [doCapture]);

  const doPrint = useCallback(async () => {
    setError(null);
    setPrinting(true);
    try {
      const { jsPDF } = await import("jspdf");
      const imgUrl = preview || await doCapture();

      const pdf = new jsPDF({ orientation, unit: "mm", format: format === "A3" ? "a3" : "a4" });
      const pw  = pdf.internal.pageSize.getWidth();
      const ph  = pdf.internal.pageSize.getHeight();
      const mg  = 8;
      pdf.addImage(imgUrl, "JPEG", mg, mg, pw - mg * 2, ph - mg * 2);
      pdf.save(`carte_${format}_${Date.now()}.pdf`);
    } catch (e) { setError(e.message); }
    setPrinting(false);
  }, [preview, doCapture, format, orientation]);

  const inp = { fontFamily: F, fontSize: 11, padding: "5px 9px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" };

  const ARROWS = { "top-left": "↖", "top-right": "↗", "bottom-left": "↙", "bottom-right": "↘" };
  const LEG_POS = ["top-left", "top-right", "bottom-left", "bottom-right"];

  return (
    <>
      {veil && preview && (
        <PreviewVeil
          dataUrl={preview}
          legendPos={legendPos}
          onLegendPos={handleLegendPosChange}
          onClose={() => setVeil(false)}
          previewing={previewing}
        />
      )}

      <div ref={panelRef} style={{
        position: "fixed",
        ...(pos.x !== null
          ? { left: pos.x, top: pos.y }
          : { top: 50, right: 10 }
        ),
        zIndex: 25, width: 310,
        background: C.card, borderRadius: 10, border: `0.5px solid ${C.bdr}`,
        boxShadow: "0 4px 24px rgba(0,0,0,0.3)", padding: 12,
        display: "flex", flexDirection: "column", gap: 9,
        maxHeight: "84vh", overflowY: "auto",
        userSelect: "none",
      }}>

        {/* Header — zone de drag */}
        <div
          onMouseDown={onDragStart}
          style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "grab", paddingBottom: 2 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ fontSize: 11, color: C.dim, letterSpacing: 2 }}>⠿</span>
            ⎙ Impression
          </div>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", fontSize: 16 }}>✕</button>
        </div>

        {/* Prérequis 
        <div style={{ fontSize: 9, color: C.dim, background: C.hover, border: `0.5px solid ${C.bdr}`, borderRadius: 5, padding: "3px 7px" }}>
          App.jsx → <code style={{ fontFamily: M, color: C.acc, fontSize: 9 }}>{"<Map preserveDrawingBuffer={true}>"}</code>
        </div>
        */}
        {/* Titre */}
        <div>
          <div style={{ fontSize: 9, color: C.dim, marginBottom: 2, textTransform: "uppercase", letterSpacing: ".05em" }}>Titre</div>
          <input value={title} onChange={e => setTitle(e.target.value)} style={inp} />
        </div>

        {/* Sous-titre */}
        <div>
          <div style={{ fontSize: 9, color: C.dim, marginBottom: 2, textTransform: "uppercase", letterSpacing: ".05em" }}>Sous-titre</div>
          <input value={subtitle} onChange={e => setSubtitle(e.target.value)} placeholder="Optionnel" style={{ ...inp, fontSize: 10 }} />
        </div>

        {/* Sources */}
        <div>
          <div style={{ fontSize: 9, color: C.dim, marginBottom: 2, textTransform: "uppercase", letterSpacing: ".05em" }}>Sources</div>
          <input value={sources} onChange={e => setSources(e.target.value)} style={{ ...inp, fontSize: 9 }} />
        </div>

        {/* Format + Orientation */}
        <div style={{ display: "flex", gap: 6 }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 2, textTransform: "uppercase", letterSpacing: ".05em" }}>Format</div>
            <div style={{ display: "flex", gap: 3 }}>
              {["A4", "A3"].map(f => (
                <button key={f} onClick={() => setFormat(f)} style={{ fontFamily: F, fontSize: 10, padding: "4px 0", borderRadius: 4, flex: 1, background: format === f ? C.acc + "18" : "transparent", border: `0.5px solid ${format === f ? C.acc + "66" : C.bdr}`, color: format === f ? C.acc : C.dim, cursor: "pointer" }}>{f}</button>
              ))}
            </div>
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 2, textTransform: "uppercase", letterSpacing: ".05em" }}>Orientation</div>
            <div style={{ display: "flex", gap: 3 }}>
              {[["landscape","Paysage"],["portrait","Portrait"]].map(([k,l]) => (
                <button key={k} onClick={() => setOrientation(k)} style={{ fontFamily: F, fontSize: 9, padding: "4px 0", borderRadius: 4, flex: 1, background: orientation === k ? C.acc + "18" : "transparent", border: `0.5px solid ${orientation === k ? C.acc + "66" : C.bdr}`, color: orientation === k ? C.acc : C.dim, cursor: "pointer" }}>{l}</button>
              ))}
            </div>
          </div>
        </div>

        <div style={{ borderTop: `0.5px solid ${C.bdr}` }} />

        {/* Éléments carte — une ligne */}
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", flexShrink: 0, marginRight: 2 }}>Carte</div>
          {[["Légende", showLegend, setShowLegend], ["Nord", showNorth, setShowNorth], ["Échelle", showScale, setShowScale]].map(([lbl, val, set]) => (
            <button key={lbl} onClick={() => set(v => !v)} style={{ fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4, background: val ? C.acc + "18" : "transparent", border: `0.5px solid ${val ? C.acc + "55" : C.bdr}`, color: val ? C.acc : C.dim, cursor: "pointer", whiteSpace: "nowrap" }}>{val ? "✓ " : ""}{lbl}</button>
          ))}
        </div>

        {/* Position légende — une ligne, 4 boutons flèches */}
        {showLegend && (
          <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
            <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", flexShrink: 0, marginRight: 2 }}>Légende</div>
            {LEG_POS.map(pos => (
              <button key={pos} onClick={() => setLegendPos(pos)} title={pos.replace("-", " ")}
                style={{
                  fontFamily: F, fontSize: 14, padding: "2px 0", borderRadius: 4, flex: 1,
                  background: legendPos === pos ? C.acc + "20" : "transparent",
                  border: `0.5px solid ${legendPos === pos ? C.acc + "66" : C.bdr}`,
                  color: legendPos === pos ? C.acc : C.dim,
                  cursor: "pointer", lineHeight: 1,
                  display: "flex", alignItems: "center", justifyContent: "center", height: 28,
                }}>
                {legendPos === pos
                  ? <span style={{ fontSize: 9, fontWeight: 600 }}>✓</span>
                  : ARROWS[pos]
                }
              </button>
            ))}
          </div>
        )}

        <div style={{ borderTop: `0.5px solid ${C.bdr}` }} />

        {/* Erreur */}
        {error && (
          <div style={{ fontSize: 10, color: C.red, background: C.red + "12", border: `0.5px solid ${C.red}33`, borderRadius: 5, padding: "6px 8px", lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>❌ {error}</div>
        )}

        {/* Bouton aperçu */}
        <button onClick={generatePreview} disabled={previewing} style={{ fontFamily: F, fontSize: 11, padding: "7px 12px", borderRadius: 6, background: C.hover, color: C.txt, border: `0.5px solid ${C.bdr}`, cursor: previewing ? "default" : "pointer", opacity: previewing ? 0.6 : 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 6 }}>
          {previewing ? "⏳ Capture…" : "🔍 Aperçu carte"}
        </button>

        {/* Preview */}
        {preview && (
          <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
            <div
              style={{ position: "relative", borderRadius: 7, overflow: "hidden", border: `0.5px solid ${C.bdr}`, cursor: "pointer" }}
              onClick={() => setVeil(true)}
            >
              <img src={preview} style={{ width: "100%", display: "block" }} alt="Aperçu" />
              <div
                style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", display: "flex", alignItems: "center", justifyContent: "center", opacity: 0, transition: "opacity .18s" }}
                onMouseEnter={e => e.currentTarget.style.opacity = 1}
                onMouseLeave={e => e.currentTarget.style.opacity = 0}
              >
                <div style={{ background: "rgba(0,0,0,0.7)", borderRadius: 16, padding: "5px 12px", fontSize: 11, color: "#fff" }}>🔎 Plein écran</div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={() => setVeil(true)} style={{ fontFamily: F, fontSize: 9, padding: "4px 0", borderRadius: 4, flex: 1, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.mut, cursor: "pointer" }}>🔎 Plein écran</button>
              <button onClick={generatePreview} disabled={previewing} style={{ fontFamily: F, fontSize: 9, padding: "4px 0", borderRadius: 4, flex: 1, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>↺ Rafraîchir</button>
              <button onClick={() => setPreview(null)} style={{ fontFamily: F, fontSize: 9, padding: "4px 0", borderRadius: 4, flex: 1, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>✕ Effacer</button>
            </div>
          </div>
        )}

        {/* Export */}
        <button onClick={doPrint} disabled={printing} style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 16px", borderRadius: 7, background: printing ? C.acc + "88" : C.acc, color: "#fff", border: "none", cursor: printing ? "default" : "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6 }}>
          {printing ? "⏳ Génération…" : "⎙ Exporter PDF"}
        </button>

        <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.5 }}>
          Titre, sources et éléments sont intégrés dans l'aperçu et le PDF.
        </div>
      </div>
    </>
  );
}

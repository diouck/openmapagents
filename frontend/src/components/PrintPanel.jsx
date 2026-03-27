import { useState, useCallback, useRef } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";
import { Btn } from "./ui";

export default function PrintPanel({ mapRef, layers, viewState, onClose }) {
  const C = getTheme();
  const [title, setTitle] = useState("Carte Overture Maps");
  const [subtitle, setSubtitle] = useState("");
  const [sources, setSources] = useState("Overture Maps Foundation | OpenStreetMap | OpenFreeMap");
  const [showLegend, setShowLegend] = useState(true);
  const [showNorth, setShowNorth] = useState(true);
  const [showScale, setShowScale] = useState(true);
  const [format, setFormat] = useState("A4");
  const [orientation, setOrientation] = useState("landscape");
  const [dpi, setDpi] = useState(150);
  const [printing, setPrinting] = useState(false);
  const [preview, setPreview] = useState(null);

  const generatePreview = useCallback(() => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    const canvas = map.getCanvas();
    setPreview(canvas.toDataURL("image/png"));
  }, [mapRef]);

  const doPrint = useCallback(async () => {
    setPrinting(true);
    try {
      const { jsPDF } = await import("jspdf");
      const map = mapRef.current?.getMap?.();
      if (!map) throw new Error("Carte non disponible");

      // Get map canvas as image
      const canvas = map.getCanvas();
      const mapImg = canvas.toDataURL("image/jpeg", 0.95);

      // PDF dimensions
      const isA3 = format === "A3";
      const isPortrait = orientation === "portrait";
      const pdf = new jsPDF({
        orientation,
        unit: "mm",
        format: isA3 ? "a3" : "a4",
      });

      const pw = pdf.internal.pageSize.getWidth();
      const ph = pdf.internal.pageSize.getHeight();
      const mg = 12;

      // ── TITLE ──
      let y = mg;
      pdf.setFont("helvetica", "bold");
      pdf.setFontSize(18);
      pdf.setTextColor(30, 30, 30);
      pdf.text(title || "Carte", mg, y + 6);
      y += 8;

      if (subtitle) {
        pdf.setFont("helvetica", "normal");
        pdf.setFontSize(11);
        pdf.setTextColor(100, 100, 100);
        pdf.text(subtitle, mg, y + 5);
        y += 7;
      }

      // Coordinates + date
      pdf.setFontSize(8);
      pdf.setTextColor(150);
      const vs = viewState;
      pdf.text(
        `${vs.longitude.toFixed(4)}, ${vs.latitude.toFixed(4)} | zoom ${vs.zoom.toFixed(1)} | ${new Date().toLocaleDateString("fr-FR")} ${new Date().toLocaleTimeString("fr-FR")}`,
        mg, y + 4
      );
      y += 8;

      // ── MAP IMAGE ──
      const mapY = y;
      const legendW = showLegend ? 55 : 0;
      const mapW = pw - mg * 2 - legendW;
      const mapH = ph - y - mg - 14;
      pdf.addImage(mapImg, "JPEG", mg, mapY, mapW, mapH);
      pdf.setDrawColor(180);
      pdf.setLineWidth(0.3);
      pdf.rect(mg, mapY, mapW, mapH);

      // ── NORTH ARROW ──
      if (showNorth) {
        const nx = mg + mapW - 8;
        const ny = mapY + 10;
        pdf.setFillColor(40, 40, 40);
        pdf.triangle(nx, ny - 6, nx - 3, ny + 2, nx + 3, ny + 2, "F");
        pdf.setFontSize(7);
        pdf.setTextColor(40);
        pdf.text("N", nx - 1.5, ny - 7);
      }

      // ── SCALE BAR ──
      if (showScale) {
        const sx = mg + 4;
        const sy = mapY + mapH - 6;
        const mpp = 156543.03 * Math.cos(vs.latitude * Math.PI / 180) / Math.pow(2, vs.zoom);
        const scaleM = Math.round(mpp * mapW * (dpi / 25.4) / 4);
        const barMM = mapW / 4;
        const label = scaleM >= 1000 ? `${(scaleM / 1000).toFixed(1)} km` : `${scaleM} m`;
        pdf.setDrawColor(40);
        pdf.setLineWidth(0.5);
        pdf.line(sx, sy, sx + barMM, sy);
        pdf.line(sx, sy - 1, sx, sy + 1);
        pdf.line(sx + barMM, sy - 1, sx + barMM, sy + 1);
        pdf.setFontSize(7);
        pdf.setTextColor(40);
        pdf.text(label, sx + barMM / 2, sy - 2, { align: "center" });
      }

      // ── LEGEND ──
      if (showLegend && layers.length > 0) {
        const lx = pw - mg - legendW + 4;
        let ly = mapY + 4;
        pdf.setFontSize(9);
        pdf.setFont("helvetica", "bold");
        pdf.setTextColor(60);
        pdf.text("Legende", lx, ly + 3);
        ly += 8;
        pdf.setFont("helvetica", "normal");

        layers.filter(l => l.visible).forEach(l => {
          // Color swatch
          const hex = l.color || "#888";
          const r = parseInt(hex.slice(1, 3), 16);
          const g = parseInt(hex.slice(3, 5), 16);
          const b = parseInt(hex.slice(5, 7), 16);
          pdf.setFillColor(r, g, b);
          pdf.circle(lx + 3, ly, 2, "F");

          pdf.setFontSize(8);
          pdf.setTextColor(60);
          const name = l.name.length > 22 ? l.name.slice(0, 22) + "..." : l.name;
          pdf.text(`${name} (${l.featureCount})`, lx + 8, ly + 1);
          ly += 6;

          // Classification entries
          if (l.classResult?.type === "categorized") {
            l.classResult.entries?.slice(0, 6).forEach(e => {
              const cr = parseInt(e.color.slice(1, 3), 16);
              const cg = parseInt(e.color.slice(3, 5), 16);
              const cb = parseInt(e.color.slice(5, 7), 16);
              pdf.setFillColor(cr, cg, cb);
              pdf.rect(lx + 6, ly - 2, 4, 4, "F");
              pdf.setFontSize(6);
              pdf.setTextColor(100);
              const val = String(e.value).length > 18 ? String(e.value).slice(0, 18) + "..." : e.value;
              pdf.text(`${val} (${e.count})`, lx + 13, ly + 0.5);
              ly += 4;
            });
          }
          if (l.classResult?.type === "graduated") {
            l.classResult.classes?.forEach(c => {
              const cr = parseInt(c.color.slice(1, 3), 16);
              const cg = parseInt(c.color.slice(3, 5), 16);
              const cb = parseInt(c.color.slice(5, 7), 16);
              pdf.setFillColor(cr, cg, cb);
              pdf.rect(lx + 6, ly - 2, 4, 4, "F");
              pdf.setFontSize(6);
              pdf.setTextColor(100);
              pdf.text(`${c.min.toFixed(1)}-${c.max.toFixed(1)} (${c.count})`, lx + 13, ly + 0.5);
              ly += 4;
            });
          }
          ly += 2;
        });
      }

      // ── SOURCES / FOOTER ──
      pdf.setFontSize(7);
      pdf.setTextColor(150);
      pdf.text(sources || "Overture Maps", mg, ph - mg + 2);
      pdf.text(`Overture Maps Explorer | ${format} ${orientation}`, pw - mg, ph - mg + 2, { align: "right" });

      pdf.save(`carte_${format}_${Date.now()}.pdf`);
    } catch (e) {
      alert("Erreur impression: " + e.message);
    }
    setPrinting(false);
  }, [mapRef, title, subtitle, sources, showLegend, showNorth, showScale, format, orientation, dpi, layers, viewState]);

  return (
    <div style={{
      position: "absolute", top: 50, right: 10, zIndex: 25, width: 320,
      background: C.card, borderRadius: 10, border: `0.5px solid ${C.bdr}`,
      boxShadow: "0 4px 20px rgba(0,0,0,0.25)", padding: 14,
      display: "flex", flexDirection: "column", gap: 10, maxHeight: "80vh", overflowY: "auto",
    }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: C.txt }}>Impression</div>
        <button onClick={onClose} style={{ fontSize: 12, background: "none", border: "none", color: C.dim, cursor: "pointer", fontFamily: F }}>✕</button>
      </div>

      {/* Title */}
      <div>
        <div style={{ fontSize: 10, color: C.dim, marginBottom: 2, textTransform: "uppercase" }}>Titre</div>
        <input value={title} onChange={e => setTitle(e.target.value)}
          style={{ fontFamily: F, fontSize: 12, padding: "6px 10px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
      </div>

      {/* Subtitle */}
      <div>
        <div style={{ fontSize: 10, color: C.dim, marginBottom: 2, textTransform: "uppercase" }}>Sous-titre</div>
        <input value={subtitle} onChange={e => setSubtitle(e.target.value)} placeholder="Description optionnelle"
          style={{ fontFamily: F, fontSize: 12, padding: "6px 10px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
      </div>

      {/* Sources */}
      <div>
        <div style={{ fontSize: 10, color: C.dim, marginBottom: 2, textTransform: "uppercase" }}>Sources</div>
        <input value={sources} onChange={e => setSources(e.target.value)}
          style={{ fontFamily: F, fontSize: 11, padding: "6px 10px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
      </div>

      {/* Format + Orientation */}
      <div style={{ display: "flex", gap: 8 }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 10, color: C.dim, marginBottom: 2, textTransform: "uppercase" }}>Format</div>
          <div style={{ display: "flex", gap: 3 }}>
            {["A4", "A3"].map(f => (
              <button key={f} onClick={() => setFormat(f)} style={{
                fontFamily: F, fontSize: 11, padding: "4px 12px", borderRadius: 5, flex: 1,
                background: format === f ? C.acc + "18" : "transparent",
                border: `0.5px solid ${format === f ? C.acc + "55" : C.bdr}`,
                color: format === f ? C.acc : C.dim, cursor: "pointer",
              }}>{f}</button>
            ))}
          </div>
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 10, color: C.dim, marginBottom: 2, textTransform: "uppercase" }}>Orientation</div>
          <div style={{ display: "flex", gap: 3 }}>
            {[["landscape", "Paysage"], ["portrait", "Portrait"]].map(([k, l]) => (
              <button key={k} onClick={() => setOrientation(k)} style={{
                fontFamily: F, fontSize: 10, padding: "4px 8px", borderRadius: 5, flex: 1,
                background: orientation === k ? C.acc + "18" : "transparent",
                border: `0.5px solid ${orientation === k ? C.acc + "55" : C.bdr}`,
                color: orientation === k ? C.acc : C.dim, cursor: "pointer",
              }}>{l}</button>
            ))}
          </div>
        </div>
      </div>

      {/* Options */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {[
          ["Legende", showLegend, setShowLegend],
          ["Nord", showNorth, setShowNorth],
          ["Echelle", showScale, setShowScale],
        ].map(([label, val, setter]) => (
          <button key={label} onClick={() => setter(!val)} style={{
            fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
            background: val ? C.acc + "18" : "transparent",
            border: `0.5px solid ${val ? C.acc + "44" : C.bdr}`,
            color: val ? C.acc : C.dim, cursor: "pointer",
          }}>{val ? "✓" : ""} {label}</button>
        ))}
      </div>

      {/* Preview */}
      <button onClick={generatePreview} style={{
        fontFamily: F, fontSize: 11, padding: "6px 12px", borderRadius: 6,
        background: C.hover, color: C.txt, border: `0.5px solid ${C.bdr}`, cursor: "pointer",
      }}>Apercu</button>

      {preview && (
        <div style={{ borderRadius: 6, overflow: "hidden", border: `0.5px solid ${C.bdr}` }}>
          <img src={preview} style={{ width: "100%", display: "block" }} alt="preview" />
        </div>
      )}

      {/* Print */}
      <button onClick={doPrint} disabled={printing} style={{
        fontFamily: F, fontSize: 12, fontWeight: 500, padding: "8px 16px", borderRadius: 6,
        background: C.acc, color: "#fff", border: "none",
        cursor: printing ? "default" : "pointer", opacity: printing ? 0.6 : 1,
      }}>{printing ? "Generation..." : "Exporter PDF"}</button>

      <div style={{ fontSize: 9, color: C.dim }}>
        Naviguez et zoomez la carte avant d'exporter. Le PDF capture l'etat actuel de la carte.
      </div>
    </div>
  );
}

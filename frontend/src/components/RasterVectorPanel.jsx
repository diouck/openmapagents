/**
 * RasterVectorPanel.jsx — Vectorisation raster : polygones + courbes de niveau.
 *
 * Sur un GeoTIFF mono-bande importé :
 *  • Polygones  → POST /api/raster/polygonize (classes → polygones par classe).
 *  • Contours   → POST /api/raster/contours (isolignes à N niveaux ou intervalle).
 * Les deux renvoient un GeoJSON (WGS84) ajouté à la carte comme couche vecteur.
 */
import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

export default function RasterVectorPanel({ layers = [], onAddLayer }) {
  const C = useThemeContext();
  // Rasters mono-bande importés (jeton) ET toute couche image géoréférencée
  // (viewshed, scène, image calée…) → vectorisation par luminance.
  const rasters = useMemo(() => layers.filter((l) => l.kind === "image" && (l.rasterToken || (l.imageUrl && l.coordinates))), [layers]);
  const [rid, setRid] = useState(rasters[0]?.id || "");
  const raster = rasters.find((l) => l.id === rid) || rasters[0] || null;

  const [tab, setTab] = useState("poly");
  const [classes, setClasses] = useState(5);
  const [count, setCount] = useState(10);
  const [ivStr, setIvStr] = useState("");
  const [band, setBand] = useState(1);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [msg, setMsg] = useState(null);

  const call = useCallback(async (path, body, layerName, theme) => {
    if (!raster) return;
    const src = raster.rasterToken ? { raster_token: raster.rasterToken, band: Number(band) }
      : { image_b64: raster.imageUrl, coordinates: raster.coordinates };
    setBusy(true); setErr(null); setMsg(null);
    try {
      const r = await fetch(`${API}/raster/${path}`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...src, ...body }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      if (d.geojson?.features?.length) {
        onAddLayer?.(d.geojson, `${layerName} · ${raster.name}`, theme);
        setMsg(d.message || `${d.count} entité(s) ajoutée(s).`);
      } else {
        setMsg("Aucune entité générée.");
      }
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [raster, band, onAddLayer]);

  const runPoly = () => call("polygonize", { classes: Number(classes) }, "Polygones", "polygonize");
  const runContours = () => {
    const iv = parseFloat(ivStr);
    call("contours", iv > 0 ? { interval: iv } : { count: Number(count) }, "Contours", "contour");
  };

  const tabBtn = (id, label) => (
    <button key={id} onClick={() => setTab(id)}
      style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>
      {label}
    </button>
  );
  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };
  const Code = ({ children }) => (<code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>);
  const runBtn = (onClick, label) => (
    <button onClick={onClick} disabled={busy || !raster}
      style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: (busy || !raster) ? "not-allowed" : "pointer",
        background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: !raster ? 0.5 : 1 }}>
      {busy ? "Calcul…" : label}
    </button>
  );

  const rasterSelect = (
    <div>
      <div style={lbl}>Raster ou couche image</div>
      {rasters.length === 0 ? (
        <div style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucune — importez un GeoTIFF ou ajoutez une couche image (viewshed, scène…).</div>
      ) : (
        <select value={rid} onChange={(e) => setRid(e.target.value)} style={inp}>
          {rasters.map((l) => <option key={l.id} value={l.id}>{l.name}</option>)}
        </select>
      )}
      {raster?.rasterToken && raster?.bands > 1 && (
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 5 }}>
          <span style={{ fontFamily: F, fontSize: 10.5, color: C.dim }}>Bande</span>
          <select value={band} onChange={(e) => setBand(Number(e.target.value))} style={{ ...inp, width: "auto" }}>
            {Array.from({ length: raster.bands }, (_, i) => <option key={i} value={i + 1}>Bande {i + 1}</option>)}
          </select>
        </div>
      )}
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("poly", "Polygones")}
        {tabBtn("cont", "Contours")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Convertit un <b>raster</b> en couche vecteur : GeoTIFF mono-bande importé, ou <b>toute couche image géoréférencée</b> (viewshed, scène satellite, image calée) — vectorisée par sa luminance (les zones transparentes = sans donnée).</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Polygones</div>
            <p style={{ margin: 0, color: C.mut }}>Découpe le raster en <b>N classes</b> d'intervalles égaux et vectorise chaque classe en polygones (propriétés <Code>class</Code>, <Code>min</Code>, <Code>max</Code>). Idéal pour une carte d'occupation issue d'un indice, ou pour délimiter des zones (pente forte, altitude…).</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Contours (courbes de niveau)</div>
            <p style={{ margin: 0, color: C.mut }}>Trace des <b>isolignes</b> à des valeurs régulières — soit un <b>nombre de niveaux</b>, soit un <b>intervalle</b> fixe (ex. tous les 10 m sur un MNT). Chaque ligne porte sa valeur (<Code>level</Code>).</p>
          </div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
Fonctionne sur un GeoTIFF mono-bande importé ou toute couche image géoréférencée (vectorisée par luminance). Sortie bornée (≈20 000 polygones / 300 000 sommets) : réduisez le nombre de classes ou augmentez l'intervalle si c'est tronqué. La géométrie est renvoyée en WGS84, stylable et exportable comme toute couche vecteur.
          </div>
        </div>
      ) : tab === "poly" ? (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {rasterSelect}
          <div>
            <div style={lbl}>Nombre de classes · {classes}</div>
            <input type="range" min={2} max={20} step={1} value={classes} onChange={(e) => setClasses(e.target.value)} style={{ width: "100%" }} />
          </div>
          {runBtn(runPoly, "Vectoriser en polygones")}
          {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
          {msg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{msg}</div>}
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {rasterSelect}
          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Niveaux · {count}</div>
              <input type="range" min={2} max={40} step={1} value={count} onChange={(e) => setCount(e.target.value)} disabled={parseFloat(ivStr) > 0} style={{ width: "100%" }} />
            </div>
            <div style={{ width: 120 }}>
              <div style={lbl}>Intervalle</div>
              <input type="number" value={ivStr} onChange={(e) => setIvStr(e.target.value)} placeholder="(auto)" min={0} style={inp} />
            </div>
          </div>
          <div style={{ fontFamily: F, fontSize: 10.5, color: C.dim }}>Renseignez un intervalle (ex. 10) pour des courbes tous les 10 unités ; sinon {count} niveaux répartis.</div>
          {runBtn(runContours, "Générer les contours")}
          {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
          {msg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{msg}</div>}
        </div>
      )}
    </div>
  );
}

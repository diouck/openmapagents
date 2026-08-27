/**
 * RasterAnalysisPanel.jsx — Analyse raster : statistiques zonales + calculatrice.
 *
 * Opère sur les rasters MONO-BANDE importés (GeoTIFF → jeton `rasterToken`).
 *  • Stats zonales : agrège les pixels par polygone d'une couche vecteur
 *    → POST /api/raster/zonal ; tableau + FeatureCollection enrichie (zs_*).
 *  • Calculatrice : applique une expression (A = le raster) → POST /api/raster/calc
 *    → nouveau raster ajouté à la carte comme couche image.
 *  • Définition : aide embarquée (fonctions, exemples, limites).
 */
import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

const CALC_SAMPLES = [
  ["Seuil (A > 0)", "where(A > 0, 1, 0)"],
  ["Masquer les négatifs", "where(A < 0, nan, A)"],
  ["Logarithme", "log(A)"],
  ["Multiplier ×10", "A * 10"],
  ["Borner 0–100", "clip(A, 0, 100)"],
];

const FUNC_DOC = [
  ["where(cond, a, b)", "a si la condition est vraie, sinon b (reclassement, masque)."],
  ["clip(A, lo, hi)", "borne les valeurs entre lo et hi."],
  ["log(A), sqrt(A), exp(A)", "transformations mathématiques usuelles."],
  ["abs(A), floor(A), ceil(A)", "valeur absolue, arrondis."],
  ["minimum(A, v), maximum(A, v)", "min / max terme à terme."],
  ["(A > 0) & (A < 10)", "combinaisons logiques (& et, | ou)."],
  ["nan", "valeur « pas de donnée » (transparent) : where(A < 0, nan, A)."],
];

export default function RasterAnalysisPanel({ layers = [], onAddLayer, onAddImageLayer }) {
  const C = useThemeContext();

  // Rasters mono-bande importés (jeton présent) et couches vecteur (zones).
  const rasters = useMemo(
    () => layers.filter((l) => l.kind === "image" && l.rasterToken),
    [layers]
  );
  const vecs = useMemo(
    () => layers.filter((l) => l.geojson?.features?.length),
    [layers]
  );

  const [tab, setTab] = useState("zonal");
  const [rid, setRid] = useState(rasters[0]?.id || "");
  const raster = rasters.find((l) => l.id === rid) || rasters[0] || null;

  // ── Stats zonales ──
  const [zid, setZid] = useState(vecs[0]?.id || "");
  const [zres, setZres] = useState(null);
  const [zbusy, setZbusy] = useState(false);
  const [zerr, setZerr] = useState(null);

  const runZonal = useCallback(async () => {
    const zone = vecs.find((l) => l.id === zid) || vecs[0];
    if (!raster?.rasterToken || !zone?.geojson) return;
    setZbusy(true); setZerr(null); setZres(null);
    try {
      const r = await fetch(`${API}/raster/zonal`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ raster_token: raster.rasterToken, zones: zone.geojson }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      setZres({ ...(await r.json()), zoneName: zone.name });
    } catch (e) { setZerr(e.message || String(e)); }
    finally { setZbusy(false); }
  }, [raster, vecs, zid]);

  const addZonesToMap = () => {
    if (zres?.zones?.features?.length) {
      onAddLayer?.(zres.zones, `Stats zonales · ${zres.zoneName || "zones"}`, "zonal");
    }
  };

  // ── Calculatrice ──
  const [expr, setExpr] = useState("A");
  const [cbusy, setCbusy] = useState(false);
  const [cerr, setCerr] = useState(null);
  const [cmsg, setCmsg] = useState(null);

  const runCalc = useCallback(async () => {
    const q = expr.trim();
    if (!raster?.rasterToken || !q) return;
    setCbusy(true); setCerr(null); setCmsg(null);
    try {
      const r = await fetch(`${API}/raster/calc`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ raster_token: raster.rasterToken, expr: q, name: `calc · ${q.slice(0, 40)}` }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({
        name: d.name,
        imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.9,
        rasterToken: d.raster_token, bands: d.bands,
        vmin: d.vmin, vmax: d.vmax, dataMin: d.data_min, dataMax: d.data_max,
        fit: false,
      });
      setCmsg(`Nouvelle couche ajoutée (min ${d.data_min}, max ${d.data_max}).`);
    } catch (e) { setCerr(e.message || String(e)); }
    finally { setCbusy(false); }
  }, [raster, expr, onAddImageLayer]);

  const applyCalc = (tpl) => {
    let s = tpl;
    if (raster && (raster.dataMin != null) && (raster.dataMax != null)) {
      s = s.replaceAll("{min}", raster.dataMin).replaceAll("{max}", raster.dataMax);
    }
    setExpr(s); setTab("calc");
  };

  // ── UI helpers ──
  const tabBtn = (id, label) => (
    <button key={id} onClick={() => setTab(id)}
      style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>
      {label}
    </button>
  );
  const Code = ({ children }) => (
    <code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>
  );
  const selStyle = { fontFamily: F, fontSize: 12, padding: "5px 8px", background: C.bg2 || C.bg, color: C.txt,
    border: `1px solid ${C.bdr}`, borderRadius: 6, maxWidth: "100%" };
  const th = { textAlign: "left", padding: "4px 8px", fontFamily: F, fontSize: 11, fontWeight: 600, color: C.mut, borderBottom: `1px solid ${C.bdr}`, whiteSpace: "nowrap", position: "sticky", top: 0, background: C.bg2 || C.bg };
  const td = { padding: "3px 8px", fontFamily: M, fontSize: 11, color: C.txt, borderBottom: `0.5px solid ${C.bdr}`, whiteSpace: "nowrap" };

  const rasterSelect = (
    <label style={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <span style={{ fontFamily: F, fontSize: 11, color: C.mut }}>Raster (mono-bande importé)</span>
      {rasters.length === 0 ? (
        <span style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucun — importez un GeoTIFF mono-bande.</span>
      ) : (
        <select value={rid} onChange={(e) => setRid(e.target.value)} style={selStyle}>
          {rasters.map((l) => <option key={l.id} value={l.id}>{l.name}</option>)}
        </select>
      )}
    </label>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("zonal", "Stats zonales")}
        {tabBtn("calc", "Calculatrice")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12,
          fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>
            <b>Analyse raster</b> travaille sur les <b>GeoTIFF mono-bande importés</b> (altitude, indice,
            température…). Deux outils :
          </p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Statistiques zonales</div>
            <p style={{ margin: 0, color: C.mut }}>
              Agrège les pixels du raster à l'intérieur de chaque polygone d'une couche vecteur :
              <b> nombre, min, moyenne, max, écart-type, somme</b>. Le résultat est un tableau et une couche
              de zones enrichie (propriétés <Code>zs_mean</Code>, <Code>zs_max</Code>…) ajoutable à la carte.
            </p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Calculatrice raster (map algebra)</div>
            <p style={{ margin: 0, color: C.mut }}>
              Applique une expression où <Code>A</Code> désigne le raster, et crée une nouvelle couche.
              Fonctions disponibles :
            </p>
            <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 6 }}>
              {FUNC_DOC.map(([fn, desc]) => (
                <div key={fn} style={{ display: "flex", gap: 8, alignItems: "baseline", flexWrap: "wrap" }}>
                  <Code>{fn}</Code><span style={{ color: C.mut, fontSize: 11.5 }}>{desc}</span>
                </div>
              ))}
            </div>
          </div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px" }}>
            <div style={{ fontWeight: 600, marginBottom: 4 }}>À savoir</div>
            <ul style={{ margin: 0, paddingLeft: 18, color: C.mut, fontSize: 11.5, display: "flex", flexDirection: "column", gap: 3 }}>
              <li>Expression <b>sécurisée</b> (pas d'exécution de code) : seuls A, des nombres et les fonctions listées.</li>
              <li>Rasters <b>mono-bande</b> uniquement (les imports RGB 3 bandes ne sont pas mis en cache).</li>
              <li>Stats zonales bornées à 2 000 zones ; le raster expire côté serveur après 1 h (réimportez au besoin).</li>
            </ul>
          </div>
        </div>
      ) : tab === "zonal" ? (
        <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 10 }}>
          {rasterSelect}
          <label style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            <span style={{ fontFamily: F, fontSize: 11, color: C.mut }}>Zones (couche vecteur de polygones)</span>
            {vecs.length === 0 ? (
              <span style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucune — chargez une couche de polygones.</span>
            ) : (
              <select value={zid} onChange={(e) => setZid(e.target.value)} style={selStyle}>
                {vecs.map((l) => <option key={l.id} value={l.id}>{l.name} · {l.geojson.features.length}</option>)}
              </select>
            )}
          </label>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            <button onClick={runZonal} disabled={zbusy || !raster || vecs.length === 0}
              style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "7px 16px",
                cursor: (zbusy || !raster || vecs.length === 0) ? "not-allowed" : "pointer",
                background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: (!raster || vecs.length === 0) ? 0.5 : 1 }}>
              {zbusy ? "Calcul…" : "Calculer les statistiques"}
            </button>
            {zres?.zones?.features?.length > 0 && (
              <button onClick={addZonesToMap}
                style={{ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "7px 12px", cursor: "pointer",
                  background: "transparent", color: C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7, marginLeft: "auto" }}>
                🗺 Ajouter les zones enrichies
              </button>
            )}
          </div>
          {zerr && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{zerr}</div>}
          {zres && (
            <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 4 }}>
              <div style={{ fontFamily: F, fontSize: 11, color: C.mut }}>{zres.message}</div>
              {zres.columns?.length > 0 && (
                <div style={{ flex: 1, minHeight: 0, overflow: "auto", border: `1px solid ${C.bdr}`, borderRadius: 8 }}>
                  <table style={{ borderCollapse: "collapse", width: "100%" }}>
                    <thead><tr>{zres.columns.map((c) => <th key={c} style={th}>{c}</th>)}</tr></thead>
                    <tbody>
                      {zres.rows.map((row, i) => (
                        <tr key={i}>{row.map((v, j) => <td key={j} style={td} title={String(v ?? "")}>{v === null ? <span style={{ color: C.dim }}>—</span> : String(v)}</td>)}</tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      ) : (
        <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 10 }}>
          {rasterSelect}
          <label style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            <span style={{ fontFamily: F, fontSize: 11, color: C.mut }}>Expression (A = le raster sélectionné)</span>
            <textarea value={expr} onChange={(e) => setExpr(e.target.value)} spellCheck={false}
              onKeyDown={(e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") runCalc(); }}
              placeholder="where(A > 0, 1, 0)"
              style={{ width: "100%", minHeight: 72, resize: "vertical", fontFamily: M, fontSize: 12.5, lineHeight: 1.5, padding: 10,
                background: C.bg2 || C.bg, color: C.txt, border: `1px solid ${C.bdr}`, borderRadius: 8, outline: "none", boxSizing: "border-box" }} />
          </label>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            <button onClick={runCalc} disabled={cbusy || !raster}
              style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "7px 16px",
                cursor: (cbusy || !raster) ? "not-allowed" : "pointer",
                background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: !raster ? 0.5 : 1 }}>
              {cbusy ? "Calcul…" : "▶ Calculer → couche"}
            </button>
            <span style={{ fontFamily: F, fontSize: 10.5, color: C.dim }}>Ctrl/⌘ + Entrée</span>
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {CALC_SAMPLES.map(([label, tpl]) => (
              <button key={label} onClick={() => applyCalc(tpl)} title={tpl}
                style={{ fontFamily: F, fontSize: 10.5, padding: "3px 8px", cursor: "pointer", background: "transparent",
                  border: `0.5px solid ${C.bdr}`, borderRadius: 5, color: C.dim }}>
                {label}
              </button>
            ))}
          </div>
          {raster && (raster.dataMin != null) && (
            <div style={{ fontFamily: M, fontSize: 10.5, color: C.dim }}>
              Plage du raster : min {raster.dataMin} · max {raster.dataMax}
              {" · normaliser : "}<Code>{`(A - ${raster.dataMin}) / (${raster.dataMax} - ${raster.dataMin})`}</Code>
            </div>
          )}
          {cerr && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{cerr}</div>}
          {cmsg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}55`, borderRadius: 6, padding: "6px 10px" }}>{cmsg}</div>}
        </div>
      )}
    </div>
  );
}

/**
 * SpatialStatsPanel.jsx — Statistiques spatiales sur une couche vecteur.
 *
 * Envoie la couche + un champ numérique à POST /api/spatialstats/run :
 *  • indice de Moran (autocorrélation globale) + test par permutations,
 *  • hotspots locaux Getis-Ord Gi* par entité (gi_z, gi_p, gi_class).
 * La couche enrichie est ajoutée à la carte (stylable par gi_class).
 */
import { useState, useMemo, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

const HOT_COLORS = {
  "point chaud 99%": "#b2182b", "point chaud 95%": "#ef8a62", "point chaud 90%": "#fddbc7",
  "non significatif": "#cccccc",
  "point froid 90%": "#d1e5f0", "point froid 95%": "#67a9cf", "point froid 99%": "#2166ac",
};

export default function SpatialStatsPanel({ layers = [], onAddLayer }) {
  const C = useThemeContext();
  const vecs = useMemo(() => layers.filter((l) => l.geojson?.features?.length), [layers]);
  const [lid, setLid] = useState(vecs[0]?.id || "");
  const layer = vecs.find((l) => l.id === lid) || vecs[0] || null;

  const numFields = useMemo(() => {
    const feats = (layer?.geojson?.features || []).slice(0, 40);
    const keys = new Set();
    feats.forEach((f) => Object.keys(f.properties || {}).forEach((k) => keys.add(k)));
    return [...keys].filter((k) => {
      let num = 0, tot = 0;
      for (const f of feats) {
        const v = f.properties?.[k];
        if (v == null || v === "") continue;
        tot++;
        if (typeof v === "number" || (typeof v === "string" && v.trim() !== "" && isFinite(Number(v)))) num++;
      }
      return tot > 0 && num / tot >= 0.7;
    });
  }, [layer]);

  const [field, setField] = useState("");
  const [k, setK] = useState(8);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [res, setRes] = useState(null);

  useEffect(() => { setField((f) => (numFields.includes(f) ? f : numFields[0] || "")); setRes(null); }, [numFields]);

  const run = useCallback(async () => {
    if (!layer?.geojson || !field) { setErr("Choisissez une couche et un champ numérique."); return; }
    setBusy(true); setErr(null); setRes(null);
    try {
      const r = await fetch(`${API}/spatialstats/run`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ geojson: layer.geojson, field, k: Number(k), permutations: 999 }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      setRes(await r.json());
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [layer, field, k]);

  const addHotspots = () => {
    if (res?.geojson?.features?.length) onAddLayer?.(res.geojson, `Hotspots · ${field}`, "hotspot");
  };

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };
  const m = res?.moran;

  // ordre d'affichage des classes Gi*
  const GI_ORDER = ["point chaud 99%", "point chaud 95%", "point chaud 90%", "non significatif", "point froid 90%", "point froid 95%", "point froid 99%"];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
        Autocorrélation spatiale (Moran) et points chauds/froids (Getis-Ord Gi*) d'un champ numérique.
      </div>

      <div>
        <div style={lbl}>Couche vecteur</div>
        {vecs.length === 0 ? (
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucune — chargez une couche de points ou polygones.</div>
        ) : (
          <select value={lid} onChange={(e) => setLid(e.target.value)} style={inp}>
            {vecs.map((l) => <option key={l.id} value={l.id}>{l.name} · {l.geojson.features.length}</option>)}
          </select>
        )}
      </div>

      <div style={{ display: "flex", gap: 8 }}>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Champ numérique</div>
          {numFields.length === 0 ? (
            <div style={{ fontFamily: F, fontSize: 11, color: C.dim }}>Aucun champ numérique détecté.</div>
          ) : (
            <select value={field} onChange={(e) => setField(e.target.value)} style={inp}>
              {numFields.map((k2) => <option key={k2} value={k2}>{k2}</option>)}
            </select>
          )}
        </div>
        <div style={{ width: 96 }}>
          <div style={lbl}>Voisins (k)</div>
          <input type="number" value={k} min={1} max={50} onChange={(e) => setK(e.target.value)} style={inp} />
        </div>
      </div>

      <button onClick={run} disabled={busy || !layer || !field}
        style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: (busy || !layer || !field) ? "not-allowed" : "pointer",
          background: C.acc, color: "#fff", border: "none", borderRadius: 7, opacity: (!layer || !field) ? 0.5 : 1 }}>
        {busy ? "Analyse…" : "Analyser"}
      </button>

      {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}

      {m && (
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 10 }}>
          {/* Moran global */}
          <div style={{ border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "10px 12px", background: C.bg2 || C.bg }}>
            <div style={{ fontFamily: F, fontSize: 11, color: C.mut, marginBottom: 6 }}>Autocorrélation globale (Moran)</div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8, flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 22, fontWeight: 700, color: C.txt }}>I = {m.I}</span>
              <span style={{ fontFamily: F, fontSize: 12, color: m.verdict.startsWith("agrégé") ? "#b2182b" : m.verdict.startsWith("dispersé") ? "#2166ac" : C.mut }}>{m.verdict}</span>
            </div>
            <div style={{ fontFamily: M, fontSize: 11, color: C.dim, marginTop: 4 }}>
              attendu {m.expected} · p = {m.p ?? "—"} ({m.permutations} permutations) · {m.n} entités · {m.k} voisins
            </div>
          </div>

          {/* Hotspots Gi* */}
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
              <span style={{ fontFamily: F, fontSize: 11, color: C.mut }}>Points chauds / froids (Gi*)</span>
              <button onClick={addHotspots} style={{ marginLeft: "auto", fontFamily: F, fontSize: 11, fontWeight: 500, padding: "4px 10px", cursor: "pointer", background: "transparent", color: C.acc, border: `1px solid ${C.acc}66`, borderRadius: 6 }}>
                🗺 Ajouter la couche
              </button>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {GI_ORDER.filter((cl) => res.giClasses[cl]).map((cl) => (
                <div key={cl} style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: F, fontSize: 11.5 }}>
                  <span style={{ width: 12, height: 12, borderRadius: 3, background: HOT_COLORS[cl], border: "0.5px solid rgba(0,0,0,.2)", flexShrink: 0 }} />
                  <span style={{ color: C.txt }}>{cl}</span>
                  <span style={{ marginLeft: "auto", fontFamily: M, color: C.mut }}>{res.giClasses[cl]}</span>
                </div>
              ))}
            </div>
            <div style={{ fontFamily: F, fontSize: 10.5, color: C.dim, marginTop: 6 }}>
              Couche enrichie de <code style={{ fontFamily: M }}>gi_z</code>, <code style={{ fontFamily: M }}>gi_p</code>, <code style={{ fontFamily: M }}>gi_class</code> — colorez par <code style={{ fontFamily: M }}>gi_class</code> dans le gestionnaire de couches.
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

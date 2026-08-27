/**
 * StacPanel.jsx — Navigateur STAC + COG : chercher des scènes satellite sur
 * l'emprise visible et les ajouter à la carte (aperçu COG lu à la volée).
 *
 * Recherche → POST /api/stac/search (Earth Search, Sentinel-2…). Ajout d'une
 * scène → POST /api/stac/scene : le backend lit le COG « visual » (RVB) via
 * /vsicurl, le reprojette en 3857 et renvoie un overlay branché sur addImageLayer.
 */
import { useState, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

const COLLECTIONS = [
  ["sentinel-2-l2a", "Sentinel-2 L2A (surface)"],
  ["sentinel-2-c1-l2a", "Sentinel-2 C1 L2A"],
  ["sentinel-2-l1c", "Sentinel-2 L1C (TOA)"],
  ["landsat-c2-l2", "Landsat C2 L2"],
];

const ymd = (d) => d.toISOString().slice(0, 10);
const today = new Date();
const threeMonthsAgo = new Date(today.getTime() - 92 * 864e5);

export default function StacPanel({ mapRef, onAddImageLayer }) {
  const C = useThemeContext();
  const [coll, setColl] = useState("sentinel-2-l2a");
  const [from, setFrom] = useState(ymd(threeMonthsAgo));
  const [to, setTo] = useState(ymd(today));
  const [cloud, setCloud] = useState(20);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [res, setRes] = useState(null);
  const [adding, setAdding] = useState(null);   // id en cours d'ajout

  const search = useCallback(async () => {
    const map = mapRef?.current?.getMap?.();
    const b = map?.getBounds?.();
    if (!b) { setErr("Carte indisponible."); return; }
    const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    setBusy(true); setErr(null); setRes(null);
    try {
      const r = await fetch(`${API}/stac/search`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ collection: coll, bbox, date_from: from, date_to: to, cloud_max: Number(cloud), limit: 12 }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      setRes(await r.json());
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [mapRef, coll, from, to, cloud]);

  const addScene = useCallback(async (it) => {
    if (!it.visual) return;
    setAdding(it.id); setErr(null);
    try {
      const name = `${it.collection} · ${(it.datetime || "").slice(0, 10)}`;
      const r = await fetch(`${API}/stac/scene`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ href: it.visual, name }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({
        name: d.name,
        imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity: 1,
        rasterToken: null, bands: d.bands, fit: true,
      });
    } catch (e) { setErr(e.message || String(e)); }
    finally { setAdding(null); }
  }, [onAddImageLayer]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 11.5, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", boxSizing: "border-box" };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
        Cherche des scènes satellite (STAC · Earth Search) sur l'emprise affichée, puis ajoute leur aperçu COG à la carte.
      </div>

      <div>
        <div style={lbl}>Collection</div>
        <select value={coll} onChange={(e) => setColl(e.target.value)} style={{ ...inp, width: "100%" }}>
          {COLLECTIONS.map(([id, label]) => <option key={id} value={id}>{label}</option>)}
        </select>
      </div>

      <div style={{ display: "flex", gap: 8 }}>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Du</div>
          <input type="date" value={from} max={to} onChange={(e) => setFrom(e.target.value)} style={{ ...inp, width: "100%" }} />
        </div>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Au</div>
          <input type="date" value={to} min={from} onChange={(e) => setTo(e.target.value)} style={{ ...inp, width: "100%" }} />
        </div>
      </div>

      <div>
        <div style={lbl}>Nuages max · {cloud}%</div>
        <input type="range" min={0} max={100} step={5} value={cloud} onChange={(e) => setCloud(e.target.value)} style={{ width: "100%" }} />
      </div>

      <button onClick={search} disabled={busy}
        style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "8px 16px", cursor: busy ? "wait" : "pointer",
          background: C.acc, color: "#fff", border: "none", borderRadius: 7 }}>
        {busy ? "Recherche…" : "Rechercher dans la vue"}
      </button>

      {err && (
        <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>
          {err}
        </div>
      )}

      {res && (
        <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ fontFamily: F, fontSize: 11, color: C.mut }}>
            {res.count} scène{res.count > 1 ? "s" : ""}{res.matched != null ? ` sur ${res.matched}` : ""} · triées par couverture nuageuse
          </div>
          <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8 }}>
            {res.items.length === 0 && (
              <div style={{ fontFamily: F, fontSize: 11.5, color: C.dim }}>Aucune scène — élargissez la période, la zone ou le seuil de nuages.</div>
            )}
            {res.items.map((it) => (
              <div key={it.id} style={{ display: "flex", gap: 10, padding: 8, border: `0.5px solid ${C.bdr}`, borderRadius: 8, background: C.bg2 || C.bg }}>
                {it.thumb ? (
                  <img src={it.thumb} alt="" loading="lazy" onError={(e) => { e.currentTarget.style.display = "none"; }}
                    style={{ width: 64, height: 64, objectFit: "cover", borderRadius: 6, flexShrink: 0, background: C.hover }} />
                ) : (
                  <div style={{ width: 64, height: 64, borderRadius: 6, flexShrink: 0, background: C.hover }} />
                )}
                <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", gap: 3 }}>
                  <div style={{ fontFamily: M, fontSize: 10.5, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={it.id}>{it.id}</div>
                  <div style={{ fontFamily: F, fontSize: 11, color: C.mut }}>
                    {(it.datetime || "").slice(0, 10)}
                    {it.cloud != null && <span style={{ color: C.dim }}> · ☁ {Number(it.cloud).toFixed(1)}%</span>}
                  </div>
                  {it.visual ? (
                    <button onClick={() => addScene(it)} disabled={adding === it.id}
                      style={{ alignSelf: "flex-start", fontFamily: F, fontSize: 11, fontWeight: 500, padding: "4px 10px", marginTop: 2,
                        cursor: adding === it.id ? "wait" : "pointer", background: "transparent", color: C.acc, border: `1px solid ${C.acc}66`, borderRadius: 6 }}>
                      {adding === it.id ? "Lecture COG…" : "Ajouter à la carte"}
                    </button>
                  ) : (
                    <span style={{ fontFamily: F, fontSize: 10.5, color: C.dim, marginTop: 2 }}>Aperçu COG indisponible pour cette collection</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

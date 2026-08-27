/**
 * SqlPanel.jsx — SQL Workspace : éditeur SQL spatial (DuckDB) → carte.
 *
 * Envoie la requête + les couches vecteur chargées à POST /api/sql/run. Le backend
 * les enregistre comme tables (ST_Read) puis VERROUILLE l'accès externe avant
 * d'exécuter le SQL (app publique). Si le résultat a une géométrie, on peut
 * l'ajouter à la carte comme couche vecteur.
 */
import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Réplique le _safe_ident du backend : nom de table réellement créé côté serveur.
function safeIdent(name) {
  let s = (name || "layer").trim().replace(/[^A-Za-z0-9_]/g, "_");
  if (!s || !/[A-Za-z]/.test(s[0])) s = "t_" + s;
  return s.slice(0, 60) || "layer";
}

const SAMPLES = [
  ["Aperçu (10 lignes)", 'SELECT * FROM "{t}" LIMIT 10'],
  ["Compter les entités", 'SELECT count(*) AS n FROM "{t}"'],
  ["Centroïdes", 'SELECT *, ST_Centroid(geom) AS geom FROM "{t}"'],
  ["Tampon 500 m (~°)", 'SELECT ST_Buffer(geom, 0.005) AS geom FROM "{t}"'],
  ["Surface (m², approx.)", 'SELECT *, ST_Area(geom) AS aire FROM "{t}"'],
];

export default function SqlPanel({ onAddLayer, layers = [] }) {
  const C = useThemeContext();
  const vec = useMemo(() => layers.filter((l) => l.geojson?.features?.length), [layers]);
  const tables = useMemo(() => vec.map((l) => ({ raw: l.name, sql: safeIdent(l.name), n: l.geojson.features.length })), [vec]);
  const firstT = tables[0]?.sql;

  const [sql, setSql] = useState(firstT ? `SELECT * FROM "${firstT}" LIMIT 10` : "SELECT 1 AS x");
  const [res, setRes] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [hist, setHist] = useState([]);

  const run = useCallback(async () => {
    const q = sql.trim();
    if (!q) return;
    setBusy(true); setErr(null);
    try {
      const body = { sql: q, layers: vec.map((l) => ({ name: l.name, geojson: l.geojson })) };
      const r = await fetch(`${API}/api/sql/run`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      setRes(d);
      setHist((h) => [q, ...h.filter((x) => x !== q)].slice(0, 12));
    } catch (e) { setErr(e.message || String(e)); setRes(null); }
    finally { setBusy(false); }
  }, [sql, vec]);

  const addToMap = () => {
    if (res?.geojson?.features?.length) {
      onAddLayer?.(res.geojson, `SQL · ${res.geojson.features.length} entités`, "sql");
    }
  };

  const insertTable = (t) => setSql((s) => (s.includes("{t}") ? s.replaceAll("{t}", t) : s));
  const applySample = (tpl) => setSql(tpl.replaceAll("{t}", firstT || "ma_table"));

  const th = { textAlign: "left", padding: "4px 8px", fontFamily: F, fontSize: 11, fontWeight: 600, color: C.mut, borderBottom: `1px solid ${C.bdr}`, whiteSpace: "nowrap", position: "sticky", top: 0, background: C.bg2 || C.bg };
  const td = { padding: "3px 8px", fontFamily: M, fontSize: 11, color: C.txt, borderBottom: `0.5px solid ${C.bdr}`, maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0 }}>
      {/* Tables disponibles */}
      <div>
        <div style={{ fontFamily: F, fontSize: 11, color: C.mut, marginBottom: 4 }}>
          Tables disponibles {tables.length === 0 && <span style={{ color: C.dim }}>— charge des couches vecteur pour les requêter</span>}
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
          {tables.map((t) => (
            <button key={t.sql} onClick={() => setSql(`SELECT * FROM "${t.sql}" LIMIT 10`)} title={`${t.raw} — ${t.n} entités\n(cliquer : requête d'aperçu)`}
              style={{ fontFamily: M, fontSize: 11, padding: "3px 8px", cursor: "pointer", background: C.acc + "14", border: `0.5px solid ${C.acc}55`, borderRadius: 5, color: C.acc }}>
              {t.sql} <span style={{ color: C.dim }}>· {t.n}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Éditeur */}
      <textarea value={sql} onChange={(e) => setSql(e.target.value)} spellCheck={false}
        onKeyDown={(e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") run(); }}
        placeholder="SELECT * FROM &quot;ma_table&quot; LIMIT 100"
        style={{ width: "100%", minHeight: 96, resize: "vertical", fontFamily: M, fontSize: 12.5, lineHeight: 1.5, padding: 10,
          background: C.bg2 || C.bg, color: C.txt, border: `1px solid ${C.bdr}`, borderRadius: 8, outline: "none", boxSizing: "border-box" }} />

      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <button onClick={run} disabled={busy}
          style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "7px 16px", cursor: busy ? "wait" : "pointer",
            background: C.acc, color: "#fff", border: "none", borderRadius: 7 }}>
          {busy ? "Exécution…" : "▶ Exécuter"}
        </button>
        <span style={{ fontFamily: F, fontSize: 10.5, color: C.dim }}>Ctrl/⌘ + Entrée</span>
        {res?.geojson?.features?.length > 0 && (
          <button onClick={addToMap}
            style={{ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "7px 12px", cursor: "pointer",
              background: "transparent", color: C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7, marginLeft: "auto" }}>
            🗺 Ajouter à la carte
          </button>
        )}
      </div>

      {/* Exemples */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {SAMPLES.map(([label, tpl]) => (
          <button key={label} onClick={() => applySample(tpl)} title={tpl}
            style={{ fontFamily: F, fontSize: 10.5, padding: "3px 8px", cursor: "pointer", background: "transparent",
              border: `0.5px solid ${C.bdr}`, borderRadius: 5, color: C.dim }}>
            {label}
          </button>
        ))}
      </div>

      {err && (
        <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>
          {err}
        </div>
      )}

      {/* Résultats */}
      {res && !err && (
        <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 4 }}>
          <div style={{ fontFamily: F, fontSize: 11, color: C.mut }}>
            {res.message || `${res.rowCount} ligne${res.rowCount > 1 ? "s" : ""}${res.truncated ? ` (tronqué à ${res.rowCount})` : ""}`}
            {res.geojson?.features?.length ? ` · ${res.geojson.features.length} géométries` : ""}
          </div>
          {res.columns?.length > 0 && (
            <div style={{ flex: 1, minHeight: 0, overflow: "auto", border: `1px solid ${C.bdr}`, borderRadius: 8 }}>
              <table style={{ borderCollapse: "collapse", width: "100%" }}>
                <thead><tr>{res.columns.map((c) => <th key={c} style={th}>{c}</th>)}</tr></thead>
                <tbody>
                  {res.rows.map((row, i) => (
                    <tr key={i}>{row.map((v, j) => <td key={j} style={td} title={String(v ?? "")}>{v === null ? <span style={{ color: C.dim }}>NULL</span> : String(v)}</td>)}</tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Historique */}
      {hist.length > 0 && (
        <div>
          <div style={{ fontFamily: F, fontSize: 10.5, color: C.dim, marginBottom: 3 }}>Historique</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 96, overflow: "auto" }}>
            {hist.map((h, i) => (
              <button key={i} onClick={() => setSql(h)} title={h}
                style={{ textAlign: "left", fontFamily: M, fontSize: 10.5, padding: "2px 6px", cursor: "pointer", background: "transparent",
                  border: "none", color: C.mut, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {h}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

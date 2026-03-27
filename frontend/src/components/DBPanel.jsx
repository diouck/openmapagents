import { useState, useCallback, useRef } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

const DB_TYPES = [
  { key: "postgresql", label: "PostgreSQL / PostGIS", port: 5432, icon: "🐘" },
  { key: "mysql",      label: "MySQL / MariaDB",      port: 3306, icon: "🐬" },
  { key: "sqlite",     label: "SQLite",               port: null, icon: "📁" },
];

const SQL_TEMPLATES = {
  postgresql: [
    { label: "Table avec géométrie PostGIS", sql: "SELECT id, name, ST_AsGeoJSON(geom) AS geom_json\nFROM ma_table\nLIMIT 500" },
    { label: "Table avec WKT",               sql: "SELECT id, name, ST_AsText(geom) AS geom_wkt\nFROM ma_table\nLIMIT 500" },
    { label: "Points lat/lon",               sql: "SELECT id, nom, latitude, longitude\nFROM ma_table\nLIMIT 1000" },
    { label: "Filtrer par attribut",         sql: "SELECT id, name, ST_AsGeoJSON(geom) AS geom_json\nFROM ma_table\nWHERE commune = 'Nantes'\nLIMIT 500" },
  ],
  mysql: [
    { label: "Table avec géométrie",         sql: "SELECT id, name, ST_AsGeoJSON(geom) AS geom_json\nFROM ma_table\nLIMIT 500" },
    { label: "Points lat/lon",               sql: "SELECT id, nom, lat, lng\nFROM ma_table\nLIMIT 1000" },
  ],
  sqlite: [
    { label: "Points lat/lon",               sql: "SELECT id, name, latitude, longitude\nFROM ma_table\nLIMIT 1000" },
  ],
};

// ── Composant champ de formulaire ─────────────────────────────
function Field({ label, children }) {
  const C = getTheme();
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>{label}</div>
      {children}
    </div>
  );
}

// ── Composant badge statut ────────────────────────────────────
function StatusBadge({ status, message }) {
  const C = getTheme();
  const colors = { ok: C.acc, error: C.red, testing: C.amb };
  const icons  = { ok: "✓", error: "✕", testing: "⏳" };
  const col = colors[status] || C.dim;
  return (
    <div style={{
      fontSize: 10, padding: "5px 10px", borderRadius: 5,
      background: col + "15", border: `0.5px solid ${col}44`,
      color: col, lineHeight: 1.5, wordBreak: "break-word",
    }}>
      {icons[status]} {message}
    </div>
  );
}

export default function DBPanel({ onAddLayer }) {
  const C = getTheme();

  // Connexion
  const [dbType,    setDbType]    = useState("postgresql");
  const [host,      setHost]      = useState("localhost");
  const [port,      setPort]      = useState(5432);
  const [database,  setDatabase]  = useState("");
  const [username,  setUsername]  = useState("");
  const [password,  setPassword]  = useState("");
  const [showPass,  setShowPass]  = useState(false);
  const [useUrl,    setUseUrl]    = useState(false);
  const [connUrl,   setConnUrl]   = useState("");

  // État UI
  const [connStatus,  setConnStatus]  = useState(null);  // {status, message}
  const [tables,      setTables]      = useState([]);
  const [loadingConn, setLoadingConn] = useState(false);
  const [loadingTbl,  setLoadingTbl]  = useState(false);
  const [loadingQry,  setLoadingQry]  = useState(false);

  // Requête
  const [sql,          setSql]          = useState("");
  const [geomColumn,   setGeomColumn]   = useState("geom");
  const [layerName,    setLayerName]    = useState("");
  const [queryResult,  setQueryResult]  = useState(null);  // {columns, rows} pour aperçu
  const [queryError,   setQueryError]   = useState(null);
  const [activeTab,    setActiveTab]    = useState("connection"); // "connection" | "query"
  const [showTables,   setShowTables]   = useState(false);

  const getConn = () => ({
    type: dbType,
    host, port, database, username, password,
    url: useUrl ? connUrl : null,
  });

  // ── Changer le type de BDD ──────────────────────────────────
  const handleDbTypeChange = (key) => {
    setDbType(key);
    const t = DB_TYPES.find(d => d.key === key);
    if (t?.port) setPort(t.port);
    setSql(SQL_TEMPLATES[key]?.[0]?.sql || "");
    setConnStatus(null);
    setTables([]);
  };

  // ── Tester la connexion ─────────────────────────────────────
  const testConnection = useCallback(async () => {
    setLoadingConn(true);
    setConnStatus({ status: "testing", message: "Connexion en cours…" });
    setTables([]);
    try {
      const res = await fetch(`${API}/api/db/test`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(getConn()),
      });
      const data = await res.json();
      if (res.ok) {
        setConnStatus({ status: "ok", message: data.message });
        setActiveTab("query");
      } else {
        setConnStatus({ status: "error", message: data.detail || "Connexion échouée" });
      }
    } catch (e) {
      setConnStatus({ status: "error", message: "Impossible de joindre le backend : " + e.message });
    }
    setLoadingConn(false);
  }, [dbType, host, port, database, username, password, useUrl, connUrl]);

  // ── Lister les tables ───────────────────────────────────────
  const loadTables = useCallback(async () => {
    setLoadingTbl(true);
    try {
      const res = await fetch(`${API}/api/db/tables`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(getConn()),
      });
      const data = await res.json();
      if (res.ok) {
        setTables(data.tables || []);
        setShowTables(true);
      } else {
        setConnStatus({ status: "error", message: data.detail });
      }
    } catch (e) {
      setConnStatus({ status: "error", message: e.message });
    }
    setLoadingTbl(false);
  }, [dbType, host, port, database, username, password, useUrl, connUrl]);

  // ── Clic sur une table → injecter SQL ──────────────────────
  const injectTable = (table) => {
    const hasGeom = table.has_geometry;
    const geomCol = table.columns.find(c =>
      ["geom", "geometry", "the_geom", "shape", "wkb_geometry"].includes(c.toLowerCase())
    ) || "geom";

    let q;
    if (hasGeom && dbType === "postgresql") {
      q = `SELECT id, name, ST_AsGeoJSON(${geomCol}) AS geom_json\nFROM ${table.name}\nLIMIT 500`;
    } else if (hasGeom) {
      q = `SELECT id, name, ST_AsGeoJSON(${geomCol}) AS geom_json\nFROM ${table.name}\nLIMIT 500`;
    } else {
      const latCol = table.columns.find(c => ["lat","latitude","y"].includes(c.toLowerCase())) || "latitude";
      const lonCol = table.columns.find(c => ["lon","lng","longitude","x"].includes(c.toLowerCase())) || "longitude";
      q = `SELECT *, ${latCol}, ${lonCol}\nFROM ${table.name}\nLIMIT 1000`;
    }
    setSql(q);
    setGeomColumn(geomCol);
    setLayerName(table.name);
    setShowTables(false);
    setActiveTab("query");
  };

  // ── Exécuter la requête ─────────────────────────────────────
  const executeQuery = useCallback(async (asLayer = true) => {
    if (!sql.trim()) return;
    setLoadingQry(true);
    setQueryError(null);
    setQueryResult(null);
    try {
      const res = await fetch(`${API}/api/db/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          connection: getConn(),
          sql: sql.trim(),
          geom_column: geomColumn || "geom",
          limit: 5000,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Erreur requête");

      if (asLayer) {
        const name = layerName || `db_${database}_${Date.now()}`;
        onAddLayer(data, name, "database");
      } else {
        // Aperçu tableau
        setQueryResult({
          columns: data.metadata?.columns || [],
          rows: data.features?.slice(0, 20).map(f => f.properties) || [],
          total: data.metadata?.total || 0,
        });
      }
    } catch (e) {
      setQueryError(e.message);
    }
    setLoadingQry(false);
  }, [sql, geomColumn, layerName, database, dbType, host, port, username, password, useUrl, connUrl, onAddLayer]);

  const inp = {
    fontFamily: F, fontSize: 11, padding: "5px 8px",
    borderRadius: 5, background: C.input, color: C.txt,
    border: `0.5px solid ${C.bdr}`, outline: "none",
    width: "100%", boxSizing: "border-box",
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>

      {/* ── Onglets ─────────────────────────────────────────── */}
      <div style={{ display: "flex", borderBottom: `0.5px solid ${C.bdr}`, flexShrink: 0 }}>
        {[["connection", "🔌 Connexion"], ["query", "🔍 Requête"]].map(([key, label]) => (
          <button key={key} onClick={() => setActiveTab(key)} style={{
            fontFamily: F, fontSize: 11, padding: "8px 14px", border: "none", cursor: "pointer",
            background: activeTab === key ? C.acc + "15" : "transparent",
            color: activeTab === key ? C.acc : C.mut,
            borderBottom: activeTab === key ? `2px solid ${C.acc}` : "2px solid transparent",
            flex: 1,
          }}>{label}</button>
        ))}
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>

        {/* ══ ONGLET CONNEXION ══════════════════════════════════ */}
        {activeTab === "connection" && (
          <>
            {/* Type de BDD */}
            <Field label="Type de base">
              <div style={{ display: "flex", gap: 4 }}>
                {DB_TYPES.map(t => (
                  <button key={t.key} onClick={() => handleDbTypeChange(t.key)} style={{
                    fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, flex: 1,
                    background: dbType === t.key ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${dbType === t.key ? C.acc + "66" : C.bdr}`,
                    color: dbType === t.key ? C.acc : C.dim, cursor: "pointer",
                    display: "flex", flexDirection: "column", alignItems: "center", gap: 2,
                  }}>
                    <span style={{ fontSize: 14 }}>{t.icon}</span>
                    <span style={{ fontSize: 9 }}>{t.key === "postgresql" ? "PostGIS" : t.key === "mysql" ? "MySQL" : "SQLite"}</span>
                  </button>
                ))}
              </div>
            </Field>

            {/* Toggle URL directe */}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <button onClick={() => setUseUrl(v => !v)} style={{
                fontFamily: F, fontSize: 9, padding: "2px 8px", borderRadius: 4,
                background: useUrl ? C.acc + "18" : "transparent",
                border: `0.5px solid ${useUrl ? C.acc + "55" : C.bdr}`,
                color: useUrl ? C.acc : C.dim, cursor: "pointer",
              }}>{useUrl ? "✓ " : ""}URL directe</button>
              <span style={{ fontSize: 9, color: C.dim }}>ex: postgresql://user:pass@host/db</span>
            </div>

            {useUrl ? (
              <Field label="URL de connexion">
                <input value={connUrl} onChange={e => setConnUrl(e.target.value)}
                  placeholder="postgresql://user:pass@host:5432/mydb"
                  style={{ ...inp, fontFamily: M, fontSize: 10 }} />
              </Field>
            ) : (
              <>
                {/* Host + Port */}
                {dbType !== "sqlite" && (
                  <div style={{ display: "flex", gap: 6 }}>
                    <div style={{ flex: 3 }}>
                      <Field label="Hôte">
                        <input value={host} onChange={e => setHost(e.target.value)}
                          placeholder="localhost" style={inp} />
                      </Field>
                    </div>
                    <div style={{ flex: 1 }}>
                      <Field label="Port">
                        <input type="number" value={port} onChange={e => setPort(parseInt(e.target.value))}
                          style={inp} />
                      </Field>
                    </div>
                  </div>
                )}

                {/* Base */}
                <Field label={dbType === "sqlite" ? "Chemin du fichier" : "Base de données"}>
                  <input value={database} onChange={e => setDatabase(e.target.value)}
                    placeholder={dbType === "sqlite" ? "/data/mydb.sqlite" : "mydb"}
                    style={inp} />
                </Field>

                {/* User + Password */}
                {dbType !== "sqlite" && (
                  <div style={{ display: "flex", gap: 6 }}>
                    <div style={{ flex: 1 }}>
                      <Field label="Utilisateur">
                        <input value={username} onChange={e => setUsername(e.target.value)}
                          placeholder="postgres" style={inp} />
                      </Field>
                    </div>
                    <div style={{ flex: 1 }}>
                      <Field label="Mot de passe">
                        <div style={{ position: "relative" }}>
                          <input
                            type={showPass ? "text" : "password"}
                            value={password} onChange={e => setPassword(e.target.value)}
                            style={{ ...inp, paddingRight: 28 }} />
                          <button onClick={() => setShowPass(v => !v)} style={{
                            position: "absolute", right: 6, top: "50%", transform: "translateY(-50%)",
                            background: "none", border: "none", cursor: "pointer",
                            color: C.dim, fontSize: 12, padding: 0,
                          }}>{showPass ? "🙈" : "👁"}</button>
                        </div>
                      </Field>
                    </div>
                  </div>
                )}
              </>
            )}

            {/* Statut */}
            {connStatus && <StatusBadge status={connStatus.status} message={connStatus.message} />}

            {/* Bouton tester */}
            <button onClick={testConnection} disabled={loadingConn || !database} style={{
              fontFamily: F, fontSize: 11, fontWeight: 500, padding: "8px 12px", borderRadius: 6,
              background: database ? C.acc : C.hover,
              color: database ? "#fff" : C.dim, border: "none",
              cursor: database ? "pointer" : "default",
              opacity: loadingConn ? 0.6 : 1,
            }}>
              {loadingConn ? "⏳ Test en cours…" : "🔌 Tester la connexion"}
            </button>
          </>
        )}

        {/* ══ ONGLET REQUÊTE ════════════════════════════════════ */}
        {activeTab === "query" && (
          <>
            {/* Statut connexion compact */}
            {connStatus && (
              <div style={{ fontSize: 9, padding: "3px 8px", borderRadius: 4, background: (connStatus.status === "ok" ? C.acc : C.red) + "15", color: connStatus.status === "ok" ? C.acc : C.red, border: `0.5px solid ${(connStatus.status === "ok" ? C.acc : C.red)}33` }}>
                {connStatus.status === "ok" ? "✓" : "✕"} {connStatus.status === "ok" ? `Connecté à ${database}` : "Non connecté"}
                {connStatus.status !== "ok" && (
                  <button onClick={() => setActiveTab("connection")} style={{ marginLeft: 8, fontFamily: F, fontSize: 9, background: "none", border: "none", color: C.acc, cursor: "pointer", textDecoration: "underline" }}>Configurer</button>
                )}
              </div>
            )}

            {/* Tables disponibles */}
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={loadTables} disabled={loadingTbl || connStatus?.status !== "ok"} style={{
                fontFamily: F, fontSize: 10, padding: "5px 10px", borderRadius: 5, flex: 1,
                background: "transparent", border: `0.5px solid ${C.bdr}`,
                color: C.mut, cursor: "pointer", opacity: connStatus?.status !== "ok" ? 0.4 : 1,
              }}>
                {loadingTbl ? "⏳" : "📋"} Tables
              </button>
              {tables.length > 0 && (
                <button onClick={() => setShowTables(v => !v)} style={{
                  fontFamily: F, fontSize: 10, padding: "5px 10px", borderRadius: 5,
                  background: showTables ? C.acc + "18" : "transparent",
                  border: `0.5px solid ${showTables ? C.acc + "55" : C.bdr}`,
                  color: showTables ? C.acc : C.dim, cursor: "pointer",
                }}>
                  {tables.length} tables {showTables ? "▲" : "▼"}
                </button>
              )}
            </div>

            {/* Liste des tables */}
            {showTables && tables.length > 0 && (
              <div style={{ maxHeight: 180, overflowY: "auto", border: `0.5px solid ${C.bdr}`, borderRadius: 6, background: C.bg }}>
                {tables.map(t => (
                  <div key={t.name} onClick={() => injectTable(t)}
                    style={{
                      padding: "6px 10px", cursor: "pointer", borderBottom: `0.5px solid ${C.bdr}`,
                      display: "flex", alignItems: "center", gap: 6,
                    }}
                    onMouseEnter={e => e.currentTarget.style.background = C.hover}
                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                  >
                    <span style={{ fontSize: 12 }}>{t.has_geometry ? "🗺" : "📄"}</span>
                    <span style={{ fontSize: 11, color: C.txt, flex: 1 }}>{t.name}</span>
                    <span style={{ fontSize: 9, color: C.dim }}>{t.type}</span>
                    {t.has_geometry && <span style={{ fontSize: 9, color: C.acc }}>geo</span>}
                  </div>
                ))}
              </div>
            )}

            {/* Templates SQL */}
            <Field label="Modèles de requête">
              <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
                {(SQL_TEMPLATES[dbType] || []).map(tpl => (
                  <button key={tpl.label} onClick={() => setSql(tpl.sql)} style={{
                    fontFamily: F, fontSize: 9, padding: "3px 7px", borderRadius: 4,
                    background: "transparent", border: `0.5px solid ${C.bdr}`,
                    color: C.mut, cursor: "pointer",
                  }}>{tpl.label}</button>
                ))}
              </div>
            </Field>

            {/* Éditeur SQL */}
            <Field label="Requête SQL">
              <textarea value={sql} onChange={e => setSql(e.target.value)}
                rows={6} spellCheck={false}
                placeholder={`SELECT id, name, ST_AsGeoJSON(geom) AS geom_json\nFROM ma_table\nLIMIT 500`}
                style={{
                  ...inp, fontFamily: M, fontSize: 11, lineHeight: 1.6,
                  resize: "vertical", minHeight: 100,
                }} />
            </Field>

            {/* Colonne géométrie + Nom couche */}
            <div style={{ display: "flex", gap: 6 }}>
              <div style={{ flex: 1 }}>
                <Field label="Colonne géom.">
                  <input value={geomColumn} onChange={e => setGeomColumn(e.target.value)}
                    placeholder="geom" style={inp} />
                </Field>
              </div>
              <div style={{ flex: 2 }}>
                <Field label="Nom de la couche">
                  <input value={layerName} onChange={e => setLayerName(e.target.value)}
                    placeholder="Ma couche DB" style={inp} />
                </Field>
              </div>
            </div>

            {/* Erreur */}
            {queryError && (
              <div style={{ fontSize: 10, color: C.red, background: C.red + "12", border: `0.5px solid ${C.red}33`, borderRadius: 5, padding: "6px 8px", lineHeight: 1.5, wordBreak: "break-word" }}>
                ❌ {queryError}
              </div>
            )}

            {/* Aperçu résultat */}
            {queryResult && (
              <div style={{ fontSize: 10, color: C.acc }}>
                ✓ {queryResult.total} features — aperçu ci-dessous
              </div>
            )}
            {queryResult?.rows?.length > 0 && (
              <div style={{ overflowX: "auto", border: `0.5px solid ${C.bdr}`, borderRadius: 5, maxHeight: 140 }}>
                <table style={{ borderCollapse: "collapse", fontSize: 9, width: "100%" }}>
                  <thead>
                    <tr style={{ background: C.hover }}>
                      {queryResult.columns.filter(c => !["geom","geometry","geom_json","geom_wkt"].includes(c.toLowerCase())).slice(0, 6).map(c => (
                        <th key={c} style={{ padding: "3px 6px", borderBottom: `0.5px solid ${C.bdr}`, color: C.dim, fontWeight: 500, textAlign: "left", whiteSpace: "nowrap" }}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {queryResult.rows.slice(0, 8).map((row, i) => (
                      <tr key={i} style={{ borderBottom: `0.5px solid ${C.bdr}` }}>
                        {queryResult.columns.filter(c => !["geom","geometry","geom_json","geom_wkt"].includes(c.toLowerCase())).slice(0, 6).map(c => (
                          <td key={c} style={{ padding: "2px 6px", color: C.txt, maxWidth: 80, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {row[c] != null ? String(row[c]).slice(0, 40) : ""}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Boutons action */}
            <div style={{ display: "flex", gap: 6 }}>
              <button onClick={() => executeQuery(false)} disabled={loadingQry || !sql.trim()} style={{
                fontFamily: F, fontSize: 10, padding: "6px 0", borderRadius: 5, flex: 1,
                background: "transparent", border: `0.5px solid ${C.bdr}`,
                color: C.mut, cursor: sql.trim() ? "pointer" : "default",
                opacity: loadingQry ? 0.6 : 1,
              }}>
                👁 Aperçu
              </button>
              <button onClick={() => executeQuery(true)} disabled={loadingQry || !sql.trim() || connStatus?.status !== "ok"} style={{
                fontFamily: F, fontSize: 10, fontWeight: 500, padding: "6px 0", borderRadius: 5, flex: 2,
                background: sql.trim() && connStatus?.status === "ok" ? C.acc : C.hover,
                color: sql.trim() && connStatus?.status === "ok" ? "#fff" : C.dim,
                border: "none", cursor: sql.trim() ? "pointer" : "default",
                opacity: loadingQry ? 0.6 : 1,
              }}>
                {loadingQry ? "⏳ Chargement…" : "➕ Ajouter comme couche"}
              </button>
            </div>

            {/* Aide */}
            <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.7, background: C.hover, borderRadius: 5, padding: "6px 8px" }}>
              <strong style={{ color: C.mut }}>Colonnes géométrie reconnues :</strong><br />
              PostGIS : <code style={{ fontFamily: M }}>ST_AsGeoJSON(geom) AS geom_json</code><br />
              WKT : <code style={{ fontFamily: M }}>ST_AsText(geom) AS geom_wkt</code><br />
              Points : colonnes <code style={{ fontFamily: M }}>latitude</code> / <code style={{ fontFamily: M }}>longitude</code>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/**
 * JoinPanel.jsx — Jointure attributaire : un CSV vers une couche vecteur.
 *
 * Le panneau est construit autour du DIAGNOSTIC plutôt que du bouton d'action :
 * une jointure qui « ne marche pas » est presque toujours une histoire de clés
 * qui ne se correspondent pas littéralement. On montre donc le taux
 * d'appariement en direct, des exemples de clés orphelines des deux côtés, et
 * un bouton qui cherche automatiquement les options de normalisation les plus
 * payantes — avant que quoi que ce soit ne soit écrit dans la couche.
 */
import { useState, useMemo, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { Sel, Lbl } from "./ui";
import { IcUpload, IcCheck, IcAlert, IcTable, IcZap } from "../icons";
import { parseCSV, analyzeJoin, applyJoin } from "../utils/tableJoin";

export default function JoinPanel({ layers, onAddLayer }) {
  const C = useThemeContext();
  const fileRef = useRef(null);

  const [layerId, setLayerId] = useState("");
  const [csv, setCsv] = useState(null);            // { columns, rows, delimiter, name }
  const [geoKey, setGeoKey] = useState("");
  const [csvKey, setCsvKey] = useState("");
  const [cols, setCols] = useState([]);            // colonnes à rapatrier
  const [opts, setOpts] = useState({ caseless: false, noAccents: false, pad: 0 });
  const [prefix, setPrefix] = useState("");
  const [err, setErr] = useState(null);
  const [done, setDone] = useState(null);

  const layer = layers.find(l => l.id === layerId);
  const features = layer?.geojson?.features || [];

  // Union des propriétés (et non celles du 1er objet seulement : un GeoJSON
  // hétérogène a des entités auxquelles il manque des champs).
  const geoProps = useMemo(() => {
    const s = new Set();
    for (const f of features.slice(0, 500)) Object.keys(f.properties || {}).forEach(k => s.add(k));
    return [...s];
  }, [features]);

  const analysis = useMemo(() => {
    if (!layer || !csv || !geoKey || !csvKey) return null;
    return analyzeJoin(features, geoKey, csv.rows, csvKey, opts);
  }, [layer, csv, geoKey, csvKey, opts, features]);

  const loadFile = (file) => {
    if (!file) return;
    setErr(null); setDone(null);
    const rd = new FileReader();
    rd.onload = e => {
      try {
        const p = parseCSV(e.target.result);
        if (!p.columns.length) throw new Error("Aucune colonne détectée.");
        if (!p.rows.length)    throw new Error("Aucune ligne de données.");
        setCsv({ ...p, name: file.name });
        setCsvKey(""); setCols([]);
      } catch (ex) { setErr(ex.message); }
    };
    rd.onerror = () => setErr("Lecture du fichier impossible.");
    // Beaucoup de CSV français sortent d'Excel en Windows-1252 : UTF-8 seul
    // afficherait « AmbÃ©rieu ». On tente UTF-8, le navigateur remplace les
    // octets invalides — d'où le repli explicite proposé ci-dessous.
    rd.readAsText(file, "utf-8");
  };

  /** Cherche la combinaison d'options qui apparie le plus. */
  const optimize = () => {
    if (!csv || !geoKey || !csvKey) return;
    // Longueur dominante des clés géo numériques → candidat de remplissage
    const lens = features.slice(0, 300)
      .map(f => String(f?.properties?.[geoKey] ?? "").trim())
      .filter(v => /^\d+$/.test(v)).map(v => v.length);
    const pads = [0, ...new Set(lens)].slice(0, 4);
    let best = null;
    for (const caseless of [false, true])
      for (const noAccents of [false, true])
        for (const pad of pads) {
          const o = { caseless, noAccents, pad };
          const a = analyzeJoin(features, geoKey, csv.rows, csvKey, o);
          if (!best || a.matched > best.a.matched) best = { o, a };
        }
    if (best) setOpts(best.o);
  };

  const run = () => {
    setErr(null);
    if (!layer)          return setErr("Choisissez une couche.");
    if (!csv)            return setErr("Chargez un fichier CSV.");
    if (!geoKey || !csvKey) return setErr("Choisissez les deux colonnes de jointure.");
    if (!cols.length)    return setErr("Sélectionnez au moins une colonne à rapatrier.");
    try {
      const gj = applyJoin(layer.geojson, geoKey, csv.rows, csvKey, cols, { ...opts, prefix });
      onAddLayer(gj, `${layer.name} + ${csv.name.replace(/\.csv$/i, "")}`, "data");
      setDone(`${analysis.matched} entité(s) enrichie(s) — nouvelle couche créée.`);
    } catch (ex) { setErr(ex.message); }
  };

  const box = { background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`,
                display: "flex", flexDirection: "column", gap: 8 };
  const chk = (label, key) => (
    <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10.5, color: C.mut, cursor: "pointer" }}>
      <input type="checkbox" checked={!!opts[key]} onChange={e => setOpts(o => ({ ...o, [key]: e.target.checked }))}
        style={{ accentColor: C.acc, cursor: "pointer" }} />
      {label}
    </label>
  );

  const pct = analysis ? Math.round(analysis.rate * 100) : 0;
  const tone = pct >= 90 ? C.acc : pct >= 50 ? C.amb : C.red;

  return (
    <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto" }}>

      <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
        Rapatrie les colonnes d'un tableau vers les entités d'une couche, en les
        appariant sur une valeur commune (code INSEE, identifiant, nom…).
      </div>

      {/* 1. Couche cible */}
      <div style={box}>
        <Lbl>1 · Couche à enrichir</Lbl>
        <Sel value={layerId} onChange={v => { setLayerId(v); setGeoKey(""); setDone(null); }}
          options={[{ value: "", label: "-- Choisir --" },
                    ...layers.map(l => ({ value: l.id, label: `${l.name} (${l.featureCount})` }))]} />
      </div>

      {/* 2. Tableau */}
      <div style={box}>
        <Lbl>2 · Tableau CSV</Lbl>
        <label style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", borderRadius: 7,
                        background: C.hover, border: `0.5px dashed ${C.bdr}`, cursor: "pointer", fontSize: 11, color: C.mut }}>
          <IcUpload size={13} />
          {csv ? `${csv.name} — ${csv.rows.length} lignes, ${csv.columns.length} colonnes` : "Choisir un fichier .csv"}
          <input ref={fileRef} type="file" accept=".csv,.txt,.tsv" style={{ display: "none" }}
            onChange={e => loadFile(e.target.files?.[0])} />
        </label>
        {csv && (
          <div style={{ fontSize: 9.5, color: C.dim, fontFamily: M }}>
            Séparateur détecté : « {csv.delimiter === "\t" ? "tabulation" : csv.delimiter} »
          </div>
        )}
      </div>

      {/* 3. Clés */}
      {layer && csv && (
        <div style={box}>
          <Lbl>3 · Colonnes de jointure</Lbl>
          <div>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 3 }}>DANS LA COUCHE</div>
            <Sel value={geoKey} onChange={setGeoKey}
              options={[{ value: "", label: "-- Choisir --" }, ...geoProps.map(a => ({ value: a, label: a }))]} />
          </div>
          <div>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 3 }}>DANS LE CSV</div>
            <Sel value={csvKey} onChange={setCsvKey}
              options={[{ value: "", label: "-- Choisir --" }, ...csv.columns.map(a => ({ value: a, label: a }))]} />
          </div>

          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
            {chk("Ignorer la casse", "caseless")}
            {chk("Ignorer les accents", "noAccents")}
            <label style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 10.5, color: C.mut }}>
              Zéros à gauche
              <input type="number" min="0" max="12" value={opts.pad}
                onChange={e => setOpts(o => ({ ...o, pad: parseInt(e.target.value) || 0 }))}
                style={{ width: 42, fontFamily: M, fontSize: 10.5, padding: "3px 5px", borderRadius: 5,
                         background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />
            </label>
          </div>
          <button onClick={optimize} disabled={!geoKey || !csvKey} style={{
            fontFamily: F, fontSize: 10.5, padding: "5px 9px", borderRadius: 6, cursor: "pointer",
            background: "transparent", border: `0.5px solid ${C.acc}66`, color: C.acc,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 5,
            opacity: (!geoKey || !csvKey) ? 0.4 : 1,
          }}>
            <IcZap size={12} /> Trouver le meilleur appariement
          </button>
        </div>
      )}

      {/* 4. Diagnostic */}
      {analysis && (
        <div style={{ ...box, borderColor: tone + "55", background: tone + "0d" }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: 7 }}>
            <span style={{ fontFamily: M, fontSize: 20, fontWeight: 700, color: tone }}>{pct}%</span>
            <span style={{ fontSize: 11, color: C.mut }}>
              {analysis.matched} / {analysis.total} entités appariées
            </span>
          </div>
          <div style={{ fontSize: 10, color: C.dim, fontFamily: M }}>
            {analysis.csvRows} lignes CSV · {analysis.csvKeys} clés uniques
            {analysis.dupes > 0 && ` · ${analysis.dupes} doublon(s) ignoré(s)`}
          </div>

          {pct < 100 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: 9.5, color: C.dim }}>
              {analysis.unmatchedGeo.length > 0 && (
                <div><b style={{ color: C.mut }}>Sans correspondance (couche)</b> :{" "}
                  <span style={{ fontFamily: M }}>{analysis.unmatchedGeo.map(v => `«${v}»`).join(" ")}</span></div>
              )}
              {analysis.unmatchedCsv.length > 0 && (
                <div><b style={{ color: C.mut }}>Sans correspondance (CSV)</b> :{" "}
                  <span style={{ fontFamily: M }}>{analysis.unmatchedCsv.map(v => `«${v}»`).join(" ")}</span></div>
              )}
              <div style={{ lineHeight: 1.45 }}>
                Comparez ces valeurs : un zéro initial absent, une casse ou un accent
                différent suffisent à empêcher l'appariement.
              </div>
            </div>
          )}
          {analysis.dupes > 0 && (
            <div style={{ display: "flex", gap: 6, fontSize: 9.5, color: C.amb, lineHeight: 1.45 }}>
              <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} />
              Plusieurs lignes CSV partagent la même clé : seule la première est retenue
              pour chaque entité.
            </div>
          )}
        </div>
      )}

      {/* 5. Colonnes à rapatrier */}
      {analysis && (
        <div style={box}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <Lbl>4 · Colonnes à rapatrier</Lbl>
            <button onClick={() => setCols(cols.length ? [] : csv.columns.filter(c => c !== csvKey))}
              style={{ fontFamily: F, fontSize: 9.5, background: "transparent", border: "none", color: C.acc, cursor: "pointer" }}>
              {cols.length ? "Aucune" : "Toutes"}
            </button>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 160, overflowY: "auto" }}>
            {csv.columns.filter(c => c !== csvKey).map(c => {
              const clash = geoProps.includes(c);
              return (
                <label key={c} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.mut, cursor: "pointer", padding: "1px 0" }}>
                  <input type="checkbox" checked={cols.includes(c)} style={{ accentColor: C.acc, cursor: "pointer" }}
                    onChange={e => setCols(p => e.target.checked ? [...p, c] : p.filter(x => x !== c))} />
                  <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c}</span>
                  {clash && <span title="Ce nom existe déjà dans la couche : il sera écrasé sans préfixe"
                    style={{ fontSize: 8.5, color: C.amb, border: `0.5px solid ${C.amb}55`, borderRadius: 3, padding: "0 3px" }}>existe</span>}
                </label>
              );
            })}
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10.5, color: C.mut }}>
            Préfixe
            <input value={prefix} onChange={e => setPrefix(e.target.value)} placeholder="ex. csv_"
              style={{ flex: 1, fontFamily: M, fontSize: 10.5, padding: "4px 7px", borderRadius: 5,
                       background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />
          </label>
        </div>
      )}

      {err && (
        <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.red, background: C.red + "12",
                      borderRadius: 6, padding: "7px 9px" }}>
          <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
        </div>
      )}
      {done && (
        <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.acc, background: C.acc + "12",
                      borderRadius: 6, padding: "7px 9px" }}>
          <IcCheck size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {done}
        </div>
      )}

      <button onClick={run} disabled={!analysis || !cols.length} style={{
        fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "9px 0", borderRadius: 7,
        background: (!analysis || !cols.length) ? C.hover : C.acc,
        color: (!analysis || !cols.length) ? C.dim : "#fff",
        border: "none", cursor: (!analysis || !cols.length) ? "default" : "pointer",
        display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
      }}>
        <IcTable size={14} /> Créer la couche jointe
      </button>
      <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
        La couche d'origine n'est pas modifiée : le résultat est ajouté comme
        nouvelle couche.
      </div>
    </div>
  );
}

/**
 * FieldCalcBlock.jsx — Calculateur de champ, façon QGIS.
 *
 * Zone d'expression libre + arbre de fonctions cliquables qui insèrent au
 * curseur, et aperçu en direct du résultat avant d'écrire quoi que ce soit.
 *
 * L'évaluation passe par utils/fieldCalc (analyseur maison) et non par eval :
 * les couches sont sauvegardées et partagées, une expression rejouée chez un
 * tiers ne doit pas pouvoir exécuter de code.
 */
import { useState, useMemo, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcChevronDown, IcChevronRight, IcSliders, IcAlert, IcCheck } from "../icons";
import { preview, applyField, GEO_VARS, FUNC_NAMES, AGG_NAMES } from "../utils/fieldCalc";

const CATS = [
  { id: "fields", label: "Champs" },
  { id: "ops",    label: "Opérateurs" },
  { id: "math",   label: "Maths" },
  { id: "geom",   label: "Géométrie" },
  { id: "agg",    label: "Agrégats" },
];

const OP_TOKENS = ["+", "-", "*", "/", "%", "^", "(", ")", ",", "<", "<=", ">", ">=", "=", "<>"];
// Fonctions dont l'assistant propose un second argument numérique
const ARITY2 = ["ARRONDI", "PUISSANCE"];

const HELP = {
  SOMME: "Somme du champ sur toute la couche — pour une part du total.",
  MOYENNE: "Moyenne du champ sur la couche — pour un écart à la moyenne.",
  MINIMUM: "Plus petite valeur du champ sur la couche.",
  MAXIMUM: "Plus grande valeur du champ sur la couche.",
  NOMBRE: "Nombre d'entités où le champ est renseigné.",
  SI: "SI(condition, alors, sinon) — ex. SI(pop>1000, 1, 0).",
  ARRONDI: "ARRONDI(valeur, décimales).",
  RACINE: "Racine carrée.", LOG: "Logarithme décimal.", LN: "Logarithme népérien.",
  PUISSANCE: "PUISSANCE(base, exposant).", ABS: "Valeur absolue.",
  MIN: "Plus petite de plusieurs valeurs.", MAX: "Plus grande de plusieurs valeurs.",
  EXP: "Exponentielle.",
  $aire_m2: "Aire de l'entité en m².", $aire_ha: "Aire en hectares.",
  $aire_km2: "Aire en km² — le dénominateur d'une densité.",
  $perimetre_m: "Périmètre en mètres.", $longueur_m: "Longueur d'une ligne en mètres.",
  $x: "Longitude du centre.", $y: "Latitude du centre.",
};

export default function FieldCalcBlock({ layer, onApply }) {
  const C = useThemeContext();
  const taRef = useRef(null);
  const [open, setOpen] = useState(false);
  const [cat, setCat] = useState("fields");
  const [expr, setExpr] = useState("");
  const [name, setName] = useState("");
  const [hint, setHint] = useState(null);
  const [confirming, setConfirming] = useState(false);
  const [done, setDone] = useState(null);

  // Assistant de composition — évite la saisie manuelle des noms de champs,
  // origine de la quasi-totalité des expressions rejetées.
  const [wiz, setWiz] = useState("binary");   // binary | func | agg
  const [wA, setWA] = useState("");
  const [wB, setWB] = useState("");
  const [wBmode, setWBmode] = useState("field");
  const [wOp, setWOp] = useState("+");
  const [wFn, setWFn] = useState(FUNC_NAMES[0]);
  const [wAgg, setWAgg] = useState(AGG_NAMES[0]);

  const feats = layer?.geojson?.features || [];
  const props = useMemo(() => {
    const s = new Set();
    for (const f of feats.slice(0, 300)) Object.keys(f.properties || {}).forEach(k => s.add(k));
    return [...s];
  }, [feats]);

  // La colonne qu'on vient de créer existe désormais : la signaler comme
  // « déjà présente » serait absurde tant que le nom n'a pas été retouché.
  const exists = name && props.includes(name) && name !== done?.col;

  const prev = useMemo(() => {
    if (!expr.trim() || !feats.length) return null;
    try { return { ok: true, ...preview(expr, feats) }; }
    catch (e) { return { ok: false, msg: e.message }; }
  }, [expr, feats]);

  /** Insère au curseur — un clic ne doit pas écraser ce qui est déjà tapé. */
  const insert = (txt) => {
    const ta = taRef.current;
    const needsQuotes = /[^A-Za-z0-9_$.]/.test(txt) && !OP_TOKENS.includes(txt) && !txt.startsWith("$");
    const t = needsQuotes ? `"${txt}"` : txt;
    if (!ta) { setExpr(e => e + t); return; }
    const s = ta.selectionStart ?? expr.length, e2 = ta.selectionEnd ?? s;
    const next = expr.slice(0, s) + t + expr.slice(e2);
    setExpr(next);
    requestAnimationFrame(() => { ta.focus(); ta.setSelectionRange(s + t.length, s + t.length); });
  };

  const items = cat === "fields" ? props
    : cat === "ops" ? OP_TOKENS
    : cat === "math" ? FUNC_NAMES.map(f => `${f}(`)
    : cat === "geom" ? GEO_VARS
    : AGG_NAMES.map(f => `${f}(`);

  const run = () => {
    setDone(null);
    if (!name.trim()) return setHint("Donnez un nom à la colonne.");
    if (!prev?.ok) return setHint(prev?.msg || "Expression invalide.");
    if (exists && !confirming) { setConfirming(true); return; }
    try {
      const gj = applyField(layer.geojson, expr, name.trim());
      onApply(gj, name.trim());
      setConfirming(false); setHint(null);
      setDone({ col: name.trim(), msg: `Colonne « ${name.trim()} » calculée sur ${prev.total - prev.failed} entité(s).` });
    } catch (e) { setHint(e.message); }
  };

  const fmt = v => v === null ? "—" : (Math.abs(v) >= 1000 ? v.toFixed(0) : v.toFixed(3).replace(/\.?0+$/, ""));

  const sel = {
    flex: "1 1 90px", minWidth: 0, fontFamily: F, fontSize: 10, padding: "4px 6px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none",
  };

  // Opérandes proposées : champs de la couche + variables géométriques, pour
  // qu'une densité se compose entièrement à la souris.
  const operands = useMemo(() => [...props, ...GEO_VARS], [props]);

  /** Citation d'un nom : seuls les noms « simples » peuvent rester nus. */
  const q = (v) => (!v ? "" : (v.startsWith("$") || /^[A-Za-z_][A-Za-z0-9_]*$/.test(v) ? v : `"${v}"`));

  // Fragment construit par l'assistant — toujours syntaxiquement valide
  const built = useMemo(() => {
    if (wiz === "binary") {
      if (!wA || wB === "") return "";
      const b = wBmode === "const" ? String(wB).trim().replace(",", ".") : q(wB);
      if (wBmode === "const" && !/^-?\d+(\.\d+)?$/.test(b)) return "";
      return `${q(wA)} ${wOp} ${b}`;
    }
    if (wiz === "func") {
      if (!wA) return "";
      const second = ARITY2.includes(wFn) ? String(wB).trim().replace(",", ".") : "";
      if (ARITY2.includes(wFn) && !/^-?\d+(\.\d+)?$/.test(second)) return "";
      return `${wFn}(${q(wA)}${second ? `, ${second}` : ""})`;
    }
    return wA ? `${wAgg}(${q(wA)})` : "";
  }, [wiz, wA, wB, wBmode, wOp, wFn, wAgg]);

  return (
    <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8, marginTop: 2 }}>
      <button onClick={() => setOpen(o => !o)} style={{
        width: "100%", display: "flex", alignItems: "center", gap: 6, background: "transparent",
        border: "none", cursor: "pointer", padding: 0, color: C.txt,
      }}>
        {open ? <IcChevronDown size={13} /> : <IcChevronRight size={13} />}
        <IcSliders size={12} />
        <span style={{ fontSize: 11, fontWeight: 500, flex: 1, textAlign: "left" }}>Calculer un champ</span>
      </button>

      {open && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 8 }}>

          <textarea ref={taRef} value={expr} onChange={e => { setExpr(e.target.value); setConfirming(false); }}
            rows={3} placeholder={'ex.  population / $aire_km2\n     "pop_h" + "pop_f"\n     pop / SOMME(pop) * 100'}
            style={{ fontFamily: M, fontSize: 11, padding: "7px 9px", borderRadius: 6, resize: "vertical",
                     background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />

          {/* ── Assistant : compose une expression VALIDE sans rien taper ──
              Les noms de champs viennent de listes, jamais de la frappe : c'est
              là que se produisent les fautes qui font échouer un calcul. */}
          <div style={{ background: C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: 8,
                        display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", gap: 3 }}>
              {[["binary", "A ⋄ B"], ["func", "Fonction"], ["agg", "Agrégat"]].map(([k, l]) => (
                <button key={k} onClick={() => setWiz(k)} style={{
                  flex: 1, fontFamily: F, fontSize: 9.5, padding: "3px 6px", borderRadius: 5, cursor: "pointer",
                  background: wiz === k ? C.acc + "18" : "transparent",
                  border: `0.5px solid ${wiz === k ? C.acc + "66" : C.bdr}`,
                  color: wiz === k ? C.acc : C.dim,
                }}>{l}</button>
              ))}
            </div>

            {wiz === "binary" && (
              <div style={{ display: "flex", gap: 4, alignItems: "center", flexWrap: "wrap" }}>
                <select value={wA} onChange={e => setWA(e.target.value)} style={sel}>
                  <option value="">variable A…</option>
                  {operands.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
                <select value={wOp} onChange={e => setWOp(e.target.value)} style={{ ...sel, flex: "0 0 52px" }}>
                  {["+", "-", "*", "/", "%", ">", "<", ">=", "<=", "=", "<>"].map(o => <option key={o} value={o}>{o}</option>)}
                </select>
                <select value={wBmode} onChange={e => setWBmode(e.target.value)} style={{ ...sel, flex: "0 0 76px" }}>
                  <option value="field">variable</option>
                  <option value="const">nombre</option>
                </select>
                {wBmode === "field" ? (
                  <select value={wB} onChange={e => setWB(e.target.value)} style={sel}>
                    <option value="">variable B…</option>
                    {operands.map(o => <option key={o} value={o}>{o}</option>)}
                  </select>
                ) : (
                  <input value={wB} onChange={e => setWB(e.target.value)} placeholder="100"
                    style={{ ...sel, fontFamily: M }} />
                )}
              </div>
            )}

            {wiz === "func" && (
              <div style={{ display: "flex", gap: 4, alignItems: "center", flexWrap: "wrap" }}>
                <select value={wFn} onChange={e => setWFn(e.target.value)} style={{ ...sel, flex: "0 0 104px" }}>
                  {FUNC_NAMES.map(f => <option key={f} value={f}>{f}</option>)}
                </select>
                <select value={wA} onChange={e => setWA(e.target.value)} style={sel}>
                  <option value="">variable…</option>
                  {operands.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
                {ARITY2.includes(wFn) && (
                  <input value={wB} onChange={e => setWB(e.target.value)} placeholder="2e argument"
                    style={{ ...sel, fontFamily: M, flex: "0 0 88px" }} />
                )}
              </div>
            )}

            {wiz === "agg" && (
              <div style={{ display: "flex", gap: 4, alignItems: "center", flexWrap: "wrap" }}>
                <select value={wAgg} onChange={e => setWAgg(e.target.value)} style={{ ...sel, flex: "0 0 104px" }}>
                  {AGG_NAMES.map(f => <option key={f} value={f}>{f}</option>)}
                </select>
                <select value={wA} onChange={e => setWA(e.target.value)} style={sel}>
                  <option value="">champ…</option>
                  {props.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
              </div>
            )}

            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <code style={{ flex: 1, minWidth: 0, fontFamily: M, fontSize: 10, color: built ? C.acc : C.dim,
                             overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {built || "choisissez les éléments…"}
              </code>
              <button onClick={() => built && insert(built)} disabled={!built} style={{
                fontFamily: F, fontSize: 9.5, padding: "4px 9px", borderRadius: 5, flexShrink: 0,
                background: built ? C.acc : C.hover, color: built ? "#fff" : C.dim,
                border: "none", cursor: built ? "pointer" : "default",
              }}>Insérer</button>
            </div>
          </div>

          {/* Catégories cliquables : insertion au curseur, comme dans QGIS */}
          <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
            {CATS.map(c => (
              <button key={c.id} onClick={() => setCat(c.id)} style={{
                fontFamily: F, fontSize: 9.5, padding: "3px 7px", borderRadius: 5, cursor: "pointer",
                background: cat === c.id ? C.acc + "18" : "transparent",
                border: `0.5px solid ${cat === c.id ? C.acc + "66" : C.bdr}`,
                color: cat === c.id ? C.acc : C.dim,
              }}>{c.label}</button>
            ))}
          </div>
          <div style={{ display: "flex", gap: 3, flexWrap: "wrap", maxHeight: 108, overflowY: "auto",
                        background: C.hover, borderRadius: 6, padding: 6, border: `0.5px solid ${C.bdr}` }}>
            {items.length === 0 && <span style={{ fontSize: 10, color: C.dim }}>Rien à insérer ici.</span>}
            {items.map(it => {
              const key = it.replace(/\($/, "");
              return (
                <button key={it} onClick={() => insert(it)}
                  onMouseEnter={() => HELP[key] && setHint(HELP[key])}
                  title={HELP[key] || "Insérer"}
                  style={{ fontFamily: M, fontSize: 10, padding: "2px 6px", borderRadius: 4, cursor: "pointer",
                           background: C.card, border: `0.5px solid ${C.bdr}`, color: C.mut }}>{it}</button>
              );
            })}
          </div>

          {/* Aperçu : voir avant d'écrire */}
          {prev && (prev.ok ? (
            <div style={{ background: C.acc + "0d", border: `0.5px solid ${C.acc}33`, borderRadius: 6, padding: "7px 9px",
                          display: "flex", flexDirection: "column", gap: 3 }}>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>
                Aperçu — {prev.total - prev.failed}/{prev.total} calculées
              </div>
              <div style={{ fontFamily: M, fontSize: 10.5, color: C.txt }}>
                {prev.sample.map(fmt).join("  ·  ")}{prev.total > prev.sample.length ? "  …" : ""}
              </div>
              {prev.min !== null && (
                <div style={{ fontFamily: M, fontSize: 9.5, color: C.dim }}>
                  min {fmt(prev.min)} · max {fmt(prev.max)}
                </div>
              )}
              {prev.failed > 0 && (
                <div style={{ fontSize: 9.5, color: C.amb, lineHeight: 1.4 }}>
                  {prev.failed} entité(s) sans résultat : champ vide, texte non numérique
                  ou division par zéro. La colonne y sera nulle.
                </div>
              )}
            </div>
          ) : (
            <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.red, background: C.red + "12",
                          borderRadius: 6, padding: "7px 9px" }}>
              <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {prev.msg}
            </div>
          ))}

          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>Nouvelle colonne</span>
            <input value={name} onChange={e => { setName(e.target.value); setConfirming(false); }}
              placeholder="densite" style={{ flex: 1, minWidth: 0, fontFamily: M, fontSize: 10.5,
                padding: "5px 8px", borderRadius: 5, background: C.input, color: C.txt,
                border: `0.5px solid ${exists ? C.amb : C.bdr}`, outline: "none" }} />
          </div>

          {exists && (
            <div style={{ display: "flex", gap: 6, fontSize: 9.5, color: C.amb, lineHeight: 1.4 }}>
              <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} />
              « {name} » existe déjà dans cette couche : l'appliquer remplacera ses valeurs.
            </div>
          )}
          {hint && !prev?.msg && (
            <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.4 }}>{hint}</div>
          )}
          {done && (
            <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.acc, background: C.acc + "12",
                          borderRadius: 6, padding: "7px 9px" }}>
              <IcCheck size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {done.msg}
            </div>
          )}

          <button onClick={run} disabled={!prev?.ok || !name.trim()} style={{
            fontFamily: F, fontSize: 10.5, fontWeight: 600, padding: "7px 0", borderRadius: 6,
            background: confirming ? C.amb : (!prev?.ok || !name.trim()) ? C.hover : C.acc,
            color: (!prev?.ok || !name.trim()) ? C.dim : "#fff",
            border: "none", cursor: (!prev?.ok || !name.trim()) ? "default" : "pointer",
          }}>
            {confirming ? "Confirmer le remplacement" : "Calculer la colonne"}
          </button>
        </div>
      )}
    </div>
  );
}

import { useState, useMemo, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX } from "../icons";

// ── Icône entonnoir SVG ──────────────────────────────────────
export function FunnelIcon({ size = 12, color = "currentColor", filled = false }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
      {filled ? (
        <path
          d="M1 2.5A.5.5 0 0 1 1.5 2h13a.5.5 0 0 1 .37.82L10 8.88V14a.5.5 0 0 1-.78.42l-3-2A.5.5 0 0 1 6 12V8.88L1.13 2.82A.5.5 0 0 1 1 2.5Z"
          fill={color}
        />
      ) : (
        <path
          d="M1.5 2h13l-5 6.5V14l-3-2V8.5L1.5 2Z"
          stroke={color} strokeWidth="1.2" strokeLinejoin="round" fill="none"
        />
      )}
    </svg>
  );
}

// ── Opérateurs par type ──────────────────────────────────────
const OPS_NUMBER = [
  { value: "=",       label: "= égal" },
  { value: "!=",      label: "≠ différent" },
  { value: ">",       label: "> supérieur" },
  { value: ">=",      label: "≥ supérieur ou égal" },
  { value: "<",       label: "< inférieur" },
  { value: "<=",      label: "≤ inférieur ou égal" },
  { value: "null",    label: "est vide (NULL)" },
  { value: "notnull", label: "n'est pas vide" },
];
const OPS_STRING = [
  { value: "=",          label: "= égal à" },
  { value: "!=",         label: "≠ différent de" },
  { value: "contains",   label: "contient" },
  { value: "startswith", label: "commence par" },
  { value: "endswith",   label: "se termine par" },
  { value: "regex",      label: "expression régulière" },
  { value: "null",       label: "est vide (NULL)" },
  { value: "notnull",    label: "n'est pas vide" },
];
const OPS_BOOL = [
  { value: "=true",  label: "est vrai" },
  { value: "=false", label: "est faux" },
  { value: "null",   label: "est vide (NULL)" },
];

const NO_VALUE_OPS = new Set(["null", "notnull", "=true", "=false"]);
const FREE_TEXT_OPS = new Set(["contains", "startswith", "endswith", "regex"]);

// ── Détecter le type d'un attribut ───────────────────────────
function detectType(values) {
  const nonNull = values.filter(v => v != null && v !== "");
  if (!nonNull.length) return "string";
  if (nonNull.every(v => typeof v === "boolean")) return "boolean";
  if (nonNull.every(v => typeof v === "number" || (!isNaN(Number(v)) && String(v).trim() !== ""))) return "number";
  return "string";
}

// ── Extraire schéma depuis le GeoJSON source (non filtré) ────
function extractSchema(geojson) {
  const features = geojson?.features || [];
  if (!features.length) return [];
  const map = new Map();
  for (const f of features) {
    const props = f.properties || {};
    for (const [k, v] of Object.entries(props)) {
      if (!map.has(k)) map.set(k, new Set());
      if (v != null && v !== "") map.get(k).add(v);
    }
  }
  return [...map.entries()].map(([name, set]) => {
    const samples = [...set];
    const type = detectType(samples);
    const sorted = samples.sort((a, b) => {
      if (type === "number") return Number(a) - Number(b);
      return String(a).localeCompare(String(b), "fr", { numeric: true });
    });
    return { name, type, uniques: sorted };
  });
}

// ── Évaluer une règle ─────────────────────────────────────────
function evalRule(rule, props) {
  const raw = props?.[rule.field];
  const op  = rule.op;
  const val = rule.value;

  if (op === "null")    return raw == null || raw === "";
  if (op === "notnull") return raw != null && raw !== "";
  if (op === "=true")   return raw === true || raw === "true" || raw === 1;
  if (op === "=false")  return raw === false || raw === "false" || raw === 0;
  if (raw == null)      return false;

  if (rule.fieldType === "number") {
    const n = Number(raw), v = Number(val);
    if (isNaN(n) || isNaN(v)) return false;
    if (op === "=")  return n === v;
    if (op === "!=") return n !== v;
    if (op === ">")  return n > v;
    if (op === ">=") return n >= v;
    if (op === "<")  return n < v;
    if (op === "<=") return n <= v;
  }

  const s = String(raw), sv = String(val ?? "");
  if (op === "=")          return s === sv;
  if (op === "!=")         return s !== sv;
  if (op === "contains")   return s.toLowerCase().includes(sv.toLowerCase());
  if (op === "startswith") return s.toLowerCase().startsWith(sv.toLowerCase());
  if (op === "endswith")   return s.toLowerCase().endsWith(sv.toLowerCase());
  if (op === "regex")      { try { return new RegExp(sv, "i").test(s); } catch { return false; } }
  return false;
}

// ── Appliquer le filtre sur un GeoJSON (export utilisé par LayerPanel) ──
export function applyFilter(geojson, filterState) {
  if (!filterState?.rules?.length) return geojson;
  const { rules, logic } = filterState;
  const filtered = (geojson?.features || []).filter(f => {
    const p = f.properties || {};
    return logic === "AND" ? rules.every(r => evalRule(r, p)) : rules.some(r => evalRule(r, p));
  });
  return { ...geojson, features: filtered };
}

// ── Compter les features correspondantes ─────────────────────
function countMatch(geojson, rules, logic) {
  if (!rules.length) return geojson?.features?.length ?? 0;
  return (geojson?.features || []).filter(f => {
    const p = f.properties || {};
    return logic === "AND" ? rules.every(r => evalRule(r, p)) : rules.some(r => evalRule(r, p));
  }).length;
}

// ── Nouvelle règle vide ───────────────────────────────────────
function emptyRule(schema) {
  const first = schema[0];
  const ops = first?.type === "number" ? OPS_NUMBER : first?.type === "boolean" ? OPS_BOOL : OPS_STRING;
  return {
    id:        Math.random().toString(36).slice(2),
    field:     first?.name || "",
    fieldType: first?.type || "string",
    op:        ops[0].value,
    value:     "",
  };
}

// ── Style commun selects/inputs ───────────────────────────────
const mkSel = (C) => ({
  fontFamily: F, fontSize: 11, padding: "5px 7px", borderRadius: 5,
  background: C.input, color: C.txt,
  border: `0.5px solid ${C.bdr}`, outline: "none", cursor: "pointer",
  minWidth: 0,
});

// ── Ligne de règle ────────────────────────────────────────────
function RuleRow({ rule, schema, index, logic, onChange, onRemove, C }) {
  const field   = schema.find(s => s.name === rule.field);
  const ops     = !field ? OPS_STRING
    : field.type === "number" ? OPS_NUMBER
    : field.type === "boolean" ? OPS_BOOL
    : OPS_STRING;
  const noValue  = NO_VALUE_OPS.has(rule.op);
  const freeText = FREE_TEXT_OPS.has(rule.op);
  const ss       = mkSel(C);

  const handleField = (e) => {
    const name = e.target.value;
    const s    = schema.find(x => x.name === name);
    const newOps = s?.type === "number" ? OPS_NUMBER : s?.type === "boolean" ? OPS_BOOL : OPS_STRING;
    onChange({ ...rule, field: name, fieldType: s?.type || "string", op: newOps[0].value, value: "" });
  };

  return (
    <div>
      {/* Badge ET/OU entre les règles */}
      {index > 0 && (
        <div style={{ display: "flex", alignItems: "center", padding: "0 14px", gap: 8 }}>
          <div style={{ flex: 1, height: "0.5px", background: C.bdr + "55" }} />
          <span style={{
            fontFamily: F, fontSize: 9, fontWeight: 700, letterSpacing: ".08em",
            padding: "1px 8px", borderRadius: 10,
            background: logic === "AND" ? C.acc + "18" : (C.amb ?? "#f59e0b") + "18",
            color: logic === "AND" ? C.acc : (C.amb ?? "#f59e0b"),
            border: `0.5px solid ${logic === "AND" ? C.acc + "55" : (C.amb ?? "#f59e0b") + "55"}`,
          }}>
            {logic}
          </span>
          <div style={{ flex: 1, height: "0.5px", background: C.bdr + "55" }} />
        </div>
      )}

      <div style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 14px" }}>
        {/* Numéro de règle */}
        <span style={{ fontFamily: M, fontSize: 9, color: C.dim, minWidth: 14, textAlign: "right", flexShrink: 0 }}>
          {index + 1}
        </span>

        {/* Champ — liste déroulante des noms uniquement */}
        <select value={rule.field} onChange={handleField}
          style={{ ...ss, flex: "0 1 130px" }}>
          {schema.map(s => (
            <option key={s.name} value={s.name}>{s.name}</option>
          ))}
        </select>

        {/* Opérateur */}
        <select value={rule.op} onChange={e => onChange({ ...rule, op: e.target.value, value: "" })}
          style={{ ...ss, flex: "0 0 148px" }}>
          {ops.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>

        {/* Valeur */}
        {!noValue && (
          freeText
            ? (
              <input
                type="text"
                value={rule.value}
                onChange={e => onChange({ ...rule, value: e.target.value })}
                placeholder={rule.op === "regex" ? "ex: ^Paris.*" : "texte…"}
                style={{ ...ss, flex: 1, cursor: "text" }}
              />
            ) : (
              /* Select avec toutes les valeurs disponibles dans la couche */
              <select
                value={rule.value}
                onChange={e => onChange({ ...rule, value: e.target.value })}
                style={{ ...ss, flex: 1 }}
              >
                <option value="">— sélectionner —</option>
                {(field?.uniques || []).map(v => (
                  <option key={String(v)} value={String(v)}>{String(v)}</option>
                ))}
              </select>
            )
        )}

        {noValue && <div style={{ flex: 1 }} />}

        {/* Supprimer */}
        <button
          onClick={onRemove}
          title="Supprimer cette règle"
          style={{
            background: "none", border: `0.5px solid ${C.bdr}`,
            borderRadius: 4, cursor: "pointer", color: C.dim,
            padding: "3px 6px", fontSize: 11, flexShrink: 0, lineHeight: 1,
          }}
          onMouseEnter={e => { e.currentTarget.style.color = C.red ?? "#ef4444"; e.currentTarget.style.borderColor = (C.red ?? "#ef4444") + "66"; }}
          onMouseLeave={e => { e.currentTarget.style.color = C.dim; e.currentTarget.style.borderColor = C.bdr; }}
        ><IcX size={12}/></button>
      </div>
    </div>
  );
}

// ── Modal principal ──────────────────────────────────────────
export default function FilterModal({ layer, onClose, onApply }) {
  const C = useThemeContext();

  // Toujours travailler sur la source originale (avant filtre)
  const sourceGeojson = layer?._sourceGeojson || layer?.geojson;
  const schema = useMemo(() => extractSchema(sourceGeojson), [sourceGeojson]);

  const initial = layer?.filterState || { rules: [], logic: "AND" };
  const [rules, setRules] = useState(() => initial.rules);
  const [logic, setLogic] = useState(initial.logic || "AND");

  const total   = sourceGeojson?.features?.length ?? 0;
  const matched = useMemo(() => countMatch(sourceGeojson, rules, logic), [sourceGeojson, rules, logic]);
  const ratio   = total > 0 ? matched / total : 1;

  const barColor = ratio >= 1 ? C.acc
    : ratio > 0.5 ? (C.amb ?? "#f59e0b")
    : ratio > 0   ? "#f97316"
    : (C.red ?? "#ef4444");

  const addRule    = useCallback(() => schema.length && setRules(r => [...r, emptyRule(schema)]), [schema]);
  const updateRule = useCallback((id, upd) => setRules(r => r.map(x => x.id === id ? upd : x)), []);
  const removeRule = useCallback((id)      => setRules(r => r.filter(x => x.id !== id)), []);

  const handleApply = () => { onApply({ rules, logic }); onClose(); };
  const handleClear = () => { onApply({ rules: [], logic: "AND" }); onClose(); };

  return (
    <>
      {/* Overlay */}
      <div onClick={onClose} style={{
        position: "fixed", inset: 0,
        background: "rgba(0,0,0,0.4)",
        zIndex: 1200,
        backdropFilter: "blur(2px)",
      }} />

      {/* Modal */}
      <div style={{
        position: "fixed",
        top: "50%", left: "50%",
        transform: "translate(-50%, -50%)",
        zIndex: 1201,
        width: "min(620px, 95vw)",
        maxHeight: "82vh",
        display: "flex", flexDirection: "column",
        background: C.bg,
        borderRadius: 10,
        border: `0.5px solid ${C.bdr}`,
        boxShadow: "0 24px 64px rgba(0,0,0,0.4)",
        overflow: "hidden",
      }}>

        {/* ── En-tête ── */}
        <div style={{
          padding: "13px 16px 11px",
          borderBottom: `0.5px solid ${C.bdr}`,
          display: "flex", alignItems: "center", gap: 10, flexShrink: 0,
        }}>
          <FunnelIcon size={15} color={C.acc} filled={rules.length > 0} />
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.txt, fontFamily: F }}>
              Filtre attributaire
            </div>
            <div style={{
              fontSize: 10, color: C.dim, fontFamily: M, marginTop: 1,
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
            }}>
              {layer?.name}
            </div>
          </div>
          <button onClick={onClose} style={{
            background: "none", border: "none", cursor: "pointer",
            color: C.dim, display: "flex", padding: "2px 4px", borderRadius: 4,
          }}><IcX size={15}/></button>
        </div>

        {/* ── Barre résultat temps réel ── */}
        <div style={{
          padding: "9px 16px 8px",
          borderBottom: `0.5px solid ${C.bdr}`,
          flexShrink: 0,
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 5 }}>
            <span style={{ fontSize: 10, color: C.dim, fontFamily: F }}>
              {rules.length === 0
                ? `${total.toLocaleString("fr")} entité${total > 1 ? "s" : ""} — aucun filtre actif`
                : "Entités correspondantes"}
            </span>
            <span style={{ fontSize: 11, fontFamily: M, fontWeight: 700, color: barColor }}>
              {matched.toLocaleString("fr")} / {total.toLocaleString("fr")}
            </span>
          </div>
          <div style={{ height: 4, borderRadius: 2, background: C.hover ?? C.bdr + "44", overflow: "hidden" }}>
            <div style={{
              height: "100%", width: `${ratio * 100}%`,
              background: barColor, borderRadius: 2,
              transition: "width .2s ease, background .2s ease",
            }} />
          </div>
        </div>

        {/* ── Toggle ET / OU ── */}
        {rules.length >= 2 && (
          <div style={{
            padding: "7px 16px",
            display: "flex", alignItems: "center", gap: 8,
            borderBottom: `0.5px solid ${C.bdr}`,
            flexShrink: 0,
          }}>
            <span style={{ fontSize: 10, color: C.dim, fontFamily: F }}>Combiner avec :</span>
            {[{ v: "AND", label: "ET — toutes les règles" }, { v: "OR", label: "OU — au moins une" }].map(({ v, label }) => (
              <button key={v} onClick={() => setLogic(v)} style={{
                fontFamily: F, fontSize: 10, fontWeight: 700,
                padding: "3px 11px", borderRadius: 20, cursor: "pointer",
                border: `1px solid ${logic === v ? C.acc : C.bdr}`,
                background: logic === v ? C.acc + "22" : "transparent",
                color: logic === v ? C.acc : C.dim,
                transition: "all .15s",
              }}>
                {label}
              </button>
            ))}
          </div>
        )}

        {/* ── Règles ── */}
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
          {!schema.length && (
            <div style={{ padding: 24, textAlign: "center", fontSize: 11, color: C.dim }}>
              Aucun attribut disponible dans cette couche.
            </div>
          )}

          {schema.length > 0 && rules.length === 0 && (
            <div style={{
              padding: "28px 20px",
              display: "flex", flexDirection: "column", alignItems: "center", gap: 12,
            }}>
              <FunnelIcon size={30} color={C.bdr} filled={false} />
              <div style={{ fontSize: 11, color: C.dim, fontFamily: F, lineHeight: 1.7, textAlign: "center" }}>
                Aucun filtre actif — toutes les entités sont affichées.<br />
                <span style={{ color: C.txt }}>Ajoutez une règle pour filtrer la couche.</span>
              </div>
            </div>
          )}

          {rules.map((rule, i) => (
            <RuleRow
              key={rule.id}
              rule={rule}
              schema={schema}
              index={i}
              logic={logic}
              onChange={upd => updateRule(rule.id, upd)}
              onRemove={() => removeRule(rule.id)}
              C={C}
            />
          ))}
        </div>

        {/* ── Pied de page ── */}
        <div style={{
          padding: "10px 16px",
          borderTop: `0.5px solid ${C.bdr}`,
          display: "flex", gap: 8, alignItems: "center", flexShrink: 0,
        }}>
          <button
            onClick={addRule}
            disabled={!schema.length}
            style={{
              fontFamily: F, fontSize: 11, fontWeight: 600,
              padding: "6px 13px", borderRadius: 6,
              cursor: schema.length ? "pointer" : "default",
              background: "transparent",
              border: `0.5px solid ${C.acc}`,
              color: C.acc,
              opacity: schema.length ? 1 : 0.4,
              display: "flex", alignItems: "center", gap: 5,
            }}
          >
            <span style={{ fontSize: 14, lineHeight: 1 }}>+</span> Ajouter une règle
          </button>

          <div style={{ flex: 1 }} />

          {rules.length > 0 && (
            <button onClick={handleClear} style={{
              fontFamily: F, fontSize: 11,
              padding: "6px 13px", borderRadius: 6, cursor: "pointer",
              background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim,
            }}>
              Effacer tout
            </button>
          )}

          <button onClick={handleApply} style={{
            fontFamily: F, fontSize: 11, fontWeight: 700,
            padding: "6px 20px", borderRadius: 6, cursor: "pointer",
            background: C.acc, border: "none", color: "#fff",
            boxShadow: `0 2px 8px ${C.acc}44`,
          }}>
            Appliquer
          </button>
        </div>
      </div>
    </>
  );
}

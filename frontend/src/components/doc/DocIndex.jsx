/**
 * DocIndex.jsx — Accueil de la documentation (le « blog »).
 * Recherche + filtre par catégorie + grille de cartes + rail (catégories, récents).
 * Route : /doc
 */
import { useState, useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useTheme } from "../../theme";
import { F, M } from "../../config";
import { IcSearch, IcInfo } from "../../icons";
import DocShell from "./DocShell";
import { DOC_PAGES, DOC_CATEGORIES, RECENT_PAGES, searchDocs } from "../../utils/docRegistry";

const fmtDate = (iso) => {
  try { return new Date(iso).toLocaleDateString("fr", { day: "numeric", month: "short" }); }
  catch { return iso; }
};

function DocCard({ p, C }) {
  const Ic = p.icon;
  return (
    <Link to={`/doc/${p.slug}`} style={{
      background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "15px 16px",
      display: "flex", flexDirection: "column", gap: 9, textDecoration: "none", color: C.txt,
      transition: "border-color .15s, transform .15s",
    }}
      onMouseEnter={e => { e.currentTarget.style.borderColor = C.mut + "66"; e.currentTarget.style.transform = "translateY(-2px)"; }}
      onMouseLeave={e => { e.currentTarget.style.borderColor = C.bdr; e.currentTarget.style.transform = ""; }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span style={{ display: "inline-flex", color: p.color }}>{Ic ? <Ic size={16} /> : null}</span>
        <span style={{ fontSize: 10.5, textTransform: "uppercase", letterSpacing: ".06em", color: C.mut }}>{p.categoryLabel}</span>
        {p.kind === "tool" && (
          <span style={{ marginLeft: "auto", fontSize: 9, textTransform: "uppercase", letterSpacing: ".06em", color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 4, padding: "1px 5px" }}>Outil</span>
        )}
      </div>
      <h3 style={{ fontSize: 15, fontWeight: 500, margin: 0, lineHeight: 1.25 }}>{p.title}</h3>
      <p style={{
        fontSize: 12, color: C.mut, lineHeight: 1.55, margin: 0,
        display: "-webkit-box", WebkitLineClamp: 3, WebkitBoxOrient: "vertical", overflow: "hidden",
      }}>{p.abstract}</p>
      {(p.source || p.dataLine) && (
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 2, paddingTop: 9, borderTop: `0.5px solid ${C.bdr}` }}>
          <span style={{ fontFamily: M, fontSize: 10, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {p.source ? `${p.source.label}${p.source.res ? ` · ${p.source.res}` : ""}` : p.dataLine}
          </span>
        </div>
      )}
    </Link>
  );
}

export default function DocIndex() {
  const { name, C, toggle } = useTheme();
  const [params] = useSearchParams();
  const [q, setQ] = useState(params.get("q") || "");
  const [cat, setCat] = useState(params.get("cat") || null);

  const pages = useMemo(() => {
    let list = searchDocs(q);
    if (cat) list = list.filter(p => p.category === cat);
    return list;
  }, [q, cat]);

  const dot = (color) => ({ width: 7, height: 7, borderRadius: "50%", background: color, flexShrink: 0, display: "inline-block" });
  const sectH = { fontSize: 12, letterSpacing: ".1em", textTransform: "uppercase", color: C.dim, margin: "0 0 14px", fontWeight: 500 };

  const chip = (active, color, label, count, onClick) => (
    <button key={label} onClick={onClick} style={{
      fontFamily: F, fontSize: 12, padding: "6px 12px", borderRadius: 20, cursor: "pointer",
      border: `0.5px solid ${active ? C.txt : C.bdr}`,
      background: active ? C.txt : "transparent", color: active ? C.bg : C.mut,
      display: "inline-flex", alignItems: "center", gap: 6,
    }}>
      {color && <span style={dot(color)} />}{label}
      <span style={{ fontSize: 10, color: active ? C.bg : C.dim, opacity: active ? .7 : 1, fontFamily: M }}>{count}</span>
    </button>
  );

  return (
    <DocShell C={C} themeName={name} onToggleTheme={toggle}>
      {/* Hero */}
      <section style={{ padding: "34px 4px 18px" }}>
        <div style={{ fontSize: 11, letterSpacing: ".16em", textTransform: "uppercase", color: C.acc, fontWeight: 500, marginBottom: 10 }}>Documentation</div>
        <h1 style={{ fontSize: 30, fontWeight: 500, margin: "0 0 10px", letterSpacing: "-.01em", lineHeight: 1.12, maxWidth: "20ch" }}>
          Comprendre chaque indicateur et chaque outil
        </h1>
        <p style={{ fontSize: 14, color: C.mut, maxWidth: "62ch", lineHeight: 1.65, margin: "0 0 18px" }}>
          À quoi sert une mesure ou un outil, comment la lire, comment l'obtenir dans l'application —
          et un exemple complet quand c'est utile. Une page par indicateur et par outil, comme un carnet de terrain.
        </p>
        <div style={{ display: "flex", alignItems: "center", gap: 9, maxWidth: 440, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: "10px 13px" }}>
          <IcSearch size={15} style={{ color: C.dim, flexShrink: 0 }} />
          <input value={q} onChange={e => setQ(e.target.value)} placeholder="Rechercher : NDVI, humidité du sol, inondation…"
            style={{ flex: 1, background: "transparent", border: "none", outline: "none", color: C.txt, fontFamily: F, fontSize: 13 }} />
        </div>
      </section>

      {/* Chips catégories */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 7, padding: "6px 4px 22px" }}>
        {chip(cat === null, null, "Tout", DOC_PAGES.length, () => setCat(null))}
        {DOC_CATEGORIES.map(c => chip(cat === c.id, c.color, c.label, c.count, () => setCat(cat === c.id ? null : c.id)))}
      </div>

      {/* Corps : grille + rail */}
      <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1fr) 250px", gap: 26, padding: "0 4px", alignItems: "start" }}
        className="doc-body">
        <div>
          <div style={sectH}>{q || cat ? `${pages.length} résultat${pages.length > 1 ? "s" : ""}` : "Tous les indicateurs"}</div>
          {pages.length === 0 ? (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 10, padding: "50px 0", color: C.dim }}>
              <IcInfo size={26} /><span style={{ fontSize: 13 }}>Aucune fiche ne correspond à « {q} ».</span>
            </div>
          ) : (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 14 }}>
              {pages.map(p => <DocCard key={p.slug} p={p} C={C} />)}
            </div>
          )}
        </div>

        <aside style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "15px 16px" }}>
            <div style={sectH}>Catégories</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {DOC_CATEGORIES.map(c => (
                <button key={c.id} onClick={() => setCat(cat === c.id ? null : c.id)} style={{
                  display: "flex", alignItems: "center", gap: 9, padding: "6px 4px", fontFamily: F, fontSize: 12.5,
                  color: cat === c.id ? C.txt : C.mut, background: cat === c.id ? C.hover : "transparent",
                  border: "none", borderRadius: 6, cursor: "pointer", textAlign: "left", width: "100%",
                }}>
                  <span style={dot(c.color)} />{c.label}
                  <span style={{ marginLeft: "auto", fontFamily: M, fontSize: 11, color: C.dim }}>{c.count}</span>
                </button>
              ))}
            </div>
          </div>

          {RECENT_PAGES.length > 0 && (
            <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "15px 16px" }}>
              <div style={sectH}>Récemment publiés</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                {RECENT_PAGES.slice(0, 5).map(p => (
                  <Link key={p.slug} to={`/doc/${p.slug}`} style={{ textDecoration: "none", color: C.txt }}>
                    <div style={{ fontSize: 10, color: C.dim, letterSpacing: ".05em", textTransform: "uppercase" }}>{fmtDate(p.date)}</div>
                    <div style={{ fontSize: 12.5, fontWeight: 500, lineHeight: 1.3 }}>{p.title}</div>
                  </Link>
                ))}
              </div>
            </div>
          )}
        </aside>
      </div>

      <style>{`@media (max-width: 820px){ .doc-body{ grid-template-columns: 1fr !important; } .doc-body aside{ order: 2; } }`}</style>
    </DocShell>
  );
}

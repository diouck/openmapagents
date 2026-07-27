/**
 * DocArticle.jsx — Page d'un indicateur.
 * Sections : à quoi ça sert · formule · comment lire · l'utiliser dans l'app ·
 * exemple complet · limites. Rail : source + lien catalogue + CTA + « même thème ».
 * Route : /doc/:slug
 */
import { Link, useParams } from "react-router-dom";
import { useTheme } from "../../theme";
import { F, M } from "../../config";
import { IcChevronRight, IcExternalLink, IcMap, IcAlert, IcInfo } from "../../icons";
import DocShell from "./DocShell";
import { getDocPage, relatedPages } from "../../utils/docRegistry";

// Pas-à-pas générique quand la fiche n'a pas encore d'usage rédigé à la main.
function genericUsage(p) {
  const src = (p.sources || []).map(s => s.label).slice(0, 2).join(" ou ") || "une source satellite";
  return [
    `Ouvrez le menu thématique → ${p.categoryLabel} → ${p.title}.`,
    `Choisissez la source (${src}) et la période d'intérêt.`,
    "Ajustez la palette et les seuils dans le panneau de style, puis lisez les valeurs via Statistiques sur une emprise.",
  ];
}

export default function DocArticle() {
  const { slug } = useParams();
  const { name, C, toggle } = useTheme();
  const p = getDocPage(slug);

  if (!p) {
    return (
      <DocShell C={C} themeName={name} onToggleTheme={toggle}>
        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 14, padding: "80px 0", color: C.mut }}>
          <IcInfo size={30} />
          <div style={{ fontSize: 15 }}>Cette fiche n'existe pas.</div>
          <Link to="/doc" style={{ color: C.acc, textDecoration: "none", fontSize: 13 }}>← Toute la documentation</Link>
        </div>
      </DocShell>
    );
  }

  const Ic = p.icon;
  const usage = p.usage || genericUsage(p);
  const related = relatedPages(p, 4);

  const sect = (label, node) => (
    <section style={{ marginBottom: 28 }}>
      <div style={{ fontSize: 11, letterSpacing: ".13em", textTransform: "uppercase", color: C.acc, fontWeight: 500, marginBottom: 10 }}>{label}</div>
      {node}
    </section>
  );
  const para = (t) => <p style={{ fontSize: 14.5, lineHeight: 1.72, color: C.txt, margin: "0 0 12px", maxWidth: "66ch" }}>{t}</p>;

  return (
    <DocShell C={C} themeName={name} onToggleTheme={toggle}>
      {/* Fil d'Ariane */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", fontSize: 12, color: C.dim, padding: "26px 4px 16px" }}>
        <Link to="/doc" style={{ color: C.dim, textDecoration: "none" }}>Documentation</Link>
        <IcChevronRight size={12} />
        <Link to={`/doc?cat=${p.category}`} style={{ color: C.dim, textDecoration: "none" }}>{p.categoryLabel}</Link>
        <IcChevronRight size={12} />
        <span style={{ color: C.mut }}>{p.title}</span>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1fr) 288px", gap: 34, padding: "0 4px", alignItems: "start" }} className="art-body">
        {/* Article */}
        <article>
          <h1 style={{ fontSize: 27, fontWeight: 500, margin: "0 0 12px", lineHeight: 1.15, letterSpacing: "-.01em" }}>{p.title}</h1>
          <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 10, marginBottom: 26 }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 11, padding: "4px 10px", borderRadius: 16, border: `0.5px solid ${C.bdr}`, color: C.mut }}>
              <span style={{ display: "inline-flex", color: p.color }}>{Ic ? <Ic size={13} /> : null}</span>{p.categoryLabel}
            </span>
            <span style={{ fontSize: 10.5, padding: "4px 9px", borderRadius: 16, background: C.hover, color: C.mut }}>
              {p.kind === "tool" ? "Outil" : "Indicateur"}
            </span>
            {p.formula && (
              <span style={{ fontFamily: M, fontSize: 11, padding: "4px 10px", borderRadius: 16, border: `0.5px solid ${C.bdr}`, color: C.mut }}>{p.formula}</span>
            )}
          </div>

          {sect("À quoi ça sert", para(p.abstract))}

          {p.formula && sect("Comment ça se calcule", (
            <div style={{ fontFamily: M, fontSize: 15, color: C.txt, background: C.input, border: `0.5px solid ${C.bdr}`, borderLeft: `2px solid ${C.acc}`, borderRadius: "0 8px 8px 0", padding: "15px 18px", overflowX: "auto" }}>{p.formula}</div>
          ))}

          {p.reading && sect("Comment le lire", (
            <div>
              {p.scale && (
                <div style={{ marginBottom: 14 }}>
                  <div style={{ height: 20, borderRadius: 6, border: `0.5px solid ${C.bdr}`, background: p.scale.gradient }} />
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 6, fontFamily: M, fontSize: 10, color: C.dim }}>
                    {p.scale.ticks.map((t, i) => <span key={i}>{t}</span>)}
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 11 }}>
                    {p.scale.legend.map((l, i) => (
                      <span key={i} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 11.5, color: C.mut }}>
                        <span style={{ width: 12, height: 12, borderRadius: 3, background: l.c, display: "inline-block", border: `0.5px solid ${C.bdr}` }} />{l.t}
                      </span>
                    ))}
                  </div>
                </div>
              )}
              <p style={{ fontSize: 14, lineHeight: 1.7, margin: 0, maxWidth: "66ch",
                color: p.scale ? C.mut : C.txt,
                ...(p.scale ? {} : { background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 10, padding: "14px 16px" }) }}>{p.reading}</p>
            </div>
          ))}

          {sect("L'utiliser dans l'application", (
            <ol style={{ margin: 0, padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 11, counterReset: "s" }}>
              {usage.map((step, i) => (
                <li key={i} style={{ position: "relative", paddingLeft: 38, fontSize: 14, lineHeight: 1.6, color: C.txt }}>
                  <span style={{ position: "absolute", left: 0, top: -1, width: 24, height: 24, borderRadius: "50%", background: C.acc + "26", color: C.acc, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontFamily: M }}>{i + 1}</span>
                  {step}
                </li>
              ))}
            </ol>
          ))}

          {p.example && sect("Exemple complet", (
            <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "18px 20px" }}>
              <div style={{ fontSize: 14, fontWeight: 500, marginBottom: 8 }}>{p.example.title}</div>
              <p style={{ fontSize: 13.5, lineHeight: 1.68, color: C.mut, margin: 0, maxWidth: "64ch" }}>{p.example.body}</p>
              {p.example.stats && (
                <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 14 }}>
                  {p.example.stats.map((s, i) => (
                    <div key={i} style={{ background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "9px 13px", minWidth: 96 }}>
                      <div style={{ fontFamily: M, fontSize: 17, color: C.txt }}>{s.v}</div>
                      <div style={{ fontSize: 10.5, color: C.dim, marginTop: 2 }}>{s.k}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}

          {p.caveat && sect("Limites & pièges", (
            <div style={{ display: "flex", gap: 10, borderLeft: `2px solid ${C.amb}`, background: C.amb + "14", borderRadius: "0 8px 8px 0", padding: "13px 16px" }}>
              <IcAlert size={15} style={{ color: C.amb, flexShrink: 0, marginTop: 2 }} />
              <p style={{ margin: 0, fontSize: 13.5, lineHeight: 1.65, color: C.txt }}>{p.caveat}</p>
            </div>
          ))}
        </article>

        {/* Rail */}
        <aside style={{ display: "flex", flexDirection: "column", gap: 16, position: "sticky", top: 66, alignSelf: "start" }} className="art-rail">
          {p.source && (
            <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, overflow: "hidden" }}>
              <div style={{ padding: "12px 15px", borderBottom: `0.5px solid ${C.bdr}`, fontSize: 12, fontWeight: 500 }}>La donnée source</div>
              <dl style={{ margin: 0, padding: "6px 15px 12px" }}>
                {[["Capteur", p.source.label], ["Producteur", p.source.provider], ["Résolution", p.source.res], ["Revisite", p.source.revisit], ["Couverture", p.source.coverage], ["Unités", p.source.units]]
                  .filter(([, v]) => v).map(([k, v]) => (
                    <div key={k} style={{ display: "flex", justifyContent: "space-between", gap: 10, padding: "7px 0", borderBottom: `0.5px solid ${C.bdr}`, fontSize: 12 }}>
                      <dt style={{ color: C.dim, flexShrink: 0 }}>{k}</dt>
                      <dd style={{ margin: 0, color: C.txt, textAlign: "right", fontFamily: M, fontSize: 11 }}>{v}</dd>
                    </div>
                  ))}
              </dl>
              {p.source.catalog && (
                <a href={p.source.catalog} target="_blank" rel="noreferrer" style={{ display: "flex", alignItems: "center", gap: 7, margin: "0 15px 14px", fontSize: 12, color: C.blu, textDecoration: "none" }}>
                  <IcExternalLink size={13} /> Fiche catalogue Earth Engine
                </a>
              )}
            </div>
          )}

          {!p.source && p.dataLine && (
            <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "12px 15px" }}>
              <div style={{ fontSize: 12, fontWeight: 500, marginBottom: 6 }}>Données mobilisées</div>
              <div style={{ fontFamily: M, fontSize: 11, color: C.mut, lineHeight: 1.6 }}>{p.dataLine}</div>
            </div>
          )}

          <a href={`/app.html?open=${encodeURIComponent(p.key)}&kind=${p.kind}`} style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8, background: C.acc, color: "#fff", borderRadius: 9, padding: 12, fontFamily: F, fontSize: 13.5, fontWeight: 500, textDecoration: "none" }}>
            <IcMap size={15} /> Ouvrir dans l'application
          </a>

          {related.length > 0 && (
            <div style={{ background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "14px 15px" }}>
              <div style={{ fontSize: 12, letterSpacing: ".1em", textTransform: "uppercase", color: C.dim, marginBottom: 11, fontWeight: 500 }}>Sur le même thème</div>
              {related.map(r => (
                <Link key={r.slug} to={`/doc/${r.slug}`} style={{ display: "block", padding: "8px 0", borderBottom: `0.5px solid ${C.bdr}`, textDecoration: "none", color: C.txt }}>
                  <div style={{ fontSize: 12.5, fontWeight: 500 }}>{r.title}</div>
                  {r.blurb && <div style={{ fontSize: 11, color: C.dim, marginTop: 1 }}>{r.blurb}</div>}
                </Link>
              ))}
            </div>
          )}
        </aside>
      </div>

      <style>{`@media (max-width: 820px){ .art-body{ grid-template-columns: 1fr !important; } .art-rail{ position: static !important; order: 2; } }`}</style>
    </DocShell>
  );
}

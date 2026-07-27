/**
 * ThematicMenu.jsx — Tiroir latéral thématique (remplace le rail d'icônes).
 *
 * Bande d'icônes (52 px) toujours visible + panneau accordéon (~270 px) qui
 * s'ouvre/se ferme. Piloté par menuTree.js. Un item « tool » ouvre le panneau
 * existant (onActivate) ; un item « indicator » ouvre l'IndicatorModal
 * (onIndicator). Desktop : panneau en flux. Mobile : superposition + fond.
 *
 * Recherche : barre en haut du panneau qui transforme l'accordéon en liste de
 * résultats (indicateurs + outils, via menuSearch) ; loupe de la bande =
 * ouvre la palette Ctrl+K globale (onOpenSearch).
 */
import { useState, useMemo } from "react";
import { MENU_TREE, INDICATORS } from "../utils/menuTree";
import { buildSearchIndex, searchMenu } from "../utils/menuSearch";
import { IcArrow, IcStack, IcUpload, IcPrint, IcChevronLeft, IcCaretRight, IcSearch, IcX } from "../icons";

const shortLabel = (item) => {
  if (item.kind === "indicator") {
    const ind = INDICATORS[item.id];
    return ind ? ind.title.split(" — ")[0].split(" (")[0] : item.id;
  }
  return item.label;   // tool | soon
};
const itemIcon = (item) => (item.kind === "indicator" ? INDICATORS[item.id]?.icon : item.icon);
// Description courte : outil = desc saisie ; indicateur = sa fiche ; « soon » = la note d'indisponibilité.
const shortDesc = (item) =>
  (item.kind === "indicator" ? INDICATORS[item.id]?.desc : (item.desc || item.note)) || "";

export default function ThematicMenu({
  C, activeTool, onActivate, onIndicator, layersCount = 0,
  openPanels, panelIds, onImport, onPrint, onOpenSearch, isMobile = false,
}) {
  const [expanded, setExpanded] = useState(!isMobile);
  const [openTheme, setOpenTheme] = useState(MENU_TREE[0]?.id || null);
  const [query, setQuery] = useState("");

  const index = useMemo(() => buildSearchIndex(), []);
  const results = useMemo(() => (query ? searchMenu(index, query) : []), [index, query]);

  const themeActive = (t) => t.items.some(it =>
    it.kind === "tool" && (activeTool === it.id || (panelIds?.has(it.id) && openPanels?.has(it.id))));

  const onItem = (it) => {
    if (it.kind === "soon") return;   // entrée grisée : donnée absente de GEE, non actionnable
    if (it.kind === "indicator") onIndicator?.(it.id);
    else onActivate?.(it.id);
    if (isMobile) setExpanded(false);
  };

  // ── Panneau accordéon (réutilisé desktop inline + mobile overlay) ──
  const Panel = (
    <div style={{ width: 258, height: "100%", background: C.card, borderRight: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "9px 12px", borderBottom: `0.5px solid ${C.bdr}` }}>
        <span style={{ fontSize: 12, fontWeight: 600, color: C.txt, flex: 1 }}>Thématiques</span>
        <button onClick={() => setExpanded(false)} title="Replier" style={{ background: "transparent", border: "none", color: C.dim, cursor: "pointer", display: "flex", alignItems: "center" }}><IcChevronLeft size={16}/></button>
      </div>

      {/* Barre de recherche */}
      <div style={{ padding: "7px 8px", borderBottom: `0.5px solid ${C.bdr}` }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "5px 8px" }}>
          <IcSearch size={13} color={C.dim} />
          <input value={query} onChange={e => setQuery(e.target.value)}
            placeholder="Rechercher un indice / outil…"
            style={{ flex: 1, minWidth: 0, background: "transparent", border: "none", outline: "none", color: C.txt, fontSize: 11.5 }} />
          {query
            ? <button onClick={() => setQuery("")} title="Effacer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex", padding: 0 }}><IcX size={13}/></button>
            : <span title="Recherche globale (Ctrl+K)" style={{ fontSize: 8.5, color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 3, padding: "0 3px", flexShrink: 0 }}>⌘K</span>}
        </div>
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: "6px 8px" }}>
        {query ? (
          /* ── Résultats de recherche (indicateurs + outils) ── */
          results.length === 0 ? (
            <div style={{ padding: "16px 8px", textAlign: "center", color: C.dim, fontSize: 11 }}>Aucun résultat pour « {query} ».</div>
          ) : results.map(r => {
            const RIcon = r.icon;
            return (
              <button key={r.kind + ":" + r.id} onClick={() => onItem({ kind: r.kind, id: r.id })} title={r.full} style={{
                width: "100%", display: "flex", alignItems: "center", gap: 9, padding: "6px 8px", borderRadius: 6, cursor: "pointer",
                background: "transparent", border: "none", color: C.txt,
              }}>
                <span style={{ width: 16, display: "flex", justifyContent: "center", flexShrink: 0, color: C.mut }}>{RIcon && <RIcon size={15} />}</span>
                <span style={{ flex: 1, minWidth: 0, textAlign: "left" }}>
                  <span style={{ display: "block", fontSize: 11.5, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.full}</span>
                  <span style={{ display: "block", fontSize: 8.5, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.sub}</span>
                </span>
                <span style={{ fontSize: 8, color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 3, padding: "0 4px", flexShrink: 0 }}>{r.kind === "tool" ? "Outil" : "Indice"}</span>
              </button>
            );
          })
        ) : (
          /* ── Accordéon thématique ── */
          MENU_TREE.map(t => {
            const open = openTheme === t.id;
            const act = themeActive(t);
            const ThemeIcon = t.icon;
            return (
              <div key={t.id}>
                <button onClick={() => setOpenTheme(open ? null : t.id)} style={{
                  width: "100%", display: "flex", alignItems: "center", gap: 9, padding: "7px 8px", borderRadius: 7, cursor: "pointer",
                  background: open ? C.acc + "12" : "transparent", border: "none", color: act ? C.acc : C.txt,
                }}>
                  <ThemeIcon size={16} color={open || act ? C.acc : C.mut} />
                  <span style={{ fontSize: 12.5, flex: 1, textAlign: "left", color: open || act ? C.acc : C.txt }}>{t.label}</span>
                  <span style={{ display: "flex", color: C.dim, transform: open ? "rotate(90deg)" : "none", transition: "transform .15s" }}><IcCaretRight size={13}/></span>
                </button>
                {open && (
                  <div style={{ margin: "1px 0 5px 17px", paddingLeft: 8, borderLeft: `0.5px solid ${C.bdr}` }}>
                    {t.items.map(it => {
                      const isAct = it.kind === "tool" && (activeTool === it.id || (panelIds?.has(it.id) && openPanels?.has(it.id)));
                      const soon = it.kind === "soon";
                      const ItemIcon = itemIcon(it);
                      return (
                        <button key={it.id} onClick={() => onItem(it)} disabled={soon}
                          title={`${shortLabel(it)}${shortDesc(it) ? " — " + shortDesc(it) : ""}`} style={{
                          width: "100%", display: "flex", alignItems: "flex-start", gap: 9, padding: "6px 8px", borderRadius: 6,
                          cursor: soon ? "not-allowed" : "pointer", opacity: soon ? 0.55 : 1,
                          background: isAct ? C.acc + "1e" : "transparent", border: "none", color: isAct ? C.acc : C.mut,
                        }}>
                          <span style={{ width: 16, display: "flex", justifyContent: "center", flexShrink: 0, marginTop: 1 }}>{ItemIcon && <ItemIcon size={14} color={isAct ? C.acc : C.mut} />}</span>
                          <span style={{ flex: 1, minWidth: 0, textAlign: "left" }}>
                            <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
                              <span style={{ flex: 1, minWidth: 0, fontSize: 11.5, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{shortLabel(it)}</span>
                              {soon && (
                                <span style={{ flexShrink: 0, fontSize: 7.5, letterSpacing: ".04em", color: C.dim,
                                               border: `0.5px solid ${C.bdr}`, borderRadius: 3, padding: "0 3px" }}>hors GEE</span>
                              )}
                            </span>
                            {/* Description sur 2 lignes maxi : assez pour situer l'indicateur
                                sans transformer le rail en pavé de texte. */}
                            {shortDesc(it) && (
                              <span style={{
                                display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical",
                                overflow: "hidden", fontSize: 9.5, lineHeight: 1.35, color: C.dim, marginTop: 1,
                              }}>{shortDesc(it)}</span>
                            )}
                          </span>
                          {it.id === "layers" && layersCount > 0 && (
                            <span style={{ background: C.acc, color: "#fff", borderRadius: 8, fontSize: 8, padding: "0 4px", fontWeight: 700, marginTop: 1 }}>{layersCount}</span>
                          )}
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );

  // icon = composant Lucide (rendu <Icon size=18/>)
  const stripBtn = (Icon, label, onClick, active, badge) => (
    <button onClick={onClick} title={label} style={{
      width: "100%", padding: "7px 0", borderRadius: 7, border: "none", cursor: "pointer",
      background: active ? C.acc + "1e" : "transparent",
      display: "flex", flexDirection: "column", alignItems: "center", gap: 1, position: "relative",
    }}>
      <Icon size={18} color={active ? C.acc : C.mut} />
      {badge > 0 && <span style={{ position: "absolute", top: 2, right: 8, background: C.acc, color: "#fff", borderRadius: 8, fontSize: 8, padding: "0 3px", fontWeight: 700 }}>{badge}</span>}
    </button>
  );

  return (
    <div style={{ display: "flex", height: "100%", flexShrink: 0, position: "relative", zIndex: 30 }}>
      {/* Bande d'icônes 52px — toujours visible */}
      <div style={{ width: 52, background: C.card, borderRight: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", alignItems: "center", padding: "5px 0", gap: 2, flexShrink: 0, overflowY: "auto" }}>
        {stripBtn(IcSearch, "Rechercher (Ctrl+K)", () => onOpenSearch?.())}
        {stripBtn(IcArrow, "Sélection", () => onActivate?.("pointer"), activeTool === "pointer")}
        <div style={{ width: "70%", height: 1, background: C.bdr, margin: "3px 0" }} />
        {MENU_TREE.map(t => stripBtn(t.icon, t.label,
          () => { setOpenTheme(t.id); setExpanded(true); }, openTheme === t.id && expanded,
          t.id === "outils" ? layersCount : 0))}
        <div style={{ width: "70%", height: 1, background: C.bdr, margin: "3px 0" }} />
        {stripBtn(IcStack, "Couches", () => onActivate?.("layers"),
          panelIds?.has("layers") && openPanels?.has("layers"), layersCount)}
        {stripBtn(IcUpload, "Importer un fichier", () => onImport?.())}
        {stripBtn(IcPrint, "Imprimer / exporter", () => onPrint?.(), activeTool === "print")}
      </div>

      {/* Panneau déployé */}
      {expanded && (isMobile ? (
        <>
          <div onClick={() => setExpanded(false)} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,.4)", zIndex: 40 }} />
          <div style={{ position: "fixed", top: 0, left: 52, height: "100%", zIndex: 41, boxShadow: "4px 0 24px rgba(0,0,0,.3)" }}>{Panel}</div>
        </>
      ) : Panel)}
    </div>
  );
}

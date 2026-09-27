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
import { IcArrow, IcStack, IcUpload, IcPrint, IcChevronLeft, IcCaretRight, IcSearch, IcX, IcPlug } from "../icons";
import { usePluginState, isVisible, setDisabled, uninstall, CORE_IDS } from "../plugins/pluginState";
import { setSpatialTarget } from "../utils/spatialNav";
import { buildSpatialSections } from "../utils/spatialSections";

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
  openPanels, panelIds, onImport, onPrint, onOpenSearch, onOpenPlugins, isMobile = false,
}) {
  const [expanded, setExpanded] = useState(!isMobile);
  const [openTheme, setOpenTheme] = useState(MENU_TREE[0]?.id || null);
  const [openFam, setOpenFam] = useState({ vecteur: true }); // familles dépliées — Vecteur par défaut
  const [openCat, setOpenCat] = useState({ "vec_Overlay": true }); // catégories dépliées — Overlay par défaut
  const [query, setQuery] = useState("");
  const spatialSections = useMemo(buildSpatialSections, []);

  const pstate = usePluginState();               // re-render quand l'état plugins change
  const [ctxMenu, setCtxMenu] = useState(null);  // clic droit : { id, label, x, y }

  const index = useMemo(() => buildSearchIndex(), []);
  const results = useMemo(
    () => (query ? searchMenu(index, query).filter(r => r.kind !== "tool" || isVisible(r.id)) : []),
    [index, query, pstate]
  );

  const themeActive = (t) => t.items.some(it =>
    it.kind === "tool" && (activeTool === it.id || (panelIds?.has(it.id) && openPanels?.has(it.id))));

  const onItem = (it) => {
    if (it.kind === "soon") return;   // entrée grisée : donnée absente de GEE, non actionnable
    if (it.kind === "indicator") onIndicator?.(it.id);
    else if (it.kind === "spatial") { setSpatialTarget(it.section, it.category, it.tool); onActivate?.("spatial"); }
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
              <button key={r.kind + ":" + r.id} onClick={() => onItem(r)} title={r.full} style={{
                width: "100%", display: "flex", alignItems: "center", gap: 9, padding: "6px 8px", borderRadius: 6, cursor: "pointer",
                background: "transparent", border: "none", color: C.txt,
              }}>
                <span style={{ width: 16, display: "flex", justifyContent: "center", flexShrink: 0, color: C.mut }}>{RIcon && <RIcon size={15} />}</span>
                <span style={{ flex: 1, minWidth: 0, textAlign: "left" }}>
                  <span style={{ display: "block", fontSize: 11.5, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.full}</span>
                  <span style={{ display: "block", fontSize: 8.5, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.sub}</span>
                </span>
                <span style={{ fontSize: 8, color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 3, padding: "0 4px", flexShrink: 0 }}>{r.kind === "tool" ? "Outil" : r.kind === "spatial" ? "Analyse" : "Indice"}</span>
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
                    {t.items.filter(it => it.kind !== "tool" || isVisible(it.id)).map(it => {
                      const isAct = it.kind === "tool" && (activeTool === it.id || (panelIds?.has(it.id) && openPanels?.has(it.id)));
                      const soon = it.kind === "soon";
                      const ItemIcon = itemIcon(it);
                      const canManage = it.kind === "tool" && !CORE_IDS.has(it.id);
                      const hasChildren = Array.isArray(it.children) && it.children.length > 0;
                      // Item « Analyse spatiale » : pas de bouton intermédiaire (évite le
                      // doublon avec le titre du thème). Rend directement familles → catégories.
                      if (hasChildren) {
                        return (
                          <div key={it.id} style={{ marginTop: 2 }}>
                            {spatialSections.map(sec => {
                              const famOpen = !!openFam[sec.id];
                              const famIcon = it.children.find(c => c.section === sec.id)?.icon;
                              return (
                                <div key={sec.id}>
                                  <button onClick={() => { setSpatialTarget(sec.id, null); onActivate?.(it.id); if (isMobile) setExpanded(false); }}
                                    style={{ width: "100%", display: "flex", alignItems: "center", gap: 7, padding: "6px 8px", borderRadius: 5, cursor: "pointer", background: "transparent", border: "none", color: C.txt, fontSize: 12, fontWeight: 600 }}>
                                    {famIcon && <famIcon size={14} color={C.acc} />}
                                    <span style={{ flex: 1, textAlign: "left" }}>{sec.label}</span>
                                    <span onClick={(e) => { e.stopPropagation(); setOpenFam(f => ({ ...f, [sec.id]: !f[sec.id] })); }}
                                      style={{ display: "flex", color: C.dim, transform: famOpen ? "rotate(90deg)" : "none", transition: "transform .15s", padding: "0 2px" }}>
                                      <IcCaretRight size={12} />
                                    </span>
                                  </button>
                                  {famOpen && sec.groups.map(g => {
                                    const catOpen = !!openCat[g.key];
                                    return (
                                      <div key={g.key}>
                                        {/* Catégorie : clic déplie/replie ses outils ; chevron idem */}
                                        <button onClick={() => setOpenCat(c => ({ ...c, [g.key]: !c[g.key] }))}
                                          title={`${g.name} — ${g.tools.length} outil${g.tools.length > 1 ? "s" : ""}`}
                                          style={{ width: "100%", display: "flex", alignItems: "center", gap: 6, padding: "4px 8px 4px 26px", borderRadius: 5, cursor: "pointer", background: "transparent", border: "none", color: C.mut, fontSize: 10.5 }}>
                                          <IcCaretRight size={10} style={{ color: C.dim, transform: catOpen ? "rotate(90deg)" : "none", transition: "transform .15s", flexShrink: 0 }} />
                                          <span style={{ flex: 1, textAlign: "left", textTransform: "uppercase", letterSpacing: ".03em", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{g.name}</span>
                                          <span style={{ fontSize: 9, color: C.dim }}>{g.tools.length}</span>
                                        </button>
                                        {/* Outils : clic ouvre le hub sur l'outil précis */}
                                        {catOpen && g.tools.map(tool => (
                                          <button key={tool.id} onClick={() => { setSpatialTarget(sec.id, g.key, tool.id); onActivate?.(it.id); if (isMobile) setExpanded(false); }}
                                            title={tool.desc}
                                            style={{ width: "100%", display: "flex", alignItems: "center", gap: 6, padding: "3px 8px 3px 42px", borderRadius: 5, cursor: "pointer", background: "transparent", border: "none", color: C.mut, fontSize: 10.5 }}>
                                            <span style={{ flex: 1, textAlign: "left", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{tool.name}</span>
                                            {tool.implemented === false && <span style={{ fontSize: 7.5, color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 3, padding: "0 3px", flexShrink: 0 }}>bientôt</span>}
                                          </button>
                                        ))}
                                      </div>
                                    );
                                  })}
                                </div>
                              );
                            })}
                          </div>
                        );
                      }
                      return (
                        <div key={it.id}>
                        <button onClick={() => onItem(it)} disabled={soon}
                          onContextMenu={canManage ? (e) => { e.preventDefault(); setCtxMenu({ id: it.id, label: shortLabel(it), x: e.clientX, y: e.clientY }); } : undefined}
                          title={`${shortLabel(it)}${shortDesc(it) ? " — " + shortDesc(it) : ""}${canManage ? " · clic droit pour gérer" : ""}`} style={{
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
                        </div>
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
        <div style={{ width: "70%", height: 1, background: C.bdr, margin: "3px 0" }} />
        {stripBtn(IcPlug, "Plugins — installer / gérer", () => onOpenPlugins?.())}
      </div>

      {/* Panneau déployé */}
      {expanded && (isMobile ? (
        <>
          <div onClick={() => setExpanded(false)} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,.4)", zIndex: 40 }} />
          <div style={{ position: "fixed", top: 0, left: 52, height: "100%", zIndex: 41, boxShadow: "4px 0 24px rgba(0,0,0,.3)" }}>{Panel}</div>
        </>
      ) : Panel)}

      {ctxMenu && (
        <>
          <div onClick={() => setCtxMenu(null)} onContextMenu={(e) => { e.preventDefault(); setCtxMenu(null); }}
               style={{ position: "fixed", inset: 0, zIndex: 60 }} />
          <div style={{
            position: "fixed",
            left: Math.min(ctxMenu.x, (typeof window !== "undefined" ? window.innerWidth : 1200) - 196),
            top:  Math.min(ctxMenu.y, (typeof window !== "undefined" ? window.innerHeight : 800) - 96),
            zIndex: 61, minWidth: 180, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 8,
            boxShadow: "0 8px 24px rgba(0,0,0,.35)", overflow: "hidden",
          }}>
            <div style={{ padding: "6px 11px", fontSize: 9.5, letterSpacing: ".05em", textTransform: "uppercase", color: C.dim, borderBottom: `0.5px solid ${C.bdr}`, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{ctxMenu.label}</div>
            <button onClick={() => { setDisabled(ctxMenu.id, true); if (activeTool === ctxMenu.id) onActivate?.("pointer"); setCtxMenu(null); }}
                    style={{ width: "100%", textAlign: "left", padding: "8px 11px", background: "transparent", border: "none", color: C.txt, cursor: "pointer", fontSize: 12 }}>
              Désactiver le plugin
            </button>
            <button onClick={() => { uninstall(ctxMenu.id); if (activeTool === ctxMenu.id) onActivate?.("pointer"); setCtxMenu(null); }}
                    style={{ width: "100%", textAlign: "left", padding: "8px 11px", background: "transparent", border: "none", color: "#f0a8a8", cursor: "pointer", fontSize: 12 }}>
              Désinstaller
            </button>
          </div>
        </>
      )}
    </div>
  );
}

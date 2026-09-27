/**
 * selectionControl.js — Contrôle natif de sélection d'entités (IControl).
 * Ajouté à la carte via map.addControl() comme les outils de navigation.
 * Compatible Mapbox GL & MapLibre GL (même interface IControl / classes CSS).
 *
 * L'état (mode actif, compteur) et les actions vivent côté React : le contrôle
 * les lit via getState() et les déclenche via actions(), passés à la fabrique.
 */
const svg = (paths) => `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${paths}</svg>`;
const ICONS = {
  click:   svg('<path d="M3 3l7.5 18 2.6-7.9L21 10.5z"/>'),
  rect:    svg('<rect x="3" y="3" width="18" height="18" rx="2"/>'),
  polygon: svg('<path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>'),
  apply:   svg('<path d="M20 6L9 17l-5-5"/>'),
  table:   svg('<rect x="3" y="3" width="18" height="18" rx="1"/><path d="M3 9h18M3 15h18M9 3v18M15 3v18"/>'),
  clear:   svg('<path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6"/>'),
  finish:  svg('<path d="M18 6L6 18M6 6l12 12"/>'),
};

export function createSelectionControl({ actions, getState, colors }) {
  const C = colors;
  let container = null, countEl = null;
  const btns = {};

  const mkBtn = (id, title, onClick, color) => {
    const b = document.createElement("button");
    b.type = "button";
    b.title = title;
    b.setAttribute("aria-label", title);
    b.innerHTML = ICONS[id];
    if (color) b.style.color = color;
    b.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); onClick(); });
    btns[id] = b;
    return b;
  };

  return {
    onAdd() {
      container = document.createElement("div");
      container.className = "maplibregl-ctrl maplibregl-ctrl-group mapboxgl-ctrl mapboxgl-ctrl-group";

      countEl = document.createElement("div");
      countEl.style.cssText = `text-align:center;font-size:11px;font-weight:700;padding:3px 0;color:${C.acc};font-family:monospace;`;
      container.appendChild(countEl);

      container.appendChild(mkBtn("click", "Sélection par clic", () => actions().setMode("click")));
      container.appendChild(mkBtn("rect", "Sélection par rectangle", () => actions().setMode("rect")));
      container.appendChild(mkBtn("polygon", "Sélection par polygone", () => actions().setMode("polygon")));
      container.appendChild(mkBtn("apply", "Fermer le polygone", () => actions().applyPoly()));

      const sep = document.createElement("div");
      sep.style.cssText = `height:1px;background:${C.bdr};margin:2px 4px;`;
      container.appendChild(sep);

      container.appendChild(mkBtn("table", "Table de la sélection", () => actions().openTable()));
      container.appendChild(mkBtn("clear", "Effacer la sélection", () => actions().clear(), C.red));
      container.appendChild(mkBtn("finish", "Terminer", () => actions().finish()));

      this.updateUI();
      return container;
    },
    onRemove() {
      if (container && container.parentNode) container.parentNode.removeChild(container);
      container = null;
    },
    updateUI() {
      if (!container) return;
      const st = getState();
      ["click", "rect", "polygon"].forEach(m => {
        const on = st.mode === m;
        btns[m].style.background = on ? C.acc + "22" : "";
        btns[m].style.color = on ? C.acc : "";
      });
      if (countEl) countEl.textContent = st.count;
      btns.apply.style.display = (st.mode === "polygon" && st.polyLen >= 3) ? "" : "none";
    },
  };
}

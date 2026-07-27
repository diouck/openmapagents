/**
 * DocShell.jsx — Ossature commune des pages de documentation.
 * En-tête (logo → /doc, retour carte, bascule thème) + conteneur centré.
 * Le thème est créé par la page appelante (useTheme) et transmis en props :
 * la doc partage le localStorage « ome-theme » avec l'app, sans y toucher.
 */
import { Link } from "react-router-dom";
import { F } from "../../config";
import { IcMap, IcSun, IcMoon } from "../../icons";

function GlobeGlyph() {
  return (
    <svg width="15" height="15" viewBox="0 0 26 26" fill="none" aria-hidden="true">
      <circle cx="13" cy="13" r="8" stroke="#fff" strokeWidth="1.4" opacity=".95" />
      <ellipse cx="13" cy="13" rx="3.5" ry="8" stroke="#fff" strokeWidth="1.1" opacity=".7" />
      <line x1="5" y1="13" x2="21" y2="13" stroke="#fff" strokeWidth="1.1" opacity=".7" />
    </svg>
  );
}

export default function DocShell({ C, themeName, onToggleTheme, children, maxWidth = 1080 }) {
  const btn = {
    display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6,
    fontFamily: F, fontSize: 12.5, color: C.mut, textDecoration: "none",
    padding: "6px 11px", borderRadius: 8, border: `0.5px solid ${C.bdr}`,
    background: "transparent", cursor: "pointer",
  };
  return (
    <div style={{ minHeight: "100vh", background: C.bg, color: C.txt, fontFamily: F }}>
      <header style={{
        position: "sticky", top: 0, zIndex: 10, display: "flex", alignItems: "center", gap: 12,
        height: 52, padding: "0 18px", background: C.card, borderBottom: `0.5px solid ${C.bdr}`,
      }}>
        <Link to="/doc" style={{ display: "flex", alignItems: "center", gap: 9, textDecoration: "none", color: C.txt }}>
          <span style={{ width: 26, height: 26, borderRadius: 7, background: C.acc, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
            <GlobeGlyph />
          </span>
          <span style={{ lineHeight: 1 }}>
            <span style={{ fontSize: 13.5, fontWeight: 600 }}>OpenMapAgents</span>
            <span style={{ display: "block", fontSize: 9.5, color: C.dim, marginTop: 2, letterSpacing: ".04em" }}>Documentation</span>
          </span>
        </Link>
        <div style={{ flex: 1 }} />
        <a href="/app.html" style={btn}><IcMap size={14} /> Retour à la carte</a>
        <button onClick={onToggleTheme} aria-label="Basculer le thème" style={{ ...btn, width: 32, height: 32, padding: 0 }}>
          {themeName === "dark" ? <IcSun size={15} /> : <IcMoon size={15} />}
        </button>
      </header>
      <main style={{ maxWidth, margin: "0 auto", padding: "0 18px 64px" }}>
        {children}
      </main>
    </div>
  );
}

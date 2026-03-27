import { useState, useEffect, useCallback } from "react";

export const THEMES = {
  dark: {
    bg: "#0c0e12", card: "#13161c", hover: "#1a1e26", input: "#181c24",
    bdr: "rgba(255,255,255,0.06)", txt: "#e8e6e1", mut: "#8a8880", dim: "#5a5850",
    acc: "#1D9E75", amb: "#EF9F27", red: "#E24B4A", blu: "#378ADD", pnk: "#D4537E",
  },
  light: {
    bg: "#f5f5f0", card: "#ffffff", hover: "#f0efe8", input: "#f7f7f3",
    bdr: "rgba(0,0,0,0.08)", txt: "#1a1a18", mut: "#6b6b63", dim: "#9a9a90",
    acc: "#0F6E56", amb: "#BA7517", red: "#A32D2D", blu: "#185FA5", pnk: "#993556",
  },
};

// Global mutable ref for components outside React tree
let _current = null;
export function getTheme() {
  return _current || THEMES.dark;
}

export function useTheme() {
  const [name, setName] = useState(() => {
    try { return localStorage.getItem("ome-theme") || "dark"; } catch { return "dark"; }
  });
  const C = THEMES[name] || THEMES.dark;
  _current = C;

  const toggle = useCallback(() => {
    const next = name === "dark" ? "light" : "dark";
    setName(next);
    try { localStorage.setItem("ome-theme", next); } catch {}
  }, [name]);

  useEffect(() => { _current = C; }, [C]);
  return { name, C, toggle };
}

import { useThemeContext } from "../theme";
import { F } from "../config";

export const Badge = ({ children, color }) => {
  const C = useThemeContext();
  const cl = color || C.acc;
  return <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 5, background: cl + "20", color: cl, fontWeight: 500 }}>{children}</span>;
};

export const Spinner = () => {
  const C = useThemeContext();
  return <span style={{ display: "inline-block", width: 14, height: 14, border: `2px solid ${C.bdr}`, borderTop: `2px solid ${C.acc}`, borderRadius: "50%", animation: "spin .8s linear infinite" }} />;
};

export const Sel = ({ value, onChange, options, style: s = {} }) => {
  const C = useThemeContext();
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ fontFamily: F, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", cursor: "pointer", width: "100%", ...s }}>
      {options.map(o => <option key={typeof o === "string" ? o : o.value} value={typeof o === "string" ? o : o.value}>
        {typeof o === "string" ? o : o.label}
      </option>)}
    </select>
  );
};

export const Lbl = ({ children }) => {
  const C = useThemeContext();
  return <div style={{ fontSize: 10, color: C.dim, marginBottom: 3, textTransform: "uppercase", letterSpacing: ".05em" }}>{children}</div>;
};

export const Btn = ({ children, onClick, active, small, color, style: s = {} }) => {
  const C = useThemeContext();
  const cl = color || C.acc;
  return (
    <button onClick={onClick} style={{
      fontFamily: F, fontSize: small ? 10 : 11, fontWeight: 500,
      padding: small ? "3px 8px" : "5px 12px", borderRadius: small ? 4 : 6,
      background: active ? cl + "20" : "transparent",
      color: active ? cl : C.dim,
      border: `0.5px solid ${active ? cl + "55" : C.bdr}`,
      cursor: "pointer", ...s,
    }}>{children}</button>
  );
};

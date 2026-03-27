import { getTheme } from "../theme";
import { M } from "../config";

export default function Legend({ layers }) {
  const C = getTheme();
  const visible = layers.filter(l => l.visible);
  if (!visible.length) return null;

  return (
    <div style={{
      position: "absolute", bottom: 30, left: 10, zIndex: 10, maxWidth: 240,
      background: C.card + "ee", borderRadius: 8, border: `0.5px solid ${C.bdr}`,
      backdropFilter: "blur(8px)", padding: 10, maxHeight: "40vh", overflowY: "auto",
    }}>
      {visible.map(layer => {
        const cr = layer.classResult;
        return (
          <div key={layer.id} style={{ marginBottom: 8 }}>
            {/* Layer name + color swatch (always shown) */}
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: cr ? 4 : 0 }}>
              <div style={{ width: 12, height: 12, borderRadius: 3, background: layer.color, flexShrink: 0 }} />
              <span style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>{layer.name}</span>
              <span style={{ fontSize: 9, color: C.dim, fontFamily: M, marginLeft: "auto" }}>{layer.featureCount}</span>
            </div>

            {/* Classification legend entries */}
            {cr?.type === "categorized" && cr.entries?.map(e => (
              <div key={e.value} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, padding: "1px 0 1px 18px" }}>
                <div style={{ width: 10, height: 10, borderRadius: 2, background: e.color, flexShrink: 0 }} />
                <span style={{ color: C.mut, flex: 1 }}>{e.value}</span>
                <span style={{ color: C.dim, fontFamily: M }}>{e.count}</span>
              </div>
            ))}
            {cr?.type === "graduated" && cr.classes?.map((c, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, padding: "1px 0 1px 18px" }}>
                <div style={{ width: 10, height: 10, borderRadius: 2, background: c.color, flexShrink: 0 }} />
                <span style={{ color: C.mut, flex: 1 }}>{c.min.toFixed(1)} – {c.max.toFixed(1)}</span>
                <span style={{ color: C.dim, fontFamily: M }}>{c.count}</span>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

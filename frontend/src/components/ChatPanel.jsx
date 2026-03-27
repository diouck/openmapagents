import { useState, useEffect, useCallback, useRef } from "react";
import { getTheme } from "../theme";
import { API, F } from "../config";
import { Badge, Spinner } from "./ui";

import VoiceInput from "./VoiceInput";

// ─── Lightweight Markdown renderer ───────────────────────────
function MarkdownText({ text, color }) {
  if (!text) return null;
  const lines = text.split("\n");
  const elements = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    // Numbered list: "1. **Title** - desc"
    const numMatch = trimmed.match(/^(\d+)\.\s+(.+)/);
    // Bullet: "- text" or "  - text"
    const bulletMatch = trimmed.match(/^[-•]\s+(.+)/);

    if (numMatch) {
      elements.push(
        <div key={i} style={{ display: "flex", gap: 6, padding: "3px 0", alignItems: "flex-start" }}>
          <span style={{ color: "var(--md-num, #1D9E75)", fontWeight: 500, fontSize: 12, minWidth: 18, textAlign: "right", flexShrink: 0 }}>{numMatch[1]}.</span>
          <span style={{ fontSize: 12, lineHeight: 1.5 }}>{renderInline(numMatch[2], color)}</span>
        </div>
      );
    } else if (bulletMatch) {
      elements.push(
        <div key={i} style={{ display: "flex", gap: 6, padding: "1px 0 1px 20px", alignItems: "flex-start" }}>
          <span style={{ color: "var(--md-dim, #888)", fontSize: 8, marginTop: 4 }}>●</span>
          <span style={{ fontSize: 11, lineHeight: 1.5, color: "var(--md-sub, #999)" }}>{renderInline(bulletMatch[1], color)}</span>
        </div>
      );
    } else if (trimmed === "") {
      elements.push(<div key={i} style={{ height: 6 }} />);
    } else {
      elements.push(
        <div key={i} style={{ fontSize: 13, lineHeight: 1.6, padding: "1px 0" }}>{renderInline(trimmed, color)}</div>
      );
    }
    i++;
  }

  return <div style={{ color }}>{elements}</div>;
}

function renderInline(text, color) {
  // Parse **bold**, *italic*, `code`
  const parts = [];
  let remaining = text;
  let key = 0;

  while (remaining.length > 0) {
    // **bold**
    const boldMatch = remaining.match(/^(.*?)\*\*(.+?)\*\*(.*)/s);
    // `code`
    const codeMatch = remaining.match(/^(.*?)`(.+?)`(.*)/s);

    if (boldMatch && (!codeMatch || boldMatch.index <= codeMatch.index)) {
      if (boldMatch[1]) parts.push(<span key={key++}>{boldMatch[1]}</span>);
      parts.push(<span key={key++} style={{ fontWeight: 500, color: color || "inherit" }}>{boldMatch[2]}</span>);
      remaining = boldMatch[3];
    } else if (codeMatch) {
      if (codeMatch[1]) parts.push(<span key={key++}>{codeMatch[1]}</span>);
      parts.push(<span key={key++} style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, padding: "1px 4px", borderRadius: 3, background: "rgba(0,0,0,0.08)" }}>{codeMatch[2]}</span>);
      remaining = codeMatch[3];
    } else {
      parts.push(<span key={key++}>{remaining}</span>);
      break;
    }
  }
  return parts;
}

export default function ChatPanel({ onToolResult, mapContext, onSendRef }) {
  const C = getTheme();
  const [msgs, setMsgs] = useState(() => {
    try { const saved = localStorage.getItem("ome-chat"); return saved ? JSON.parse(saved) : []; } catch { return []; }
  });
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [config, setConfig] = useState(null);
  const ref = useRef(null);

  useEffect(() => { fetch(`${API}/config`).then(r => r.json()).then(setConfig).catch(() => {}); }, []);
  useEffect(() => { ref.current?.scrollTo({ top: ref.current.scrollHeight, behavior: "smooth" }); }, [msgs]);
  // Persist chat history
  useEffect(() => { try { localStorage.setItem("ome-chat", JSON.stringify(msgs.slice(-50))); } catch {} }, [msgs]);

  const send = useCallback(async () => {
    if (!input.trim() || loading) return;
    const userMsg = { role: "user", content: input.trim() };
    const newMsgs = [...msgs, userMsg];
    setMsgs(newMsgs);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch(`${API}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: newMsgs.map(m => ({ role: m.role, content: m.content })),
          map_context: mapContext,
        }),
      });
      const data = await res.json();

      if (data.tool_results) {
        data.tool_results.forEach((tr, i) => {
          const tc = data.tool_calls?.[i];
          if (tr.type === "FeatureCollection") onToolResult({ type: "add_layer", data: tr, tool: tc });
          else if (tr.action === "fly_to") onToolResult({ type: "fly_to", ...tr });
          else if (tr.action === "set_layer_style") onToolResult({ type: "set_style", ...tr });
          else if (tr.action === "remove_layer") onToolResult({ type: "remove_layer", ...tr });
          else if (tr.action === "spatial_analysis") onToolResult({ type: "spatial_analysis", ...tr });
          else if (tr.action === "compute_route") onToolResult({ type: "compute_route", ...tr });
          else if (tr.action === "compute_isochrone") onToolResult({ type: "compute_isochrone", ...tr });
        });
      }

      setMsgs(prev => [...prev, {
        role: "assistant",
        content: data.text || "Fait.",
        tools: data.tool_calls || [],
        fc: data.tool_results?.reduce((s, r) => s + (r.metadata?.total || 0), 0) || 0,
      }]);
    } catch (e) {
      setMsgs(prev => [...prev, { role: "assistant", content: `Erreur: ${e.message}` }]);
    }
    setLoading(false);
  }, [input, msgs, loading, mapContext, onToolResult]);

  // Expose a way for voice commands to inject messages
  const sendExternal = useCallback((text) => {
    if (!text?.trim() || loading) return;
    setInput(text.trim());
    // Use setTimeout to let state update, then trigger send
    setTimeout(() => {
      const userMsg = { role: "user", content: text.trim() };
      setMsgs(prev => {
        const nm = [...prev, userMsg];
        // Fire the API call
        (async () => {
          setLoading(true);
          try {
            const res = await fetch(`${API}/chat`, {
              method: "POST", headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ messages: nm.map(m => ({ role: m.role, content: m.content })), map_context: mapContext }),
            });
            const data = await res.json();
            if (data.tool_results) {
              data.tool_results.forEach((tr, i) => {
                const tc = data.tool_calls?.[i];
                if (tr.type === "FeatureCollection") onToolResult({ type: "add_layer", data: tr, tool: tc });
                else if (tr.action === "fly_to") onToolResult({ type: "fly_to", ...tr });
                else if (tr.action === "set_layer_style") onToolResult({ type: "set_style", ...tr });
                else if (tr.action === "remove_layer") onToolResult({ type: "remove_layer", ...tr });
                else if (tr.action === "spatial_analysis") onToolResult({ type: "spatial_analysis", ...tr });
          else if (tr.action === "compute_route") onToolResult({ type: "compute_route", ...tr });
          else if (tr.action === "compute_isochrone") onToolResult({ type: "compute_isochrone", ...tr });
              });
            }
            setMsgs(p => [...p, { role: "assistant", content: data.text || "Fait.", tools: data.tool_calls || [], fc: data.tool_results?.reduce((s, r) => s + (r.metadata?.total || 0), 0) || 0 }]);
          } catch (e) {
            setMsgs(p => [...p, { role: "assistant", content: `Erreur: ${e.message}` }]);
          }
          setLoading(false);
          setInput("");
        })();
        return nm;
      });
    }, 50);
  }, [loading, mapContext, onToolResult]);

  // Register sendExternal for voice input
  useEffect(() => {
    if (onSendRef) onSendRef.current = sendExternal;
  }, [sendExternal, onSendRef]);

  const suggestions = [
    "Restaurants a Nantes",
    "Batiments autour du Chateau des Ducs",
    "Reseau routier de Dakar",
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", background: C.card, borderLeft: `0.5px solid ${C.bdr}` }}>
      {/* Header */}
      <div style={{ padding: "10px 14px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ fontSize: 13, fontWeight: 500 }}>Assistant carto</div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {msgs.length > 0 && <button onClick={() => { setMsgs([]); try { localStorage.removeItem("ome-chat"); } catch {} }}
            style={{ fontFamily: F, fontSize: 9, padding: "2px 6px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>Effacer</button>}
          {config && <Badge color={C.acc}>{config.llm_provider}</Badge>}
        </div>
      </div>

      {/* Messages */}
      <div ref={ref} style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>
        {msgs.length === 0 && (
          <div style={{ textAlign: "center", padding: "30px 16px", color: C.dim, fontSize: 13 }}>
            <div style={{ marginBottom: 8, color: C.mut }}>Carte vide — demandez :</div>
            {suggestions.map(q => (
              <button key={q} onClick={() => setInput(q)} style={{
                fontFamily: F, fontSize: 12, padding: "8px 12px", borderRadius: 8,
                background: C.hover, border: `0.5px solid ${C.bdr}`, color: C.mut,
                cursor: "pointer", textAlign: "left", display: "block", width: "100%", marginTop: 6,
              }}>{q}</button>
            ))}
          </div>
        )}

        {msgs.map((m, i) => (
          <div key={i} style={{
            alignSelf: m.role === "user" ? "flex-end" : "flex-start",
            maxWidth: "88%", padding: "8px 12px", borderRadius: 10,
            background: m.role === "user" ? C.acc + "18" : C.hover,
            border: `0.5px solid ${m.role === "user" ? C.acc + "33" : C.bdr}`,
          }}>
            <MarkdownText text={m.content} color={C.txt} />
            {m.tools?.length > 0 && (
              <div style={{ marginTop: 6, display: "flex", gap: 4, flexWrap: "wrap" }}>
                {m.tools.map((t, j) => <Badge key={j} color={C.blu}>{t.name}</Badge>)}
                {m.fc > 0 && <Badge color={C.amb}>{m.fc} features</Badge>}
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div style={{
            alignSelf: "flex-start", padding: "8px 14px", borderRadius: 10,
            background: C.hover, border: `0.5px solid ${C.bdr}`,
            display: "flex", alignItems: "center", gap: 8,
          }}>
            <Spinner /><span style={{ fontSize: 12, color: C.dim }}>Recherche...</span>
          </div>
        )}
      </div>

      {/* Input */}
      <div style={{ padding: "10px 12px", borderTop: `0.5px solid ${C.bdr}`, display: "flex", gap: 6, alignItems: "center" }}>
        <input value={input} onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === "Enter" && !e.shiftKey && send()}
          placeholder="Affiche les restaurants a Nantes..."
          style={{
            flex: 1, fontFamily: F, fontSize: 13, padding: "8px 12px", borderRadius: 8,
            background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none",
          }} />
        <VoiceInput onResult={(text) => { setInput(text); setTimeout(() => { sendExternal(text); }, 100); }} />
        <button onClick={send} disabled={loading || !input.trim()} style={{
          fontFamily: F, fontSize: 12, fontWeight: 500, padding: "8px 14px", borderRadius: 8,
          background: input.trim() ? C.acc : C.hover, color: input.trim() ? "#fff" : C.dim,
          border: "none", cursor: input.trim() ? "pointer" : "default",
        }}>Envoyer</button>
      </div>
    </div>
  );
}

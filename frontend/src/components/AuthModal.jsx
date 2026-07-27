/**
 * AuthModal.jsx — Modal connexion / inscription
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";
import { IcX } from "../icons";
import { useAuth } from "../useAuth";

export default function AuthModal({ onClose }) {
  const C = useThemeContext();
  const { login, register } = useAuth();
  const [mode,  setMode]  = useState("login"); // "login" | "register"
  const [email, setEmail] = useState("");
  const [user,  setUser]  = useState("");
  const [pwd,   setPwd]   = useState("");
  const [err,   setErr]   = useState("");
  const [busy,  setBusy]  = useState(false);

  const submit = async () => {
    setErr(""); setBusy(true);
    try {
      if (mode === "login") {
        await login(email, pwd);
      } else {
        if (user.length < 3) { setErr("Nom d'utilisateur trop court"); setBusy(false); return; }
        await register(email, user, pwd);
      }
      onClose();
    } catch (e) {
      setErr(e.message);
    } finally {
      setBusy(false);
    }
  };

  const inp = {
    fontFamily: F, fontSize: 12, padding: "9px 12px",
    borderRadius: 7, width: "100%", boxSizing: "border-box",
    background: C.input, color: C.txt,
    border: `1px solid ${C.bdr}`, outline: "none",
  };

  return (
    <>
      <div onClick={onClose} style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.55)",
        zIndex: 11000, backdropFilter: "blur(4px)",
      }} />
      <div style={{
        position: "fixed", top: "50%", left: "50%",
        transform: "translate(-50%,-50%)",
        zIndex: 11001, background: C.card,
        border: `1px solid ${C.bdr}`, borderRadius: 14,
        boxShadow: "0 24px 64px rgba(0,0,0,0.4)",
        width: 360, padding: "28px 24px",
        fontFamily: F,
      }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
          <div>
            <div style={{ fontSize: 16, fontWeight: 700, color: C.txt }}>
              {mode === "login" ? "Connexion" : "Créer un compte"}
            </div>
            <div style={{ fontSize: 10, color: C.dim, marginTop: 2 }}>OpenMapAgents</div>
          </div>
          <button onClick={onClose} style={{
            background: "none", border: "none", color: C.dim,
            cursor: "pointer", display: "flex", padding: 2,
          }}><IcX size={18}/></button>
        </div>

        {/* Tabs */}
        <div style={{ display: "flex", gap: 4, marginBottom: 20,
                      background: C.bg, borderRadius: 8, padding: 3 }}>
          {[["login","Connexion"],["register","Inscription"]].map(([k, label]) => (
            <button key={k} onClick={() => { setMode(k); setErr(""); }}
              style={{
                flex: 1, fontFamily: F, fontSize: 11, fontWeight: 500,
                padding: "6px 0", borderRadius: 6, cursor: "pointer",
                background: mode === k ? C.card : "transparent",
                color: mode === k ? C.txt : C.dim,
                border: mode === k ? `1px solid ${C.bdr}` : "1px solid transparent",
                boxShadow: mode === k ? "0 1px 4px rgba(0,0,0,0.1)" : "none",
              }}>
              {label}
            </button>
          ))}
        </div>

        {/* Form */}
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <input style={inp} type="email" placeholder="Email"
            value={email} onChange={e => setEmail(e.target.value)}
            onKeyDown={e => e.key === "Enter" && submit()} />

          {mode === "register" && (
            <input style={inp} type="text" placeholder="Nom d'utilisateur"
              value={user} onChange={e => setUser(e.target.value)}
              onKeyDown={e => e.key === "Enter" && submit()} />
          )}

          <input style={inp} type="password" placeholder="Mot de passe (min 8 caractères)"
            value={pwd} onChange={e => setPwd(e.target.value)}
            onKeyDown={e => e.key === "Enter" && submit()} />

          {err && (
            <div style={{ fontSize: 11, color: "#e05", padding: "7px 10px",
                          background: "#e0550011", borderRadius: 6, border: "1px solid #e0550033" }}>
              {err}
            </div>
          )}

          <button onClick={submit} disabled={busy} style={{
            fontFamily: F, fontSize: 12, fontWeight: 600,
            padding: "10px", borderRadius: 8, cursor: busy ? "wait" : "pointer",
            background: busy ? C.dim : C.acc, color: "#fff", border: "none",
            marginTop: 4,
          }}>
            {busy ? "..." : mode === "login" ? "Se connecter" : "Créer le compte"}
          </button>
        </div>

        <div style={{ fontSize: 10, color: C.dim, textAlign: "center", marginTop: 14 }}>
          {mode === "login"
            ? <>Pas de compte ?{" "}
                <span style={{ color: C.acc, cursor: "pointer" }} onClick={() => setMode("register")}>
                  Inscription
                </span>
              </>
            : <>Déjà inscrit ?{" "}
                <span style={{ color: C.acc, cursor: "pointer" }} onClick={() => setMode("login")}>
                  Connexion
                </span>
              </>
          }
        </div>
      </div>
    </>
  );
}

/**
 * useAuth.js
 * Hook + Context pour l'authentification JWT.
 *
 * Stockage : localStorage
 *   oma_access   — access token (1h)
 *   oma_refresh  — refresh token (7j)
 *
 * Utilisation :
 *   <AuthProvider>…</AuthProvider>
 *   const { user, token, login, logout, loading } = useAuth();
 */
import {
  useState, useEffect, useCallback,
  createContext, useContext,
} from "react";
import { API } from "./config";

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user,    setUser]    = useState(null);
  const [token,   setToken]   = useState(() => localStorage.getItem("oma_access") ?? "");
  const [loading, setLoading] = useState(true);

  // ── Au montage : vérifier le token stocké ───────────────
  useEffect(() => {
    const access = localStorage.getItem("oma_access");
    if (!access) { setLoading(false); return; }

    fetch(`${API}/auth/me`, {
      headers: { Authorization: `Bearer ${access}` },
    })
      .then(async r => {
        if (r.ok) {
          const u = await r.json();
          setUser(u);
          setToken(access);
        } else {
          // Tenter le refresh
          await _doRefresh();
        }
      })
      .catch(() => _clear())
      .finally(() => setLoading(false));
  }, []);                            // eslint-disable-line react-hooks/exhaustive-deps

  // ── Refresh token ────────────────────────────────────────
  async function _doRefresh() {
    const refresh = localStorage.getItem("oma_refresh");
    if (!refresh) { _clear(); return; }

    try {
      const r = await fetch(`${API}/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: refresh }),
      });
      if (!r.ok) throw new Error();
      const data = await r.json();
      _store(data);

      // Re-charger le profil
      const me = await fetch(`${API}/auth/me`, {
        headers: { Authorization: `Bearer ${data.access_token}` },
      });
      if (me.ok) setUser(await me.json());
    } catch {
      _clear();
    }
  }

  function _store({ access_token, refresh_token }) {
    localStorage.setItem("oma_access",  access_token);
    localStorage.setItem("oma_refresh", refresh_token);
    setToken(access_token);
  }

  function _clear() {
    localStorage.removeItem("oma_access");
    localStorage.removeItem("oma_refresh");
    setToken("");
    setUser(null);
  }

  // ── API publique ─────────────────────────────────────────
  const login = useCallback(async (email, password) => {
    const r = await fetch(`${API}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      throw new Error(e.detail ?? "Connexion échouée");
    }
    const data = await r.json();
    _store(data);
    setUser(data.user);
    return data.user;
  }, []);

  const register = useCallback(async (email, username, password) => {
    const r = await fetch(`${API}/auth/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, username, password }),
    });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      throw new Error(e.detail ?? "Inscription échouée");
    }
    const data = await r.json();
    _store(data);
    setUser(data.user);
    return data.user;
  }, []);

  const logout = useCallback(() => _clear(), []);

  const updateProfile = useCallback(async (patch) => {
    const access = localStorage.getItem("oma_access");
    const r = await fetch(`${API}/auth/me`, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        Authorization:  `Bearer ${access}`,
      },
      body: JSON.stringify(patch),
    });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      throw new Error(e.detail ?? "Mise à jour échouée");
    }
    const updated = await r.json();
    setUser(updated);
    return updated;
  }, []);

  return (
    <AuthContext.Provider value={{
      user, token, loading,
      login, register, logout, updateProfile,
      isAuth: !!user,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside <AuthProvider>");
  return ctx;
}
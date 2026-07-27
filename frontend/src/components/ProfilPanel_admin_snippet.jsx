// ── À ajouter dans ProfilPanel.jsx ───────────────────────────
// 1. Import en haut
import AdminPanel from "./AdminPanel";

// 2. Dans le composant ProfilPanel, ajouter l'onglet admin si is_admin
// Exemple dans la liste des onglets :
const tabs = [
  { id: "profil",  label: "👤 Profil" },
  { id: "cartes",  label: "🗺️ Mes cartes" },
  ...(user?.is_admin ? [{ id: "admin", label: "⚙️ Admin" }] : []),
];

// 3. Dans le rendu conditionnel des onglets :
{activeTab === "admin" && user?.is_admin && (
  <AdminPanel />
)}

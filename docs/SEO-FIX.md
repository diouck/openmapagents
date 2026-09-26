# Correction SEO — OpenMapAgents

**Date** : 26 septembre 2026  
**Sujet** : Résoudre 54 pages « détectée, non indexée » et soft-404 diagnostiqués via Google Search Console

---

## Diagnostic

### Cause racine
Le serveur **renvoie HTTP 200 avec le contenu de l'accueil** pour toute URL inconnue (fallback SPA `try_files $uri /index.html` en nginx). Google voit donc :
- `/robots.txt` → HTML de 102 Ko (accueil)
- `/sitemap.xml` → HTML de 102 Ko (accueil)
- `/doc` → HTML de 102 Ko (accueil)
- `/nimporte-quoi` → HTML de 102 Ko (accueil)

**Conséquence** : Doublons de contenu + soft-404 → Google refuse d'indexer (**54 pages « non indexées »**).

### Autres problèmes détectés
| Problème | Pages | Impact |
|----------|-------|--------|
| Pas de `robots.txt` réel | 3 | Bloquées par robots.txt (en attente) |
| Pas de `canonical` | — | Doublons canoniques (4 pages) |
| Pas de `sitemap.xml` réel | — | Google ne sait pas quelles pages indexer |
| `/doc` = doublon de `/` | — | Même contenu, deux URLs |

---

## Corrections appliquées

### 1. Fichiers statiques SEO créés

#### `frontend/public/robots.txt`
```
User-agent: *
Allow: /
Disallow: /api/
Sitemap: https://openmapagents.geoafrica.fr/sitemap.xml
```
✅ Vite le copiera dans `dist/` au build.

#### `frontend/public/sitemap.xml`
```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://openmapagents.geoafrica.fr/</loc>
    <lastmod>2026-09-26</lastmod>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://openmapagents.geoafrica.fr/doc</loc>
    <lastmod>2026-09-26</lastmod>
    <priority>0.8</priority>
  </url>
  <url>
    <loc>https://openmapagents.geoafrica.fr/app.html</loc>
    <lastmod>2026-09-26</lastmod>
    <priority>0.9</priority>
  </url>
</urlset>
```
✅ Vite le copiera dans `dist/` au build.

### 2. Nouvelles pages

#### `frontend/doc.html`
Vraie page de documentation distincte de l'accueil. Contient :
- Titre unique : « Documentation — OpenMapAgents »
- Description SEO : « Installation, API, modules MCP, exemples, guides… »
- `rel="canonical"` : `https://openmapagents.geoafrica.fr/doc`
- Contenu structuré (installation, modules, API, architecture)

✅ Ajoutée au `vite.config.js` pour build en tant que `doc.html`.

### 3. Balises canoniques

Ajoutées à chaque `<head>` :
- **`index.html`** (accueil) : `<link rel="canonical" href="https://openmapagents.geoafrica.fr/" />`
- **`frontend/app.html`** (SPA) : `<link rel="canonical" href="https://openmapagents.geoafrica.fr/app.html" />`
- **`frontend/doc.html`** (docs) : `<link rel="canonical" href="https://openmapagents.geoafrica.fr/doc" />`

✅ Élimine les doublons canoniques.

### 4. Config nginx — LE CHANGEMENT CRITIQUE

**Avant** (problème) :
```nginx
location / {
    try_files $uri /index.html;  # ← Fallback SPA : retourne l'accueil pour TOUTE URL inconnue
}
```

**Après** (correctif, voir `nginx-seo-fix.conf`) :
```nginx
# robots.txt et sitemap.xml — servis statiquement
location = /robots.txt { try_files $uri =404; }
location = /sitemap.xml { try_files $uri =404; }

# API — proxy vers backend
location /api/ { proxy_pass http://backend; }

# Pages principales (MPA, pas SPA)
location ~ ^/(index\.html|app\.html|doc\.html)?$ { try_files $uri =404; }

# Toutes autres routes → 404 (pas fallback!)
location / { try_files $uri =404; }
```

✅ Les URLs inconnues retournent **HTTP 404** (pas 200 avec l'accueil) → Google arrête de traiter les doublons.

### 5. Vite config updated

Ajout de `doc.html` aux entrées build :
```js
input: {
  main: resolve(__dirname, "index.html"),
  app:  resolve(__dirname, "app.html"),
  doc:  resolve(__dirname, "doc.html"),  // ← NOUVEAU
}
```

✅ Vite compilera `doc.html` en tant que page indépendante.

---

## Étapes de déploiement

### 1. Build local (test)
```bash
cd frontend
npm run build
ls -la dist/  # Vérify: robots.txt, sitemap.xml, index.html, app.html, doc.html
cd ..
```

### 2. Déployer la nouvelle build
```bash
cd /var/www/openmapagents
git fetch origin
git reset --hard origin/main
cd frontend
npm run build  # ou npm install && npm run build si dépendances manquantes
cd ..
```

### 3. Mettre à jour nginx
```bash
# Sauvegarde
cp /etc/nginx/sites-available/openmapagents.geoafrica.fr /etc/nginx/sites-available/openmapagents.geoafrica.fr.bak

# Remplace la config avec le contenu de nginx-seo-fix.conf
# (adapte les chemins SSL et l'upstream selon ta config actuelle)
nano /etc/nginx/sites-available/openmapagents.geoafrica.fr

# Valide la config
nginx -t

# Redémarre
systemctl reload nginx
```

### 4. Tester les endpoints clés
```bash
# robots.txt réel
curl -H "Accept: */*" https://openmapagents.geoafrica.fr/robots.txt
# Doit retourner un fichier text/plain, pas du HTML

# sitemap.xml réel
curl -H "Accept: */*" https://openmapagents.geoafrica.fr/sitemap.xml
# Doit retourner du XML, pas du HTML

# page doc
curl https://openmapagents.geoafrica.fr/doc
# Doit retourner le HTML doc.html, pas l'accueil

# page inexistante → 404
curl -I https://openmapagents.geoafrica.fr/inexistant-xyz
# Doit retourner HTTP 404, pas 200
```

### 5. Signaler à Google
1. Aller dans **Google Search Console** → openmapagents.geoafrica.fr
2. Menu **Paramètres du site** → **Sitemap** → Soumettre `https://openmapagents.geoafrica.fr/sitemap.xml`
3. Menu **Index** → **Couverture** → **Demander une vérification** sur chaque page affectée

---

## Résultats attendus

| Problème actuel | Après correctif | Raison |
|---|---|---|
| 54 « Détectée, non indexée » | → Indexées (ou soft-404 corrigé) | HTTP 404 au lieu de 200 dupliqué |
| 4 doublons canoniques | → Éliminés | `rel="canonical"` sur chaque page |
| 3 bloquées robots.txt | → 0 | Vrai `robots.txt` servi statiquement |
| Pas de sitemap | → Soumis à Google | `robots.txt` y pointe, Google la découvre |
| `/doc` doublon | → Page distincte | `doc.html` = vraie page avec contenu unique |

---

## Fichiers modifiés/créés

```
frontend/
  ├── public/
  │   ├── robots.txt              [CRÉÉ]
  │   └── sitemap.xml             [CRÉÉ]
  ├── index.html                  [MODIFIÉ: +canonical]
  ├── app.html                    [MODIFIÉ: +canonical]
  ├── doc.html                    [CRÉÉ]
  └── vite.config.js              [MODIFIÉ: +doc entry]

docs/
  └── SEO-FIX.md                  [CE FICHIER]

(root)/
  └── index.html                  [MODIFIÉ: +canonical]

Scripts de déploiement:
  └── nginx-seo-fix.conf          [À adapter et déployer]
```

---

## Checklist pré-déploiement

- [ ] Build frontend testée localement
- [ ] `dist/robots.txt` existe et contient le texte correct
- [ ] `dist/sitemap.xml` existe et est valide XML
- [ ] `dist/doc.html` existe et contient le titre « Documentation »
- [ ] `canonical` dans tous les `<head>`
- [ ] nginx config validée (`nginx -t`)
- [ ] Test `/robots.txt` → `text/plain` (pas `text/html`)
- [ ] Test `/sitemap.xml` → `text/xml` (pas `text/html`)
- [ ] Test `/doc` → titre unique « Documentation » (pas l'accueil)
- [ ] Test URL inexistante → HTTP 404 (pas 200)
- [ ] Sitemap soumis à Google Search Console

---

## Monitoring post-déploiement

**Jour 1** : Google découvrira le `robots.txt` et le `sitemap.xml`.  
**Jour 2-3** : Nouveaux crawls avec HTTP 404 sur les URLs inconnues.  
**Jour 7-14** : Les 54 pages « non indexées » doivent disparaître du rapport. Les vraies pages (`/`, `/doc`, `/app.html`) seront indexées.

Vérifie dans **Google Search Console** :
- **Couverture** → Le graphique « Exclue » devrait baisser.
- **Sitemap** → Les 3 URLs doivent s'afficher comme « Traitée ».

---

## Questions ?

Si le déploiement nginx pose problème :
- Adapter les chemins SSL et l'upstream IP selon ta config.
- Tester d'abord avec `nginx -t` avant `systemctl reload nginx`.
- Consulter les logs : `tail -f /var/log/nginx/access.log && tail -f /var/log/nginx/error.log`.

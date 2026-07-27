/**
 * deck3d.js — Gestionnaire de couches 3D (deck.gl) superposées à MapLibre.
 *
 * Gère via un overlay deck.gl (MapboxOverlay, compatible MapLibre) :
 *   - 3D Tiles      (tileset.json)            → Tile3DLayer  (géoréférencé)
 *   - glTF / GLB    (modèle unique)           → ScenegraphLayer (placé sur lon/lat)
 *   - Nuages de points LAS/LAZ                → PointCloudLayer (offsets mètres)
 *
 * Les modules deck.gl / loaders.gl sont importés dynamiquement : tant que
 * `npm install` n'a pas été lancé, le reste de l'application fonctionne et seul
 * l'ajout d'une couche 3D échoue avec un message clair.
 *
 * Singleton au niveau module : les couches persistent même quand le panneau
 * Scène 3D est fermé.  Toggle globe inclus (MapLibre v5 setProjection).
 */

let _overlay = null;          // instance MapboxOverlay
let _map = null;              // carte MapLibre courante
let _mods = null;             // modules deck.gl / loaders.gl mis en cache
const _registry = new Map();  // id -> { kind, url, position, visible, opacity, name, _data }

// ── Chargement paresseux des modules lourds ───────────────────────────────
async function _loadDeck() {
  if (_mods) return _mods;
  const [mapbox, core, geo, mesh, layers, loadersCore, las, tiles3d] = await Promise.all([
    import("@deck.gl/mapbox"),
    import("@deck.gl/core"),
    import("@deck.gl/geo-layers"),
    import("@deck.gl/mesh-layers"),
    import("@deck.gl/layers"),
    import("@loaders.gl/core"),
    import("@loaders.gl/las"),
    import("@loaders.gl/3d-tiles"),
  ]);
  _mods = {
    MapboxOverlay:    mapbox.MapboxOverlay,
    COORDINATE_SYSTEM: core.COORDINATE_SYSTEM,
    Tile3DLayer:      geo.Tile3DLayer,
    ScenegraphLayer:  mesh.ScenegraphLayer,
    PointCloudLayer:  layers.PointCloudLayer,
    load:             loadersCore.load,
    LASLoader:        las.LASLoader,
    Tiles3DLoader:    tiles3d.Tiles3DLoader,
  };
  return _mods;
}

async function _ensureOverlay(map) {
  const { MapboxOverlay } = await _loadDeck();
  if (!_overlay || _map !== map) {
    _overlay = new MapboxOverlay({ interleaved: false, layers: [] });
    map.addControl(_overlay);
    _map = map;
  }
  return _overlay;
}

// ── Construction d'une couche deck.gl à partir d'une entrée du registre ───
async function _makeLayer(id, e, m) {
  const opacity = e.opacity ?? 1;

  if (e.kind === "3dtiles") {
    // ⚠️ decompressMeshes + loadBuffers = OBLIGATOIRES : beaucoup de tilesets
    // (Re:Earth, Cesium, etc.) exigent EXT_meshopt_compression. Le décodeur
    // meshopt de loaders.gl sort SANS décoder si l'une des deux options manque
    // → géométrie vide → rien ne s'affiche. loadGLTF pour parser le glTF, et
    // l'en-tête X-GOOG-API-KEY (propagé aux tuiles enfants) pour Google.
    const loadOptions = {
      "3d-tiles": { loadGLTF: true },
      gltf: { decompressMeshes: true, loadBuffers: true, loadImages: true },
      ...(e.apiKey ? { fetch: { headers: { "X-GOOG-API-KEY": e.apiKey } } } : {}),
    };
    return new m.Tile3DLayer({
      id,
      data: e.url,
      loader: m.Tiles3DLoader,
      loadOptions,
      opacity,
      pickable: true,
      pointSize: e.pointSize || 2,
      onTilesetLoad: (tileset) => {
        try {
          if (!_map) return;
          const c = tileset.cartographicCenter;
          const z = tileset.zoom || 0;
          // Tilesets MONDIAUX (Google, Re:Earth…) : centre ≈ (0,0) et zoom très
          // bas → NE PAS voler vers le centre (sinon « dans le Pacifique »). On
          // reste sur la position ACTUELLE, mais on zoome assez pour déclencher
          // le chargement (les bâtiments n'existent qu'à l'échelle ville).
          const isGlobal = !c || (Math.abs(c[0]) < 1 && Math.abs(c[1]) < 1) || z < 8;
          if (!isGlobal && c) {
            _map.flyTo({ center: [c[0], c[1]], zoom: Math.max(z - 1, 2), pitch: 45, duration: 1500 });
          } else {
            const cur = _map.getCenter();
            _map.easeTo({ center: cur, zoom: Math.max(_map.getZoom(), 16), pitch: 55, duration: 1200 });
          }
        } catch (_) {}
      },
      onTileError: (tile, message, url) => {
        console.warn("deck3d 3DTiles: tuile en erreur —", message, url);
      },
    });
  }

  if (e.kind === "model") {
    const pos = e.position || (_map ? [_map.getCenter().lng, _map.getCenter().lat, 0] : [0, 0, 0]);
    return new m.ScenegraphLayer({
      id,
      data: [{ position: pos }],
      scenegraph: e.url,
      getPosition: (d) => d.position,
      getOrientation: [0, 0, 90],          // glTF est Y-up, deck est Z-up
      sizeScale: e.sizeScale || 1,
      opacity,
      pickable: true,
      _animations: { "*": { speed: 1 } },
      _lighting: "pbr",
    });
  }

  if (e.kind === "pointcloud") {
    // LAS/LAZ : on charge une fois puis on met en cache les attributs.
    if (!e._data) {
      const mesh = await m.load(e.url, m.LASLoader);
      const attrs = mesh.attributes || {};
      const srcPos = attrs.POSITION?.value;
      if (!srcPos) throw new Error("Nuage de points illisible (pas de POSITION)");
      const count = srcPos.length / 3;

      // Recentrage : les positions LAS sont souvent dans un CRS projeté (gros
      // chiffres UTM). On les ramène en offsets mètres autour de leur centroïde
      // → le nuage se place fiablement sur le point choisi (sans reprojection).
      let cx = 0, cy = 0, cz = 0;
      for (let i = 0; i < count; i++) { cx += srcPos[i * 3]; cy += srcPos[i * 3 + 1]; cz += srcPos[i * 3 + 2]; }
      cx /= count; cy /= count; cz /= count;
      const positions = new Float32Array(srcPos.length);
      for (let i = 0; i < count; i++) {
        positions[i * 3]     = srcPos[i * 3]     - cx;
        positions[i * 3 + 1] = srcPos[i * 3 + 1] - cy;
        positions[i * 3 + 2] = srcPos[i * 3 + 2] - cz;
      }

      // Couleur : LAS stocke parfois du 16 bits → ramener en 0-255
      let colors = attrs.COLOR_0?.value || null;
      let colorSize = 0;
      if (colors) {
        colorSize = colors.length / count;
        if (colors instanceof Uint16Array) {
          const c8 = new Uint8Array(colors.length);
          for (let i = 0; i < colors.length; i++) c8[i] = colors[i] >> 8;
          colors = c8;
        }
      }
      e._data = { count, positions, colors, colorSize };
    }
    const { count, positions, colors, colorSize } = e._data;
    const anchor = e.position || (_map ? [_map.getCenter().lng, _map.getCenter().lat, 0] : [0, 0, 0]);
    return new m.PointCloudLayer({
      id,
      data: {
        length: count,
        attributes: {
          getPosition: { value: positions, size: 3 },
          // normalized:true → le GPU ramène les Uint8 0-255 en 0-1 (attendu par
          // le shader deck.gl). Avec false, les 255 restaient bruts → tout blanc.
          ...(colors ? { getColor: { value: colors, size: colorSize, normalized: true } } : {}),
        },
      },
      getColor: colors ? undefined : [80, 160, 255],
      coordinateSystem: m.COORDINATE_SYSTEM.METER_OFFSETS,
      coordinateOrigin: anchor,
      pointSize: e.pointSize || 2,
      opacity,
      pickable: false,
      // Éclairage désactivé : sans normales, l'éclairage Phong par défaut de
      // deck.gl délave les points vers le blanc → les couleurs (classif/RGB)
      // ne s'affichaient pas. material:false = couleurs rendues telles quelles.
      material: false,
    });
  }

  return null;
}

async function _rebuild() {
  if (!_overlay) return;
  const m = await _loadDeck();
  const built = [];
  for (const [id, e] of _registry) {
    if (e.visible === false) continue;
    try {
      const layer = await _makeLayer(id, e, m);
      if (layer) built.push(layer);
    } catch (err) {
      console.warn(`deck3d: couche ${id} échouée —`, err);
      throw err;            // remonte pour affichage côté panneau
    }
  }
  _overlay.setProps({ layers: built });
}

// ── API publique ──────────────────────────────────────────────────────────

/** Ajoute une couche 3D. def = { id, kind:'3dtiles'|'model'|'pointcloud',
 *  url, apiKey?, position?:[lon,lat,alt], opacity?, sizeScale?, pointSize?, name } */
export async function add3DLayer(map, def) {
  await _ensureOverlay(map);
  _registry.set(def.id, {
    kind: def.kind, url: def.url, apiKey: def.apiKey || null, position: def.position || null,
    visible: true, opacity: def.opacity ?? 1,
    sizeScale: def.sizeScale, pointSize: def.pointSize, name: def.name || def.url,
  });
  await _rebuild();
  // Recentrage pour modèles / nuages de points placés manuellement
  if ((def.kind === "model" || def.kind === "pointcloud") && def.position && map) {
    try { map.flyTo({ center: [def.position[0], def.position[1]], zoom: Math.max(map.getZoom(), 15), pitch: 45, duration: 1200 }); } catch (_) {}
  }
}

/** Ajoute un nuage de points à partir de données déjà décodées (converties par
 *  le backend laspy : gère toutes versions LAS/LAZ).  def = { id, name, count,
 *  positions: Float32Array (offsets mètres x,y,z), colors: Uint8Array|null,
 *  anchor:[lon,lat,alt], pointSize?, opacity? } */
export async function add3DPointCloudData(map, def) {
  await _ensureOverlay(map);
  const colorSize = def.colors && def.count ? def.colors.length / def.count : 0;
  _registry.set(def.id, {
    kind: "pointcloud", url: null, position: def.anchor || null,
    visible: true, opacity: def.opacity ?? 1, pointSize: def.pointSize || 2,
    name: def.name || "Nuage de points",
    _data: { count: def.count, positions: def.positions, colors: def.colors || null, colorSize },
  });
  await _rebuild();
  // fly:false lors d'un restyle (toggle classe, couleur…) pour ne pas re-centrer
  if (def.fly !== false && def.anchor && map) {
    try { map.flyTo({ center: [def.anchor[0], def.anchor[1]], zoom: Math.max(map.getZoom(), 15), pitch: 50, duration: 1200 }); } catch (_) {}
  }
}

export async function remove3DLayer(map, id) {
  const e = _registry.get(id);
  // Libère l'URL objet d'un fichier local importé
  if (e && typeof e.url === "string" && e.url.startsWith("blob:")) {
    try { URL.revokeObjectURL(e.url); } catch (_) {}
  }
  _registry.delete(id);
  await _rebuild();
}

export async function set3DVisible(map, id, visible) {
  const e = _registry.get(id);
  if (e) { e.visible = visible; await _rebuild(); }
}

export async function set3DOpacity(map, id, opacity) {
  const e = _registry.get(id);
  if (e) { e.opacity = opacity; await _rebuild(); }
}

export function list3DLayers() {
  return [..._registry.entries()].map(([id, e]) => ({
    id, kind: e.kind, name: e.name, visible: e.visible !== false, opacity: e.opacity ?? 1,
  }));
}

/** Bascule la projection globe / mercator (MapLibre GL v5). */
export function setGlobe(map, on) {
  try {
    map.setProjection({ type: on ? "globe" : "mercator" });
    return true;
  } catch (e) {
    console.warn("setGlobe:", e);
    return false;
  }
}

export function isGlobe(map) {
  try {
    const p = map.getProjection?.();
    return !!p && (p.type === "globe" || p.name === "globe");
  } catch (_) { return false; }
}

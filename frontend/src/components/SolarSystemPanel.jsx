/**
 * SolarSystemPanel.jsx — Système solaire : viewer 3D de corps célestes texturés.
 *
 * Rendu Three.js (dépendance déjà présente) : une sphère texturée par corps
 * (Soleil → Neptune, Lune), anneaux pour Saturne, fond étoilé. Les textures
 * (Solar System Scope, CC-BY) sont servies EN MÊME ORIGINE par le backend
 * (/api/planet/texture/{body}) car WebGL refuse les textures cross-origin.
 *
 * Interaction : glisser = tourner, molette = zoom, rotation automatique.
 */
import { useRef, useEffect } from "react";
import * as THREE from "three";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

const BODIES = [
  ["sun", "Soleil", "Étoile — Ø 1 392 000 km"],
  ["mercury", "Mercure", "Tellurique — Ø 4 879 km"],
  ["venus", "Vénus", "Tellurique — Ø 12 104 km"],
  ["earth", "Terre", "Tellurique — Ø 12 742 km"],
  ["moon", "Lune", "Satellite — Ø 3 474 km"],
  ["mars", "Mars", "Tellurique — Ø 6 779 km"],
  ["jupiter", "Jupiter", "Géante gazeuse — Ø 139 820 km"],
  ["saturn", "Saturne", "Géante gazeuse (anneaux) — Ø 116 460 km"],
  ["uranus", "Uranus", "Géante de glaces — Ø 50 724 km"],
  ["neptune", "Neptune", "Géante de glaces — Ø 49 244 km"],
];
const TILT = { earth: 23.4, mars: 25.2, saturn: 26.7, neptune: 28.3, jupiter: 3.1, uranus: 97.8, mercury: 0.03, venus: 2.6, moon: 6.7, sun: 7.25 };

export default function SolarSystemPanel({ body: bodyProp, onBody }) {
  const C = useThemeContext();
  const body = bodyProp || "earth";
  const mountRef = useRef(null);
  const S = useRef(null);        // état Three.js persistant
  const loadTok = useRef(0);
  const meta = BODIES.find((b) => b[0] === body);

  // ── Init Three.js (une fois) ──
  useEffect(() => {
    const mount = mountRef.current;
    if (!mount) return;
    const w = mount.clientWidth || 400, h = mount.clientHeight || 360;

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(45, w / h, 0.05, 200);
    camera.position.set(0, 0, 3.2);
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.setSize(w, h);
    mount.appendChild(renderer.domElement);

    // Lumières (pour les planètes ; le Soleil est auto-éclairé)
    const sunLight = new THREE.DirectionalLight(0xffffff, 1.25);
    sunLight.position.set(5, 2, 4);
    scene.add(sunLight);
    const ambient = new THREE.AmbientLight(0xffffff, 0.32);
    scene.add(ambient);

    // Corps : groupe (sphère + anneaux éventuels) pour gérer l'inclinaison
    const group = new THREE.Group();
    scene.add(group);
    const geo = new THREE.SphereGeometry(1, 64, 48);
    const mat = new THREE.MeshStandardMaterial({ color: 0x888888, roughness: 1, metalness: 0 });
    const sphere = new THREE.Mesh(geo, mat);
    group.add(sphere);

    // Fond étoilé (grande sphère inversée)
    const starGeo = new THREE.SphereGeometry(90, 32, 16);
    const starMat = new THREE.MeshBasicMaterial({ side: THREE.BackSide, color: 0x0a0a12 });
    const stars = new THREE.Mesh(starGeo, starMat);
    scene.add(stars);

    S.current = { scene, camera, renderer, group, sphere, stars, sunLight, ambient,
      ring: null, raf: 0, spin: 0.0016, rotX: 0.1, targetX: 0.1, drag: null, mount };

    // Fond étoilé texturé (même origine)
    new THREE.TextureLoader().load(`${API}/planet/texture/stars`, (t) => {
      t.colorSpace = THREE.SRGBColorSpace;
      starMat.map = t; starMat.color.set(0xffffff); starMat.needsUpdate = true;
    }, undefined, () => {});

    // ── Interaction : glisser + molette ──
    const el = renderer.domElement;
    const onDown = (e) => { S.current.drag = { x: e.clientX, y: e.clientY }; };
    const onMove = (e) => {
      const st = S.current; if (!st.drag) return;
      const dx = e.clientX - st.drag.x, dy = e.clientY - st.drag.y;
      st.group.rotation.y += dx * 0.006;
      st.targetX = Math.max(-1.3, Math.min(1.3, st.targetX + dy * 0.006));
      st.drag = { x: e.clientX, y: e.clientY };
    };
    const onUp = () => { if (S.current) S.current.drag = null; };
    const onWheel = (e) => {
      e.preventDefault();
      const st = S.current; if (!st) return;
      st.camera.position.z = Math.max(1.5, Math.min(8, st.camera.position.z + (e.deltaY > 0 ? 0.3 : -0.3)));
    };
    el.addEventListener("pointerdown", onDown);
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    el.addEventListener("wheel", onWheel, { passive: false });

    // ── Boucle de rendu ──
    const tick = () => {
      const st = S.current; if (!st) return;
      st.group.rotation.y += st.spin;
      st.rotX += (st.targetX - st.rotX) * 0.1;
      st.group.rotation.x = st.rotX;
      st.renderer.render(st.scene, st.camera);
      st.raf = requestAnimationFrame(tick);
    };
    tick();

    // Redimensionnement
    const ro = new ResizeObserver(() => {
      const st = S.current; if (!st) return;
      const W = mount.clientWidth || 400, H = mount.clientHeight || 360;
      st.camera.aspect = W / H; st.camera.updateProjectionMatrix();
      st.renderer.setSize(W, H);
    });
    ro.observe(mount);

    return () => {
      const st = S.current;
      cancelAnimationFrame(st.raf);
      ro.disconnect();
      el.removeEventListener("pointerdown", onDown);
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      el.removeEventListener("wheel", onWheel);
      try { mount.removeChild(el); } catch (_) {}
      scene.traverse((o) => { if (o.geometry) o.geometry.dispose?.(); if (o.material) { const m = o.material; (m.map && m.map.dispose?.()); m.dispose?.(); } });
      renderer.dispose();
      S.current = null;
    };
  }, []);

  // ── Changement de corps : texture + matériau + anneaux + inclinaison ──
  useEffect(() => {
    const st = S.current; if (!st) return;
    const tok = ++loadTok.current;
    const isSun = body === "sun";

    // anneaux : retirer l'ancien
    if (st.ring) { st.group.remove(st.ring); st.ring.geometry.dispose(); st.ring.material.map?.dispose?.(); st.ring.material.dispose(); st.ring = null; }

    new THREE.TextureLoader().load(`${API}/planet/texture/${body}`, (t) => {
      if (tok !== loadTok.current || !S.current) return;
      t.colorSpace = THREE.SRGBColorSpace;
      const old = st.sphere.material;
      st.sphere.material = isSun
        ? new THREE.MeshBasicMaterial({ map: t })                 // auto-éclairé
        : new THREE.MeshStandardMaterial({ map: t, roughness: 1, metalness: 0 });
      old.map?.dispose?.(); old.dispose?.();
      st.sunLight.intensity = isSun ? 0.0 : 1.15;
      st.ambient.intensity = isSun ? 1.0 : 0.32;
      st.group.rotation.set(0, st.group.rotation.y, 0);
      st.group.rotation.z = THREE.MathUtils.degToRad(TILT[body] || 0);
      st.spin = isSun ? 0.0008 : 0.0016;
    }, undefined, () => {});

    // Anneaux de Saturne
    if (body === "saturn") {
      new THREE.TextureLoader().load(`${API}/planet/texture/saturn_ring`, (rt) => {
        if (tok !== loadTok.current || !S.current) return;
        rt.colorSpace = THREE.SRGBColorSpace;
        const inner = 1.25, outer = 2.3;
        const rg = new THREE.RingGeometry(inner, outer, 128);
        // UV radiale : distance au centre → axe X de la texture (bande radiale)
        const pos = rg.attributes.position, uv = rg.attributes.uv, v = new THREE.Vector3();
        for (let i = 0; i < pos.count; i++) { v.fromBufferAttribute(pos, i); const r = v.length(); uv.setXY(i, (r - inner) / (outer - inner), 0.5); }
        const rm = new THREE.MeshBasicMaterial({ map: rt, transparent: true, side: THREE.DoubleSide, opacity: 0.95 });
        const ring = new THREE.Mesh(rg, rm);
        ring.rotation.x = Math.PI / 2;   // à plat autour de l'équateur
        st.group.add(ring); st.ring = ring;
      }, undefined, () => {});
    }
  }, [body]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
        <select value={body} onChange={(e) => onBody?.(e.target.value)}
          style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "6px 10px", borderRadius: 7, cursor: "pointer",
            border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none" }}>
          {BODIES.map(([k, label]) => <option key={k} value={k}>{label}</option>)}
        </select>
        <span style={{ fontFamily: F, fontSize: 11, color: C.mut }}>{meta?.[2]}</span>
      </div>

      <div ref={mountRef} style={{ flex: 1, minHeight: 220, borderRadius: 10, overflow: "hidden", background: "#05060a", cursor: "grab", border: `0.5px solid ${C.bdr}` }} />

      <div style={{ fontFamily: F, fontSize: 10, color: C.dim, textAlign: "center" }}>
        Glisser pour tourner · molette pour zoomer · Textures © <span style={{ fontFamily: M }}>Solar System Scope</span> (CC-BY 4.0)
      </div>
    </div>
  );
}

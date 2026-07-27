/**
 * StarField.jsx — Fond spatial animé pour le mode Globe.
 *
 * Canvas plein cadre (position absolue, derrière la carte) : dégradé spatial
 * sombre + étoiles scintillantes + étoiles filantes occasionnelles. Ne s'affiche
 * que quand le globe est actif (monté/démonté par App). En projection globe,
 * l'espace autour de la sphère laisse transparaître ce fond.
 *
 * pointerEvents: none → n'intercepte aucune interaction carte.
 */
import { useRef, useEffect } from "react";

export default function StarField() {
  const ref = useRef(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    let raf = 0, W = 0, H = 0, stars = [], shooters = [], running = true;

    const resize = () => {
      const p = canvas.parentElement;
      W = canvas.width = p ? p.clientWidth : window.innerWidth;
      H = canvas.height = p ? p.clientHeight : window.innerHeight;
      // Densité d'étoiles ∝ surface
      const n = Math.min(420, Math.round(W * H / 4200));
      stars = Array.from({ length: n }, () => ({
        x: Math.random() * W, y: Math.random() * H,
        r: Math.random() * 1.3 + 0.2,
        base: Math.random() * 0.5 + 0.25,
        tw: Math.random() * 0.02 + 0.004,   // vitesse de scintillement
        ph: Math.random() * Math.PI * 2,
      }));
    };
    resize();
    const ro = new ResizeObserver(resize);
    if (canvas.parentElement) ro.observe(canvas.parentElement);

    const spawnShooter = () => {
      const edge = Math.random();
      const x = Math.random() * W * 0.9;
      const y = Math.random() * H * 0.4;
      const ang = (Math.PI / 4) + (Math.random() - 0.5) * 0.5;   // ~45° vers le bas-droite
      const spd = 8 + Math.random() * 8;
      shooters.push({
        x, y, vx: Math.cos(ang) * spd, vy: Math.sin(ang) * spd,
        life: 0, max: 40 + Math.random() * 30, len: 80 + Math.random() * 120,
      });
    };

    let t = 0;
    const draw = () => {
      if (!running) return;
      t++;
      // Fond spatial (dégradé radial sombre)
      const g = ctx.createRadialGradient(W * 0.5, H * 0.45, 0, W * 0.5, H * 0.45, Math.max(W, H) * 0.75);
      g.addColorStop(0, "#0a0f1e");
      g.addColorStop(0.6, "#060812");
      g.addColorStop(1, "#010206");
      ctx.fillStyle = g;
      ctx.fillRect(0, 0, W, H);

      // Étoiles scintillantes
      for (const s of stars) {
        s.ph += s.tw;
        const a = s.base + Math.sin(s.ph) * 0.25;
        ctx.globalAlpha = Math.max(0, Math.min(1, a));
        ctx.beginPath();
        ctx.arc(s.x, s.y, s.r, 0, Math.PI * 2);
        ctx.fillStyle = "#ffffff";
        ctx.fill();
      }
      ctx.globalAlpha = 1;

      // Étoiles filantes
      if (Math.random() < 0.012 && shooters.length < 3) spawnShooter();
      for (let i = shooters.length - 1; i >= 0; i--) {
        const sh = shooters[i];
        sh.life++; sh.x += sh.vx; sh.y += sh.vy;
        const prog = sh.life / sh.max;
        const alpha = prog < 0.15 ? prog / 0.15 : (1 - prog);
        const nx = sh.x - sh.vx, ny = sh.y - sh.vy;
        const tx = sh.x - (sh.vx / Math.hypot(sh.vx, sh.vy)) * sh.len;
        const ty = sh.y - (sh.vy / Math.hypot(sh.vx, sh.vy)) * sh.len;
        const grad = ctx.createLinearGradient(sh.x, sh.y, tx, ty);
        grad.addColorStop(0, `rgba(255,255,255,${Math.max(0, alpha)})`);
        grad.addColorStop(1, "rgba(255,255,255,0)");
        ctx.strokeStyle = grad;
        ctx.lineWidth = 1.6;
        ctx.lineCap = "round";
        ctx.beginPath();
        ctx.moveTo(sh.x, sh.y);
        ctx.lineTo(tx, ty);
        ctx.stroke();
        // tête brillante
        ctx.globalAlpha = Math.max(0, alpha);
        ctx.beginPath();
        ctx.arc(sh.x, sh.y, 1.6, 0, Math.PI * 2);
        ctx.fillStyle = "#eaf2ff";
        ctx.fill();
        ctx.globalAlpha = 1;
        void nx; void ny;
        if (sh.life > sh.max || sh.x > W + sh.len || sh.y > H + sh.len) shooters.splice(i, 1);
      }

      raf = requestAnimationFrame(draw);
    };
    raf = requestAnimationFrame(draw);

    return () => { running = false; cancelAnimationFrame(raf); ro.disconnect(); };
  }, []);

  return (
    <canvas ref={ref} style={{
      position: "absolute", inset: 0, width: "100%", height: "100%",
      // PAS de z-index : le parent (position:relative sans z-index) ne crée pas
      // de contexte d'empilement, donc un z-index négatif ferait passer le canvas
      // DERRIÈRE le fond des ancêtres → invisible. En z-index auto, il se peint
      // avant la carte (ordre du DOM) : espace derrière le globe. ✓
      pointerEvents: "none",
    }} />
  );
}

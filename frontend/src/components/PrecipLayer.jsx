/**
 * PrecipLayer.jsx — Précipitations animées en surimpression de la carte.
 *
 * MapLibre 5.21 n'a pas de précipitations natives (`setRain`/`setSnow` sont des
 * fonctions de Mapbox GL v3, absentes ici — vérifié dans les typings du paquet).
 * On dessine donc sur un canvas 2D placé APRÈS la carte dans le DOM : il se
 * peint par-dessus les tuiles, mais reste sous la légende et les fenêtres
 * flottantes, qui viennent après lui.
 *
 * Profondeur simulée : chaque particule tire une « distance » d ∈ [0,1] qui
 * pilote conjointement sa taille, sa vitesse et son opacité. Ce faisceau de
 * variations corrélées suffit à faire lire un volume, là où des éléments
 * uniformes donneraient un rideau plat collé à l'écran.
 *
 * Pluie et neige partagent cette profondeur mais rien d'autre : la pluie est
 * rapide et rectiligne, la neige lente et louvoyante. C'est ce contraste de
 * mouvement qui les distingue, bien plus que la forme des particules.
 *
 * pointerEvents: none → n'intercepte aucune interaction carte.
 */
import { useRef, useEffect } from "react";

export default function PrecipLayer({ type = "rain", intensity = 1, wind = 0.22 }) {
  const ref = useRef(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const snow = type === "snow";
    let raf = 0, W = 0, H = 0, parts = [], running = true;

    const spawn = (atTop = false) => {
      const d = Math.random();                       // 0 = loin, 1 = près
      return {
        x: Math.random() * W * 1.3 - W * 0.15,       // marge : le vent pousse latéralement
        y: atTop ? -Math.random() * H * 0.3 : Math.random() * H,
        d,
        len:   snow ? 0.8 + d * 2.2 : 5 + d * 20,    // rayon (neige) ou longueur (pluie)
        speed: snow ? 0.4 + d * 1.5 : 5 + d * 15,
        alpha: snow ? 0.3 + d * 0.6 : 0.10 + d * 0.32,
        w:     0.5 + d * 1.1,
        // Neige : dérive latérale sinusoïdale, chaque flocon a sa phase
        ph:    Math.random() * Math.PI * 2,
        sway:  0.4 + d * 1.1,
      };
    };

    const resize = () => {
      const p = canvas.parentElement;
      W = canvas.width = p ? p.clientWidth : window.innerWidth;
      H = canvas.height = p ? p.clientHeight : window.innerHeight;
      // Un pixel par particule tous les N pixels d'écran. Valeurs calibrées pour
      // une averse et une chute de neige franches : à 1600×900 cela donne
      // ~410 gouttes et ~590 flocons, ce que le canvas absorbe sans peine.
      const div = snow ? 2400 : 3500;
      const n = Math.max(80, Math.round((W * H) / div * intensity));
      parts = Array.from({ length: n }, () => spawn());
    };
    resize();
    const ro = new ResizeObserver(resize);
    if (canvas.parentElement) ro.observe(canvas.parentElement);

    const draw = () => {
      if (!running) return;
      ctx.clearRect(0, 0, W, H);
      ctx.lineCap = "round";
      for (const r of parts) {
        if (snow) {
          r.ph += 0.02;
          r.y += r.speed;
          r.x += wind * r.speed * 0.6 + Math.sin(r.ph) * r.sway;
        } else {
          r.y += r.speed;
          r.x += wind * r.speed;
        }
        if (r.y - r.len > H || r.x - r.len > W) Object.assign(r, spawn(true));

        ctx.globalAlpha = r.alpha;
        if (snow) {
          ctx.fillStyle = "#ffffff";
          ctx.beginPath();
          ctx.arc(r.x, r.y, r.len, 0, Math.PI * 2);
          ctx.fill();
        } else {
          ctx.strokeStyle = "#cfe0f0";
          ctx.lineWidth = r.w;
          ctx.beginPath();
          ctx.moveTo(r.x, r.y);
          ctx.lineTo(r.x - wind * r.len, r.y - r.len);   // traînée vers l'arrière
          ctx.stroke();
        }
      }
      ctx.globalAlpha = 1;
      raf = requestAnimationFrame(draw);
    };
    raf = requestAnimationFrame(draw);

    return () => { running = false; cancelAnimationFrame(raf); ro.disconnect(); };
  }, [type, intensity, wind]);

  return (
    <canvas ref={ref} style={{
      position: "absolute", inset: 0, width: "100%", height: "100%",
      pointerEvents: "none",   // pas de z-index : l'ordre du DOM suffit (après la carte)
    }} />
  );
}

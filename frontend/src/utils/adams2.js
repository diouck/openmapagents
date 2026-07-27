/**
 * adams2.js — Projection Adams « World in a Square II » (1925) + Spilhaus.
 *
 * La projection SPILHAUS (la carte des océans : toutes les mers réunies en un
 * seul bassin, Antarctique au centre) = Adams II + rotation canonique
 * (66.95°E, 49.56°S, inclinaison 40.18°) — mêmes paramètres qu'ESRI et
 * PROJ (+proj=spilhaus). Absente de d3-geo-projection, d'où ce module.
 *
 * Port JS d'après PROJ (PJ_adams.c, licence MIT/X11) et le notebook
 * « Adams world in a square I & II » de Torben Jansen (Observable, ISC).
 * Maths VALIDÉES numériquement (Python, session 2026-07-18) :
 *   - ellipticF vs intégration directe : écart < 1e-10 sur [-89°, 89°], K(0.5) exact ;
 *   - repères du carré : pôles → (0, ±2.62181), (±180°,0°) → (±2.62206, 0) ;
 *   - aller-retour forward→invert : erreur max 3e-10 rad, 0 échec sur grille 20°.
 * L'invert (Newton 2D, jacobien par différences finies) est indispensable à la
 * reprojection raster pixel par pixel du ProjectionExplorer.
 */
import { geoProjection } from "d3";

const { abs, acos, asin, atan, cos, log, max, min, sin, sqrt, tan, PI } = Math;
const HALFPI = PI / 2;

// Intégrale elliptique incomplète de 1re espèce F(phi | m) — algo de Bulirsch.
function ellipticF(phi, m) {
  const C1 = 10e-4, C2 = 10e-10, TOL = 10e-6;
  const sp = sin(phi);
  if (sp === 0) return 0;                      // F(0|m) = 0 (évite h=0 plus bas)
  let k = sqrt(1 - m), h = sp * sp;
  if (h >= 1 || abs(phi) === HALFPI) {
    // Intégrale complète K(m)
    if (k <= TOL) return sp < 0 ? -Infinity : Infinity;
    let mm = 1, hh = mm;
    mm += k;
    while (abs(hh - k) > C1 * mm) {
      k = sqrt(hh * k);
      mm /= 2; hh = mm; mm += k;
    }
    return sp < 0 ? -PI / mm : PI / mm;
  }
  if (k <= TOL) return log((1 + sp) / (1 - sp)) / 2;
  let mm = 1, n = 0, g = mm, p = mm * k;
  mm += k;
  let y = sqrt((1 - h) / h);
  if (abs(y -= p / y) <= 0) y = C2 * sqrt(p);
  while (abs(g - k) > C1 * g) {
    k = 2 * sqrt(p); n += n;
    if (y < 0) n += 1;
    p = mm * k; g = mm; mm += k;
    if (abs(y -= p / y) <= 0) y = C2 * sqrt(p);
  }
  if (y < 0) n += 1;
  const r = (atan(mm / y) + PI * n) / mm;
  return sp < 0 ? -r : r;
}

// Clamp [-1,1] : les arrondis flottants près de la couture (λ=±180°, pôles)
// produisent des arguments hors domaine → sqrt/asin/acos renverraient NaN,
// qui contamine le forward (et contourne les tests « > tolérance »).
const _c1 = (v) => max(-1, min(1, v));

function ellipticFactory(a, b, sm, sn) {
  let m = asin(_c1(sqrt(max(0, 1 + min(0, cos(a + b))))));
  if (sm) m = -m;
  let n = asin(_c1(sqrt(abs(1 - max(0, cos(a - b))))));
  if (sn) n = -n;
  return [ellipticF(m, 0.5), ellipticF(n, 0.5)];
}

export function adamsSquareIIRaw(lambda, phi) {
  const sp = _c1(tan(0.5 * phi));
  const a0 = _c1(cos(asin(sp)) * sin(0.5 * lambda));
  const sm = sp + a0 < 0;
  const sn = sp - a0 < 0;
  const b = acos(sp);
  const a = acos(a0);
  const [x, y] = ellipticFactory(a, b, sm, sn);
  // Rotation de 45° : pôles aux milieux des bords haut/bas du carré.
  return [Math.SQRT1_2 * (x - y), Math.SQRT1_2 * (x + y)];
}

// Invert par Newton 2D (pas de forme fermée pour Adams II).
adamsSquareIIRaw.invert = function (x, y) {
  let phi = max(min(y / 2.62181347, 1), -1) * HALFPI;
  let lam = max(min(x / 2.62205760 / max(cos(phi), 1e-9), 1), -1) * PI;
  let dLamX = 0, dLamY = 0, dPhiX = 0, dPhiY = 0;
  for (let i = 0; i < 15; i++) {
    const [xa, ya] = adamsSquareIIRaw(lam, phi);
    const dX = xa - x, dY = ya - y;
    if (abs(dX) < 1e-10 && abs(dY) < 1e-10) return [lam, phi];
    if (abs(dX) > 1e-6 || abs(dY) > 1e-6) {
      // Jacobien par différences finies (vers l'intérieur du domaine)
      const dLam = lam > 0 ? -1e-6 : 1e-6;
      let [x2, y2] = adamsSquareIIRaw(lam + dLam, phi);
      const dXLam = (x2 - xa) / dLam, dYLam = (y2 - ya) / dLam;
      const dPhi = phi > 0 ? -1e-6 : 1e-6;
      [x2, y2] = adamsSquareIIRaw(lam, phi + dPhi);
      const dXPhi = (x2 - xa) / dPhi, dYPhi = (y2 - ya) / dPhi;
      const det = dXLam * dYPhi - dXPhi * dYLam;
      if (det !== 0) {
        dLamX = dYPhi / det;  dLamY = -dXPhi / det;
        dPhiX = -dYLam / det; dPhiY = dXLam / det;
      }
    }
    if (x !== 0) lam = max(-PI, min(PI, lam - max(min(dX * dLamX + dY * dLamY, 0.3), -0.3)));
    if (y !== 0) phi = max(-HALFPI, min(HALFPI, phi - max(min(dX * dPhiX + dY * dPhiY, 0.3), -0.3)));
  }
  return [lam, phi];
};

/** Adams World in a Square II « brute » (monde entier dans un carré). */
export function geoAdams2() {
  return geoProjection(adamsSquareIIRaw);
}

/**
 * Spilhaus : Adams II centrée (66.95°E, 49.56°S), inclinée 40.18° (canon ESRI/PROJ).
 * .angle(-45) redresse le losange en CARRÉ droit avec l'orientation canonique
 * (vérifié numériquement : Amériques en haut à gauche, Asie/Australie à droite,
 * Antarctique au centre — comme les planches IGN/ESRI).
 */
export function geoSpilhaus() {
  return geoProjection(adamsSquareIIRaw)
    .rotate([-66.94970198, 49.56371678, 40.17823482])
    .angle(-45);
}

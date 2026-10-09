"""
flood_sim.py — Simulation d'inondation pluviale 2D (version simple, numpy).

Modèle : automate cellulaire pondéré (WCA2D / « fill & spill ») sur un MNT.
Chaque pas de temps :
  • +pluie (mm/h) et −infiltration sur chaque cellule,
  • routage D4 : l'eau s'écoule des cellules à niveau d'eau (z+d) élevé vers les
    voisines plus basses, flux conservatif borné par une fraction de la hauteur
    disponible (stable si c ≤ 0.25), réparti au prorata des dénivelés.

Pensé pour l'inondation PLUVIALE URBAINE (pluie sur grille). Entrées = MNT
(GEE ou importé) + pluie + durée + rugosité/infiltration. Sorties = profondeur
par pas de temps (frames PNG), profondeur max, et séries KPI (surface inondée,
profondeur max, volume). Vectorisé ; grille bornée côté routes (~220 cell/côté).
"""
import io
import base64

import numpy as np

# Palette profondeur d'eau (clair à sec → bleu profond).
_WATER = ["#c6e8ff", "#7fc3f5", "#4a97d8", "#2a6fb0", "#1b4a86", "#0d2a5c"]
DRY_M = 0.02  # seuil « mouillé » (m) sous lequel la cellule est considérée sèche


def _hex(h):
    h = h.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _colormap_depth(depth, vmax, palette=_WATER):
    """depth 2D (m) → RGBA uint8 ; transparent sous DRY_M."""
    pal = np.array([_hex(c) for c in palette], dtype=float)
    a = np.clip(depth / max(1e-6, vmax), 0, 1)
    n = len(pal) - 1
    idx = a * n
    lo = np.clip(np.floor(idx).astype(int), 0, n - 1)
    hi = np.clip(lo + 1, 0, n)
    frac = (idx - lo)[..., None]
    rgb = pal[lo] * (1 - frac) + pal[hi] * frac
    rgba = np.dstack([rgb.astype(np.uint8), np.full(depth.shape, 255, np.uint8)])
    rgba[..., 3] = np.where(depth < DRY_M, 0, 230).astype(np.uint8)
    return rgba


def _png_b64(rgba):
    from PIL import Image
    buf = io.BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, "PNG")
    return base64.b64encode(buf.getvalue()).decode()


def simulate_flood(z, cell_m, rain_mm_h, duration_min, manning=0.05,
                   infil_mm_h=0.0, n_frames=12, nodata_mask=None):
    """
    Lance la simulation. Retourne un dict :
      frames[{t_min, depth(2D), flooded_ha, max_depth_m, volume_m3}],
      max_depth(2D), vmax, kpi{t_min[], flooded_ha[], max_depth_m[], volume_m3[]}.
    `z` = MNT (m), `cell_m` = taille de cellule (m). `nodata_mask` True = hors-domaine.
    """
    z = np.asarray(z, dtype=float)
    if nodata_mask is None:
        nodata_mask = ~np.isfinite(z)
    wall = nodata_mask | ~np.isfinite(z)
    # Les murs (nodata) : niveau très haut → aucune eau n'y entre ni n'en sort.
    z = np.where(wall, 1e6, z)

    cell_area = float(cell_m * cell_m)
    rain_ms = rain_mm_h / 1000.0 / 3600.0
    infil_ms = infil_mm_h / 1000.0 / 3600.0
    total_s = duration_min * 60.0

    # Pas de temps : compromis stabilité/vitesse (le facteur c ≤ 0.25 assure la stabilité).
    dt = max(1.0, min(30.0, cell_m / 2.0))
    nsteps = max(1, int(np.ceil(total_s / dt)))
    c = 0.25  # fraction max de la hauteur évacuée par pas

    d = np.zeros_like(z)
    dmax = np.zeros_like(z)
    frame_steps = {int(round(i * nsteps / n_frames)) for i in range(1, n_frames + 1)}

    frames, kpi_t, kpi_area, kpi_dmax, kpi_vol = [], [], [], [], []
    # Voisins D4 : (shift, axis) pour np.roll ; haut, bas, gauche, droite.
    shifts = [(1, 0), (-1, 0), (1, 1), (-1, 1)]

    for step in range(1, nsteps + 1):
        d = d + rain_ms * dt
        if infil_ms > 0:
            d = np.maximum(0.0, d - infil_ms * dt)
        d[wall] = 0.0

        wl = z + d
        dhs = []
        sum_dh = np.zeros_like(d)
        for sh, ax in shifts:
            dh = np.clip(wl - np.roll(wl, sh, axis=ax), 0, None)
            dhs.append((dh, sh, ax))
            sum_dh += dh

        outflow = np.where(sum_dh > 0, np.minimum(d, c * sum_dh), 0.0)
        safe_sum = np.where(sum_dh > 0, sum_dh, 1.0)
        new_d = d - outflow
        for dh, sh, ax in dhs:
            share = outflow * dh / safe_sum
            # Ce qui sort vers (sh,ax) arrive chez le voisin → roll inverse.
            new_d = new_d + np.roll(share, -sh, axis=ax)
        d = new_d
        d[wall] = 0.0
        np.maximum(dmax, d, out=dmax)

        if step in frame_steps:
            t_min = round(step * dt / 60.0, 1)
            wet = d >= DRY_M
            area_ha = float(wet.sum()) * cell_area / 10000.0
            vol = float(d[wet].sum()) * cell_area
            frames.append({"t_min": t_min, "depth": d.copy(),
                           "flooded_ha": round(area_ha, 2),
                           "max_depth_m": round(float(d.max()), 2),
                           "volume_m3": round(vol, 1)})
            kpi_t.append(t_min); kpi_area.append(round(area_ha, 2))
            kpi_dmax.append(round(float(d.max()), 2)); kpi_vol.append(round(vol, 1))

    wetmax = dmax >= DRY_M
    vmax = max(0.1, float(np.percentile(dmax[wetmax], 95)) if wetmax.any() else 0.1)
    return {
        "frames": frames, "max_depth": dmax, "vmax": vmax,
        "kpi": {"t_min": kpi_t, "flooded_ha": kpi_area, "max_depth_m": kpi_dmax, "volume_m3": kpi_vol},
    }


def frames_to_payload(sim, coords_bbox):
    """Convertit la sortie de simulate_flood en payload JSON (PNG + coords + légende)."""
    w, s, e, n = coords_bbox
    coords = [[w, n], [e, n], [e, s], [w, s]]
    vmax = sim["vmax"]
    out_frames = []
    for f in sim["frames"]:
        out_frames.append({
            "t_min": f["t_min"], "flooded_ha": f["flooded_ha"],
            "max_depth_m": f["max_depth_m"], "volume_m3": f["volume_m3"],
            "png_b64": _png_b64(_colormap_depth(f["depth"], vmax)),
        })
    max_png = _png_b64(_colormap_depth(sim["max_depth"], vmax))
    legend = [{"color": _WATER[i], "value": round(vmax * i / (len(_WATER) - 1), 2),
               "label": f"{round(vmax * i / (len(_WATER) - 1), 2)} m"} for i in range(len(_WATER))]
    return {
        "status": "ok", "image_coordinates": coords, "bbox": [w, s, e, n],
        "vmax": round(vmax, 2), "legend": legend,
        "frames": out_frames, "max_depth_png_b64": max_png, "kpi": sim["kpi"],
    }

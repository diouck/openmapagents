/**
 * IndicatorDoc.jsx — Fiche descriptive d'un indicateur et de sa source.
 *
 * Affichée dans l'IndicatorModal via le bouton ⓘ de l'en-tête. Le contenu vient
 * de utils/datasetMeta.js ; le lien de catalogue Earth Engine porte la citation
 * scientifique et la licence officielles.
 */
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcExternalLink, IcInfo, IcAlert } from "../icons";
import { INDICATOR_DOC, SOURCE_META, geeCatalog } from "../utils/datasetMeta";

export default function IndicatorDoc({ indKey, dataset, title }) {
  const C = useThemeContext();
  const doc = INDICATOR_DOC[indKey];
  const src = SOURCE_META[dataset];
  const cat = src && geeCatalog(src.asset);

  const h = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 4 };
  const p = { fontSize: 11.5, color: C.mut, lineHeight: 1.6, margin: 0 };
  const row = (k, v) => v ? (
    <div key={k} style={{ display: "flex", gap: 8, fontSize: 10.5, padding: "2px 0" }}>
      <span style={{ color: C.dim, width: 84, flexShrink: 0 }}>{k}</span>
      <span style={{ color: C.mut, fontFamily: M, flex: 1, minWidth: 0 }}>{v}</span>
    </div>
  ) : null;

  if (!doc && !src) {
    return (
      <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.dim }}>
        <IcInfo size={13} /> Pas encore de fiche pour cet indicateur.
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>

      {doc && (
        <>
          <div>
            <div style={h}>{title || indKey}</div>
            <p style={p}>{doc.abstract}</p>
          </div>

          {doc.formula && (
            <div>
              <div style={h}>Calcul</div>
              <div style={{
                fontFamily: M, fontSize: 11.5, color: C.txt, background: C.hover,
                border: `0.5px solid ${C.bdr}`, borderRadius: 6, padding: "8px 10px",
                overflowX: "auto", whiteSpace: "nowrap",
              }}>{doc.formula}</div>
            </div>
          )}

          {doc.reading && (
            <div>
              <div style={h}>Lecture des valeurs</div>
              <p style={p}>{doc.reading}</p>
            </div>
          )}

          {doc.caveat && (
            <div style={{
              display: "flex", gap: 8, background: C.amb + "12",
              border: `0.5px solid ${C.amb}33`, borderRadius: 7, padding: "8px 10px",
            }}>
              <span style={{ color: C.amb, flexShrink: 0, marginTop: 1 }}><IcAlert size={13} /></span>
              <div>
                <div style={{ fontSize: 9.5, color: C.amb, fontWeight: 600, marginBottom: 2 }}>À SAVOIR</div>
                <p style={{ ...p, fontSize: 11 }}>{doc.caveat}</p>
              </div>
            </div>
          )}
        </>
      )}

      {src && (
        <div>
          <div style={h}>Métadonnées</div>
          <div style={{ fontSize: 12, fontWeight: 600, color: C.txt, marginBottom: 2 }}>{src.label}</div>
          <div style={{ fontSize: 10.5, color: C.dim, marginBottom: 7 }}>{src.provider}</div>

          {src.note && <p style={{ ...p, fontSize: 11, marginBottom: 8 }}>{src.note}</p>}

          <div style={{ background: C.hover, borderRadius: 7, padding: "7px 10px", border: `0.5px solid ${C.bdr}` }}>
            {row("Résolution", src.res)}
            {row("Fréquence", src.revisit)}
            {row("Couverture", src.coverage)}
            {row("Unités", src.units)}
            {row("Asset GEE", src.asset)}
          </div>

          {cat ? (
            <a href={cat} target="_blank" rel="noopener noreferrer" style={{
              display: "inline-flex", alignItems: "center", gap: 6, marginTop: 9,
              fontFamily: F, fontSize: 11, color: C.acc, textDecoration: "none",
              border: `0.5px solid ${C.acc}66`, borderRadius: 6, padding: "6px 10px",
            }}>
              <IcExternalLink size={12} /> Fiche officielle : citation et licence
            </a>
          ) : (
            <div style={{ fontSize: 10, color: C.dim, marginTop: 8, lineHeight: 1.5 }}>
              Jeu hébergé hors du catalogue public Earth Engine : se référer au producteur pour la citation et la licence.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

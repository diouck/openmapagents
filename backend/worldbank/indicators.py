"""
worldbank/indicators.py — Dictionnaire des indicateurs World Bank supportés
Groupés par thème, avec label fr + en et unité.
"""

INDICATORS: dict[str, dict] = {

    # ── Population ─────────────────────────────────────────────
    "SP.POP.TOTL": {
        "label_fr": "Population totale",
        "label_en": "Total population",
        "unit": "habitants",
        "theme": "population",
        "format": "integer",
    },
    "SP.POP.GROW": {
        "label_fr": "Croissance démographique",
        "label_en": "Population growth",
        "unit": "%",
        "theme": "population",
        "format": "float1",
    },
    "SP.URB.TOTL.IN.ZS": {
        "label_fr": "Population urbaine",
        "label_en": "Urban population",
        "unit": "%",
        "theme": "population",
        "format": "float1",
    },
    "SP.POP.65UP.TO.ZS": {
        "label_fr": "Population 65 ans et plus",
        "label_en": "Population 65+ years",
        "unit": "%",
        "theme": "population",
        "format": "float1",
    },
    "EN.POP.DNST": {
        "label_fr": "Densité de population",
        "label_en": "Population density",
        "unit": "hab/km²",
        "theme": "population",
        "format": "float1",
    },

    # ── Économie ───────────────────────────────────────────────
    "NY.GDP.MKTP.CD": {
        "label_fr": "PIB (USD courants)",
        "label_en": "GDP (current USD)",
        "unit": "USD",
        "theme": "economy",
        "format": "float0",
    },
    "NY.GDP.PCAP.CD": {
        "label_fr": "PIB par habitant",
        "label_en": "GDP per capita",
        "unit": "USD/hab",
        "theme": "economy",
        "format": "float0",
    },
    "NY.GDP.MKTP.KD.ZG": {
        "label_fr": "Croissance du PIB",
        "label_en": "GDP growth",
        "unit": "%",
        "theme": "economy",
        "format": "float1",
    },
    "SI.POV.GINI": {
        "label_fr": "Indice de Gini (inégalités)",
        "label_en": "Gini index",
        "unit": "indice",
        "theme": "economy",
        "format": "float1",
    },
    "SL.UEM.TOTL.ZS": {
        "label_fr": "Taux de chômage",
        "label_en": "Unemployment rate",
        "unit": "%",
        "theme": "economy",
        "format": "float1",
    },

    # ── Santé ──────────────────────────────────────────────────
    "SP.DYN.LE00.IN": {
        "label_fr": "Espérance de vie",
        "label_en": "Life expectancy",
        "unit": "ans",
        "theme": "health",
        "format": "float1",
    },
    "SH.DYN.MORT": {
        "label_fr": "Mortalité infantile",
        "label_en": "Child mortality",
        "unit": "pour 1000",
        "theme": "health",
        "format": "float1",
    },
    "SH.XPD.CHEX.GD.ZS": {
        "label_fr": "Dépenses de santé",
        "label_en": "Health expenditure",
        "unit": "% PIB",
        "theme": "health",
        "format": "float1",
    },
    "SH.STA.WASH.P5": {
        "label_fr": "Accès eau potable",
        "label_en": "Access to clean water",
        "unit": "%",
        "theme": "health",
        "format": "float1",
    },

    # ── Éducation ─────────────────────────────────────────────
    "SE.ADT.LITR.ZS": {
        "label_fr": "Taux d'alphabétisation",
        "label_en": "Adult literacy rate",
        "unit": "%",
        "theme": "education",
        "format": "float1",
    },
    "SE.XPD.TOTL.GD.ZS": {
        "label_fr": "Dépenses éducation",
        "label_en": "Education expenditure",
        "unit": "% PIB",
        "theme": "education",
        "format": "float1",
    },

    # ── Environnement ─────────────────────────────────────────
    "EN.ATM.CO2E.PC": {
        "label_fr": "Émissions CO₂ par habitant",
        "label_en": "CO2 emissions per capita",
        "unit": "tonnes/hab",
        "theme": "environment",
        "format": "float2",
    },
    "AG.LND.FRST.ZS": {
        "label_fr": "Couverture forestière",
        "label_en": "Forest area",
        "unit": "% superficie",
        "theme": "environment",
        "format": "float1",
    },
    "EG.ELC.ACCS.ZS": {
        "label_fr": "Accès à l'électricité",
        "label_en": "Access to electricity",
        "unit": "%",
        "theme": "environment",
        "format": "float1",
    },
    "AG.LND.ARBL.ZS": {
        "label_fr": "Terres arables",
        "label_en": "Arable land",
        "unit": "% superficie",
        "theme": "environment",
        "format": "float1",
    },
}

# Index par thème
INDICATORS_BY_THEME: dict[str, list[str]] = {}
for code, meta in INDICATORS.items():
    INDICATORS_BY_THEME.setdefault(meta["theme"], []).append(code)

# Tous les codes supportés
ALL_CODES = list(INDICATORS.keys())


def get_indicator(code: str) -> dict | None:
    return INDICATORS.get(code)


def find_indicator_by_keyword(query: str) -> str | None:
    """
    Recherche d'un indicateur par mots-clés fr/en.
    Retourne le code World Bank le plus pertinent.
    """
    q = query.lower()
    best_score = 0
    best_code  = None

    for code, meta in INDICATORS.items():
        score = 0
        for field in ("label_fr", "label_en", "theme", "unit"):
            words = meta[field].lower().split()
            score += sum(1 for w in words if w in q)
        if score > best_score:
            best_score = score
            best_code  = code

    return best_code if best_score > 0 else "SP.POP.TOTL"  # fallback population

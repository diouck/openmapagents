"""
mcp_servers/mcp_worldbank.py — MCP Server World Bank
=====================================================
Réutilise worldbank/indicators.py et worldbank/fetcher.py existants.
Remplace world_bank_indicator de l'agent.py.

Tools exposés :
    get_indicator           → choroplèthe mondial par indicateur + année
    get_country_profile     → ensemble d'indicateurs clés pour un pays
    list_indicators         → liste des indicateurs disponibles
    compare_countries       → comparaison multi-pays sur un indicateur
    get_indicator_timeseries→ évolution temporelle d'un indicateur
"""

import os
import logging
import asyncio

log = logging.getLogger("mcp_worldbank")

# ── Palettes choroplèthe ──────────────────────────────────────
PALETTES = {
    "default":     ["f7fbff","deebf7","c6dbef","9ecae1",
                    "6baed6","4292c6","2171b5","08519c","08306b"],
    "diverging":   ["d73027","f46d43","fdae61","fee090",
                    "ffffbf","e0f3f8","abd9e9","74add1","4575b4"],
    "sequential":  ["ffffd9","edf8b1","c7e9b4","7fcdbb",
                    "41b6c4","1d91c0","225ea8","253494","081d58"],
    "red_green":   ["d73027","f46d43","fdae61","fee08b",
                    "d9ef8b","a6d96a","66bd63","1a9850"],
    "population":  ["fff5eb","fee6ce","fdd0a2","fdae6b",
                    "fd8d3c","f16913","d94801","a63603","7f2704"],
    "gdp":         ["f7fcf5","e5f5e0","c7e9c0","a1d99b",
                    "74c476","41ab5d","238b45","006d2c","00441b"],
    "health":      ["fff7f3","fde0dd","fcc5c0","fa9fb5",
                    "f768a1","dd3497","ae017e","7a0177","49006a"],
    "education":   ["fff7bc","fee391","fec44f","fe9929",
                    "ec7014","cc4c02","993404","662506","3d1700"],
    "environment": ["f7fcf5","e5f5e0","c7e9c0","a1d99b",
                    "74c476","41ab5d","238b45","006d2c","00441b"],
}

# ── Catégories d'indicateurs ──────────────────────────────────
INDICATOR_CATEGORIES = {
    "economie":     ["NY.GDP.PCAP.CD","NY.GDP.MKTP.CD","NY.GDP.MKTP.KD.ZG",
                     "SL.UEM.TOTL.ZS","FP.CPI.TOTL.ZG","GC.DOD.TOTL.GD.ZS"],
    "population":   ["SP.POP.TOTL","SP.POP.GROW","EN.POP.DNST",
                     "SP.URB.TOTL.IN.ZS","SP.POP.65UP.TO.ZS"],
    "sante":        ["SP.DYN.LE00.IN","SH.DYN.MORT","SH.XPD.CHEX.GD.ZS",
                     "SH.STA.WASH.P5","SH.HIV.INCD.ZS"],
    "education":    ["SE.ADT.LITR.ZS","SE.XPD.TOTL.GD.ZS",
                     "SE.ENR.PRIM.FM.ZS","SE.SEC.ENRR"],
    "environnement":["EN.ATM.CO2E.PC","AG.LND.FRST.ZS","EG.ELC.ACCS.ZS",
                     "AG.LND.ARBL.ZS","ER.H2O.FWTL.ZS"],
    "inegalites":   ["SI.POV.GINI","SI.POV.DDAY","SI.DST.FRST.20"],
}

# ── Mapping ISO2 → ISO3 pour jointure GeoJSON ─────────────────
# Complété dynamiquement depuis pycountry si disponible
ISO2_TO_ISO3 = {
    "AF":"AFG","AL":"ALB","DZ":"DZA","AO":"AGO","AR":"ARG","AM":"ARM",
    "AU":"AUS","AT":"AUT","AZ":"AZE","BD":"BGD","BY":"BLR","BE":"BEL",
    "BJ":"BEN","BO":"BOL","BA":"BIH","BW":"BWA","BR":"BRA","BG":"BGR",
    "BF":"BFA","BI":"BDI","KH":"KHM","CM":"CMR","CA":"CAN","CF":"CAF",
    "TD":"TCD","CL":"CHL","CN":"CHN","CO":"COL","CD":"COD","CG":"COG",
    "CR":"CRI","CI":"CIV","HR":"HRV","CU":"CUB","CY":"CYP","CZ":"CZE",
    "DK":"DNK","DJ":"DJI","DO":"DOM","EC":"ECU","EG":"EGY","SV":"SLV",
    "ET":"ETH","FI":"FIN","FR":"FRA","GA":"GAB","GH":"GHA","GR":"GRC",
    "GT":"GTM","GN":"GIN","GW":"GNB","GY":"GUY","HT":"HTI","HN":"HND",
    "HU":"HUN","IN":"IND","ID":"IDN","IR":"IRN","IQ":"IRQ","IE":"IRL",
    "IL":"ISR","IT":"ITA","JM":"JAM","JP":"JPN","JO":"JOR","KZ":"KAZ",
    "KE":"KEN","KW":"KWT","KG":"KGZ","LA":"LAO","LB":"LBN","LS":"LSO",
    "LR":"LBR","LY":"LBY","LT":"LTU","MG":"MDG","MW":"MWI","MY":"MYS",
    "ML":"MLI","MR":"MRT","MX":"MEX","MD":"MDA","MN":"MNG","MA":"MAR",
    "MZ":"MOZ","MM":"MMR","NA":"NAM","NP":"NPL","NL":"NLD","NZ":"NZL",
    "NI":"NIC","NE":"NER","NG":"NGA","NO":"NOR","OM":"OMN","PK":"PAK",
    "PA":"PAN","PY":"PRY","PE":"PER","PH":"PHL","PL":"POL","PT":"PRT",
    "PR":"PRI","QA":"QAT","RO":"ROU","RU":"RUS","RW":"RWA","SA":"SAU",
    "SN":"SEN","SL":"SLE","SO":"SOM","ZA":"ZAF","SS":"SSD","ES":"ESP",
    "LK":"LKA","SD":"SDN","SR":"SUR","SZ":"SWZ","SE":"SWE","CH":"CHE",
    "SY":"SYR","TW":"TWN","TJ":"TJK","TZ":"TZA","TH":"THA","TG":"TGO",
    "TT":"TTO","TN":"TUN","TR":"TUR","TM":"TKM","UG":"UGA","UA":"UKR",
    "AE":"ARE","GB":"GBR","US":"USA","UY":"URY","UZ":"UZB","VE":"VEN",
    "VN":"VNM","YE":"YEM","ZM":"ZMB","ZW":"ZWE","XK":"XKX","PS":"PSE",
}


def _iso2_to_iso3(iso2: str) -> str:
    iso2 = iso2.upper()
    if iso2 in ISO2_TO_ISO3:
        return ISO2_TO_ISO3[iso2]
    # Essayer pycountry si dispo
    try:
        import pycountry
        c = pycountry.countries.get(alpha_2=iso2)
        if c: return c.alpha_3
    except ImportError:
        pass
    return iso2


def _resolve_palette(indicator_code: str) -> list:
    """Retourne la palette appropriée selon la catégorie d'indicateur."""
    for cat, codes in INDICATOR_CATEGORIES.items():
        if indicator_code in codes:
            mapping = {
                "economie":     PALETTES["gdp"],
                "population":   PALETTES["population"],
                "sante":        PALETTES["health"],
                "education":    PALETTES["education"],
                "environnement":PALETTES["environment"],
                "inegalites":   PALETTES["diverging"],
            }
            return mapping.get(cat, PALETTES["default"])
    return PALETTES["default"]


def _normalize_country_name(name: str) -> str:
    """Normalise un nom de pays en langage naturel → ISO3."""
    name_lower = name.lower().strip()
    # Table de correspondance noms courants fr/en → ISO3
    NAME_MAP = {
        # Français
        "france": "FRA", "allemagne": "DEU", "espagne": "ESP",
        "italie": "ITA", "royaume-uni": "GBR", "angleterre": "GBR",
        "états-unis": "USA", "etats-unis": "USA", "usa": "USA",
        "chine": "CHN", "japon": "JPN", "inde": "IND",
        "brésil": "BRA", "bresil": "BRA", "russie": "RUS",
        "canada": "CAN", "australie": "AUS", "mexique": "MEX",
        "sénégal": "SEN", "senegal": "SEN", "côte d'ivoire": "CIV",
        "maroc": "MAR", "algérie": "DZA", "tunisie": "TUN",
        "nigeria": "NGA", "ghana": "GHA", "kenya": "KEN",
        "afrique du sud": "ZAF", "cameroun": "CMR", "mali": "MLI",
        "niger": "NER", "burkina faso": "BFA", "guinée": "GIN",
        "togo": "TGO", "bénin": "BEN", "benin": "BEN",
        "égypte": "EGY", "egypte": "EGY", "éthiopie": "ETH",
        "tanzanie": "TZA", "ouganda": "UGA", "rwanda": "RWA",
        "congo": "COG", "rdc": "COD", "mauritanie": "MRT",
        "tchad": "TCD", "soudan": "SDN", "libye": "LBY",
        # Anglais
        "france": "FRA", "germany": "DEU", "spain": "ESP",
        "italy": "ITA", "united kingdom": "GBR", "uk": "GBR",
        "united states": "USA", "china": "CHN", "japan": "JPN",
        "india": "IND", "brazil": "BRA", "russia": "RUS",
        "south africa": "ZAF", "ivory coast": "CIV",
        "egypt": "EGY", "ethiopia": "ETH",
    }
    if name_lower in NAME_MAP:
        return NAME_MAP[name_lower]
    # Essayer pycountry
    try:
        import pycountry
        matches = pycountry.countries.search_fuzzy(name)
        if matches:
            return matches[0].alpha_3
    except (ImportError, LookupError):
        pass
    return name.upper()[:3]


# ═══════════════════════════════════════════════════════════════
# WORLD BANK SERVER
# ═══════════════════════════════════════════════════════════════

class WorldbankServer:
    """
    MCP Server World Bank.
    Réutilise worldbank/indicators.py et worldbank/fetcher.py existants.
    """

    def __init__(self):
        self._indicators = None
        self._loaded     = False

    def _load(self):
        """Charge les modules WorldBank existants (lazy)."""
        if not self._loaded:
            try:
                from worldbank.indicators import INDICATORS, find_indicator_by_keyword
                from worldbank.fetcher    import build_choropleth, fetch_latest_year
                self._INDICATORS              = INDICATORS
                self._find_by_keyword         = find_indicator_by_keyword
                self._build_choropleth        = build_choropleth
                self._fetch_latest_year       = fetch_latest_year
                self._loaded = True
                log.info(f"✓ WorldBank modules chargés — {len(INDICATORS)} indicateurs")
            except ImportError as e:
                raise RuntimeError(
                    f"worldbank/ modules non trouvés: {e}. "
                    f"Vérifiez worldbank/indicators.py et worldbank/fetcher.py"
                ) from e

    async def call(self, tool: str, args: dict) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run, tool, args)

    def _run(self, tool: str, args: dict) -> dict:
        fn = {
            "get_indicator":              self.get_indicator,
            "world_bank_indicator":       self.get_indicator,   # alias rétrocompat
            "list_indicators":            self.list_indicators,
            "get_country_profile":        self.get_country_profile,
            "compare_countries":          self.compare_countries,
            "get_indicator_timeseries":   self.get_indicator_timeseries,
        }.get(tool)
        if not fn:
            return {"error": f"WorldBank tool inconnu: '{tool}'"}
        try:
            self._load()
            return fn(args)
        except RuntimeError as e:
            return {"error": str(e)}
        except Exception as e:
            log.error(f"WorldBank {tool}: {e}")
            return {"error": f"Erreur WorldBank: {e}", "tool": tool}

    # ─── GET INDICATOR ────────────────────────────────────────

    def get_indicator(self, a: dict) -> dict:
        """
        Choroplèthe mondial pour un indicateur WorldBank.

        Args:
            indicator: code WB (NY.GDP.PCAP.CD) ou alias court
            year:      année (int) — défaut: dernière année dispo
            keyword:   mot-clé si code inconnu (ex: "mortalité infantile")
            palette:   palette custom (optionnel)

        Returns:
            GeoJSON FeatureCollection mondial + métadonnées choroplèthe
        """
        code    = a.get("indicator", "")
        year    = a.get("year")
        keyword = a.get("keyword")

        # Résolution du code indicateur
        if keyword and (not code or code not in self._INDICATORS):
            code = self._find_by_keyword(keyword)
            log.info(f"Keyword '{keyword}' → code '{code}'")

        if not code or code not in self._INDICATORS:
            # Fallback : population mondiale
            log.warning(f"Code '{code}' non trouvé → fallback SP.POP.TOTL")
            code = "SP.POP.TOTL"

        meta = self._INDICATORS[code]

        # Année par défaut
        if not year:
            year, _ = self._fetch_latest_year(code)
            log.info(f"Année auto-détectée: {year} pour {code}")

        # Appel à l'implémentation existante
        result = self._build_choropleth(code, year, meta["label_fr"], meta["unit"])

        # Enrichir avec infos supplémentaires
        result.update({
            "indicator_code": code,
            "indicator_label": meta.get("label_fr", code),
            "year":           year,
            "unit":           meta.get("unit", ""),
            "palette":        a.get("palette") or _resolve_palette(code),
            "source":         "World Bank API",
        })

        # S'assurer que action est bien défini
        if "action" not in result:
            result["action"] = "add_choropleth"

        return result

    # ─── LIST INDICATORS ──────────────────────────────────────

    def list_indicators(self, a: dict) -> dict:
        """
        Liste tous les indicateurs disponibles, optionnellement filtrés.

        Args:
            category:  economie | population | sante | education |
                       environnement | inegalites (optionnel)
            search:    texte libre pour filtrer par label (optionnel)
        """
        category = a.get("category")
        search   = a.get("search", "").lower()

        indicators = []
        for code, meta in self._INDICATORS.items():
            # Filtre catégorie
            if category:
                cat_codes = INDICATOR_CATEGORIES.get(category, [])
                if code not in cat_codes:
                    continue
            # Filtre texte
            if search:
                label = meta.get("label_fr", "").lower()
                if search not in label and search not in code.lower():
                    continue
            indicators.append({
                "code":     code,
                "label_fr": meta.get("label_fr", code),
                "unit":     meta.get("unit", ""),
                "category": next(
                    (cat for cat, codes in INDICATOR_CATEGORIES.items()
                     if code in codes), "autre"
                ),
            })

        return {
            "action":      "show_list",
            "indicators":  indicators,
            "total":       len(indicators),
            "categories":  list(INDICATOR_CATEGORIES.keys()),
            "filter":      {"category": category, "search": search or None},
        }

    # ─── COUNTRY PROFILE ──────────────────────────────────────

    def get_country_profile(self, a: dict) -> dict:
        """
        Profil complet d'un pays : ensemble d'indicateurs clés.

        Args:
            country:  nom ou ISO3 du pays (ex: "Sénégal", "SEN", "France")
            year:     année (défaut: dernière dispo)
            indicators: liste de codes WB (défaut: sélection clés)
        """
        country    = a.get("country", "")
        year       = a.get("year")
        ind_codes  = a.get("indicators") or [
            "NY.GDP.PCAP.CD",   # PIB/hab
            "SP.POP.TOTL",      # Population
            "SP.DYN.LE00.IN",   # Espérance de vie
            "SH.DYN.MORT",      # Mortalité infantile
            "SE.ADT.LITR.ZS",   # Alphabétisation
            "EN.ATM.CO2E.PC",   # CO2/hab
            "EG.ELC.ACCS.ZS",   # Accès électricité
            "AG.LND.FRST.ZS",   # Couverture forestière
            "SL.UEM.TOTL.ZS",   # Chômage
            "SI.POV.GINI",      # Inégalités Gini
        ]

        if not country:
            return {"error": "country requis (nom ou code ISO3)"}

        # Normaliser vers ISO3
        iso3 = _normalize_country_name(country)
        log.info(f"Country profile: '{country}' → ISO3={iso3}")

        import requests
        profile = {
            "country":     country,
            "iso3":        iso3,
            "year":        year,
            "indicators":  {},
        }
        actual_year = year

        for code in ind_codes:
            if code not in self._INDICATORS:
                continue
            meta = self._INDICATORS[code]
            try:
                # Année auto si non fournie
                if not actual_year:
                    actual_year, _ = self._fetch_latest_year(code)

                url = (
                    f"https://api.worldbank.org/v2/country/{iso3}"
                    f"/indicator/{code}"
                    f"?format=json&date={actual_year}&mrv=1"
                )
                resp = requests.get(url, timeout=10)
                data = resp.json()
                if len(data) > 1 and data[1]:
                    val = data[1][0].get("value")
                    if val is not None:
                        profile["indicators"][code] = {
                            "label": meta.get("label_fr", code),
                            "value": round(float(val), 2),
                            "unit":  meta.get("unit", ""),
                            "year":  data[1][0].get("date", actual_year),
                        }
            except Exception as e:
                log.warning(f"Country profile {code} for {iso3}: {e}")

        profile["year"] = actual_year
        profile["action"] = "show_country_profile"
        profile["indicator_count"] = len(profile["indicators"])
        return profile

    # ─── COMPARE COUNTRIES ────────────────────────────────────

    def compare_countries(self, a: dict) -> dict:
        """
        Compare plusieurs pays sur un indicateur.

        Args:
            countries:  liste de noms ou ISO3 (ex: ["Sénégal","Mali","Ghana"])
            indicator:  code WB
            year:       année (défaut: dernière dispo)

        Returns:
            tableau comparatif trié par valeur
        """
        countries = a.get("countries", [])
        code      = a.get("indicator", "SP.POP.TOTL")
        year      = a.get("year")

        if not countries:
            return {"error": "countries requis: liste de pays"}
        if len(countries) > 20:
            return {"error": "Maximum 20 pays pour la comparaison"}
        if code not in self._INDICATORS:
            code = self._find_by_keyword(code) or "SP.POP.TOTL"

        meta = self._INDICATORS[code]
        if not year:
            year, _ = self._fetch_latest_year(code)

        import requests
        results = []
        for country_name in countries:
            iso3 = _normalize_country_name(country_name)
            try:
                url = (
                    f"https://api.worldbank.org/v2/country/{iso3}"
                    f"/indicator/{code}"
                    f"?format=json&date={year}&mrv=1"
                )
                resp = requests.get(url, timeout=8)
                data = resp.json()
                if len(data) > 1 and data[1]:
                    val = data[1][0].get("value")
                    cn  = data[1][0].get("country", {}).get("value", country_name)
                    if val is not None:
                        results.append({
                            "country": cn,
                            "iso3":    iso3,
                            "value":   round(float(val), 3),
                            "year":    data[1][0].get("date", year),
                        })
            except Exception as e:
                log.warning(f"Compare {country_name}: {e}")

        # Trier par valeur décroissante
        results.sort(key=lambda x: x["value"], reverse=True)
        for i, r in enumerate(results):
            r["rank"] = i + 1

        return {
            "action":          "show_comparison",
            "indicator_code":  code,
            "indicator_label": meta.get("label_fr", code),
            "unit":            meta.get("unit", ""),
            "year":            year,
            "countries":       results,
            "count":           len(results),
        }

    # ─── INDICATOR TIMESERIES ─────────────────────────────────

    def get_indicator_timeseries(self, a: dict) -> dict:
        """
        Évolution temporelle d'un indicateur pour un ou plusieurs pays.

        Args:
            indicator:  code WB
            countries:  liste ISO3 ou noms (défaut: monde "WLD")
            start_year: int (défaut: 2000)
            end_year:   int (défaut: année courante - 1)

        Returns:
            séries temporelles par pays — prêtes pour graphique
        """
        code       = a.get("indicator", "NY.GDP.PCAP.CD")
        countries  = a.get("countries", ["WLD"])
        start_year = a.get("start_year", 2000)
        end_year   = a.get("end_year",   2023)

        if code not in self._INDICATORS:
            code = self._find_by_keyword(code) or "NY.GDP.PCAP.CD"

        meta = self._INDICATORS[code]
        if len(countries) > 10:
            return {"error": "Maximum 10 pays pour la série temporelle"}

        import requests
        series = {}
        for country_name in countries:
            iso3 = _normalize_country_name(country_name)
            try:
                url = (
                    f"https://api.worldbank.org/v2/country/{iso3}"
                    f"/indicator/{code}"
                    f"?format=json"
                    f"&date={start_year}:{end_year}"
                    f"&per_page=100"
                )
                resp = requests.get(url, timeout=10)
                data = resp.json()
                if len(data) > 1 and data[1]:
                    points = [
                        {"year": int(d["date"]), "value": d["value"]}
                        for d in data[1]
                        if d.get("value") is not None
                    ]
                    points.sort(key=lambda x: x["year"])
                    cn = data[1][0].get("country", {}).get("value", country_name)
                    series[iso3] = {
                        "country_name": cn,
                        "iso3":         iso3,
                        "data":         points,
                        "count":        len(points),
                    }
            except Exception as e:
                log.warning(f"Timeseries {country_name}: {e}")

        return {
            "action":          "show_timeseries",
            "indicator_code":  code,
            "indicator_label": meta.get("label_fr", code),
            "unit":            meta.get("unit", ""),
            "start_year":      start_year,
            "end_year":        end_year,
            "series":          series,
            "country_count":   len(series),
        }

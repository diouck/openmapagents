"""
agents/worldbank_agent.py — Agent WorldBank
============================================
Orchestre mcp_worldbank + mcp_overture via MCPClient.
Joint données WB + GeoJSON pays pour choroplèthe.
"""

import logging
from agents.base_agent import BaseAgent

log = logging.getLogger("worldbank_agent")

# Mapping mots-clés → codes WB
KEYWORD_INDICATORS = {
    "pib par habitant":       "NY.GDP.PCAP.CD",
    "gdp per capita":         "NY.GDP.PCAP.CD",
    "pib total":              "NY.GDP.MKTP.CD",
    "croissance pib":         "NY.GDP.MKTP.KD.ZG",
    "population":             "SP.POP.TOTL",
    "densité population":     "EN.POP.DNST",
    "urbanisation":           "SP.URB.TOTL.IN.ZS",
    "croissance population":  "SP.POP.GROW",
    "espérance de vie":       "SP.DYN.LE00.IN",
    "mortalité infantile":    "SH.DYN.MORT",
    "dépenses santé":         "SH.XPD.CHEX.GD.ZS",
    "alphabétisation":        "SE.ADT.LITR.ZS",
    "dépenses éducation":     "SE.XPD.TOTL.GD.ZS",
    "chômage":                "SL.UEM.TOTL.ZS",
    "co2":                    "EN.ATM.CO2E.PC",
    "forêt":                  "AG.LND.FRST.ZS",
    "électricité":            "EG.ELC.ACCS.ZS",
    "inégalités":             "SI.POV.GINI",
    "gini":                   "SI.POV.GINI",
    "pauvreté":               "SI.POV.DDAY",
    "eau potable":            "SH.STA.WASH.P5",
}


def _detect_indicator(query: str) -> str:
    """Détecte le code WB depuis la query."""
    q = query.lower()
    for kw, code in KEYWORD_INDICATORS.items():
        if kw in q:
            return code
    return ""


class WorldBankAgent(BaseAgent):

    def __init__(self):
        super().__init__(
            name="worldbank_agent",
            domain="worldbank",
            server="worldbank",
        )

    async def run(self, query: str, context: dict,
                  rag_tools: list, agent_config: dict = None) -> dict:

        from mcp_client import get_mcp_client
        client = get_mcp_client()
        q      = query.lower()

        # ── Profil pays ──────────────────────────────────────
        if any(w in q for w in ["profil","fiche","bilan"]):
            return await self._country_profile(client, query, context)

        # ── Comparaison pays ─────────────────────────────────
        if any(w in q for w in ["comparer","comparaison","classement","ranking"]):
            return await self._compare(client, query, context)

        # ── Série temporelle ─────────────────────────────────
        if any(w in q for w in ["évolution","tendance","depuis","historique","courbe"]):
            return await self._timeseries(client, query, context)

        # ── Choroplèthe mondial (défaut) ─────────────────────
        return await self._choropleth(client, query, context)

    # ── CHOROPLÈTHE ──────────────────────────────────────────

    async def _choropleth(self, client, query, context) -> dict:
        indicator = (
            _detect_indicator(query)
            or context.get("current_indicator","SP.POP.TOTL")
        )

        import re
        year = None
        m = re.search(r'\b(19|20)\d{2}\b', query)
        if m: year = int(m.group())

        args = {"indicator": indicator}
        if year: args["year"] = year

        r = await client.call_tool("get_indicator", args, server_name="worldbank")

        if "error" in r:
            return {"text":f"Erreur WorldBank: {r['error']}",
                    "tool_calls":[{"name":"get_indicator","args":args}],
                    "tool_results":[r]}

        label = r.get("indicator_label", indicator)
        yr    = r.get("year","")
        return {
            "text":         f"Carte mondiale **{label}** ({yr}) affichée.",
            "tool_calls":   [{"name":"get_indicator","args":args}],
            "tool_results": [r],
        }

    # ── PROFIL PAYS ──────────────────────────────────────────

    async def _country_profile(self, client, query, context) -> dict:
        import re
        # Extraire le nom du pays
        m = re.search(
            r'(?:profil|fiche|bilan)\s+(?:du|de|des?|of)?\s*([A-ZÀ-Ýa-zà-ý\s]+)',
            query, re.IGNORECASE
        )
        country = m.group(1).strip() if m else ""

        if not country:
            return {
                "text": "Quel pays souhaitez-vous analyser ?",
                "tool_calls":[], "tool_results":[],
            }

        args = {"country": country}
        r    = await client.call_tool("get_country_profile", args, server_name="worldbank")
        if "error" in r:
            return {"text":f"Erreur: {r['error']}","tool_calls":[],"tool_results":[]}

        n = r.get("indicator_count",0)
        return {
            "text":         f"Profil **{country}** : {n} indicateurs chargés.",
            "tool_calls":   [{"name":"get_country_profile","args":args}],
            "tool_results": [r],
        }

    # ── COMPARAISON ──────────────────────────────────────────

    async def _compare(self, client, query, context) -> dict:
        import re
        # Détecter noms de pays (mots capitalisés)
        countries = re.findall(r'\b([A-ZÀ-Ý][a-zà-ý]+(?:\s+[A-ZÀ-Ý][a-zà-ý]+)*)\b', query)
        countries = [c for c in countries if len(c) > 2 and c.lower() not in
                     ("comparer","comparaison","classement","ranking","quel","plus")][:10]

        if len(countries) < 2:
            return {
                "text": "Précisez les pays à comparer. Ex: 'comparer France Allemagne Espagne'.",
                "tool_calls":[], "tool_results":[],
            }

        indicator = _detect_indicator(query) or "NY.GDP.PCAP.CD"
        args = {"countries": countries, "indicator": indicator}
        r    = await client.call_tool("compare_countries", args, server_name="worldbank")

        label = r.get("indicator_label", indicator)
        n     = r.get("count",0)
        return {
            "text":         f"Comparaison **{label}** pour {n} pays.",
            "tool_calls":   [{"name":"compare_countries","args":args}],
            "tool_results": [r],
        }

    # ── SÉRIE TEMPORELLE ─────────────────────────────────────

    async def _timeseries(self, client, query, context) -> dict:
        import re
        indicator = _detect_indicator(query) or context.get("current_indicator","NY.GDP.PCAP.CD")

        years = re.findall(r'\b(19|20)\d{2}\b', query)
        start_year = int(min(years)) if years else 2000
        end_year   = int(max(years)) if years else 2023

        countries  = re.findall(r'\b([A-ZÀ-Ý][a-zà-ý]+)\b', query)
        countries  = [c for c in countries if len(c)>3][:5] or ["WLD"]

        args = {
            "indicator":  indicator,
            "countries":  countries,
            "start_year": start_year,
            "end_year":   end_year,
        }
        r = await client.call_tool("get_indicator_timeseries", args, server_name="worldbank")
        label = r.get("indicator_label", indicator)
        return {
            "text":         f"Évolution **{label}** {start_year}–{end_year}.",
            "tool_calls":   [{"name":"get_indicator_timeseries","args":args}],
            "tool_results": [r],
        }

# agents/__init__.py
from agents.base_agent       import BaseAgent, AgentResult
from agents.geo_data_agent   import GeoDataAgent
from agents.satellite_agent  import SatelliteAgent
from agents.database_agent   import DatabaseAgent
from agents.routing_agent    import RoutingAgent
from agents.spatial_agent    import SpatialAgent
from agents.worldbank_agent  import WorldBankAgent
from agents.router           import classify, get_domain, get_router

AGENT_REGISTRY: dict[str, type[BaseAgent]] = {
    "geo_data":   GeoDataAgent,
    "satellite":  SatelliteAgent,
    "database":   DatabaseAgent,
    "routing":    RoutingAgent,
    "spatial":    SpatialAgent,
    "world_data": WorldBankAgent,
}

def get_agent(domain: str) -> BaseAgent:
    cls = AGENT_REGISTRY.get(domain)
    if cls is None:
        raise ValueError(f"Domaine inconnu : '{domain}'. Valides : {list(AGENT_REGISTRY.keys())}")
    return cls()

__all__ = [
    "BaseAgent", "AgentResult",
    "GeoDataAgent", "SatelliteAgent", "DatabaseAgent",
    "RoutingAgent", "SpatialAgent", "WorldBankAgent",
    "AGENT_REGISTRY", "get_agent",
    "classify", "get_domain", "get_router",
]

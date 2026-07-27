from agents.base_agent import BaseAgent, AgentResult
from agents.geo_data_agent import GeoDataAgent
from agents.satellite_agent import SatelliteAgent
from agents.database_agent import DatabaseAgent
from agents.routing_agent import RoutingAgent
from agents.spatial_agent import SpatialAgent
from agents.router import classify, get_domain, get_router
from agents.worldbank_agent import WorldBankAgent


AGENT_REGISTRY = {
    'geo_data':  GeoDataAgent,
    'satellite': SatelliteAgent,
    'database':  DatabaseAgent,
    'routing':   RoutingAgent,
    'spatial':   SpatialAgent,
    'world_data': WorldBankAgent,
}

def get_agent(domain):
    cls = AGENT_REGISTRY.get(domain)
    if cls is None:
        raise ValueError(f"Domaine inconnu : '{domain}'")
    return cls()

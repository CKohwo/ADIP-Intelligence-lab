# Master orchestrator......

from llm_system.generators.api_generator import run_api_insights
from llm_system.generators.scraper_generator import run_scraper_insights


def run_insight_pipeline(top_n: int = 10) -> dict:
    """Generate all ADIP insights."""
    print("Running API insights...")
    api = run_api_insights(top_n)
    print("Running scraper insights...")
    scraper = run_scraper_insights(top_n)
    return {"api_ingest": api, "scraper": scraper}



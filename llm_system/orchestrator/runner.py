# Master orchestrator......
import time 

from llm_system.generators.api_generator import run_api_insights
from llm_system.generators.scraper_generator import run_scraper_insights


def run_insight_pipeline(top_n: int = 10) -> dict:
    """Generate all ADIP insights."""
    print("Running API insights...")
    api = run_api_insights(top_n)
    time.sleep(15)  # Wait for 15 seconds before generating scraper insights

    print("Running scraper insights...")
    scraper = run_scraper_insights(top_n)
    time.sleep(15)  # Wait for 15 seconds before returning results
    
    return {"api_ingest": api, "scraper": scraper}

  
import json
import time

from llm_system.context.scraper_context import build_scraper_context
from llm_system.prompts.scraper_prompt import build_prompt, CATEGORY_PROMPT, BRAND_PROMPT
from llm_system.llm_agent.agents import generate_llm_insight


def generate_category_insight(top_n: int = 10) -> dict:
    context = build_scraper_context(top_n=top_n)["category_context"]
    prompt = build_prompt(CATEGORY_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="category_insight", ttl=14400)


def generate_scraper_brand_insight(top_n: int = 10) -> dict:
    context = build_scraper_context(top_n=top_n)["brand_context"]
    prompt = build_prompt(BRAND_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="sc_brand_insight", ttl=14400)


def run_scraper_insights(top_n: int = 10) -> dict:
    insights = {}

    insights["category"] = generate_category_insight(top_n)
    time.sleep(15)  # Wait for 15 seconds before generating brand insights

    insights["brand"] = generate_scraper_brand_insight(top_n)
    
    return insights
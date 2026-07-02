from llm_system.context.scraper_context import build_scraper_context
from llm_system.prompts.scraper_prompt import build_prompt, CATEGORY_PROMPT, SCRAPER_BRAND_PROMPT
from llm_system.llm_agent.agent import generate_llm_insight


def generate_category_insight(top_n: int = 20) -> dict:
    context = build_scraper_context(top_n=top_n)["category_context"]
    prompt = build_prompt(CATEGORY_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="scraper_category_insight", ttl=14400)


def generate_scraper_brand_insight(top_n: int = 20) -> dict:
    context = build_scraper_context(top_n=top_n)["brand_context"]
    prompt = build_prompt(SCRAPER_BRAND_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="scraper_brand_insight", ttl=14400)


def run_scraper_insights(top_n: int = 20) -> dict:
    return {
        "category": generate_category_insight(top_n),
        "brand": generate_scraper_brand_insight(top_n),
    }

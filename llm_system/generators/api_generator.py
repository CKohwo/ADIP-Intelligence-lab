from llm_system.context.api_context import build_api_context
from llm_system.prompts.api_prompt import build_prompt, PRODUCT_PROMPT, BRAND_PROMPT, SELLER_PROMPT
from llm_system.llm_agent.agent import generate_llm_insight


def generate_product_insight(top_n: int = 20) -> dict:
    context = build_api_context(top_n=top_n)["product_context"]
    prompt = build_prompt(PRODUCT_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="api_product_insight", ttl=14400)


def generate_brand_insight(top_n: int = 20) -> dict:
    context = build_api_context(top_n=top_n)["brand_context"]
    prompt = build_prompt(BRAND_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="api_brand_insight", ttl=14400)


def generate_seller_insight(top_n: int = 20) -> dict:
    context = build_api_context(top_n=top_n)["seller_context"]
    prompt = build_prompt(SELLER_PROMPT, context)
    return generate_llm_insight(prompt, cache_key="api_seller_insight", ttl=14400)


def run_api_insights(top_n: int = 20) -> dict:
    """Run all API insight generators."""
    return {
        "product": generate_product_insight(top_n),
        "brand": generate_brand_insight(top_n),
        "seller": generate_seller_insight(top_n),
    } 

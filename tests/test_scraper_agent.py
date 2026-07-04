import sys
from pathlib import Path 

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from llm_system.context.scraper_context import build_scraper_context
from llm_system.prompts.scraper_prompt import build_prompt, CATEGORY_PROMPT, BRAND_PROMPT
from llm_system.generators.scraper_generator import generate_scraper_brand_insight, generate_category_insight 
from llm_system.llm_agent.agent import generate_llm_insight

print("[TEST] calling API")
try:
    print("[TEST] calling build_scraper_context")
    context = build_scraper_context(top_n=20)["category_context"]

    print("[TEST] calling build_prompt")
    prompt = build_prompt(CATEGORY_PROMPT, context)

    print("[TEST] calling generate_llm_insight")
    llm_response = generate_llm_insight(prompt= prompt, cache_key="scraper_insight_test", ttl=14400)

    print("[TEST] SUCCESS:", llm_response)
    print(llm_response.get("market summary", "No market summary found in response."))  


except Exception as e:
    print("[TEST] FAILED:", e) 
 
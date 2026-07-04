import sys
from pathlib import Path 

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from llm_system.context.api_context import build_api_context
from llm_system.prompts.api_prompt import build_prompt, SELLER_PROMPT, BRAND_PROMPT, PRODUCT_PROMPT
from llm_system.generators.api_generator import generate_brand_insight, generate_product_insight, generate_seller_insight

print("[TEST] calling API")
try:
    print("[TEST] calling build_api_context")
    context = build_api_context(top_n=20)["product_context"]

    print("[TEST] calling build_prompt")
    prompt = build_prompt(PRODUCT_PROMPT, context)

    print("[TEST] calling generate_llm_insight")
    llm_response = generate_llm_insight(prompt= prompt, cache_key="api_insight_product_test", ttl=14400)

    print("[TEST] SUCCESS:", llm_response)
    print(llm_response.get("executive summary", "No executive summary found in response."))  


except Exception as e:
    print("[TEST] FAILED:", e) 
 
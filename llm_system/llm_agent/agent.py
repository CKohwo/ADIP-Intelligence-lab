"""
llm_system/llm_agent/agent.py

ADIP LLM Agent — MVP version.
One API call. One cache check. That's it.
"""

import json
import os
import time
from pathlib import Path

from google import genai 
from dotenv import load_dotenv

CACHE_DIR = Path("data/llm_insight/ai_insight_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

AGENT = genai.Client(api_key=api_key) 

def generate_llm_insight(prompt: str, cache_key: str = None, ttl: int = 14400) -> dict:
    """
    Send prompt to Gemini, return parsed JSON.
    If cache_key provided and cache is fresh, return cached result instead.
    """
    # Check cache
    if cache_key:
        cache_path = CACHE_DIR / f"{cache_key}.json"
        if cache_path.exists():
            age = time.time() - cache_path.stat().st_mtime
            if age <= ttl:
                print(f"[CACHE HIT] {cache_key}")
                return json.loads(cache_path.read_text())

    # Call API
    print(f"[API CALL] cache_key: {cache_key}, model:gemini-3.5-flash")
    response = AGENT.models.generate_content(
        model="gemini-3.5-flash",
        contents = prompt,
        config = genai.types.GenerateContentConfig(
            temperature=0.2,
            response_mime_type="application/json",
        )
    )

    raw = response.text

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse JSON from LLM response: {e}")
        raise RuntimeError("Invalid JSON response from LLM.")


    # Write cache
    if cache_key:
        cache_path = CACHE_DIR / f"{cache_key}.json"
        cache_path.write_text(json.dumps(result, indent=2))
        print(f"[CACHE WRITE] {cache_key} -> {cache_path}")

    return result
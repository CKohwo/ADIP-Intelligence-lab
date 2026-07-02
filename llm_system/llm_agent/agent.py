"""
llm_system/llm_agent/agent.py

ADIP LLM Agent — MVP version.
One API call. One cache check. That's it.
"""

import json
import os
import time
from pathlib import Path

from openai import OpenAI 
from dotenv import load_dotenv

CACHE_DIR = Path("data/llm_insight/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()

def generate_api():
    """
    Retrieve the API key and generate an OpenAI API client for Gemini.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set.")   

    agent = OpenAI(
            api_key= api_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/" 
        )

    return agent


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
                return json.loads(cache_path.read_text())

    # Call API
    agent = generate_api()

    response = agent.chat.completions.create(
        model="gemini-3.5-flash",
        messages=[
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        response_format={"type": "json_object"},
    )

    try:
        result = json.loads(response.choices[0].message.content)
    except json.JSONDecodeError:
        raise RuntimeError("Invalid JSON response from LLM.")


    # Write cache
    if cache_key:
        cache_path = CACHE_DIR / f"{cache_key}.json"
        cache_path.write_text(json.dumps(result, indent=2))

    return result
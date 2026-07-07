"""
llm_system/llm_agent/agent.py

ADIP LLM Agent — Multi-provider, tiered fallback, production-grade.
Backends: Google Gemini (primary), Google Gemini 1.5 (secondary), SiliconFlow Qwen (tertiary).
"""

import json
import os
import time
from pathlib import Path
from typing import Dict, Any

from google import genai
from google.genai.errors import ClientError
from openai import OpenAI
from dotenv import load_dotenv

from llm_system.llm_agent.providers import BACKENDS

CACHE_DIR = Path("data/llm_insight")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()

 
# ==========================================================
# PROVIDER INITIALIZATION
# ==========================================================

BACKENDS = BACKENDS  # Ensure BACKENDS is imported from providers.py 


# Google Gemini clients (same SDK, different models)
GEMINI_CLIENT = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
 
  

# ==========================================================
# CORE FUNCTION
# ==========================================================

def generate_llm_insight(
    prompt: str,
    cache_key: str = None,
    ttl: int = 14400,
) -> dict:
    """
    Generate structured JSON insight with multi-provider fallback.
    
    Tries all enabled backends in order. If all fail, returns stale cache or raises.
    """
    # Check fresh cache
    if cache_key:
        cached = _read_cache(cache_key, ttl)
        if cached is not None:
            print(f"[CACHE HIT] {cache_key}")
            return cached

    # Try each enabled backend
    last_error = None
    for backend in BACKENDS:
        print(f"[API CALL] backend={backend['name']}, cache_key={cache_key}")
        try:
            result = call_backend(backend, prompt)
            write_cache(cache_key, result)
            print(f"[SUCCESS] backend={backend['name']}")
            return result
        except Exception as e:
            print(f"[FAIL] backend={backend['name']}: {type(e).__name__}: {e}")
            last_error = e
            continue

    # All backends failed — try stale cache
    if cache_key:
        stale = _read_cache(cache_key, ttl=float("inf"))
        if stale is not None:
            print(f"[STALE FALLBACK] Returning cached insight for {cache_key}")
            stale["_meta"] = {
                "stale": True,
                "reason": f"All backends failed. Last error: {type(last_error).__name__}",
                "fallback_backend": "cache",
            }
            return stale

    # Nothing left
    raise RuntimeError(
        f"All LLM backends failed. Last error: {type(last_error).__name__}: {last_error}"
    )


# ==========================================================
# BACKEND CALLERS
# ==========================================================

def call_backend(backend: dict, prompt: str) -> dict:
    """Route to correct caller based on provider."""
    if backend["provider"] == "google":
        return call_google(backend, prompt)
    raise ValueError(f"Unknown provider: {backend['provider']}")


def call_google(backend: dict, prompt: str) -> dict:
    """Call Google GenAI SDK with retry for rate limits."""
    for attempt in range(1, 4):
        try:
            response = backend["client"].models.generate_content(
                model=backend["model"],
                contents=prompt,
                config=genai.types.GenerateContentConfig(
                    temperature=0.2,
                    response_mime_type="application/json",
                ),
            )
            raw = response.text
            return json.loads(raw)
        
        except ClientError as e:
            if e.code == 429 and attempt < 3:
                wait = min(2 ** attempt, 60)
                print(f"[RATE LIMIT] {backend['name']} attempt {attempt}, waiting {wait}s...")
                time.sleep(wait)
                continue
            raise
 

# ==========================================================
# CACHE UTILITIES
# ==========================================================

def _read_cache(cache_key: str, ttl: float) -> dict | None:
    if not cache_key:
        return None
    path = CACHE_DIR / f"{cache_key}.json"
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age > ttl:
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def write_cache(cache_key: str, data: dict) -> None:
    if not cache_key:
        return
    path = CACHE_DIR / f"{cache_key}.json"
    path.write_text(json.dumps(data, indent=2))
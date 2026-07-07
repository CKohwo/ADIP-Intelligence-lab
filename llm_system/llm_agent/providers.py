"""
These are the available LLM backends that can be used by the LLM agent. Each backend is represented as a dictionary containing the following keys:
- "name": The name of the backend.
- "provider": The provider of the backend (e.g., "google" or "siliconflow").
- "client": The client object used to interact with the backend.
"""
from dotenv import load_dotenv
from google import genai
import os
from typing import Any, Dict

load_dotenv() 

# Google Gemini clients (same SDK, different models)
GEMINI_CLIENT = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
 
# ==========================================================
# BACKEND REGISTRY
# ==========================================================

BACKENDS: list[Dict[str, Any]] = [
    {
        "name": "gemini-3.5-flash",
        "provider": "google",
        "client": GEMINI_CLIENT,
        "model": "gemini-3.5-flash",
    },
    {
        "name": "gemini-2.5-flash",
        "provider": "google",
        "client": GEMINI_CLIENT,
        "model": "gemini-2.5-flash", 
    },
    {
        "name": "gemini-1.5-flash",
        "provider": "google",
        "client": GEMINI_CLIENT,
        "model": "gemini-1.5-flash", 
},
]

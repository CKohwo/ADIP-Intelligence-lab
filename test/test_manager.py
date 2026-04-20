import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from intelligence_system.data_manager import fetch_via_http
from intelligence_system.data_manager import requests_sessions
from config import URL_ENDPOINT

# THIS WILL FEATURE THE API AUTH RAW URL 
data = "api_auth" 
URL = URL_ENDPOINT[data]

if __name__ == "__main__":
    print(f"--- 🧪 Testing Advanced Bridge {data} ---")
     
    # First Run: Should hit the internet
    try:    
        df = fetch_via_http(URL, cache_name=f"{data}.csv") 
        if df is not None:
            print(f"Data Shape: {df.shape}")
            print(df.head())

    except Exception as e:
        print(f"fetch failed: {e}")    
    
    print("\n--- 🧪 Testing Cache (Run 2) ---")
    # Second Run:.."Loading from local cache"..
    df_cached = fetch_via_http(URL, cache_name =f"{data}.csv")

    
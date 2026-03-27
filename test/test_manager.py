import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from diss.data_manager import fetch_via_http
from config import scraper_url

# THIS WILL FEATURE THE API AUTH RAW URL 
URL = scraper_url

if __name__ == "__main__":
    print("--- 🧪 Testing Advanced Bridge ---")
    try:
        # First Run: Should hit the internet
        df = fetch_via_http(URL, cache_name="scraper_data.csv") 
        if df is not None:
            print(f"Data Shape: {df.shape}")
            print(df.head())
    except Exception as e:
        print(f"fetch failed: {e}")    
    
    print("\n--- 🧪 Testing Cache (Run 2) ---")
    # Second Run:.."Loading from local cache"..
    df_cached = fetch_via_http(URL, cache_name ="scraper_data.csv")

    
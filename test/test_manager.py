from dis.data_manager import fetch_csv_via_http

# THIS WILL FEATURE THE WEBSCRAPER RAW URL (Jumia)
WEBSCRAPER_URL = "https://raw.githubusercontent.com/YOUR_USERNAME/ADIP-Ingestion/main/inputs/scraper_dataset.csv"

if __name__ == "__main__":
    print("--- 🧪 Testing Advanced Bridge ---")
    
    # First Run: Should hit the internet
    df = fetch_csv_via_http(WEBSCRAPER_URL, cache_name="scraper_data.csv")
    
    if df is not None:
        print(f"Data Shape: {df.shape}")
        print(df.head())
    
    print("\n--- 🧪 Testing Cache (Run 2) ---")
    # Second Run: Should say "Loading from local cache"
    df_cached = fetch_csv_via_http(WEBSCRAPER_URL, cache_name="scraper_data.csv")
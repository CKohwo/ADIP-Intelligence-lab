import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from dis.refinery import DataRefinery 

def run_tests():
    print("🚀 Initializing ADIP Data Refinery Test Protocol...\n")
    
    # STEP 1: Instantiate the Engine
    refinery = DataRefinery()
    
    # STEP 2: Test Pipeline 1 (Retail API)
    try:
        print("--- TESTING PIPELINE 1: RETAIL API ---")
        # Load the raw data exactly as you did in the Jupyter Lab
        api_df = pd.read_csv(".data_cache\\api_ingest.csv")
        
        # Pass the raw data through our processing engine
        clean_api_df = refinery.process_retail_api(api_df)
        
        # Verify the outputs
        print("✅ Retail API Processing Successful!")
        print("🔍 Brand Imputation Check (First 3 rows):")
        print(clean_api_df[['name', 'brand']].head(3))
        
        print("\n🔍 Dictionary Unpacking Check (Stock & Rating):")
        print(clean_api_df[['stock', 'rating']].head(3))
        print("-" * 50 + "\n")
        
    except FileNotFoundError:
        print("⚠️ api_ingest.csv not found. Check your file paths!\n")

    # STEP 3: Test Pipeline 2 (Scraper)
    try:
        print("--- TESTING PIPELINE 2: SCRAPER ---")
        scraper_df = pd.read_csv(".data_cache\\scraper.csv")
        clean_scraper_df = refinery.process_scraper(scraper_df)
        
        print("✅ Scraper Processing Successful!")
        print("🔍 Cleaned Price and Ratings Check (First 3 rows):")
        print(clean_scraper_df[['Brand', 'Price', 'Ratings']].head(3))
        print("-" * 50 + "\n")
        
    except FileNotFoundError:
        print("⚠️ scraper.csv not found. Check your file paths!\n")

if __name__ == "__main__":
    # This ensures the test only runs if we execute this file directly
    run_tests()
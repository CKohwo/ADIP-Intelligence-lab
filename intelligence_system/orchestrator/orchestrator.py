import pandas as pd
from pathlib import Path

from data_bridge import ingest_adip_data

from dis.transformers.api_ingest import transform as clean_api
from dis.transformers.scraper import transform as clean_scraper
 
# Orchestrator function to build the final dataset from both sources -- 
  
OUTPUT_DIR = Path("processed_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def build_dataset():

    print("Starting ADIP Intelligence Pipeline...")

    # -----------------------------------
    # Step 1: Fetch live datasets
    # -----------------------------------
    datasets = ingest_adip_data()

    # -----------------------------------
    # Step 2: Transform independently
    # -----------------------------------

    # API INGEST
    if "api_ingest" in datasets:

        print("Processing API Ingest dataset...")

        api_df = clean_api(datasets["api_ingest"])

        api_df.to_parquet(
            OUTPUT_DIR / "api_ingest.parquet",
            index=False
        )

        print(f"Saved api_ingest.parquet | Shape: {api_df.shape}")

    # SCRAPER
    if "scraper" in datasets:

        print("Processing Scraper dataset...")

        scraper_df = clean_scraper(datasets["scraper"])

        scraper_df.to_parquet(
            OUTPUT_DIR / "scraper.parquet",
            index=False
        )

        print(f"Saved scraper.parquet | Shape: {scraper_df.shape}")

    # API AUTH
    if "api_auth" in datasets:

        print("Processing API Auth dataset...")

        auth_df = datasets["api_auth"] 

        auth_df.to_parquet(
            OUTPUT_DIR / "api_auth.parquet",
            index=False
        )

        print(f"Saved api_auth.parquet | Shape: {auth_df.shape}")

     
if __name__ == "__main__":
    build_dataset() 
    print("Data Intelligence Pipeline Completed.") 
from pathlib import Path

from data_bridge.data_manager import ingest_adip_data

from intelligence_system.transformers.api_ingest import transform as clean_api
from intelligence_system.transformers.scraper import transform as clean_scraper


# --------------------------------------------------
# Output directories
# --------------------------------------------------

PROCESSED_DIR = Path("data/transformed")
RAW_DIR = Path("data/raw")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)


def run_transform_pipeline() -> dict:
    """
    Runs the ADIP Intelligence transformers pipeline layer.

    Responsibilities:
    - Pull datasets from the data bridge.
    - Route each source to its correct processing path.
    - Transform only the datasets that require transformation.
    - Save each source independently.
    - Return output paths for testing and downstream usage.
    """

    print("Starting ADIP Intelligence Pipeline...")

    outputs = {}

    # -----------------------------------
    # Step 1: Fetch live datasets
    # -----------------------------------
    datasets = ingest_adip_data()

    # -----------------------------------
    # Step 2: Process API Ingest
    # -----------------------------------
    if "api_ingest" in datasets:
        print("Processing API Ingest dataset...")

        api_df = clean_api(datasets["api_ingest"])

        api_path = PROCESSED_DIR / "api_ingest.parquet"
        api_df.to_parquet(api_path, index=False)

        outputs["api_ingest"] = api_path

        print(f"Saved api_ingest.parquet | Shape: {api_df.shape}")

    else:
        print("[WARNING] api_ingest dataset missing.")

    # -----------------------------------
    # Step 3: Process Scraper
    # -----------------------------------
    if "scraper" in datasets:
        print("Processing Scraper dataset...")

        scraper_df = clean_scraper(datasets["scraper"])

        scraper_path = PROCESSED_DIR / "scraper.parquet"
        scraper_df.to_parquet(scraper_path, index=False)

        outputs["scraper"] = scraper_path

        print(f"Saved scraper.parquet | Shape: {scraper_df.shape}")

    else:
        print("[WARNING] scraper dataset missing.")

    # -----------------------------------
    # Step 4: Preserve API Auth as raw passthrough
    # -----------------------------------
    if "api_auth" in datasets:
        print("Preserving API Auth dataset as raw passthrough...")

        auth_df = datasets["api_auth"]

        auth_path = RAW_DIR / "api_auth.parquet"
        auth_df.to_parquet(auth_path, index=False)

        outputs["api_auth"] = auth_path

        print(f"Saved api_auth.parquet | Shape: {auth_df.shape}")

    else:
        print("[WARNING] api_auth dataset missing.")

    print("ADIP Intelligence Pipeline Completed.")

    return outputs


if __name__ == "__main__":
    run_transform_pipeline()
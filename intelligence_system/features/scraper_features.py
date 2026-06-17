import sys
import pandas as pd
from pathlib import Path
from intelligence_system.tools.normalizer  import clean_brand_value 

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


Data_dir = Path("data/processed/scraper.parquet")
Scraper_features_dir = Path("data/processed/scraper_features")


def normalize_scraper_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function normalizes the scraper dataset by cleaning and standardizing the values.
    """
    df = df.copy()
    df["featched_time"] = pd.datetime(df["fetched_time"], utc=True, errors = "coerce") 
    df["day"] = df["fetched_time"].dt.floor("D")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["product_rating"] = pd.to_numeric(df["product_rating"], errors="coerce")

    df["brand"] = df["brand"].apply(clean_brand_value)

    df = df.dropna(subset =["product_name", "category", "price", "fetched_time"])

    # 
    df = df[(df["price"] > 0) & (df["price"] <= 10_000_000)]
    
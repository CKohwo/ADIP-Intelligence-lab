import sys
import pandas as pd
from pathlib import Path
 
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from intelligence_system.tools.normalizer import clean_brand_value
from intelligence_system.features.api_features import get_top_value  
from intelligence_system.schemas.schemas import SCRAPER_CATEGORY_FEATURES, SCRAPER_BRAND_FEATURES 


Data_dir = Path("data/transformed/scraper.parquet")
Scraper_features_dir = Path("data/features/scraper")


def normalize_scraper_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function normalizes the scraper dataset by cleaning and standardizing the values.
    """
    df = df.copy()
    df["fetched_time"] = pd.to_datetime(df["fetched_time"], utc=True, errors = "coerce") 
    df["day"] = df["fetched_time"].dt.floor("D")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["product_rating"] = pd.to_numeric(df["product_rating"], errors="coerce")

    df["brand"] = df["brand"].apply(clean_brand_value)

    df = df.dropna(subset =["product_name", "category", "price", "fetched_time"])

    # Price sanity filter - remove crazy outliers... 
    df = df[(df["price"] > 0) & (df["price"] <= 10_000_000)]

    return df
   
       
def build_category_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build category-level AI snapshot features from scraper data.
    """

    df = normalize_scraper_dataset(df)

    grouped = df.groupby("category", dropna=False).agg(
        listing_volume=("product_name", "size"),
        product_variety_count=("product_id", "nunique"),
        brand_count=("brand", lambda x: x[x != "Unknown"].nunique()),
        top_brand=("brand", get_top_value),

        median_price=("price", "median"),  

        avg_rating=("product_rating", "mean"),
        rating_coverage_pct=("product_rating", lambda x: (x.fillna(0)>0).mean() * 100),
        source=("source", "last") 
    ).reset_index()

    grouped["price_tier"] = pd.qcut(grouped["median_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[SCRAPER_CATEGORY_FEATURES]

    features = features.sort_values(
        by=["listing_volume", "product_variety_count", "brand_count", "category"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    return features


def build_brand_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build brand-level AI snapshot features from scraper data.
 
    """

    df = normalize_scraper_dataset(df)

    # Remove Unknown from brand intelligence.
    df = df[df["brand"] != "Unknown"]

    grouped = df.groupby("brand", dropna=False).agg(
        listing_volume=("product_name", "size"),
        product_variety_count=("product_id", "nunique"),
        category_count=("category", "nunique"),
        top_category=("category", get_top_value),

        median_price=("price", "median"),

        avg_rating=("product_rating", "mean"),
        rating_coverage_pct=("product_rating", lambda x: (x.fillna(0)>0).mean() * 100),
        source=("source", "last") 
    ).reset_index()

    grouped["price_tier"] = pd.qcut(grouped["median_price"], q=3, labels= ["Low", "Mid", "High"]) 

    features = grouped[SCRAPER_BRAND_FEATURES]

    features = features.sort_values(
        by=["listing_volume", "product_variety_count", "category_count", "brand"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    return features


def save_features(df: pd.DataFrame, output_path: Path, label: str) -> None:
    # Save featured output data to parquet
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"{output_path} features created and saved successfully: {df.shape}, {label}")
 
import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from intelligence_system.tools.normalizer import clean_brand_value
from intelligence_system.features.api_features import get_top_value
from intelligence_system.features.scraper_features import normalize_scraper_dataset  
from intelligence_system.schemas.schemas import SCRAPER_CATEGORY_TIMESERIES, SCRAPER_BRAND_TIMESERIES  

Data_dir = Path("data/processed/scraper.parquet")
Forecast_data_dir = Path("data/timeseries/scraper")


def prepare_timeseries_input(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function prepares the input data for time series forecasting by aggregating the data by day for (product, brand, or seller) 
    for downstream forecasting features.
    """
    df = normalize_scraper_dataset(df)

    # Convert fetched_time to date
    df["day"] = df["fetched_time"].dt.date

    df = df.sort_values(["day", "fetched_time"])

    return df 

    
def build_category_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """ This function creates product level time series data by aggregating the data by day and category 
    """
    
    df = prepare_timeseries_input(df)
    df = df.dropna(subset=["category", "fetched_time", "price"])  # Drop rows with missing category, fetched_time, or price

    # Prepare product level time series data
    category_timeseries = df.groupby(["day", "category"]).agg(
        listing_volume=("product_name", "size"),
        product_variety_count=("product_id", "nunique"),
        brand_count=("brand", lambda x: x[x != "Unknown"].nunique()),
        median_price=("price", "median"),  
        avg_rating=("product_rating", "mean"),
        rating_coverage_pct=("product_rating", lambda x: (x.fillna(0)>0).mean() * 100),
        source=("source", "last") 
    ).reset_index()

    category_timeseries = category_timeseries[SCRAPER_CATEGORY_TIMESERIES]

    category_timeseries.sort_values(["category", "day"], ascending=[True, True]).reset_index(drop=True)  

    return category_timeseries

    
def build_brand_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """ This function creates product level time series data by aggregating the data by day and brand 
    """
    
    df = prepare_timeseries_input(df)
    df = df[df["brand"] != "Unknown"]

    # Prepare product level time series data
    brand_timeseries = df.groupby(["day", "brand"]).agg(
        listing_volume=("product_name", "size"),
        product_variety_count=("product_id", "nunique"),
        category_count=("category", "nunique"),
        median_price=("price", "median"), 
        avg_rating=("product_rating", "mean"),
        rating_coverage_pct=("product_rating", lambda x: (x.fillna(0)>0).mean() * 100),
        source=("source", "last") 
    ).reset_index()

    brand_timeseries = brand_timeseries[SCRAPER_BRAND_TIMESERIES]

    brand_timeseries.sort_values(["brand", "day"], ascending=[True, True]).reset_index(drop=True, inplace=True) 

    return brand_timeseries
 
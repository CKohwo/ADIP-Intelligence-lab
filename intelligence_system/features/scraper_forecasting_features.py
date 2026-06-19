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
Forecast_data_dir = Path("data/forecasting/scraper")


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

 
def run_forecasting_features_pipeline(input_dir:Path = Data_dir, output_dir:Path = Forecast_data_dir) -> dict[str, pd.DataFrame]:

    """ This function runs the forecasting features pipeline by calling the build_category_timeseries, build_brand_timeseries, 
    """

    if not input_dir.exists():
        raise FileNotFoundError(
            f"The directory {input_dir.parent} does not exist."  
            "Please create the directory before running the pipeline.")
    
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_dir)

    category_timeseries = build_category_timeseries(df)
    brand_timeseries = build_brand_timeseries(df)
      
    # Save the forecasting features to parquet files
    output_paths = {
        "category_timeseries": output_dir / "category_timeseries.parquet",
        "brand_timeseries": output_dir / "brand_timeseries.parquet", 
    }
 
    category_timeseries.to_parquet(output_paths["category_timeseries"], index=False)
    brand_timeseries.to_parquet(output_paths["brand_timeseries"], index=False)
    
    print(f"Forecasting features created and saved successfully: {output_paths}")

    return {"category_timeseries": category_timeseries, "brand_timeseries": brand_timeseries}


if __name__ == "__main__":
    run_forecasting_features_pipeline()
    print("API forecasting features pipeline executed successfully.") 

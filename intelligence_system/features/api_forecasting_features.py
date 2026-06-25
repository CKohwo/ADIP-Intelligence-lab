import os
import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:   
    sys.path.append(str(ROOT))  # add ROOT to PATH   

from intelligence_system.features.api_features import get_top_value, normalize_dataset
from intelligence_system.tools.normalizer import clean_brand_value
from intelligence_system.schemas.schemas import PRODUCT_TIMESERIES_COLUMNS, BRAND_TIMESERIES_COLUMNS, SELLERS_TIMESERIES_COLUMNS

Processed_data_dir = Path("data/transformed/api_ingest.parquet")
Api_forecast_dir = Path("data/timeseries/api_ingest") 

def prepare_timeseries_input(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function prepares the input data for time series forecasting by aggregating the data by day for (product, brand, or seller) 
    for downstream forecasting features.
    """
    df = normalize_dataset(df)

    # Convert fetched_time to date
    df["day"] = df["fetched_time"].dt.date

    df = df.sort_values(["day", "fetched_time"])

    return df 

    
def build_product_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """ This function creates product level time series data by aggregating the data by day and product_id 
    """
    
    df = prepare_timeseries_input(df)
    df = df.dropna(subset=["product_id", "fetched_time", "price"])  # Drop rows with missing product_id, fetched_time, or price

    # Prepare product level time series data
    product_timeseries = df.groupby(["day", "product_id"]).agg(
        product_name=("product_name", "last"),
        brand=("brand", "last"),
        seller_name=("seller_name", "last"),
        observation_count=("product_id", "size"),
        avg_price=("price", "mean"),
        median_price=("price", "median"),
        current_price=("price", "last"),
        avg_rating=("product_rating", "mean"),
        current_rating=("product_rating", "last"),
        total_rating_count=("rating_count", "sum"),
        rating_coverage_pct=("rating_count", lambda x: (x.fillna(0)>0).mean() * 100),
        stock_qty=("stock_qty", "last"),
        source=("source", "last")
    ).reset_index()

    product_timeseries = product_timeseries[PRODUCT_TIMESERIES_COLUMNS]

    product_timeseries.sort_values(["product_id", "day"], ascending=[True, True]).reset_index(drop=True, inplace=True) 

    return product_timeseries


def build_brand_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """ This function creates brand level time series data by aggregating the data by day and brand
    """
    df = prepare_timeseries_input(df)
    df = df.dropna(subset=["brand"])  # Drop rows with missing brand
    df = df[df["brand"] != "Unknown"] 

    # Prepare brand level time series data
    brand_timeseries = df.groupby(["day", "brand"]).agg(
        product_count=("product_id", "nunique"),
        observation_count=("brand", "size"),
        seller_count=("seller_name", "nunique"),
        top_seller=("seller_name", get_top_value),
        avg_price=("price", "mean"),
        median_price=("price", "median"),
        avg_rating=("product_rating", "mean"),
        total_rating_count=("rating_count", "sum"),
        rating_coverage_pct=("rating_count", lambda x: (x.fillna(0)>0).mean() * 100),
        avg_stock_qty=("stock_qty", "mean"),
        source=("source", "last")
    ).reset_index()

    brand_timeseries = brand_timeseries[BRAND_TIMESERIES_COLUMNS]

    brand_timeseries.sort_values(["brand", "day"], ascending=[True, True]).reset_index(drop=True, inplace=True) 

    return brand_timeseries    


def build_seller_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """ This function creates seller level time series data by aggregating the data by day and seller_name
    """
    df = prepare_timeseries_input(df)
    df = df[df["seller_name"] != "Unknown"]

    # Prepare seller level time series data
    seller_timeseries = df.groupby(["day", "seller_name"]).agg(
        product_count=("product_id", "nunique"),
        brand_count=("brand", "nunique"),
        observation_count=("seller_name", "size"),
        top_brand=("brand", get_top_value),
        avg_price=("price", "mean"),
        median_price=("price", "median"),
        avg_rating=("product_rating", "mean"),
        total_rating_count=("rating_count", "sum"),
        rating_coverage_pct=("rating_count", lambda x: (x.fillna(0)>0).mean() * 100),
        avg_stock_qty=("stock_qty", "mean"),
        source=("source", "last")
    ).reset_index()     

    seller_timeseries = seller_timeseries[SELLERS_TIMESERIES_COLUMNS]
    seller_timeseries.sort_values(["seller_name", "day"], ascending=[True, True]).reset_index(drop=True, inplace=True)

    return seller_timeseries

 
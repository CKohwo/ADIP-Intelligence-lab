import os
import pandas as pd
from pathlib import Path
from intelligence_system.schemas import API_PRODUCT_FEATURE

Processed_data_dir = Path("data/processed/api_ingest.parquet")
Product_features_dir = Path("data/features/api_ingest/product_features.parquet")

 
def normalize_dataset(df:pd.Datframe) -> pd.DataFrame:
    """
    This function normalizes the dataset.
    """
    df = df.copy()

    df["fetched_time"] = pd.to_datetime(df["fetched_time"], utc = True, errors = "coerce")
    df["price"] = pd.to_numeric(df["price"], errors = "coerce")
    df["product_rating"] = pd.to_numeric(df["product_rating"], errors = "coerce")
    df["rating_count"] = pd.to_numeric(df["rating_count"], errors = "coerce")
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors = "coerce") 

    df = df.dropna(subset=["product_id", "fetched_time", "price"])

    return df  


def create_product_features(df:pd.DataFrame) -> pd.DataFrame:
    """This function creates product level intelligence by aggregating the data by product_id and calculating the average price, average rating, total rating count, and stock quantity for each product.
    """  
    df = normalize_dataset(df)

    # Sorting the dataset by fetched_time and product_id to ensure that the latest price and rating information is captured for each product
    df = df.sort_values(["fetched_time", "product_id"])

    grouped = df.groupby("product_id", dropna=False).agg(product_name = ("product_name", "last"), brand = ("brand", "last"), seller_name = ("seller_name", "last"), source = ("source", "last"), first_seen = ("fetched_time", "first"), last_seen = ("fetched_time", "last"),
                                                   observation_count = ("product_id", "size"), avg_price = ("price", "mean"), current_price = ("price", "last"), avg_rating = ("product_rating", "mean"), current_rating = ("product_rating", "last"), 
                                                   total_rating_count = ("product_rating", "sum"), stock_qty = ("stock_qty", "last")).reset_index()
    
    grouped["days_active"] = (grouped["last_seen"] - grouped["first_seen"]).dt.days
    
    # Creating a price tier feature based on the current price of the product using quantiles to categorize products into low, mid, and high price tiers.
    grouped["price_tier"] = pd.qcut(grouped["current_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_PRODUCT_FEATURE]

    return features


def run_product_intelligence() -> pd.DataFrame:
    """This function runs the product intelligence pipeline by reading the processed data, creating product features, and saving the features to a parquet file.
    """
    if not Processed_data_dir.parent.exists():
        raise FileNotFoundError(f"The directory {Processed_data_dir.parent} does not exist. Please create the directory before running the pipeline.")    
    
    Product_features_dir.parent.mkdir(parents=True, exist_ok=True) 

    df = pd.read_parquet(Processed_data_dir)
    product_features = create_product_features(df)
    product_features.to_parquet(Product_features_dir, index=False)
    print(f"Product features created and saved successfully: {product_features.shape[0]} products.")

    return product_features
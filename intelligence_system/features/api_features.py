import os
import pandas as pd
from pathlib import Path
from intelligence_system.schemas import API_PRODUCT_FEATURE, API_BRAND_FEATURE, API_SELLER_FEATURE

Processed_data_dir = Path("data/processed/api_ingest.parquet")
API_feature_data_dir = Path("data/features/api_ingest")

 
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


def build_product_features(df:pd.DataFrame) -> pd.DataFrame:
    """This function creates product level intelligence by aggregating the data by product_id and 
    calculating the average price, average rating, total rating count, and stock quantity for each product.
    """  
    df = normalize_dataset(df)

    # Sorting the dataset by fetched_time and product_id to ensure that the latest price and rating information is captured for each product
    df = df.sort_values(["fetched_time", "product_id"])

    grouped = df.groupby("product_id", dropna=False).agg(product_name = ("product_name", "last"), brand = ("brand", "last"), seller_name = ("seller_name", "last"), source = ("source", "last"), first_seen = ("fetched_time", "first"), last_seen = ("fetched_time", "last"),
                                                   observation_count = ("product_id", "size"), avg_price = ("price", "mean"), current_price = ("price", "last"), avg_rating = ("product_rating", "mean"), current_rating = ("product_rating", "last"), 
                                                   total_rating_count = ("product_rating", "sum"), stock_qty = ("stock_qty", "last")).reset_index()
    
    grouped["days_active"] = (grouped["last_seen"] - grouped["first_seen"]).dt.days
    
    """Creating a price tier feature based on the current price of the product using quantiles to categorize products into low, mid, and high price tiers.
    """
    grouped["price_tier"] = pd.qcut(grouped["current_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_PRODUCT_FEATURE]

    return features

# The get_top_value function takes a pandas Series as input and returns the top value for a given column based on the mode. 
def get_top_value(series: pd.Series) -> str:
    """This function returns the top value for a given column based on the mode.
    """
    clean_series = series.dropna()

    if clean_series.empty:
        return "None"
    
    value_counts = clean_series.value_counts()  
    
    return value_counts.index[0]


# The build_brand_features function takes a DataFrame as input and performs the following steps: 
def build_brand_featrures(df: pd.DataFrame) -> pd.DataFrame:
    """This function creates brand level intelligence by aggregating the data by brand and 
    calculating the average price, average rating, total rating count, and stock quantity for each brand.
    """
    df = normalize_dataset(df)

    df["brand"] = df["brand"].fillna("Unknown")
    df["seller_name"] = df["seller_name"].fillna("Unknown")

    grouped = df.groupby("brand", dropna=False).agg(product_count = ("product_id", "nunique"), observation_count = ("product_id", "size"), seller_count = ("seller_name", "nunique"), top_seller = ("seller_name", get_top_value), 
                                                   avg_price = ("price", "mean"), median_price = ("price", "median"), avg_rating = ("product_rating", "mean"), total_rating_count = ("rating_count", "sum"), rating_coverage_pct = ("product_rating", lambda x: x.notna().mean() * 100), 
                                                   avg_stock_qty = ("stock_qty", "mean"), source = ("source", "last")).reset_index()
    
    # Creating a price tier feature based on the average price of the brand using quantiles to categorize brands into low, mid, and high price tiers.
    grouped["brand_price_tier"] = pd.qcut(grouped["median_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_BRAND_FEATURE]

    return features


def build_seller_features(df: pd.DataFrame) -> pd.DataFrame:
    """This function creates seller level intelligence by aggregating the data by seller_name and 
    calculating the average price, average rating, total rating count, and stock quantity for each seller.
    """
    df = normalize_dataset(df)

    df["brand"] = df["brand"].fillna("Unknown")
    df["seller_name"] = df["seller_name"].fillna("Unknown")

    grouped = df.groupby("seller_name", dropna=False).agg(product_count = ("product_id", "nunique"), observation_count = ("product_id", "size"), brand_count = ("brand", "nunique"), top_brand = ("brand", get_top_value), 
                                                   avg_price = ("price", "mean"), median_price = ("price", "median"), avg_rating = ("product_rating", "mean"), total_rating_count = ("rating_count", "sum"), rating_coverage_pct = ("product_rating", lambda x: x.notna().mean() * 100), 
                                                   avg_stock_qty = ("stock_qty", "mean"), source = ("source", "last")).reset_index()
    
    # Creating a price tier feature based on the average price of the seller using quantiles to categorize sellers into low, mid, and high price tiers.
    grouped["seller_price_tier"] = pd.qcut(grouped["median_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_SELLER_FEATURE]

    return features


def save_features(df: pd.DataFrame, output_dir: Path, label: str) -> None:
    """This function saves the features to a parquet file."""
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_dir, index=False)
    print(f"{output_dir} features created and saved successfully: {df.shape}, {label}") 


def run_product_intelligence() -> pd.DataFrame:
    """This function runs the product intelligence pipeline by reading the processed data, creating product features, and saving the features to a parquet file.
    """
    if not Processed_data_dir.parent.exists():
        raise FileNotFoundError(f"The directory {Processed_data_dir.parent} does not exist. Please create the directory before running the pipeline.")    
    
    Product_features_dir.parent.mkdir(parents=True, exist_ok=True) 

    df = pd.read_parquet(Processed_data_dir)
    product_features = build_product_features(df)
    save_features(product_features, Product_features_dir, "Product")

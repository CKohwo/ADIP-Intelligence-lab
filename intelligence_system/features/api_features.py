import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from intelligence_system.tools.normalizer import clean_brand_value
from intelligence_system.schemas.schemas import API_PRODUCT_FEATURE, API_BRAND_FEATURE, API_SELLER_FEATURE

Processed_data_dir = Path("data/processed/api_ingest.parquet")
API_feature_dir = Path("data/features/api_ingest")

 
def normalize_dataset(df:pd.DataFrame) -> pd.DataFrame:
    """
    This function normalizes the dataset.
    """
    df = df.copy()

    df["fetched_time"] = pd.to_datetime(df["fetched_time"], utc = True, errors = "coerce")
    df["price"] = pd.to_numeric(df["price"], errors = "coerce")
    df["product_rating"] = pd.to_numeric(df["product_rating"], errors = "coerce")
    df["rating_count"] = pd.to_numeric(df["rating_count"], errors = "coerce")
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors = "coerce") 

    df["brand"] = df["brand"].apply(clean_brand_value)
    df["seller_name"] = df["seller_name"].fillna("Unknown").astype(str).str.strip()

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
                                                   total_rating_count = ("rating_count", "sum"), stock_qty = ("stock_qty", "last")).reset_index()
    
    grouped["days_active"] = (grouped["last_seen"] - grouped["first_seen"]).dt.days
    
    """Creating a price tier feature based on the current price of the product using quantiles to categorize products into low, mid, and high price tiers.
    """
    grouped["price_tier"] = pd.qcut(grouped["current_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_PRODUCT_FEATURE]
    features = features.sort_values(by = ["observation_count", "total_rating_count", "product_name"], ascending=[False, False, True]).reset_index(drop=True)

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
def build_brand_featrures(product_features: pd.DataFrame) -> pd.DataFrame:
    """This function creates brand level intelligence by aggregating the data by brand and 
    calculating the average price, average rating, total rating count, and stock quantity for each brand.
    """
    df = product_features.copy()

    df["brand"] = df["brand"].fillna("Unknown") 
    df["seller_name"] = df["seller_name"].fillna("Unknown") 
 
    grouped = df.groupby("brand", dropna=False).agg(product_count = ("product_id", "nunique"), observation_count = ("observation_count", "sum"), seller_count = ("seller_name", "nunique"), top_seller = ("seller_name", get_top_value), 
                                                   avg_price = ("current_price", "mean"), median_price = ("current_price", "median"), avg_rating = ("avg_rating", "mean"), total_rating_count = ("total_rating_count", "sum"), 
                                                   avg_stock_qty = ("stock_qty", "mean"), source = ("source", "last")).reset_index()  

    # Calculating the percentage of products with ratings for each brand to create a rating coverage feature under each brand that have received ratings.                                               
    rating_coverage_pct = (df.assign(ratings = (df["avg_rating"] > 0) & (df["total_rating_count"] > 0)).groupby("brand")["ratings"].mean().mul(100).reset_index(name="rating_coverage_pct"))
    
    grouped = grouped.merge(rating_coverage_pct, on="brand", how="left")
    
    # Creating a price tier feature based on the average price of the brand using quantiles to categorize brands into low, mid, and high price tiers.
    grouped["price_tier"] = pd.qcut(grouped["median_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_BRAND_FEATURE]

    features = features.sort_values(by = ["product_count", "observation_count", "total_rating_count", "brand"], ascending=[False, False, False, True]).reset_index(drop=True)

    return features


def build_seller_features(df: pd.DataFrame) -> pd.DataFrame:
    """This function creates seller level intelligence by aggregating the data by seller_name and 
    calculating the average price, average rating, total rating count, and stock quantity for each seller.
    """
    df = normalize_dataset(df)
 
    grouped = df.groupby("seller_name", dropna=False).agg(product_count = ("product_id", "nunique"), observation_count = ("product_id", "size"), brand_count = ("brand", "nunique"), top_brand = ("brand", get_top_value), 
                                                   avg_price = ("price", "mean"), median_price = ("price", "median"), avg_rating = ("product_rating", "mean"), total_rating_count = ("rating_count", "sum"), rating_coverage_pct = ("product_rating", lambda x: x.notna().mean() * 100), 
                                                   avg_stock_qty = ("stock_qty", "mean"), source = ("source", "last")).reset_index()
    
    # Creating a price tier feature based on the average price of the seller using quantiles to categorize sellers into low, mid, and high price tiers.
    grouped["price_tier"] = pd.qcut(grouped["median_price"], q=3, labels= ["Low", "Mid", "High"])

    features = grouped[API_SELLER_FEATURE]
    features = features.sort_values(by = ["product_count", "observation_count", "total_rating_count", "seller_name"], ascending=[False, False, False, True]).reset_index(drop=True)
    
    return features


def save_features(df: pd.DataFrame, output_dir: Path, label: str) -> None:
    """This function saves the features to a parquet file."""
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_dir, index=False)
    print(f"{output_dir} features created and saved successfully: {df.shape}, {label}") 


def run_api_feature_engine_pipeline():
    """ This function runs the API feature engineering pipeline by calling the necessary functions to build and save the product, brand, and seller features.
    """
    if not Processed_data_dir.exists():
        raise FileNotFoundError(
            f"The directory {Processed_data_dir.parent} does not exist."  
            "Please create the directory before running the pipeline.")    
    
    API_feature_dir.parent.mkdir(parents=True, exist_ok=True) 

    df = pd.read_parquet(Processed_data_dir)
    product_features = build_product_features(df)
    brand_features = build_brand_featrures(product_features)
    seller_features = build_seller_features(df)
    save_features(product_features, API_feature_dir/ "product_features.parquet", "Product")
    save_features(brand_features, API_feature_dir/ "brand_features.parquet", "Brand")
    save_features(seller_features, API_feature_dir/ "seller_features.parquet", "Seller")
    
    return{"product": product_features,"brand" : brand_features, "seller": seller_features} 
     

if __name__ == "__main__":
    run_api_feature_engine_pipeline()
    print("API feature engineering pipeline completed successfully.")  

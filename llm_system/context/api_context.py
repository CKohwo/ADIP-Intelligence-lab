"""
ADIP API_INGEST Context Builder.
Responsibility: SELECT, COMPRESS, PACKAGE.
Does NOT compute, engineer, or derive.
"""

from pathlib import Path
from typing import Dict, Any

import pandas as pd


# ==========================================================
# PATHS
# ==========================================================

API_FEATURES_DIR = Path("data/features/api_ingest")
API_TIMESERIES_DIR = Path("data/timeseries/api_ingest")

FEATURES = {
    "product": API_FEATURES_DIR / "product_features.parquet",
    "brand": API_FEATURES_DIR / "brand_features.parquet",
    "seller": API_FEATURES_DIR / "seller_features.parquet",
}

TIMESERIES = {
    "product": API_TIMESERIES_DIR / "product_timeseries.parquet",
    "brand": API_TIMESERIES_DIR / "brand_timeseries.parquet",
    "seller": API_TIMESERIES_DIR / "seller_timeseries.parquet",
}


# ==========================================================
# LOADERS
# ==========================================================

def load_api_features() -> Dict[str, pd.DataFrame]:
    """Load all API ingest feature assets. Fail fast if missing.
    """
    assets = {}
    for name, path in FEATURES.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing feature asset: {path}")
        assets[name] = pd.read_parquet(path)
    return assets


def load_api_timeseries() -> Dict[str, pd.DataFrame]:
    """Load all API ingest timeseries assets. Graceful fallback to empty."""
    assets = {}
    for name, path in TIMESERIES.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing feature asset: {path}")
        assets[name] = pd.read_parquet(path)
    return assets


def get_recent_activity(df: pd.DataFrame,entity_col: str,rows_per_entity: int = 10)-> list[dict]:
    """Extract last N rows per entity from pre-computed timeseries."""
    if df.empty or entity_col not in df.columns:
        return []
    return df.groupby(entity_col).tail(rows_per_entity).to_dict("records")

# ==========================================================
# PRODUCT CONTEXT
# ==========================================================
 
def build_product_context(features: pd.DataFrame, timeseries: pd.DataFrame, top_n: int = 20) -> Dict[str, Any]:

    product_context = {"top_rated_products": features.nlargest(top_n, "avg_rating") 
           [["product_name", "brand", "avg_rating", "total_rating_count"]]
           .to_dict("records"),
 
        "most_observed_products": features.nlarges(top_n, "observation_count")
        [["product_name", "brand", "observation_count", "days_active"]]
        .to_dict("records"),
 
        "highest_priced_products":features.nlargets(top_n, "current_price")
        [["product_name", "brand", "current_price", "avg_rating"]]
        .to_dict("records"),
 
        "recent_product_activity": get_recent_activity( timeseries, "product_id"),
                
         "market_summary": {
            "total_products": int(features.shape[0]),
            "average_product_rating": round(features["avg_rating"].mean(), 2),
            "average_product_price": round(features["avg_price"].mean(), 2)} 
    }

    return product_context
    
# ==========================================================
# BRAND CONTEXT
# ==========================================================

def build_brand_context(features: pd.DataFrame, timeseries: pd.DataFrame, top_n: int = 20) -> Dict[str, Any]:

    brand_context = {
        "top_rated_brands": features.nlargest(top_n, "avg_rating")[
            ["brand", "avg_rating", "total_rating_count"]
        ].to_dict("records"),

        "largest_brands": features.nlargest(top_n, "product_count")[
            ["brand", "product_count", "seller_count", "avg_rating"]
        ].to_dict("records"),

        "premium_brands": features.nlargest(top_n, "median_price")[
            ["brand", "median_price", "avg_rating"]
        ].to_dict("records"),

 
        "recent_brand_activity": get_recent_activity(timeseries,"brand"),
    
        "market_summary": {
            "total_brands": int(features.shape[0]), 
            "average_brand_rating": round(features["avg_rating"].mean(),2)}
     
    }

    return brand_context

# ==========================================================
# SELLER CONTEXT
# ==========================================================

def build_seller_context(features: pd.DataFrame, timeseries: pd.DataFrame, top_n: int = 20) -> Dict[str, Any]:

    seller_context = {
        "top_rated_sellers": features.nlargest(top_n, "avg_rating")[
            ["seller_name", "avg_rating", "total_rating_count"]
        ].fillna("Unknown").to_dict("records"),

        "largest_sellers": features.nlargest(top_n, "product_count")[
            ["seller_name", "product_count", "brand_count", "avg_rating"]
        ].fillna("Unknown").to_dict("records"),

        "most_diverse_sellers": features.nlargest(top_n, "brand_count")[
            ["seller_name", "brand_count", "product_count"]
        ].fillna("Unknown").to_dict("records"),

        "recent_seller_activity": get_recent_activity(timeseries, "seller_name"),

        "market_summary": {
            "total_sellers": int(features.shape[0]),
            "average_seller_rating": round(features["avg_rating"].mean(),2)
        }
    }

    return seller_context


def build_api_context(top_n: int = 10) -> Dict[str, Any]:

    features = load_api_features()
    timeseries = load_api_timeseries()

    return {

        "domain": "api_ingest",

        "generated_at": pd.Timestamp.utcnow().isoformat(),

        "product_context": build_product_context(features["product"],timeseries["product"], top_n),
             
        "brand_context": build_brand_context( features["brand"], timeseries["brand"], top_n),
                 
        "seller_context": build_seller_context(features["seller"], timeseries["seller"], top_n)
         
    }
import pandas as pd
from pathlib import Path 
from typing import Dict, Any


# ==========================================================
# PATHS
# ==========================================================
 
SCRAPER_FEATURES_DIR = Path("data/features/scraper")
SCRAPER_TIMESERIES_DIR = Path("data/timeseries/scraper") 

FEATURES = {
    "category": SCRAPER_FEATURES_DIR / "category_features.parquet",
    "brand": SCRAPER_FEATURES_DIR / "brand_features.parquet"
}

TIMESERIES ={
    "category": SCRAPER_TIMESERIES_DIR / "category_timeseries.parquet",
    "brand": SCRAPER_TIMESERIES_DIR / "brand_timeseries.parquet"
}

# ========================================
# ------------ LOADER --------------------  
# =======================================

def load_features() -> Dict[str, pd.DataFrame]:
    assets = {}
    for name, path in FEATURES.items():
        if not path.exists():
            raise FileNotFoundError (f"This directory does not exits, please create file :{path}")
        assets[name] = pd.read_parquet(path)

    return assets       


def load_timeseries() -> Dict[str, pd.DataFrame]:
    assets = {}
    for name, path in TIMESERIES.items():
        if not path.exists():
            raise FileNotFoundError (f"This directory does not exits, please create file :{path}")
        assets[name] = pd.read_parquet(path)

    return assets       


def get_recent_activity(df: pd.DataFrame,entity_col: str,rows_per_entity: int = 10)-> list[dict]:
    """Extract last N rows per entity from pre-computed timeseries."""
    if df.empty or entity_col not in df.columns:
        return []
    return df.groupby(entity_col).tail(rows_per_entity).to_dict("records")


# ==========================================================
# CATEGORY CONTEXT
# ==========================================================
 
def build_category_context(features: pd.DataFrame, timeseries: pd.DataFrame, top_n: int = 20) -> Dict[str, Any]:

    return {"largest_category": features.nlargest(top_n, "listing_volume") 
           [["category", "listing_volume", "brand_count", "avg_rating"]]
           .to_dict("records"),
 
        "most_diverse_category": features.nlargest(top_n, "product_variety_count")[
            ["category", "product_variety_count", "brand_count"] 
        ].to_dict("records"),

        "premium_category": features.nlargest(top_n, "median_price")[
            ["category", "median_price", "price_tier", "avg_rating"]
        ].to_dict("records"),

        "recent_product_activity": get_recent_activity( timeseries, "category"),
                
        "market_summary": {
            "total_categories": int(features.shape[0]),
            "average_listing_volume": round(features["listing_volume"].mean(), 2),
            "average_category_price": round(features["median_price"].mean(), 2),
            "average_category_rating": round(features["avg_rating"].mean(), 2)} 
    }


# ==========================================================
# BRAND CONTEXT
# ==========================================================

def build_brand_context(features: pd.DataFrame, timeseries: pd.DataFrame, top_n: int = 20) -> Dict[str, Any]:

    return {
        "top_rated_brands": features.nlargest(top_n, "avg_rating")[
            ["brand", "avg_rating", "category"] 
        ].to_dict("records"),

        "most_diverse_brands": features.nlargest(top_n, "product_variety_count")[
            ["brand", "product_variety_count", "listing_volume"] 
        ].to_dict("records"), 

        "largest_brands": features.nlargest(top_n, "listing_volume")[
            ["brand", "listing_volume", "category_count", "avg_rating"]
        ].to_dict("records"),

        "premium_brands": features.nlargest(top_n, "median_price")[
            ["brand", "median_price","price_tier", "avg_rating", "rating_coverage_pct"]
        ].to_dict("records"),

 
        "recent_brand_activity": get_recent_activity(timeseries,"brand"),
    
        "market_summary": {
            "total_brands": int(features.shape[0]), 
            "average_brand_rating": round(features["avg_rating"].mean(),2),
            "average_brand_price": round(features["median_price"].mean(),2),
            "average_listing_per_brand": round(features["listing_volume"].mean(),2)} 
    }

# This module defines the expected schema for the Ingested data.
API_COLUMNS = [
    "product_id",
    "product_name",
    "brand",
    "price",
    "product_rating",
    "rating_count",
    "seller_name",
    "stock_qty",
    "fetched_time",
    "source"
]

SCRAPER_COLUMNS = [
    "product_id",
    "product_name",
    "brand",
    "price",
    "product_rating",
    "category",
    "fetched_time",
    "source"
]
 

# API FEATURE ENGINEERING SCHEMA
API_PRODUCT_FEATURE = [
    "product_id",
    "product_name",
    "brand",
    "seller_name",
    "source",

    "first_seen",
    "last_seen",
    "days_active",
    "observation_count",

    "avg_price",
    "current_price",
    "price_tier",

    "avg_rating",
    "current_rating",
    "total_rating_count",
    "stock_qty",
]

API_BRAND_FEATURE = [
    "brand", "product_count", "observation_count", 
    "seller_count", "top_seller",
    "avg_price", "median_price", "price_tier",
    "avg_rating", "total_rating_count", "rating_coverage_pct",
    "avg_stock_qty", "source"] 

API_SELLER_FEATURE = [
    "seller_name", "product_count", "observation_count", 
    "brand_count", "top_brand",
    "avg_price", "median_price", "price_tier",
    "avg_rating", "total_rating_count", "rating_coverage_pct",
    "avg_stock_qty", "source"]    


# BELOW CONSIST OF THE COLUMNS REQUIRED TO FORM THE FORECASTING FEATURES
 
PRODUCT_TIMESERIES_COLUMNS = [ 
    "day", "product_id", "product_name", "brand", "seller_name",  
    "observation_count","avg_price","median_price","current_price",
    "avg_rating","current_rating","total_rating_count","rating_coverage_pct", 
    "stock_qty", "source"] 


BRAND_TIMESERIES_COLUMNS = [
    "day", "brand", "product_count", "observation_count", "seller_count", "top_seller",
    "avg_price", "median_price", "avg_rating", "total_rating_count", 
    "rating_coverage_pct", "avg_stock_qty", "source"]


SELLERS_TIMESERIES_COLUMNS = [
    "day", "seller_name", "product_count","brand_count","observation_count" ,"top_brand",
    "avg_price", "median_price", "avg_rating", "total_rating_count", 
    "rating_coverage_pct", "avg_stock_qty", "source"]


 
# --------------------------------------------
    # SCRAPER FEATURE ENGINEERING SCHEMA 
# --------------------------------------------

SCRAPER_CATEGORY_FEATURES = [
    "category", 

    "listing_volume",
    "product_variety_count",
    "brand_count",
    "top_brand",

    "median_price", "price_tier", 
    "avg_rating", "rating_coverage_pct", "source"
]

SCRAPER_BRAND_FEATURES = [
    "brand", 

    "listing_volume",
    "product_variety_count",
    "category_count",
    "top_category",

    "median_price", "price_tier",

    "avg_rating","rating_coverage_pct","source"
]
 

# SCRAPER FORECAST READINESS CANONICAL SCHEMA

SCRAPER_CATEGORY_TIMESERIES = [
    "day", "category", "listing_volume",
    "product_variety_count", "brand_count",
    "median_price", "avg_rating", "rating_coverage_pct", 
    "source"
]

SCRAPER_BRAND_TIMESERIES = [
    "day", "brand", "listing_volume",
    "product_variety_count", "category_count", 
    "median_price", "avg_rating", "rating_coverage_pct",
    "source"
]

 
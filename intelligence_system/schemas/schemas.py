# This module defines the expected schema for the Ingested data.
EXPECTED_COLUMNS = [
    "product_id",
    "product_name",
    "brand",
    "price",
    "product_rating",
    "rating_count",
    "seller_name",
    "stock_qty",
    "category",
    "fetched_time",
    "source"
]


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
    "latest_price",
    "price_tier",

    "avg_rating",
    "latest_rating",
    "total_rating_count",
    "latest_stock_qty",
]

API_BRAND_FEATURE = [
    "brand", "product_count", "observation_count", 
    "seller_count", "top_seller",
    "avg_price", "median_price", "brand_price_tier",
    "avg_rating", "total_rating_count", "rating_coverage_pct",
    "avg_stock_qty", "source"] 

API_SELLER_FEATURE = [
    "seller_name", "product_count", "observation_count", 
    "brand_count", "top_brand",
    "avg_price", "median_price", "seller_price_tier",
    "avg_rating", "total_rating_count", "rating_coverage_pct",
    "avg_stock_qty", "source"]    
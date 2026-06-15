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


READINESS_COLUMNS = [
    "entity_type", "entity_id", "entity_name",
    "start_day", "end_day", "period_count",
    "total_observations", "avg_observations_per_period",
    "eligible_for_forecasting"]
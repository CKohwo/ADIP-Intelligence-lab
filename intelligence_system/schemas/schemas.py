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


FEATURE_COLUMNS = [
    "product_id",
    "product_name",
    "brand",
    "seller_name",
    "category",
    "first_seen",
    "last_seen",
    "observation_count",
    "min_price",
    "max_price",
    "avg_price",
    "latest_price",
    "price_change_abs",
    "price_change_pct",
    "avg_rating",
    "latest_rating",
    "latest_stock_qty",
    "source",
]
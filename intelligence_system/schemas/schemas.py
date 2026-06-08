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
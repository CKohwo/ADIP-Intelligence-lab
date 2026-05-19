import pandas as pd
from dis.tools.parser import (
    extract_stock_quantity,
    extract_rating,
    extract_seller_name,
    validate_columns
)
from dis.tools.identifier import generate_product_id
from dis.schemas.schemas import EXPECTED_COLUMNS


def transform(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # --- Rename columns---
    df.rename(columns={
        "name": "product_name",
        "fetched_at": "fetched_time"
    }, inplace=True)

    # --- Parse datetime ---
    df["event_time"] = pd.to_datetime(df["fetched_time"], utc=True)

    # --- Extract nested ---
    df["stock_qty"] = df["stock"].apply(extract_stock_quantity)

    ratings = df["product_rating"].apply(extract_rating)
    df["product_rating"] = [r[0] for r in ratings]
    df["rating_count"] = [r[1] for r in ratings]

    df["seller_name"] = df["seller"].apply(extract_seller_name)

    # --- Brand fallback, assuming the first word of the product name is the brand if brand is missing ---
    df["brand"] = df["brand"].fillna(df["product_name"].str.split().str[0])

    # --- Identity,  prefer sku if available, otherwise generate from name, price, and timestamp ---
    if "sku" in df.columns:
        df["product_id"] = df["sku"]
    else:
        df["product_id"] = df.apply(
            lambda x: generate_product_id(
                x["product_name"], x["price"], x["fetched_time"]
            ),
            axis=1
        )

    # --- Add source ---
    df["source"] = "api_ingest"

    # --- Category fallback, set to None if missing --- 
    if "category" not in df.columns:
        df["category"] = None

    # --- Select the expected columns in the defined order ---    
    df = df[EXPECTED_COLUMNS]

    # --- Validation, ensure no critical fields are missing --- 
    df = validate_columns(df)

    return df
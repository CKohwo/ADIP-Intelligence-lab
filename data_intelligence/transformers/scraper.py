import pandas as pd
from data_analysis.tools.parser import clean_price
from data_analysis.tools.parser import validate_columns
from data_analysis.tools.identifier import generate_product_id
from data_analysis.schemas.schemas import EXPECTED_COLUMNS


def transform(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df.rename(columns={
        "Name": "product_name",
        "Price": "price",
        "Timestamp": "fetched_time",
        "Category": "category"
    }, inplace=True)

    df["price"] = df["price"].apply(clean_price)
    
    df["fetched_time"] = pd.to_datetime(df["fetched_time"], utc=True)

    df["product_id"] = df.apply(
        lambda x: generate_product_id(
            x["product_name"], x["price"], x["fetched_time"]
        ),
        axis=1
    )

    # Derive brand from product name, assume the first word is the brand & extract rating from the "Ratings" column 
    df["brand"] = df["product_name"].str.split().str[0]
    df["product_rating"] = df["Ratings"].str.split().str[0]
    df["product_rating"] = pd.to_numeric(df["product_rating"], errors="coerce")

    # Since the scraper doesn't provide these fields, we set them to None or a default value
    df["rating_count"] = None
    df["stock_qty"] = None 

    #Products were scrapped from JUMIA E-commerce website, Set the seller -- "JUMIA" 
    df["seller_name"] = "JUMIA"
    df["source"] = "Web_scraper"

     # --- Category fallback, set to None if missing --- 
    if "category" not in df.columns:
        df["category"] = None

    # --- Return Expected columns in the defined order ---
    df = df[EXPECTED_COLUMNS]

    # --- Validation, ensure no critical fields are missing ---
    df = validate_columns(df)

    return df
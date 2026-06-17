import pandas as pd
from intelligence_system.tools.parser import clean_price
from intelligence_system.tools.parser import validate_columns
from intelligence_system.tools.identifier import generate_product_key
from intelligence_system.schemas.schemas import SCRAPER_COLUMNS


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
    
    # Derive brand from product name, assume the first word is the brand & extract rating from the "Ratings" column 
    df["brand"] = df["product_name"].str.split().str[0]

    df["product_rating"] = df["Ratings"].str.split().str[0]
    df["product_rating"] = pd.to_numeric(df["product_rating"], errors="coerce")
    df["category"] = df["category"].fillna("unknown").astype(str).str.strip().str.lower() 

    df["product_id"] =df.apply(generate_product_key,axis=1)
     
    df["source"] = "Web_scraper"

    # --- Return Expected columns in the defined order ---
    df = df[SCRAPER_COLUMNS]

    # --- Validation, ensure no critical fields are missing ---
    df = validate_columns(df)

    return df
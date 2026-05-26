import re
import ast
import pandas as pd

# This module provides utility functions for parsing and cleaning data related to products, 
# such as prices, stock quantities, ratings, and seller information. 
# The functions are designed to handle various formats and potential inconsistencies in the input data.

def clean_price(price):
    if price is None:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(price)))
    except:
        return None


def safe_literal_eval(value):
    try:
        return ast.literal_eval(value) if isinstance(value, str) else value
    except:
        return {}


def extract_stock_quantity(stock):
    stock_dict = safe_literal_eval(stock)
    return stock_dict.get("quantity")


def extract_rating(rating):
    rating_dict = safe_literal_eval(rating)
    quality = rating_dict.get("quality", {})
    return quality.get("average"), quality.get("number_of_ratings")


def extract_seller_name(seller):
    seller_dict = safe_literal_eval(seller)
    return seller_dict.get("name")


def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
    critical_columns = ["price", "fetched_time", "product_id"] 
    # Define critical columns that must not be missing

    missing_columns = [col for col in critical_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing critical columns: {', '.join(missing_columns)}")
    row_before = len(df)

    for col in critical_columns:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            print(f"Column '{col}' has {missing_count} missing values.")

    # Drop rows with missing critical values and create a copy to avoid SettingWithCopyWarning
    df = df.dropna(subset=critical_columns).copy() 
    row_after = len(df)
    dropped_row = row_before - row_after
    if dropped_row > 0:
        print(f"Dropped {dropped_row} rows due to missing critical values.")

    return df    
 